# arrow-nv

Apache Arrow is a columnar memory format: a table is stored one column
at a time, so a program reads the columns it wants and nothing else, and
two programs can share a table without converting it. This package
implements the
[columnar format](https://arrow.apache.org/docs/format/Columnar.html)
and the
[IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
for novo-lang, over bytes the caller already holds.

## What the format is

A **schema** is a list of **fields**, and a field is a name, a logical
type and whether it may be null. A **record batch** is a schema and one
**array** per field, all of the same length.

An array is not a list of values. It is a **length**, an **offset**, and
a small number of **buffers**, which are runs of bytes. How many buffers
and what each one means is decided by the logical type. A **validity
bitmap** is one bit per element saying whether that element is present.
A **values buffer** holds fixed-width values end to end. An **offsets
buffer** holds, for a variable-length type, where each element starts in
the values buffer.

A **dictionary-encoded** field stores small integers in the batch and
the actual values once, in a separate message. The field's own type is
the type of the values, and the indices have a type of their own.

The **IPC format** writes a schema and a sequence of batches as
**messages**. Each message is a continuation marker, a metadata length,
a metadata block in the FlatBuffers encoding, and a body holding the
buffers. The **stream** form is a bare sequence ending with a
zero-length message. The **file** form is the same sequence after
`ARROW1\0\0`, followed by a footer indexing every message by byte
offset, the footer's length, and `ARROW1`.

| Quantity | Value |
| --- | --- |
| Continuation marker | `0xFFFFFFFF`, little-endian |
| File magic at the start | `ARROW1\0\0`, eight bytes |
| File magic at the end | `ARROW1`, six bytes |
| Required buffer alignment | 8 bytes |
| Recommended buffer alignment | 64 bytes |
| Null count of a slice | -1, meaning not known |
| Buffers of a `Null` array | 0 |
| Buffers of a `Struct` array | 1, the validity bitmap |
| Buffers of a `Bool` array | 2, and the values are bits |
| Buffers of a sparse union | 1, the type ids |
| Buffers of a dense union | 2, and neither is a validity bitmap |
| `Date64` values must be a multiple of | 86400000 milliseconds |
| Arrow version that added the continuation marker | 0.15 |

## Install

```
novo pkg add arrow-nv
```

## Example

```novo
use std.bytes
use std.fs
use arrowipc

fn main() [io, fs]
    match fs.read_bytes("data.arrows")
        None      => println("no such file")
        Some(src) =>
            // A reader for the stream form. The file form is the same
            // call with the other mode.
            let r = arrowipc.reader(ArrowStreamFormat)

            // Feed the bytes; take whatever messages finished. Nothing
            // is copied: every buffer is a span into `src`.
            match arrowipc.feed(r, src, 0)
                Err(f)   => println(f.message())
                Ok(step) =>
                    for e in step.events
                        match e
                            ArrowSchemaSeen(s)      => println("${s.fields.len()} columns")
                            ArrowBatchSeen(b, _, _) => println("${b.rows} rows")
                            _                       => println("other message")
```

## What the package contains

| Module | Contents |
| --- | --- |
| `arrowtype` | The logical types, fields and schemas, and the two tables saying how many buffers and how many children each type has. |
| `arrowbuf` | A buffer as a start and a length, with the bit, offset and alignment arithmetic that reads one. |
| `arrowcol` | An array over those spans, a record batch as a tree of them, slicing, the element accessors, and the flat walks the IPC metadata needs. |
| `arrowtime` | The temporal types: the unit ratios, the conversions to civil dates and times, and the questions to ask of a timestamp's zone. |
| `arrowfb` | The part of the FlatBuffers encoding Arrow's metadata uses: tables, vectors, strings and inline structs, read and written. |
| `arrowipc` | The framing and the message envelope, a feed-and-drain reader for both forms, and the file footer. |
| `arrowwrite` | The same two forms written into a `Cursor` the caller sized. |
| `arrowdf` | Two traits, so a data frame or a result set can be filled from a batch or turned into one. |
| `arrowfault` | Why a read was refused and where: a message, a field path, a buffer index and a byte offset. |

## How to choose an entry point

**`arrowipc.reader` and `feed` read a stream or a file.** Feed chunks,
take events. The reader decides nothing: it does not open the next file
and does not choose whether a batch is worth materialising.

**`arrowcol` reads a batch you already have.** The element accessors
take the body and the array, and answer one value.

**`arrowwrite` produces a stream or a file** into a cursor you sized.

**`arrowdf` is for filling another library's columns.** Implement
`ArrowColumnSink` to receive columns, `ArrowColumnSource` to produce
them.

**`arrowtype` and `arrowbuf` are for a codec author** comparing an
implementation against the layout tables.

## The rules a user needs

1. **A buffer is a span into a body the caller owns.** An `ArrowBuffer`
   is two integers. Nothing here copies a column or allocates one, so a
   million-row batch reaches a caller as the `Bytes` it already had plus
   a tree of small structs.
2. **A slice shares its parent's buffers.** `arrowcol.slice` changes
   only the offset, the length and the null count, so it costs the same
   whatever the array's size.
3. **A validity bit is at `offset + i`, not at `i`.** A sliced array
   shares its parent's bitmap, so element 0 of an array with offset 1000
   is bit 1000. `arrowcol.is_valid` takes the array and applies the
   offset. `arrowbuf.bit_at` is the raw spelling, and reading a bitmap
   with it gives plausible values and wrong answers.
4. **A slice's null count is -1, meaning not known.** Recomputing it is
   a scan of the bitmap, which would turn a constant-time operation into
   a linear one. `arrowcol.recount_nulls` is that scan, and it is the
   caller's call.
5. **Re-associating a batch's flat buffer list with its schema is a
   pre-order walk.** A field, then its children left to right, depth
   first, each node contributing its buffers in `arrowbuf.layout_of`
   order. The format does not write that down, so
   `arrowcol.flatten_nodes`, `flatten_buffers` and `node_paths` are
   published: a caller comparing two producers needs the same walk this
   package uses. `ArrowBufferCountWrong` carries how many buffers the
   schema wanted and how many arrived.
6. **A boolean column's values are bits.** Reading them as bytes gives
   garbage eight elements at a time.
7. **A struct array has one buffer and a union has no validity
   bitmap.** A null in a union is a null in the child its type id
   selects. A reader that allocates a bitmap for one is off by a buffer
   for the rest of the batch.
8. **A dictionary is a property of the field, not of the type.** The
   field's `data_type` is the type of the values; the array in the batch
   holds indices, whose type is in `ArrowDict.index_type`; the values
   arrive in a separate message keyed by `ArrowDict.id`. That is why
   `arrowcol.dict_span_at` takes two bodies.
9. **A delta dictionary appends.** Indices in later batches count past
   the end of the first dictionary. `arrowcol.install_dictionary` takes
   `is_delta`, and a reader that treated a delta as a replacement
   resolves every later index to nothing.
10. **An empty timestamp zone is not UTC.** It means the values have no
    zone at all: a wall clock rather than an instant. Two rows recorded
    in Oslo and Tokyo at the same moment carry different numbers on
    purpose. `arrowtime.is_wall_clock` is the question, and
    `arrowtime.zone_of` hands the name back unread, because resolving a
    zone needs a timezone database.
11. **`Date64` is milliseconds and must be an exact multiple of a
    day.** `arrowtime.check_date64` is the producer's check.
    `arrowtime.date64_to_civil` truncates, because a reader that refused
    would refuse files that exist.
12. **The continuation marker is checked, and the older framing is off
    by default.** Writers before Arrow 0.15 omit the marker, so a reader
    that assumed it reads the metadata length as the marker.
    `arrowipc.with_legacy` accepts the older form for a caller reading
    an archive. Accepting both silently would read a corrupt stream as a
    legacy one.
13. **The limits cross the interface before anything is allocated.** A
    header can claim a two-gigabyte metadata block.
    `arrowipc.with_metadata_limit` and `with_body_limit` set the bounds,
    and a message past one is refused carrying both numbers.
14. **A stream reader reads a file's middle.** The two forms are one
    reader and a mode argument.
15. **The file footer indexes messages, not columns.**
    `arrowipc.blocks_for` therefore answers every block for any
    projection. `arrowipc.ranges_for` answers the byte ranges within one
    message's body that a projection needs, with ranges less than 4096
    bytes apart joined, which is what a host reading over a network
    issues. A caller that wants
    column-level seeking wants
    [parquet-nv](https://novo-lang.org/packages/parquet-nv).
16. **A `Map` is a list of key-value structs, and `keys_sorted` is a
    producer's claim no reader verifies.** A consumer that binary
    searches on the strength of it reads wrong answers from a producer
    that lied.
17. **The narrowing to a data frame is answered up front.** Arrow has
    thirty-odd logical types and a data frame has four.
    `arrowdf.cell_kind_of` is a function of the type alone, so a caller
    checks its schema once. `arrowdf.narrowing_of` and
    `arrowdf.lossy_columns` name the three conversions that lose
    something: an unsigned 64-bit value past 2^63 keeps its bits and
    loses its sign, a decimal scaled into a float loses digits, and a
    dictionary column is expanded to its values.
18. **A dictionary-encoded field's children are not in the batch.**
    Its array holds indices, and its values, children included, arrive
    in a dictionary batch. The pre-order walk, `arrowtype.field_paths`
    and `node_count_of` leave them out.

## What is not included

- **Memory mapping.** It belongs to whichever package opens the file. A
  mapped file and a read file are the same `Bytes` to every signature
  here.
- **The C data interface.** Passing `ArrowArray` and `ArrowSchema`
  structs across a foreign function boundary needs foreign-function
  effects, which this package does not have. It is a separate package on
  the bindings shelf.
- **Body compression.** Arrow's IPC compression is per-buffer LZ4 or
  ZSTD, and taking both codecs here would put them in every consumer's
  dependency closure for the minority of streams that use them.
  `arrowipc.compression_of` reads the field and refuses, naming the
  package that would close it.
- **The view layouts, run-end encoding and tensors.** Valid Arrow this
  package does not read. A tensor message is skipped rather than
  refused, because its metadata gives the body length.
- **A general FlatBuffers implementation.** `arrowfb` is the corner
  Arrow's metadata uses: tables with scalar and offset fields, vectors,
  strings and inline structs. A table field whose vtable entry is zero
  is absent, which is normal and is why a file written by a newer Arrow
  still reads here.
- **A build for a microcontroller.** A schema is a tree of boxed structs
  with strings in it and a batch is a growable list of arrays, so this
  package does not build for a microcontroller with no heap allocator.
- **Any input or output.** No file is opened, no memory is mapped and no
  clock is read.

## Related packages

- [calendar-nv](https://novo-lang.org/packages/calendar-nv) is the only
  dependency. Arrow stores a date as a day count and a timestamp as a
  count since the epoch, and the civil date and datetime types are what
  a reader wants them as.
- [parquet-nv](https://novo-lang.org/packages/parquet-nv) is the
  on-disk columnar format, with per-column indexes and compression built
  in. Arrow is the in-memory one.
- [dataframe-nv](https://novo-lang.org/packages/dataframe-nv) is the
  data frame this package's two traits are shaped for. The dependency
  runs that way round, so that the format does not depend on one of its
  consumers.
- [csv-nv](https://novo-lang.org/packages/csv-nv) and the database
  packages produce typed records, which are the same shape
  `ArrowColumnSource` describes.
- [lz4-nv](https://novo-lang.org/packages/lz4-nv) and
  [zstd-nv](https://novo-lang.org/packages/zstd-nv) are the two codecs
  Arrow's body compression uses.

## Tests

```bash
novo test tests/arrowtype_tests.nv     # 11 tests: the types and the layout tables
novo test tests/arrowcol_tests.nv      #  8 tests: arrays, slices and batches
novo test tests/arrowipc_tests.nv      # 10 tests: the framing and the reader
novo test tests/arrowwrite_tests.nv    #  7 tests: the writer
novo test tests/golden_tests.nv        # 39 tests: Apache Arrow's own integration files
novo test tests/edges_tests.nv         # 12 tests: the tables over every type, the faults, time
novo test tests/ipc_edges_tests.nv     # 17 tests: hand-made messages, the writer, the frame traits
bash tests/coverage.sh                 # line coverage over src/, merged across the suites
```

The oracle is the Apache Arrow project's integration data, from the
`arrow-testing` repository. Arrow's C++ implementation wrote each IPC
stream and file there beside a JSON file that spells out the same
schema and every value. `tools/golden.py` fetches eighteen streams and
three files at a fixed commit and writes `tests/golden_tests.nv`: the
bytes, and a check made from the JSON of every column's length,
validity, values, offsets and union type ids. The suite reads each
stream, reads each file by feeding and through its footer, then writes
every stream back out with `arrowwrite`, reads it again and runs the
same checks. The files cover every primitive type, dates, times,
timestamps with and without zones, durations, intervals, lists, large
lists, fixed-size lists, structs, maps, sparse and dense unions,
dictionaries, nested dictionaries and custom metadata.

The API suites assert the layout entries a hand-written reader gets
wrong: a `Null` array's zero buffers, a boolean's bits, a struct's
validity-only buffer, a fixed-size list's missing offsets, and the two
unions' absent bitmaps. They assert that a slice shares its parent's
buffers, that a validity bit is read at the array's offset, that a
slice's null count is not known, that a missing continuation marker is
refused unless the legacy form was asked for, and that a message past a
limit is refused. The edge suites build malformed messages with
`arrowfb.encode`, each different from a valid one in the one field under
test, and reach every refusal.

## Licence

Apache-2.0. See `LICENSE`.

<!-- docs/writing-a-readme.md is the style guide for this page. -->
