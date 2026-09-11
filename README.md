# arrow-nv

**Status: NOT IMPLEMENTED — interface only.**

Every public function below is published with its signature and its
effect row, and every body is `todo()`. Installing this package works;
calling it panics with `not implemented`.

## What this is

The Apache Arrow columnar format as values, sans-IO: the schema with
every logical type the specification has, the buffer layout each of them
implies, a record batch whose buffers are **spans into one body the
caller owns**, and the IPC stream and file formats as a feed-and-drain
reader and a writer into a cursor the caller sized.

- `arrowtype` — the logical types, fields and schemas, and the two
  tables that say how many buffers and how many children each type has;
- `arrowbuf` — a buffer as `(start, len)`, and the bit and offset
  arithmetic that reads one;
- `arrowcol` — an array over those spans, a record batch as a tree of
  them, slicing, and the element accessors;
- `arrowtime` — the temporal logical types, against calendar-nv;
- `arrowfb` — the FlatBuffers subset Arrow's metadata needs, and nothing
  else;
- `arrowipc` — the framing, the message envelope, the feed-and-drain
  reader and the file footer;
- `arrowwrite` — the same two formats, written into a `Cursor`;
- `arrowdf` — the data frame conversion, declared as two traits;
- `arrowfault` — why, and where: a message, a field path, a buffer and a
  byte.

```
novo pkg add arrow-nv
novo pkg build
novo test
```

## The one example that will work

```novo ignore
use arrowipc
use arrowcol

// Every batch in a stream, read without copying a single buffer.
fn batches(src: Bytes) -> Result<[ArrowBatch], ArrowFault>
    var out = []
    var r = arrowipc.reader(ArrowStreamFormat)
    let step = arrowipc.feed(r, src, 0)!
    for e in step.events
        match e
            ArrowBatchSeen(b, _, _) => out = list.push(out, b)
            _                       => out = out
    Ok(out)
```

## The load-bearing interface: `ArrowArray`

```novo ignore
pub struct ArrowArray
    data_type: ArrowType
    length: Int
    null_count: Int
    offset: Int
    buffers: [ArrowBuffer]
    children: [ArrowArray]
    dictionary_id: Int
```

An `ArrowBuffer` is two integers — a start and a length into **one body
the caller holds** — so this package never copies a column and never
allocates one. That is not a performance note; it is the reason Arrow
exists, and a port that materialised its columns into novo-lang lists
would have spent exactly what the format was designed to save.

Three consequences follow, and the fourth field is where they live:

| | |
| --- | --- |
| a read allocates nothing | a million-row batch reaches a caller as one `Bytes` it already had, plus a tree of small structs |
| `slice` is free | a slice shares its parent's buffers and changes only `offset`, `length` and `null_count` |
| every validity bit is `offset + i` | and `is_valid` takes the **array**, so there is no spelling in which a caller forgets |

That last row is the one to read twice. A validity bitmap is
bit-addressed from the start of the *array*, and a sliced array shares
its parent's bitmap. Element 0 of an array with `offset = 1000` is bit
1000. A reader that indexes the bitmap at `i` reads its parent's nulls —
plausible values, no error, wrong answers, three layers from the line
that caused it. `arrowbuf.bit_at` is the raw spelling and its own
comment says not to use it.

And `null_count` is `-1` on a slice, meaning **not known**. Recomputing
it is a scan over the bitmap, which would turn an O(1) operation into an
O(n) one; `recount_nulls` is the call that costs what it costs, and it
is the caller's. An implementation that carried its parent's count
through a slice would hand a caller a number larger than the array's own
length.

## A batch is a tree, its buffers are a flat list, and the order between them is a convention

The IPC metadata carries `nodes` and `buffers` as two flat lists.
Re-associating them with the schema is a **pre-order walk** — a field,
then its children left to right, depth first — and each node contributes
its buffers in `arrowbuf.layout_of` order. The format never writes that
down anywhere.

So `flatten_nodes`, `flatten_buffers` and `node_paths` are **published**
rather than internal: a caller comparing its own producer against
somebody else's needs the same walk this package uses, not one it wrote
from the same prose. Every implementation that walked breadth-first, or
that emitted a struct's own (non-existent) values buffer, produces a
file that reads as garbage from the first nested column onward — and
`ArrowBufferCountWrong` is the fault that catches it, carrying how many
buffers the schema wanted and how many arrived.

## Six layout entries a hand-written reader gets wrong

`arrowtype.buffer_count_of` and `arrowbuf.layout_of` are the columnar
specification's two tables, and the test suite is these six rather than
the twenty that everybody gets right:

| type | buffers | the trap |
| --- | --- | --- |
| `Null` | 0 | a length and nothing else |
| `Bool` | 2 | the values are **bits**, not bytes |
| `Struct` | 1 | validity only — no values buffer at all |
| `FixedSizeList` | 1 | no offsets: the width is in the type |
| sparse union | 1 | type ids, and **no validity** |
| dense union | 2 | type ids and offsets, still no validity |

A union has no validity bitmap because a null in a union is a null in
the child its type id selects. A reader that allocates one is off by a
buffer for the whole rest of the batch.

## A dictionary is a property of the field, not of the type

This is the part most ports have backwards. A dictionary-encoded field's
`data_type` is the type of the **values**; the array in the batch holds
**indices**, whose type is in `ArrowDict.index_type`; and the values
arrive in a separate dictionary-batch message keyed by `ArrowDict.id`.

So `arrowcol.dict_span_at` takes **two bodies** — the batch's, where the
indices are, and the dictionary message's, where the values are. An
implementation that assumed one body reads a dictionary column as
whatever happened to be at that offset in the batch.

`install_dictionary` takes `is_delta` because a delta **appends**: the
indices in later batches count past the end of the first dictionary, and
a reader that took a delta as a replacement resolves every index past it
to nothing.

## An empty timestamp zone is not UTC

```novo ignore
ArrowTimestamp(ArrowMicro, "")             // a WALL CLOCK
ArrowTimestamp(ArrowMicro, "UTC")          // an INSTANT
ArrowTimestamp(ArrowMicro, "Europe/Oslo")  // an INSTANT, displayed there
```

An empty zone means the values have no zone at all — they are what a
clock on a wall said, and two rows recorded in Oslo and Tokyo at the
same moment carry different numbers on purpose. Every implementation
that defaults an empty zone to UTC shifts somebody's data by their
offset, with no error anywhere.

This package refuses to guess. `is_wall_clock` is the question,
`zone_of` hands the name back unread, and resolving a zone needs a
timezone database, which is a **host's** and not a `core` package's.

`Date64` has a companion rule most producers break: it is milliseconds
since the epoch **and** the specification requires an exact multiple of
86 400 000. `check_date64` is the producer-side check;
`date64_to_civil` truncates, because a reader that refused would refuse
files that exist.

## The continuation marker, and why `with_legacy` is off

An encapsulated message is four parts:

```
0xFFFFFFFF      the continuation marker, little-endian
int32           the metadata length, padded to 8
<metadata>      a FlatBuffers Message
<body>          the buffers, at the offsets the metadata gives
```

The marker was added in Arrow 0.15 and older writers omit it — so a
reader that assumed it reads the metadata length as the marker and the
metadata as the length. `with_legacy` accepts the older framing for a
caller reading an archive, and it is **off by default**: a reader that
silently accepted both would read a corrupt stream as a legacy one, and
the corruption would surface as a schema with forty thousand fields.

The limits are the same argument. A header can claim a two-gigabyte
metadata block, so `with_metadata_limit` and `with_body_limit` cross the
interface **before** anything is allocated, and a message past one is
refused carrying both numbers.

## Two formats, one reader

The **stream** format is a sequence of messages ending with a
zero-length one. The **file** format is the same sequence with
`ARROW1\0\0` at both ends and a footer that indexes every message by
byte offset. So a stream reader reads a file's middle, and
`ArrowIpcMode` is a constructor argument rather than two types.

`arrowipc.blocks_for` answers **every** block for any projection, and
that is the honest answer rather than a missing feature: Arrow's file
footer indexes *messages*, not columns. What it *can* do is
`ranges_for`, which answers the byte ranges within one message's body
that a projection actually needs, coalesced — a host reading over a
network issues those. A caller that expected column-level seeking wants
parquet-nv, and the difference between the two formats is exactly this.

## The data frame conversion is two traits, not a dependency

The plan's note for this package is *"the interchange format behind
dataframe-nv"*. A format that is **behind** a consumer cannot depend on
it: the edge dataframe-nv eventually wants — from its columns to these
buffers — would close a cycle, and the two could never both be
published.

So `arrowdf` declares `ArrowColumnSink` and `ArrowColumnSource`, the
other side implements them, and this package's dependency list stays at
one entry. The same pair serves a database's result set and a CSV
reader's typed records, which are the same shape, and naming the
interface rather than the package is what makes that true.

Each `put_*` takes values and a presence mask as **two lists** rather
than a list of optionals: that is what an Arrow validity bitmap decodes
to, it is the shape dataframe-nv's `with_nulls` already takes, and a
`[?Float]` would allocate per element on a column where almost nothing
is null.

And the narrowing is answered up front. Arrow has thirty-odd logical
types; a data frame has four. `cell_kind_of` is a pure function of the
**type**, so a caller checks its schema once and learns which columns it
can take — rather than failing on the eleventh column of somebody's
file. `ArrowCellUnsupported` is a value, not an error, and
`narrowing_of` names the four conversions that lose something: an
unsigned 64-bit value past 2⁶³, a decimal scaled into a float,
nanoseconds past 2262, and a dictionary column expanded to its values.

## The layer, and the two things that are not here

`core` — no effects. A record batch is bytes the caller already holds
plus the arithmetic that says where each column is inside them, and that
arithmetic performs nothing: no file is opened, no memory is mapped, no
clock is read. The IPC reader is a feed-and-drain state machine and the
writer fills a `Cursor`.

**No memory mapping, and no C data interface**, and both belong
elsewhere rather than being missing:

- **Memory mapping** belongs to whatever `host` package opens the file.
  It changes nothing here — a mapped file and a read file are the same
  `Bytes` to every signature in this package — which is itself the
  argument for the span design.
- **The C data interface** (`ArrowArray` / `ArrowSchema` structs passed
  across an FFI boundary) is a `sys` package, `arrow-c-sys` on the
  bindings shelf. It needs `[ffi]`, and a `core` package cannot depend
  on a `sys` one, so it can only ever be a separate row — which is the
  layer design working rather than a gap.

**No body compression.** Arrow's IPC compression is per-buffer LZ4 frame
or ZSTD; taking those two codecs here would put them in every consumer's
dependency closure to serve the minority of streams that use them.
`arrowipc.compression_of` reads the field and refuses, and names the
`arrow-ipc-zip-nv` row that would close it.

**No `@tier(embedded)` claim, and none is intended.** A schema is a tree
of boxed structs with strings in it and a batch is a growable list of
arrays. The audit's `core-embedded` row passes as *makes no device
claim*.

**calendar-nv, for the temporal logical types and nothing else.** Arrow
stores a date as a day count and a timestamp as a count since the epoch;
the value a notebook wants is a civil date or datetime, and `CivilDate`
/ `CivilDateTime` are exactly that in a `core` package that already owns
the proleptic Gregorian arithmetic. Re-deriving days-from-civil here
would be a second implementation of the one calculation in the whole
file format that is easy to get wrong at the March boundary.

## The FlatBuffers subset is a module, not a dependency

Arrow's metadata is a FlatBuffers message, and FlatBuffers as a format
has a schema language, a code generator, a verifier and a JSON bridge.
Arrow uses a small corner: tables with scalar and offset fields, vectors
of offsets and of scalars, strings, and fixed inline structs
(`FieldNode`, `Buffer`). `arrowfb` is that corner.

A `flatbuffers-nv` is a row somebody may want; it is not this row, and
depending on a package that does not exist is not a design. What Arrow
needs is fifteen functions of read-only pointer arithmetic over bytes
the caller holds, each one line of the FlatBuffers binary
specification — which a reviewer can check against it without reading a
generated file.

The one thing worth knowing about that format: a table's first field is
a signed offset **backward** to a vtable listing each field's position,
or zero for absent. So `table_field_offset` answering zero is normal,
not a fault, and it is why an Arrow file written by a newer Arrow still
reads here.

## Where the names come from

Every public type and every enum variant is prefixed `Arrow`, because
struct and enum identity is keyed by name across a whole program and
`Schema`, `Field`, `Buffer` and `Block` are names four other packages
would want. `ArrowBuffer` is a *span*, not a byte string, and the name
is the same one the IPC metadata uses for the same two integers.

`ArrowFbPos` is a struct around one integer on purpose: a bare `Int`
would be indistinguishable from a field *value* that is itself an
integer, and the two are read differently — an offset field holds a
relative offset that must be added to its own position.

## The reference implementation

Apache Arrow — the columnar format specification, the IPC format
specification, and `arrow-rs` / `pyarrow` for the API shape. The
constants asserted in `tests/` (`ARROW1\0\0`, the `0xFFFFFFFF`
continuation marker, the 8- and 64-byte alignments, `Date64`'s
multiple-of-a-day rule, the four unit ratios) are the specification's
own, so a port that disagrees with one disagrees with every Arrow
implementation there is.

The API subset is the one a notebook and a Parquet reader use: the
schema whole, the layout tables whole, record batches, dictionaries, the
two IPC formats. Left out and named above: the view layouts, run-end
encoding, tensors, body compression, the C data interface, memory
mapping.

## Status

Interface only. `novo pkg build` is clean, `novo doc` renders, the API
tests under `tests/` are red against `todo()` bodies, and the three
shard rows — `effect-budget`, `dep-layer`, `no-discharge-in-core` — are
green. The first implementation is the `0.1.0` published over this.
