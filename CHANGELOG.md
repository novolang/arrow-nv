# Changelog

All notable changes to arrow-nv are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 rule that a breaking change bumps the MINOR number.

## 0.1.0 — 2026-09-28

The first implementation of the interface published as 0.0.1: the
schema and its checks, arrays and batches over spans, the temporal
conversions, the FlatBuffers subset read and written, the IPC reader
for the stream and file forms, the writer, and the data frame traits.
It requires novo 0.14.0 and calendar-nv `^0.2.0`.

Tests cover `arrowipc.read_all` over a source that fails or ends with
`IoClosed`, and `arrowwrite.write_all` into a sink that fails or accepts
no bytes.

### Breaking changes

- `arrowipc.ranges_for` takes the message and its schema:
  `ranges_for(src, header, schema, names)`.  The interface took a footer
  and a block, which do not say where a column's buffers are; only the
  message's metadata does.  The ranges are counted from the start of the
  body, and ranges less than 4096 bytes apart are joined.
- `ArrowFault` has four more variants: `ArrowTypeInvalid`, for a type
  whose parameters the specification does not allow; `ArrowMisaligned`,
  from `arrowbuf.check_alignment`; `ArrowLengthMismatch`, for a column
  whose length is not the batch's; and `ArrowWrongType`, for an
  accessor asked for one kind of value from a column of another.  A
  `match` over `ArrowFault` needs the new arms.
- `ArrowIpcReader` carries the schema, the dictionaries and its
  position, and `ArrowWriter` has a `dictionary_blocks` list beside
  `blocks`.  Code that built either with a struct literal needs the new
  fields; `arrowipc.reader` and `arrowwrite.writer` are unchanged.
- `ArrowFbBuilder` has a `capacity` field, and `arrowfb.encode`,
  `ArrowFbObject` and `ArrowFbField` are new: the builder is what
  `encode` may lay down and reports what it did.
- `arrowtype.field_paths`, `node_count_of` and `buffer_count_of_schema`
  leave out a dictionary-encoded field's children, which are not in a
  record batch.

### Added

- `arrowfault.in_message`, which moves a fault's coordinates onto a
  stream.
- `arrowfb.encode` and the two enums it takes.

### Behaviour the interface left open

- A file ends with `ARROW1`, six bytes, after the footer's length; it
  begins with the eight bytes `file_magic` answers.  `footer_position`
  reads the last ten bytes.
- The reader keeps no bytes: a message that straddles the end of a chunk
  is left unconsumed and fed again.  `finish` answers `ArrowStreamEnded`
  for a stream that ends between messages without its end marker.
- A buffer's span in a batch the reader answers is into the chunk it
  was fed, and a dictionary entry's body is the chunk its message came
  in.  A dictionary delta is kept as a second entry under its id.
- `arrowcol.slice` leaves children as they are; `struct_field` applies a
  struct's offset to the child it answers.  `is_valid` answers `false`
  for a `Null` array and `true` for a union, whose validity is in its
  children.
- `arrowdf.cell_kind_of` answers `ArrowCellFloat` for a decimal, whose
  narrowing is `ArrowNarrowPrecisionLost`.  A nanosecond timestamp
  narrows exactly, so no type answers `ArrowNarrowRangeLost`.
- The writer writes metadata version V5, writes the end-of-stream
  marker before a file's footer, and refuses an array with a non-zero
  offset, since the metadata has no offset field.
- `arrowwrite.write_floats` rounds to nearest with ties to even at 16
  and 32 bits.

### Tests

- `tests/golden_tests.nv`, written by `tools/golden.py`, reads eighteen
  of Apache Arrow's integration streams and three files at a fixed
  commit of `arrow-testing`, checks every value against the JSON beside
  them, and writes each stream back and reads it again.
- `tests/edges_tests.nv` and `tests/ipc_edges_tests.nv` cover the tables
  over every type, every fault, the temporal conversions, and each
  refusal of the reader, the writer and the accessors.
- Three API test fixtures were wrong and are corrected: the file tail
  carried eight bytes of magic where Arrow writes six, a FlatBuffers
  buffer's root pointed at its vtable, and a 32-bit offsets buffer read
  as 64-bit was expected to start at zero.
- Line coverage over `src/` is 100%, measured by `tests/coverage.sh`.

## 0.0.2 — 2026-09-15

README rewritten to the package README style guide (docs/writing-a-readme.md); no change to the interface.

## 0.0.1 — 2026-09-11

The **interface**: every signature and every effect row, and no bodies.
`stability = "draft"`, and the release is recorded `implemented = false`.

### Added

- `arrowtype` — every logical type the columnar specification has, with
  fields and schemas, and the two tables that say how many buffers and
  how many children each type implies.
- `arrowbuf` — a buffer as a `(start, len)` span into the caller's body,
  the role of each buffer position per type, and the bit and offset
  arithmetic that reads one.
- `arrowcol` — `ArrowArray` over those spans, `ArrowBatch` as a tree of
  them, a free `slice`, the element accessors and the published
  pre-order walk.
- `arrowtime` — the temporal logical types against calendar-nv's civil
  values, with the unit ratios and the `Date64` rule.
- `arrowfb` — the FlatBuffers subset Arrow's metadata needs, read-only
  over the caller's bytes.
- `arrowipc` — the encapsulated-message framing, the event enum, the
  feed-and-drain reader, and the file footer with its range arithmetic.
- `arrowwrite` — both formats written into a `Cursor` the caller sized,
  with the sizing published above the writers.
- `arrowdf` — the data frame conversion as two traits and a narrowing
  answered before any data is read.
- `arrowfault` — every refusal, each naming a message, a field path, a
  buffer and a byte, classified incomplete / corrupt / unsupported.

### Known

- **`ArrowArray` is the load-bearing interface**, and its `offset` is
  the field the design rests on: every validity bit is `offset + i`, so
  `is_valid` takes the array and a caller cannot forget.
- **A slice is free and its null count is `-1`.** Recomputing would turn
  an O(1) operation into an O(n) one; `recount_nulls` is the caller's.
- **The pre-order walk is published**, because the format never writes
  it down and it is where producers disagree.
- **A dictionary is a property of the FIELD**, so `dict_span_at` takes
  two bodies — the batch's and the dictionary message's.
- **An empty timestamp zone is a wall clock, not UTC**, and this package
  refuses to guess.
- **The continuation marker is required by default**; `with_legacy` is a
  decision a caller makes by name.
- **The data frame conversion is two traits, not a dependency**, because
  depending on dataframe-nv would close a cycle.
- **No memory mapping, no C data interface, no body compression** — each
  named in the README with the layer or the row it belongs to.
- **No `@tier(embedded)` claim.** A schema is a tree of boxed structs.
- One dependency, calendar-nv, for the temporal logical types.
