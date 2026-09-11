# Changelog

All notable changes to arrow-nv are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 rule that a breaking change bumps the MINOR number.

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
