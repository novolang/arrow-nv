#!/usr/bin/env python3
"""Write tests/golden_tests.nv from Apache Arrow's own integration files.

pyarrow is not installed on the machine this package is built on, so
the oracle is the `arrow-testing` repository's integration data: IPC
streams and files written by Arrow's C++ implementation, each beside a
JSON file that spells out the same schema and every value.  The files
are fetched at the commit named below, so the test data does not move.
A map's children are expected under their canonical names, `entries`,
`key` and `value`, which is what Arrow's writer puts in the IPC files
whatever the JSON calls them.  In `nested_dictionary` Arrow's writer
numbers the dictionaries of nested fields per field, so the ids in the
IPC bytes are not the JSON's, and that file's dictionary values are not
compared; its batches' indices are.

For every file taken, the test file holds the IPC bytes as hex and a
check written from the JSON: the field paths, each batch's row count,
and for every column and child its length, its validity bits, and its
values, offsets and union type ids.  The suite reads each stream with
`arrowipc.feed`, and each file form both by feeding and through its
footer.  It then writes every stream back out with `arrowwrite`, reads
the result, and runs the same checks on it.

Floats are compared within a relative 1e-6 for SINGLE and 1e-12 for
DOUBLE, because the JSON prints a decimal approximation.  An unsigned
64-bit value past 2^63 is expected as its two's-complement bit pattern,
which is what `arrowcol.int_at` answers.

Run from the package root:  python3 tools/golden.py
The output is passed through `novo fmt`.
"""
import gzip
import json
import subprocess
import urllib.request

COMMIT = '9ff285c88565f0f6abc855918c6a342e70e4909c'
BASE = ('https://raw.githubusercontent.com/apache/arrow-testing/%s/data/arrow-ipc-stream/'
        'integration/1.0.0-littleendian/' % COMMIT)
STREAMS = ['null_trivial', 'null', 'primitive', 'primitive_zerolength', 'primitive_large_offsets',
           'datetime', 'interval', 'nested', 'nested_large_offsets', 'recursive_nested', 'map',
           'map_non_canonical', 'union', 'dictionary', 'dictionary_unsigned', 'nested_dictionary',
           'custom_metadata', 'duplicate_fieldnames']
FILES = ['null_trivial', 'nested', 'dictionary']
# Arrow's writer numbers the dictionaries of these files' nested fields
# per field, so the ids in the IPC bytes are not the JSON's, and the
# dictionary values are not compared.  The batches' indices still are.
RENUMBERED = ['nested_dictionary']


def fetch(name):
    with urllib.request.urlopen(BASE + name, timeout=60) as r:
        return r.read()


def lit(s):
    return '"' + s.replace('\\\\', '\\\\\\\\').replace('"', '\\\\"').replace('$', '\\\\$').replace('\\n', '\\\\n') + '"'


def bools(xs):
    return '[' + ', '.join('true' if x else 'false' for x in xs) + ']'


def ints(xs):
    return '[' + ', '.join(str(x) for x in xs) + ']'


def as_int(v, t):
    v = int(v)
    if v >= 1 << 63:
        v -= 1 << 64
    return v


class Gen:
    def __init__(self):
        self.lines = []
        self.n = 0
        self.batch = 'b0'

    def var(self):
        self.n += 1
        return 'c%d' % self.n

    def emit(self, s):
        self.lines.append('    ' + s)

    def column(self, expr, field, col, dict_field=False, fexpr=None, dict_values=None):
        """Checks for one column JSON against the array `expr`."""
        v = self.var()
        self.emit('let %s = %s' % (v, expr))
        count = col['count']
        self.emit('test.assert_eq(%s.length, %d)' % (v, count))
        t = field['type']
        name = t['name']
        valid = col.get('VALIDITY')
        if name == 'null':
            self.emit('check_all_null(body, %s)' % v)
            return
        if valid is not None and name not in ('union',):
            self.emit('check_valid(body, %s, %s)' % (v, bools(valid)))
        present = valid if valid is not None else [1] * count
        data = col.get('DATA')
        if field.get('dictionary') is not None and not dict_field:
            self.emit('check_ints(body, %s, %s, %s)' % (v, ints([int(x) for x in data]), bools(present)))
            if dict_values is not None and field['type']['name'] == 'utf8':
                want = [dict_values[int(x)] if p else '' for x, p in zip(data, present)]
                self.emit('check_dict_strs(body, %s, %s, [%s], %s)'
                          % (self.batch, v, ', '.join(lit(x) for x in want), bools(present)))
            return
        if name in ('int', 'date', 'time', 'timestamp', 'duration'):
            self.emit('check_ints(body, %s, %s, %s)' % (v, ints([as_int(x, t) for x in data]), bools(present)))
        elif name == 'floatingpoint':
            tol = {'HALF': '0.001', 'SINGLE': '0.000001', 'DOUBLE': '0.000000000001'}[t['precision']]
            self.emit('check_floats(body, %s, [%s], %s, %s)'
                      % (v, ', '.join(repr(float(x)) for x in data), bools(present), tol))
        elif name == 'bool':
            self.emit('check_bools(body, %s, %s, %s)' % (v, bools(data), bools(present)))
        elif name in ('utf8', 'largeutf8'):
            self.emit('check_strs(body, %s, [%s], %s)' % (v, ', '.join(lit(x) for x in data), bools(present)))
        elif name in ('binary', 'largebinary', 'fixedsizebinary'):
            self.emit('check_hex(body, %s, [%s], %s)' % (v, ', '.join(lit(x.lower()) for x in data), bools(present)))
        elif name == 'interval':
            if t['unit'] == 'YEAR_MONTH':
                self.emit('check_words(body, %s, %s, 4, 1, %s)' % (v, ints(data), bools(present)))
            elif t['unit'] == 'DAY_TIME':
                flat = []
                for d in data:
                    flat += [d['days'], d['milliseconds']]
                self.emit('check_words(body, %s, %s, 4, 2, %s)' % (v, ints(flat), bools(present)))
            else:
                raise SystemExit('interval unit %s not generated' % t['unit'])
        elif name in ('list', 'largelist', 'map'):
            offs = [int(x) for x in col['OFFSET']]
            self.emit('check_offsets(body, %s, %s, %s)' % (v, ints(offs), 'true' if name == 'largelist' else 'false'))
            self.emit('check_ranges(body, %s, %s, %s)' % (v, ints(offs), 'true' if name == 'map' else 'false'))
        elif name == 'union':
            self.emit('check_type_ids(body, %s, %s)' % (v, ints(col['TYPE_ID'])))
            ids = t.get('typeIds') or list(range(len(field['children'])))
            children = [ids.index(x) for x in col['TYPE_ID']]
            places = [int(x) for x in col['OFFSET']] if t['mode'] == 'DENSE' else list(range(count))
            if t['mode'] == 'DENSE':
                self.emit('check_union_offsets(body, %s, %s)' % (v, ints(col['OFFSET'])))
            if fexpr is not None:
                self.emit('check_union_at(body, %s, %s, %s, %s)' % (v, fexpr, ints(children), ints(places)))
        elif name == 'struct':
            self.emit('check_struct(%s, %d)' % (v, len(field.get('children', []))))
        elif name == 'fixedsizelist':
            w = t['listSize']
            self.emit('check_ranges(body, %s, %s, false)' % (v, ints([k * w for k in range(count + 1)])))
        else:
            raise SystemExit('type %s not generated' % name)
        for k, (cf, cc) in enumerate(zip(field.get('children', []), col.get('children', []))):
            child_f = None if fexpr is None else '%s.children[%d]' % (fexpr, k)
            self.column('%s.children[%d]' % (v, k), cf, cc, False, child_f)


def paths(fields, prefix='', role=None):
    """The pre-order walk of field names.  A dictionary-encoded field's
    children are not in the walk, because its array holds indices.  A
    map's children are named `entries`, `key` and `value` in the IPC
    files whatever the JSON calls them, since the names of a map's
    children carry no meaning (the columnar specification, "Map")."""
    out = []
    for k, f in enumerate(fields):
        name = f['name']
        if role == 'map':
            name = 'entries'
        elif role == 'entries':
            name = ['key', 'value'][k]
        here = name if prefix == '' else prefix + '.' + name
        out.append(here)
        child_role = 'map' if f['type']['name'] == 'map' else ('entries' if role == 'map' else None)
        if f.get('dictionary') is None:
            out += paths(f.get('children', []), here, child_role)
    return out


def dict_fields(fields, out):
    for f in fields:
        if f.get('dictionary') is not None:
            out[f['dictionary']['id']] = f
        dict_fields(f.get('children', []), out)
    return out


head = '''// golden_tests.nv — Apache Arrow's own integration files, read and
// checked value by value, and written back.
//
// Written by tools/golden.py; do not edit by hand.  The script's
// docstring says where the files come from and what is compared.

use std.test
use std.bytes
use std.list
use std.float
use arrowipc
use arrowtype
use arrowbuf
use arrowcol
use arrowwrite

fn check_all_null(body: Bytes, a: ArrowArray)
    for k in 0..a.length
        match arrowcol.is_null(body, a, k)
            Ok(v)  => test.assert(v)
            Err(e) => test.fail(e.message())

fn check_valid(body: Bytes, a: ArrowArray, want: [Bool])
    for k in 0..list.len(want)
        match arrowcol.is_valid(body, a, k)
            Ok(v)  => test.assert_eq(v, want[k])
            Err(e) => test.fail(e.message())

fn check_ints(body: Bytes, a: ArrowArray, want: [Int], present: [Bool])
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.int_at(body, a, k)
                Ok(v)  => test.assert_eq(v, want[k])
                Err(e) => test.fail(e.message())

fn check_floats(body: Bytes, a: ArrowArray, want: [Float], present: [Bool], tol: Float)
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.float_at(body, a, k)
                Ok(v)  =>
                    let d = if v > want[k] then v - want[k] else want[k] - v
                    let m = if want[k] < 0.0 then 0.0 - want[k] else want[k]
                    test.assert(d <= tol * (if m > 1.0 then m else 1.0))
                Err(e) => test.fail(e.message())

fn check_bools(body: Bytes, a: ArrowArray, want: [Bool], present: [Bool])
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.bool_at(body, a, k)
                Ok(v)  => test.assert_eq(v, want[k])
                Err(e) => test.fail(e.message())

fn check_strs(body: Bytes, a: ArrowArray, want: [Str], present: [Bool])
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.str_at(body, a, k)
                Ok(v)  => test.assert_eq(v, want[k])
                Err(e) => test.fail(e.message())

fn check_hex(body: Bytes, a: ArrowArray, want: [Str], present: [Bool])
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.span_at(body, a, k)
                Ok(s)  => test.assert_eq(bytes.to_hex(bytes.slice(body, s.start, s.start + s.len)), want[k])
                Err(e) => test.fail(e.message())

// Fixed-width elements read as `per` words of `width` bytes each.
fn check_words(body: Bytes, a: ArrowArray, want: [Int], width: Int, per: Int, present: [Bool])
    for k in 0..list.len(present)
        if present[k]
            for w in 0..per
                match arrowbuf.int_at(body, a.buffers[1], (a.offset + k) * per + w, width)
                    Ok(v)  => test.assert_eq(v, want[k * per + w])
                    Err(e) => test.fail(e.message())

fn check_offsets(body: Bytes, a: ArrowArray, want: [Int], large: Bool)
    for k in 0..list.len(want)
        let got = if large then arrowbuf.offset64_at(body, a.buffers[1], k) else arrowbuf.offset32_at(body, a.buffers[1], k)
        match got
            Ok(v)  => test.assert_eq(v, want[k])
            Err(e) => test.fail(e.message())

// What `child_range` and `list_at` answer for each element of a list,
// a map or a fixed-size list, against its offsets.
fn check_ranges(body: Bytes, a: ArrowArray, offsets: [Int], is_map: Bool)
    for k in 0..a.length
        match arrowcol.child_range(body, a, k)
            Ok(r)  =>
                test.assert_eq(r.0, offsets[k])
                test.assert_eq(r.1, offsets[k + 1])
            Err(e) => test.fail(e.message())
        match arrowcol.list_at(body, a, k)
            Ok(l)  => test.assert_eq(l.length, offsets[k + 1] - offsets[k])
            Err(e) => test.fail(e.message())
        if is_map
            match arrowcol.map_at(body, a, k)
                Ok(kv) =>
                    let (keys, values) = kv
                    test.assert_eq(keys.length, offsets[k + 1] - offsets[k])
                    test.assert_eq(values.length, offsets[k + 1] - offsets[k])
                Err(e) => test.fail(e.message())

fn check_struct(a: ArrowArray, n: Int)
    for k in 0..n
        match arrowcol.struct_field(a, k)
            Ok(c)  => test.assert_eq(c.length, a.length)
            Err(e) => test.fail(e.message())

fn check_union_at(body: Bytes, a: ArrowArray, f: ArrowField, children: [Int], places: [Int])
    for k in 0..list.len(children)
        match arrowcol.union_at(body, a, f, k)
            Ok(p)  =>
                test.assert_eq(p.0, children[k])
                test.assert_eq(p.1, places[k])
            Err(e) => test.fail(e.message())

fn check_dict_strs(body: Bytes, b: ArrowBatch, a: ArrowArray, want: [Str], present: [Bool])
    for k in 0..list.len(want)
        if present[k]
            match arrowcol.dict_str_at(body, b, a, k)
                Ok(v)  => test.assert_eq(v, want[k])
                Err(e) => test.fail(e.message())

fn check_type_ids(body: Bytes, a: ArrowArray, want: [Int])
    for k in 0..list.len(want)
        match arrowbuf.int_at(body, a.buffers[0], k, 1)
            Ok(v)  => test.assert_eq(v, want[k])
            Err(e) => test.fail(e.message())

fn check_union_offsets(body: Bytes, a: ArrowArray, want: [Int])
    for k in 0..list.len(want)
        match arrowbuf.int_at(body, a.buffers[1], k, 4)
            Ok(v)  => test.assert_eq(v, want[k])
            Err(e) => test.fail(e.message())

// Every event of a whole stream or file, fed in one chunk.
fn events_of(src: Bytes, mode: ArrowIpcMode) -> [ArrowIpcEvent]
    match arrowipc.feed(arrowipc.reader(mode), src, 0)
        Ok(step) =>
            test.assert_eq(step.consumed, bytes.len(src))
            step.events[:]
        Err(e)   =>
            test.fail(e.message())
            []

fn schema_in(events: [ArrowIpcEvent]) -> ArrowSchema
    for e in events
        if let ArrowSchemaSeen(s) = e
            return s
    test.fail("no schema")
    arrowtype.schema([])

fn batches_in(events: [ArrowIpcEvent]) -> [ArrowBatch]
    var out: [ArrowBatch] = []
    for e in events
        if let ArrowBatchSeen(b, _, _) = e
            list.push(out, b)
    out

fn dictionaries_in(events: [ArrowIpcEvent]) -> [ArrowArray]
    var out: [ArrowArray] = []
    for e in events
        if let ArrowDictionarySeen(_, values, _, _, _) = e
            list.push(out, values)
    out

// The stream written back out: the schema, each dictionary, each batch
// with its buffers gathered, and the end.
fn rewrite(src: Bytes, mode: ArrowIpcMode) -> Bytes
    let events = events_of(src, ArrowStreamFormat)
    var w = arrowwrite.writer(mode, schema_in(events))
    var c = bytes.cursor_le(bytes.zeros(4 * bytes.len(src) + 4096))
    w = ok_writer(arrowwrite.write_prologue(w, c))
    w = ok_writer(arrowwrite.write_schema(w, c))
    for e in events
        match e
            ArrowDictionarySeen(id, values, delta, _, _) =>
                w = ok_writer(arrowwrite.write_dictionary(w, ArrowDictEntry { id: id, values: values, body: src }, delta, c))
            ArrowBatchSeen(b, _, _)                      =>
                var parts: [Bytes] = []
                for buf in arrowcol.flatten_buffers(b)
                    list.push(parts, bytes.slice(src, buf.start, buf.start + buf.len))
                w = ok_writer(arrowwrite.write_batch_parts(w, b, parts, c))
            _                                            => ()
    w = ok_writer(arrowwrite.finish(w, c))
    bytes.slice(c.finish(), 0, w.written)

fn ok_writer(r: Result<(ArrowWriter, Int), ArrowFault>) -> ArrowWriter
    match r
        Ok(pair) =>
            let (w, _) = pair
            w
        Err(e)   =>
            test.fail(e.message())
            arrowwrite.writer(ArrowStreamFormat, arrowtype.schema([]))
'''

out = [head]
for name in STREAMS:
    raw = fetch('generated_%s.stream' % name)
    j = json.loads(gzip.decompress(fetch('generated_%s.json.gz' % name)))
    fields = j['schema']['fields']
    out.append('fn %s_stream() -> Bytes\n    bytes.from_hex("%s") ?? bytes.zeros(0)\n' % (name, raw.hex()))
    if name in FILES:
        out.append('fn %s_file() -> Bytes\n    bytes.from_hex("%s") ?? bytes.zeros(0)\n'
                   % (name, fetch('generated_%s.arrow_file' % name).hex()))
    # The checks, as one function over the events of a read.
    g = Gen()
    g.emit('let s = schema_in(events)')
    g.emit('test.assert_eq(arrowtype.field_paths(s), [%s])' % ', '.join(lit(p) for p in paths(fields)))
    g.emit('let batches = batches_in(events)')
    g.emit('test.assert_eq(list.len(batches), %d)' % len(j['batches']))
    for k, batch in enumerate(j['batches']):
        g.emit('let b%d = batches[%d]' % (k, k))
        g.emit('test.assert_eq(b%d.rows, %d)' % (k, batch['count']))
        g.emit('test.assert(arrowcol.batch_ok(body, b%d))' % k)
        g.batch = 'b%d' % k
        for i, (f, col) in enumerate(zip(fields, batch['columns'])):
            values = None
            if f.get('dictionary') is not None and name not in RENUMBERED:
                for d in j.get('dictionaries', []):
                    if d['id'] == f['dictionary']['id']:
                        values = d['data']['columns'][0]['DATA']
            g.column('b%d.columns[%d]' % (k, i), f, col, False, 's.fields[%d]' % i, values)
    dicts = [] if name in RENUMBERED else j.get('dictionaries', [])
    if dicts:
        by_id = dict_fields(fields, {})
        g.emit('let dicts = dictionaries_in(events)')
        g.emit('test.assert_eq(list.len(dicts), %d)' % len(dicts))
        for k, d in enumerate(dicts):
            f = by_id[d['id']]
            values = {'name': f['name'], 'type': f['type'], 'children': f.get('children', [])}
            g.column('dicts[%d]' % k, values, d['data']['columns'][0], True)
    out.append('fn check_%s(body: Bytes, events: [ArrowIpcEvent])\n%s\n' % (name, '\n'.join(g.lines)))
    out.append('@test\nfn test_%s_stream_reads_as_its_json_says()\n'
               '    check_%s(%s_stream(), events_of(%s_stream(), ArrowStreamFormat))\n' % (name, name, name, name))
    out.append('@test\nfn test_%s_stream_written_back_reads_the_same()\n'
               '    let again = rewrite(%s_stream(), ArrowStreamFormat)\n'
               '    check_%s(again, events_of(again, ArrowStreamFormat))\n' % (name, name, name))
    if name in FILES:
        out.append('''@test
fn test_%s_file_reads_by_feeding_and_through_its_footer()
    let src = %s_file()
    check_%s(src, events_of(src, ArrowFileFormat))
    let tail = bytes.slice(src, bytes.len(src) - 10, bytes.len(src))
    match arrowipc.footer_position(tail, bytes.len(src))
        Err(e) => test.fail(e.message())
        Ok(at) =>
            match arrowipc.read_footer(src, at, bytes.len(src) - 10 - at)
                Err(e) => test.fail(e.message())
                Ok(f)  =>
                    test.assert_eq(list.len(f.batches), %d)
                    test.assert_eq(arrowtype.field_paths(f.schema), [%s])
    let again = rewrite(%s_stream(), ArrowFileFormat)
    check_%s(again, events_of(again, ArrowFileFormat))
''' % (name, name, name, len(j['batches']), ', '.join(lit(p) for p in paths(fields)), name, name))

path = 'tests/golden_tests.nv'
open(path, 'w').write('\n'.join(out))
subprocess.run(['novo', 'fmt', path], check=False)
