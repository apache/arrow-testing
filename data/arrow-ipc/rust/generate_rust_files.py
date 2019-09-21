# imports
import pyarrow as pa  # 0.14.0
import pyarrow.parquet as pq
import pandas as pd  # 0.25.1
import numpy as np  # 1.17.2
from datetime import datetime, date, time

print(pa.__version__)
print(pd.__version__)
print(np.__version__)

# create files with record batches

# all primitives that are supported by Rust
primitive_schema = pa.schema(
    [
        pa.field("bools", "bool", True),
        pa.field("int8s", "i1", True),
        pa.field("int16s", "i2", True),
        pa.field("int32s", "i4", True),
        pa.field("int64s", "i8", True),
        pa.field("uint8s", "u1", True),
        pa.field("uint16s", "u2", True),
        pa.field("uint32s", "u4", True),
        pa.field("uint64s", "u8", False),
        pa.field("float32s", "f4", True),
        pa.field("float64s", "f8", False),
        pa.field("date32s", "date32", False),
        pa.field("date64s", "date64", False),
        pa.field("time32s", "time32[s]", True),
        pa.field("time32ms", "time32[ms]", True),
        pa.field("time64us", "time64[us]", False),
        pa.field("time64ns", "time64[ns]", False),
        pa.field("timestamps", "timestamp[s]", False),
        pa.field("timestampms", "timestamp[ms]", False),
        pa.field("timestampus", "timestamp[us]", False),
        pa.field("timestampns", "timestamp[ns]", False),
    ]
)
list_schema = pa.schema(
    [
        pa.field("varbinary", pa.binary(), False),
        pa.field("fixedbinary", pa.binary(4), True),
        pa.field("emptylist", pa.list_(pa.field("int64s", "i8", True))),
        pa.field("numericlist", pa.list_(pa.field("int32s", "i4", False))),
        pa.field("varbinarylist", pa.list_(pa.field("varbinary", pa.binary(), True))),
        pa.field(
            "fixedbinarylist", pa.list_(pa.field("fixedbinary", pa.binary(5), True))
        ),
    ]
)
struct_schema = pa.schema(
    [
        pa.field(
            "structs",
            pa.struct(
                [
                    pa.field("bools", "bool", True),
                    pa.field("int8s", "i1", True),
                    pa.field("varbinary", pa.binary(), True),
                    pa.field("numericlist", pa.list_(pa.field("int32s", "i1", False))),
                    pa.field("floatlist", pa.list_(pa.field("f32s", "f4", False))),
                ]
            ),
            True,
        )
    ]
)
# bitmask
mask = np.array([0, 1, 1, 0, 0], dtype=bool)
# add data
lists_json = """
{
    "emptylist": [[], [], [], [], []], 
    "numericlist": [[1,2,3,4], null, [5,6], [7], [8,9,10]],
    "floatlist": [[1.1,2.2,3.3,4.4], null, [5.5,6.6], [7.7], [8.8,9.9,10.0]],
    "varbinary": [["a", "bb"], null, ["ccc"], ["dddd"], ["eé", "ffff", "gggg"]],
    "fixedbinary": [["aaaaa", "bbbbb"], null, ["ccccc"], ["ddddd"], ["eéé", "fffff", "ggggg"]]
}"""
lists_df = pd.read_json(lists_json)
lists_pa = pa.Table.from_pandas(lists_df)
empty_list = lists_pa.column("emptylist")
numeric_list = lists_pa.column("numericlist")
float_list = lists_pa.column("floatlist")
varbin_list = lists_pa.column("varbinary")
fixbin_list = lists_pa.column("fixedbinary")

# struct array
struct_list = pa.StructArray.from_arrays(
    [
        pa.array([True, False, True, False, True], type="bool", mask=mask),
        pa.array([-1, -2, -3, -4, -5], type="i1", mask=mask),
        pa.array(["foo", "bar", "baz", "qux", "quux"], type=pa.binary()),
        numeric_list.data.chunk(0),
        float_list.data.chunk(0),
    ],
    ["bools", "int8s", "varbinary", "numericlist", "floatlist"],
)  # 'bools', 'int8s', 'varbinary', 'numericlist'

primitive_data = [
    [
        pa.array([True, False, True, False, True], type="bool", mask=mask),
        pa.array([-1, -2, -3, -4, -5], type="i1", mask=mask),
        pa.array([-1, -2, -3, -4, -5], type="i2", mask=mask),
        pa.array([-1, -2, -3, -4, -5], type="i4", mask=mask),
        pa.array([-1, -2, -3, -4, -5], type="i8", mask=mask),
        pa.array([+1, +2, +3, +4, +5], type="u1", mask=mask),
        pa.array([+1, +2, +3, +4, +5], type="u2", mask=mask),
        pa.array([+1, +2, +3, +4, +5], type="u4", mask=mask),
        pa.array([+1, +2, +3, +4, +5], type="u8"),
        pa.array([+1, +2, +3, +4, +5], type="f4", mask=mask),
        pa.array([+1, +2, +3, +4, +5], type="f8"),
        pa.array(
            [
                date(2018, 2, 26),
                date(1970, 1, 1),
                date(2010, 12, 31),
                date(2050, 11, 15),
                date(2050, 11, 15),
            ],
            type="date32",
            mask=mask,
        ),
        pa.array(
            [
                date(2018, 2, 26),
                date(1970, 1, 1),
                date(2010, 12, 31),
                date(2050, 11, 15),
                date(2050, 11, 15),
            ],
            type="date64",
            mask=mask,
        ),
        pa.array([10e3, 20e3, 30e3, 40e3, 50e3], type="i4"),
        pa.array([10e6, 20e6, 30e6, 40e6, 50e6], type="i4"),
        pa.array([10e9, 20e9, 30e9, 40e9, 50e9], type="i8"),
        pa.array([10e12, 20e12, 30e12, 40e12, 50e12], type="i8"),
        pa.array(
            [
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
            ],
            type="timestamp[s]",
        ),
        pa.array(
            [
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
            ],
            type="timestamp[ms]",
        ),
        pa.array(
            [
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
            ],
            type="timestamp[us]",
        ),
        pa.array(
            [
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
                datetime(2018, 2, 26, 13, 45, 56, 123123),
            ],
            type="timestamp[ns]",
        ),
    ]
]

list_data = [
    [
        pa.array(["foo", "bar", "baz", "qux", "quux"], type=pa.binary()),
        pa.array(["aaaa", "bbbb", "cccc", u"eeé", u"éé"], type=pa.binary(4)),
        empty_list.data.chunk(0),
        numeric_list.data.chunk(0),
        varbin_list.data.chunk(0),
        fixbin_list.data.chunk(0),
    ]
]

struct_data = [[struct_list]]

# write primitive files
sink = pa.BufferOutputStream()
primitive_writer = pa.RecordBatchFileWriter(sink, primitive_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(primitive_data[0], primitive_schema)
    primitive_writer.write_batch(batch)
    pass
primitive_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full file format
f = open("primitive_types.file.arrow", "wb")
f.write(b)
f.close()

sink = pa.BufferOutputStream()
primitive_writer = pa.RecordBatchStreamWriter(sink, primitive_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(primitive_data[0], primitive_schema)
    primitive_writer.write_batch(batch)
    pass
primitive_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format
f = open("primitive_types.stream.arrow", "wb")
f.write(b)
f.close()

# write list files
sink = pa.BufferOutputStream()
list_writer = pa.RecordBatchFileWriter(sink, list_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(list_data[0], list_schema)
    list_writer.write_batch(batch)
    pass
list_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full file format
f = open("list_types.file.arrow", "wb")
f.write(b)
f.close()

sink = pa.BufferOutputStream()
list_writer = pa.RecordBatchStreamWriter(sink, list_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(list_data[0], list_schema)
    list_writer.write_batch(batch)
    pass
list_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format
f = open("list_types.stream.arrow", "wb")
f.write(b)
f.close()

# write struct files
sink = pa.BufferOutputStream()
struct_writer = pa.RecordBatchFileWriter(sink, struct_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(struct_data[0], struct_schema)
    struct_writer.write_batch(batch)
    pass
struct_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full file format
f = open("struct_types.file.arrow", "wb")
f.write(b)
f.close()

sink = pa.BufferOutputStream()
struct_writer = pa.RecordBatchStreamWriter(sink, struct_schema)

for _ in range(3):
    batch = pa.RecordBatch.from_arrays(struct_data[0], struct_schema)
    struct_writer.write_batch(batch)
    pass
struct_writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format
f = open("struct_types.stream.arrow", "wb")
f.write(b)
f.close()
