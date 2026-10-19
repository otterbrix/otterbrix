"""A Python dict in the `{"key": [...], "value": [...]}` shape is a MAP constant, and a MAP constant must be usable.

`ConstantExpression(d, conn)` builds the value (python_conversion.cpp `transform_dictionary_to_map`
-> `make_map`, whose payload is a LIST of STRUCT<key, value>) and stores it in
`expression_factory_t::values`. The value is read back when a relation using it is executed:
`expression_factory_t::get_params` hands every stored constant to the query as a parameter. So a dict
that the module itself knows how to turn into a MAP can be compared with a MAP column (#703 item 1).

The case runs in a child interpreter so that, whatever the failure mode of a given build (a
RuntimeError in the wheel, possibly an abort where asserts are on), the other tests survive.
Expected: the filter keeps the row whose map equals the dict and hands the map back as a dict.
"""

import subprocess
import sys

_BODY = """
import sys

import pyarrow as pa
import otterbrix
from otterbrix import ColumnExpression, ConstantExpression

conn = otterbrix.connect(sys.argv[1])
maps = pa.array([[("k", 1), ("j", 2)], [("z", 9)]], type=pa.map_(pa.string(), pa.int32()))
relation = conn.from_object(pa.table({"id": [1, 2], "m": maps}))
wanted = ConstantExpression({"key": ["k", "j"], "value": [1, 2]}, conn)
rows = relation.filter(ColumnExpression("m", conn) == wanted).fetchall()
print("MAP-FILTER:%r" % (rows,))
"""


def test_a_dict_in_map_shape_matches_the_map_it_describes(tmp_path):
    proc = subprocess.run(
        [sys.executable, "-c", _BODY, str(tmp_path / "db")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert proc.returncode == 0, "child failed with %s\n%s" % (proc.returncode, proc.stderr[-600:])
    assert "MAP-FILTER:[(1, {'k': 1, 'j': 2})]" in proc.stdout, proc.stdout[-600:]
