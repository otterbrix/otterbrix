"""The Python bindings must name the arena they allocate from.

Rule 14 bans both spellings of "the process default" — `std::pmr::get_default_resource()`
and `std::pmr::new_delete_resource()`. The binding layer used to be the largest remaining
cluster of them in the tree, and the reason written next to each one was always the same:
a pybind entry point has no connection, no space and no caller object in scope, so there
was no arena to take. The answer is an arena OWNED BY THE MODULE: created in the
`PYBIND11_MODULE` body in integration/python/main.cpp and threaded into
`type_creation::initialize` / `otterbrix_py_type_t::initialize` / `connect` as an argument.

The arena is `module_arena_t` (integration/python/module_arena.hpp): a
`boost::intrusive_ref_counter` wrapping one `core::pmr::otterbrix_resource`. The module owns
it through a capsule parked on the module object itself, `otterbrix.__arena__`, and every
object built out of it holds its OWN counted reference — so the arena outlives the module
when it has to, and is released when its last owner goes, not before and not never.

Four tests here, and they check different halves of that:

* `test_python_bindings_name_their_arena` is the rule itself. It is a source check because
  the arena is not reachable from Python: nothing the interpreter can hold tells you which
  `memory_resource` a `complex_logical_type` was built on. What IS checkable, exactly and
  without a build, is that no production source under integration/python reaches for the
  process-global arena.

* `test_the_module_owns_its_arena_through_a_capsule` is the FORM, and it is the one thing
  about the arena that Python can see directly: an anonymous file-local static satisfies the
  source check above just as well and is what this replaced, so without this case the whole
  "owned by the module" half of the design is untested. Deleting the `m.add_object` line in
  main.cpp is what turns it red.

* `test_type_surface_survives_the_call_that_built_it` is the half a source check cannot see.
  A module-owned arena is only correct if it OUTLIVES the objects allocated from it, and the
  objects here are handed to the interpreter, which may hold them for the rest of the process.
  A `STRUCT`/`MAP` type keeps its child list in a `std::pmr::vector` built on the arena it was
  given (components/types/types.cpp, `struct_logical_type_extension` copies with
  `fields.get_allocator()`), so an arena that dies at module teardown — or one bound to a
  shorter-lived owner — turns every one of these objects into a dangling read.

* `test_a_schema_type_keeps_the_space_it_was_read_from_alive` is the OTHER arena. Not every
  `OtterBrixPyType` comes from the module's factories: `relation.types` copies types out of a
  relation's schema, and those stand on the ENGINE's arena. Same rule, other owner — see its
  own docstring for the measurement.

`test_type_from_dict_does_not_steal_the_caller_s_reference` is a different defect found on the
same path and kept here because it is the same object: see its own docstring.
"""

import gc
import pathlib
import subprocess
import sys
import textwrap

import pytest

import otterbrix


BANNED_SPELLINGS = (
    "std::pmr::get_default_resource()",
    "std::pmr::new_delete_resource()",
)

PYTHON_BINDING_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _production_sources():
    """Every C++ source of the binding except the tests' own."""
    for path in sorted(PYTHON_BINDING_ROOT.rglob("*")):
        if path.suffix not in (".cpp", ".hpp"):
            continue
        if "tests" in path.relative_to(PYTHON_BINDING_ROOT).parts:
            continue
        yield path


def _code_lines(path):
    """(line number, code) with comments removed.

    Comments are stripped because the ban is on CALLING the process-global arena, and the
    files deliberately name both spellings in prose to say why they are not used. A `//`
    inside a string literal would be over-stripped; that can only hide a violation, never
    invent one, and no such literal exists here.
    """
    in_block = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw
        if in_block:
            end = line.find("*/")
            if end < 0:
                continue
            line = line[end + 2:]
            in_block = False
        while True:
            start = line.find("/*")
            if start < 0:
                break
            end = line.find("*/", start + 2)
            if end < 0:
                line = line[:start]
                in_block = True
                break
            line = line[:start] + line[end + 2:]
        yield number, line.split("//", 1)[0]


def test_python_bindings_name_their_arena():
    offenders = []
    for path in _production_sources():
        for number, code in _code_lines(path):
            for spelling in BANNED_SPELLINGS:
                if spelling in code:
                    offenders.append(
                        "{0}:{1}: {2}".format(
                            path.relative_to(PYTHON_BINDING_ROOT), number, code.strip()
                        )
                    )
    assert offenders == [], (
        "{0} production site(s) under integration/python still reach for the "
        "process-global arena instead of the module-owned one:\n  {1}".format(
            len(offenders), "\n  ".join(offenders)
        )
    )


def test_type_from_dict_does_not_steal_the_caller_s_reference():
    """`OtterBrixPyType({...})` must not consume a reference it was only lent.

    `from_dictionary` in otterbrix_wrapper/pytype.cpp took the caller's borrowed
    `py::object` with `reinterpret_steal`, which claims ownership WITHOUT an incref. The
    temporary then decref'd it on the way out, so one `OtterBrixPyType({...})` call left the
    caller's dict one reference short: the interpreter freed it while the caller still held
    it, and the process died at shutdown reading freed memory (10 runs out of 10).

    The dict is deliberately kept alive by several extra references, so an unbalanced call
    shows up as arithmetic here rather than as a crash inside the test.
    """
    fields = {"x": "INTEGER", "y": "VARCHAR"}
    anchors = [fields] * 8

    before = sys.getrefcount(fields)
    built = otterbrix.typing.OtterBrixPyType(fields)
    after = sys.getrefcount(fields)

    assert after == before, (
        "OtterBrixPyType({{...}}) changed the argument's reference count by {0} "
        "(before={1}, after={2}); a borrowed argument must be handed back as it "
        "was lent".format(after - before, before, after)
    )
    assert repr(built) == "STRUCT"
    assert [name for name, _ in built.children] == ["x", "y"]
    # Reading the anchors keeps them from being optimised away and proves the dict survived.
    assert anchors[0] is fields
    assert fields == {"x": "INTEGER", "y": "VARCHAR"}


def test_type_surface_survives_the_call_that_built_it():
    """Every module-level type factory, read back long after its call returned.

    The nested types (STRUCT, MAP) are the ones that matter: their child list lives in a
    pmr vector on the arena the factory was given. Holding several hundred of them and
    reading them back is what catches an arena that does not outlive its objects.
    """
    integer = otterbrix.sqltype("INTEGER")
    varchar = otterbrix.dtype("VARCHAR")

    kept = []
    for _ in range(200):
        kept.append(otterbrix.struct_type({"a": integer, "b": varchar}))
        kept.append(otterbrix.row_type([integer, varchar]))
        kept.append(otterbrix.map_type(varchar, otterbrix.type("BIGINT")))
        kept.append(otterbrix.decimal_type(18, 4))
        kept.append(otterbrix.list_type(otterbrix.sqltype("DOUBLE")))
        kept.append(otterbrix.array_type(integer, 4))
        kept.append(otterbrix.string_type())
        kept.append(otterbrix.typing.OtterBrixPyType("BIGINT"))
        kept.append(otterbrix.typing.OtterBrixPyType({"n": "INTEGER"}))

    structs = kept[0::9]
    maps = kept[2::9]
    decimals = kept[3::9]
    lists = kept[4::9]
    arrays = kept[5::9]

    assert len(structs) == 200
    for built in structs:
        assert [name for name, _ in built.children] == ["a", "b"]
        assert [repr(child) for _, child in built.children] == ["INTEGER", "STRING_LITERAL"]
    for built in maps:
        assert [name for name, _ in built.children] == ["key", "value"]
        assert repr(built.key) == "STRING_LITERAL"
        assert repr(built.value) == "BIGINT"
    for built in decimals:
        assert built.children == [("precision", 18), ("scale", 4)]
    for built in lists:
        assert repr(built.child) == "DOUBLE"
    for built in arrays:
        assert built.children[1] == ("size", 4)


def test_type_refusals_carry_their_message():
    """The refusal paths are the only ones that ALLOCATE a string from the arena.

    `create_decimal` and `string_to_logical_type` touch their resource only when they
    refuse, so these three are what proves the arena handed to the factories is usable and
    not, say, a null resource smuggled in to dodge rule 14.
    """
    with pytest.raises(RuntimeError) as out_of_window:
        otterbrix.decimal_type(5, 7)
    assert "DECIMAL" in str(out_of_window.value)

    with pytest.raises(RuntimeError) as unknown_name:
        otterbrix.sqltype("NOT_A_TYPE")
    assert "NOT_A_TYPE" in str(unknown_name.value)

    with pytest.raises(RuntimeError) as unknown_object:
        otterbrix.typing.OtterBrixPyType("NOT_A_TYPE")
    assert "NOT_A_TYPE" in str(unknown_object.value)


def test_the_module_owns_its_arena_through_a_capsule():
    """The arena belongs to the MODULE, and that is visible from Python.

    A file-local `static core::pmr::otterbrix_resource` inside `PYBIND11_MODULE` passes the
    source check above word for word — it names its arena, it never calls the process default
    — and that is exactly what this design replaced. What tells the two apart is ownership:
    the arena is created as a counted `module_arena_t` and one owning reference is parked on
    the extension module itself as `__arena__`, so the thing that owns it has a name, and a
    test can substitute a `resource_tracer_t` for it because the dependency travels as an
    argument rather than as a hidden static.

    Deleting the `m.add_object("__arena__", ...)` line in main.cpp is the injection that turns
    this red; nothing else in the suite notices it.
    """
    # `otterbrix` is the PACKAGE; the arena belongs to the extension module the
    # PYBIND11_MODULE body builds, which the package re-exports from. `__arena__` is
    # deliberately not one of the names __init__.py lifts into the package: it is the
    # module's own reference, not part of the API.
    extension = otterbrix.otterbrix
    assert hasattr(extension, "__arena__"), (
        "the module does not own an arena: `otterbrix.otterbrix.__arena__` is missing, so "
        "whatever the type factories were handed belongs to nobody the interpreter can name"
    )
    arena = extension.__arena__
    assert type(arena).__name__ == "PyCapsule", (
        "`otterbrix.otterbrix.__arena__` is a %s, not the PyCapsule that holds the module's "
        "owning reference to module_arena_t" % (type(arena).__name__,)
    )
    # It is the module's reference and stays the same object across reads: a capsule rebuilt
    # per access would be a second owner appearing out of nowhere.
    assert extension.__arena__ is arena
    assert "__arena__" not in otterbrix.__all__


# The engine's own teardown trace is the clock here. `base_otterbrix_t::~base_otterbrix_t`
# (integration/cpp/base_spaces.cpp) logs "delete spaces" as its first act, so the position of
# that line relative to a marker the script prints says exactly WHEN the space died.
_SCHEMA_TYPE_OUTLIVES_ITS_RELATION = """
import gc
import sys
import tempfile

import pandas as pd

import otterbrix

conn = otterbrix.connect(sys.argv[1])
rel = conn.from_df(pd.DataFrame({"s": [{"a": 1, "b": 2}] * 4, "n": [1, 2, 3, 4]}))
# `types` is a copy of the relation's schema types. The STRUCT keeps its child list in a
# pmr vector on the ENGINE's arena -- a copy of a nested complex_logical_type keeps the
# source's allocator -- so these objects are standing in the space's pool.
types = list(rel.types)
assert [repr(t) for t in types] == ["STRUCT", "BIGINT"], [repr(t) for t in types]

del rel
conn.close()
del conn
gc.collect()
print("MARKER-CONNECTION-GONE", flush=True)

assert [name for name, _ in types[0].children] == ["a", "b"], types[0].children
del types
gc.collect()
print("MARKER-TYPES-GONE", flush=True)
"""


def test_a_schema_type_keeps_the_space_it_was_read_from_alive(tmp_path):
    """`relation.types` hands Python bytes that live in the ENGINE's arena.

    Not every OtterBrixPyType comes from the module's factories. `py_relation_t::column_types`
    copies each column type out of the relation's schema, and `complex_logical_type`'s copy
    constructor keeps the SOURCE's allocator (`struct_logical_type_extension` copies with
    `fields.get_allocator()`), so a STRUCT column hands out a child vector that is still
    sitting in the space's pool. The object was built holding NEITHER owner: not the module's
    arena, not the space. Dropping the relation and closing the connection released that pool
    while the Python object still pointed into it.

    It did not crash — a released `synchronized_pool_resource` hands its chunks back to
    `operator delete` and the bytes usually survive being read once — so the assertion is
    not "did it segfault" but WHEN THE SPACE DIED. The engine's destructor logs "delete
    spaces" as its first line, so the fix is visible as an ordering: with the type holding the
    space, that line lands AFTER the connection is gone and only when the last type is
    released.

    MEASURED. Injecting the defect back (the owner-less `otterbrix_py_type_t(col.type())`
    that column_types used to build) puts "delete spaces" at output line 140 and
    MARKER-CONNECTION-GONE at line 471: the engine was 331 lines into being torn down before
    the connection object was even released, and the `types[0].children` read that follows is
    reading a pool that has been handed back. With the space held, the same run gives
    MARKER-CONNECTION-GONE at line 126, "delete spaces" at 127 and the last teardown line at
    236, one line before MARKER-TYPES-GONE — the whole teardown moved between the markers.
    """
    finished = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_SCHEMA_TYPE_OUTLIVES_ITS_RELATION), str(tmp_path / "schema_db")],
        capture_output=True,
        text=True,
    )
    combined = finished.stdout + finished.stderr
    assert finished.returncode == 0, "the case did not finish: returncode=%d\n%s" % (
        finished.returncode,
        combined[-4000:],
    )

    lines = combined.splitlines()
    def position(marker):
        for index, line in enumerate(lines):
            if marker in line:
                return index
        raise AssertionError("marker %r never appeared:\n%s" % (marker, combined[-4000:]))

    connection_gone = position("MARKER-CONNECTION-GONE")
    types_gone = position("MARKER-TYPES-GONE")
    space_died = position("delete spaces")

    # THE DISCRIMINATOR. If the schema types hold no owner, the last reference to the space is
    # the connection's, so the engine tears down at `del conn` -- before this marker. Holding
    # the space moves that teardown past it.
    assert connection_gone < space_died, (
        "the space was destroyed at output line %d, BEFORE the connection was released at line "
        "%d: the OtterBrixPyType objects read out of relation.types are pointing into a pool "
        "that has already been given back" % (space_died, connection_gone)
    )
    assert space_died < types_gone, (
        "the engine never shut down while the test was watching (delete spaces at line %d, "
        "MARKER-TYPES-GONE at line %d)" % (space_died, types_gone)
    )
    # The whole point: the space outlived the connection object and went only with the last
    # type read out of its schema. Everything the destructor logs therefore has to sit AFTER
    # the second marker's predecessor, i.e. the teardown must not have completed before the
    # types were dropped.
    teardown_end = max(
        index for index, line in enumerate(lines) if "delete manager_dispatcher_t" in line
    )
    assert connection_gone < teardown_end < types_gone, (
        "the engine tore down at lines %d..%d, but the connection was released at line %d and "
        "the schema types only at line %d: the types were not holding the space they point "
        "into" % (space_died, teardown_end, connection_gone, types_gone)
    )
