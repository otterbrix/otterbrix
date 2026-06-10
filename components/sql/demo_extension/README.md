# demo_extension — reference parser extension

A complete, minimal parser extension: the toy language `DEMO <arithmetic>`
(e.g. `DEMO 2 + 3 * 4`). Copy this directory as the template for a real grammar
extension (ksql, an S3 source, …).

A parser extension lets you teach otterbrix syntax the core SQL grammar does not
know. The core bison parser always runs **first**; only queries it rejects are
offered to the registered extensions (DuckDB's strategy — a successful core
parse never pays for extensions). An extension that recognizes the query owns it
end to end: its own lexer, its own grammar, its own AST, lowered to a
`logical_plan` that runs on the normal engine.

## The two stages

```
query string ──parse──▶ ExtensionNode (wraps your AST) ──transform──▶ logical_plan
```

- **parse** (`demo_scan.l` + `demo_gram.y`, driven from `demo_extension.cpp`):
  recognize your syntax, build your own AST, wrap its root in an `ExtensionNode`
  envelope. Returns a `core::result_wrapper_t<List*>`:
  - a non-NIL tree → you claimed and parsed it,
  - an `error_t` → it's yours but malformed,
  - `NIL` → not yours; the next extension (or the core error) is tried.
- **transform** (`demo_extension.cpp`): the transformer routes the `ExtensionNode`
  back to you by `extension_id` and you walk your AST into a `logical_plan` node.

## Files

| File | Role |
|------|------|
| `demo_ast.hpp`        | your own AST nodes (plain arena-allocated structs — no PG nodes) |
| `demo_gram.y`         | reentrant bison grammar, `%name-prefix="demo_yy"`, wraps the root via `make_extension_node` |
| `demo_scan.l`         | reentrant flex scanner, `%option prefix="demo_yy"`, tokenizes + the `DEMO` keyword |
| `demo_extension.cpp`  | `parse` (scanner driver) + `transform` + `make_demo_extension()` factory |
| `demo_extension.hpp`  | the two stage declarations + the registrable factory |
| `CMakeLists.txt`      | one `otterbrix_add_parser_extension(...)` call |

## Authoring your own

1. **AST** — define your node structs (`demo_ast.hpp`). Allocate from the parser
   arena (`resource->allocate(...)`); the tree lives as long as the parse.
2. **Grammar + scanner** — `<name>_gram.y` and `<name>_scan.l`, each with a
   unique reentrant prefix (`%name-prefix` / `%option prefix`) so they never
   clash with the core `base_yy` / `core_yy` parser. The grammar's top rule wraps
   its root: `*out = make_extension_node(resource, "<id>", root);`
   (see `make_extension_node` in `parser/nodes/parsenodes.h`).
3. **parse** — stand the scanner up with the one-line guard
   `using my_scanner = EXTENSION_FLEX_SCANNER(<name>_yy);`, peek the first token
   to claim the query (return `NIL` if it isn't yours), then call `<name>_yyparse`.
4. **transform** — `static_cast` the `ExtensionNode::data` back to your root and
   build a `logical_plan` node. `params` is there if your syntax has placeholders.
5. **factory** — `make_<name>_extension()` returns
   `parser_extension_t{"<id>", &parse, &transform}`. The `<id>` ties the parse-time
   envelope to the transform-time routing.
6. **CMake** — `otterbrix_add_parser_extension(<lib> PREFIX <name> SOURCES <name>.cpp)`.

## Registering

Per **instance**, not globally — an extension enabled on one database is invisible
to another:

```cpp
space.dispatcher()->add_parser_extension(make_demo_extension());
// now `DEMO 1 + 20` runs on this instance and yields 21
```

Or use the parser standalone (no instance), handing a registry straight to
`raw_parser`:

```cpp
parser_extension_registry_t registry;
registry.add(make_demo_extension());
List* tree = raw_parser(arena, "DEMO 7", registry);
```

See `components/sql/test/test_parser_extension.cpp` (parser-level) and
`integration/cpp/test/test_demo_extension.cpp` (end-to-end + per-instance
isolation) for the exercised behaviour.
