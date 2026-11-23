# Voyager OGM Query Spec Interchange Protocol (v1)

## Overview

The **Query Spec Interchange Protocol** is the canonical, language-agnostic Abstract Syntax Tree (AST) exchange contract for Voyager OGM. It defines how graph queries, traversal chains, filter predicates, DML mutations, aggregations, and pagination are serialized across FFI boundaries between:

- Python SDK (`voyager-ogm`)
- TypeScript / Node.js SDK (`@voyager-ogm/client`)
- Native Rust Core Engine (`voyager-ogm` / `voyager-pyo3`)
- AI Model Context Protocol (MCP) servers and language agents

This formal ABI contract guarantees cross-language stability and deterministic translation into target dialects (**openCypher**, **ISO/IEC 39075:2024 GQL**, **SQL:2023 PGQ**, and **Apache AGE**).

---

## 1. Specification Versioning (`spec_version: 1`)

Every query spec document **must** carry a top-level integer attribute:
```json
{
  "spec_version": 1
}
```

`spec_version` is required only at the document root. Nested specs — each
`matches[i]` and `with_clauses[i]` object, a nested spec passed to `exists` or
`count`, and an object-wrapped expression — may omit it. If a nested object does
declare it, it must be `1`; any other value is rejected at that nested
`spec_version` key.

### Compatibility Rules
1. **Major Version Invariance**: The native Rust compiler (`compile_query_from_spec`) and internal builders strictly reject unknown major versions:
   - Any payload missing a root `"spec_version"` is rejected immediately with:
     `Missing required 'spec_version' in query spec dictionary. Expected 'spec_version': 1.`
   - Any payload with an unsupported version (`spec_version: 2`) is rejected with:
     `Unsupported query spec_version: 2. Supported major versions: 1.`
2. **Clause Versioning**: A nested clause or expression object (`matches[i]`, `with_clauses[i]`, an `exists`/`count` subquery spec and an object-wrapped expression) may optionally declare `"spec_version": 1`. If present, unknown values are rejected at that nested key.

---

## Errors

Every structural failure in the Query Spec v1 consumer raises `VoyagerError`
(a `ValueError` subclass) with:

- `code == "INVALID_SPEC"`.
- `path` set to a JSON Pointer to the offending element.
- the same pointer in the message, prefixed with `Invalid query spec at`.

The document root is the empty pointer `""`. Object members append `/name` and
array items append `/index`, so `/matches/0/paths/1/2` is the third step of the
second path of the first match. JSON Pointer escaping applies (`~` becomes
`~0`, `/` becomes `~1`).

Messages name JSON types (`string`, `number`, `boolean`, `null`, `array`,
`object`), not Python class names, because spec authors write JSON. An
expression-position rejection also names the explicit spelling to use:
`["lit", value]` for a scalar and `["lit", [...]]` / `["list", [...]]` for a
list.

Examples:

| Spec | `path` |
| :--- | :--- |
| `{"spec_version": 1, "bogus": 1}` | `/bogus` |
| `{"spec_version": 1}` (empty spec) | `""` |
| `{"matches": []}` (missing `spec_version`) | `/spec_version` |
| `{"spec_version": 1, "matches": 42}` | `/matches` |
| `{"spec_version": 1, "matches": [{"paths": [[["node","n",["N"],"extra"]]]}]}` (non-object node properties) | `/matches/0/paths/0/0/3` |
| `{"spec_version": 1, "matches": [{"paths": [[["node","n",["N"],{"name":"x"}]]]}]}` (bare scalar property value) | `/matches/0/paths/0/0/3/name` |
| `{"spec_version": 1, "matches": [{"paths": [[["node","n",["N"]],["edge","out",["E"],null,null,null,{},1]]]}]}` (8th edge item) | `/matches/0/paths/0/1/7` |
| `{"spec_version": 1, "skip": -1, ...}` | `/skip` |

### `null` means omitted

At every optional key and optional array position, JSON `null` means the same as
omitting the key. This covers root `load_csv`, `projections`, `distinct`,
`order_by`, `skip`, `offset`, `limit` and `execution_mode`; match `optional`,
`path_mode`, `path_variable` and `where`; with-clause `distinct`, `where`,
`order_by`, `skip` and `limit`; the optional node and edge path-step items; and
the nullable expression positions (`case` operand and else branch, `list_comp`
where and map expressions, and `pattern_comp` where). Required keys do not accept
`null`: root `spec_version`, `matches[i].paths`, `with_clauses[i].projections`,
and the `expr` key of an object-wrapped expression.

The following are rejected:

- Unknown object keys at the root, in a match clause, in a with clause, and in
  an object-wrapped expression. The key sets are closed.
- Wrong container types for `load_csv`, `unwinds`, `matches`, `paths`,
  `mutations`, `with_clauses`, `projections`, `order_by`, `linear_clauses`, and
  merge set lists.
- Unknown or wrongly spelled enum values. `path_mode` (`trail`, `simple`,
  `acyclic`, `walk`, `none`), `execution_mode` (`normal`, `explain`, `profile`,
  `explain_and_profile`), edge `direction` (`outgoing`/`out`/`->`,
  `incoming`/`in`/`<-`, `undirected`/`both`/`-`/`--`) and aggregation function
  names (`count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `collect`) are
  exact and case-sensitive. So are the mutation, path-step, projection and
  `linear_clauses` tags.
- The undocumented tag aliases `literal` (for `lit`) and `func` / `call` (for
  `fn`). They are rejected as unknown expression tags; write the canonical `lit`
  and `fn` tags.
- Boolean, negative or non-integral `skip` / `offset` / `limit` values. An
  integral JSON number such as `0.0` or `3.0` is accepted (the schema's
  `integer` type accepts it). `skip` and `offset` are aliases of the same
  position, so providing both is rejected at the second key in document order.
- Negative `min_hops` / `max_hops` values. `min_hops` has minimum 0; `max_hops`
  has minimum 0 or may be `"unbounded"` for open upper bounds. When
  `max_hops` is `null`, the traversal is exactly `min_hops` hops. When
  `min_hops` is omitted or `null` and `max_hops` is set, the lower bound is 1.
- A bare or untagged array in an expression position: an empty array, a non-string
  first element (for example `[1, 2, 3]`) or an unknown first string (for
  example `["propp", ...]` or `["red", "green"]`). Every expression array must
  be tagged (see [Expression arrays are always tagged](#expression-arrays-are-always-tagged)).
- An `agg` array in an expression position. `agg` is a projection item only;
  the error at the array's path says aggregations are only valid as projection
  items instead of suggesting a literal spelling (see
  [Aggregations are projection items only](#aggregations-are-projection-items-only)).
- A path step `properties` item that is neither an object nor `null`, a
  non-null item past a step's documented shape (5th node item, 8th edge item),
  or a property value that is not a tagged expression.
- A `where` array whose first element is a scalar other than a string (for
  example `[1, 2, 3]`), which is neither one expression nor an array of
  expressions (see [`where`: one expression or a list](#where-one-expression-or-a-list-of-expressions)).
- An **empty spec**: a document that has none of `load_csv`, `unwinds`,
  `matches`, `mutations`, `with_clauses`, `projections`, or `linear_clauses`
  with content. `{"spec_version": 1}` and `{"spec_version": 1, "matches": []}`
  are both empty. Pagination and `execution_mode` alone do not make a spec
  non-empty.
- Root `distinct`, `order_by`, `skip`, `offset` or `limit` without a non-empty
  root `projections` list. These belong to the final `RETURN`: without one they
  would be dropped, or (when `with_clauses` are present) fold into the last
  `WITH`, silently changing the query. The error is at the first offending key,
  for example `/limit`, and names `projections` in the message.
- A spec whose emitted statement would end in `WITH`: `with_clauses` without
  root `projections` and without mutations. Error at `/projections` ("a query
  ending in WITH needs root projections"). Mutations emit after `WITH`, so a
  spec that ends in a mutation is still valid.
- A `with_clauses[i].projections: []` empty list (error at
  `/with_clauses/i/projections`) and a `matches[i].paths: []` empty list (error
  at `/matches/i/paths`). Both would otherwise drop or misattach their
  `where`/pagination.
- A variable reference the fixed emission order cannot honour, such as a linear
  filter reading an alias a later `WITH` defines, or a mutation after a `WITH`
  targeting a variable that clause dropped. The error is at the identifier's own
  pointer and names the variable (see
  [Clause order and scope](#5-clause-order-and-scope)).
- A `WITH` projection that is not a bare variable and has no alias, such as
  `["field", "p", "name", null]` or `["agg", "p", "age", "count", null]`. The
  error is at the projection item's pointer (see
  [Clause order and scope](#5-clause-order-and-scope)).
- Any item past a fixed-arity shape (see the shapes below): `load_csv`,
  `unwinds` entries, every mutation tag, every projection tag, `order_by`
  entries, `linear_clauses` entries, merge `ON CREATE SET` / `ON MATCH SET`
  entries, `case` branches, and every expression tag. The error is at the extra
  item's own path, for example `/projections/0/4`. Path steps keep their
  separate documented trailing-`null` tolerance.
- A non-string `alias` name in `["alias", expression, name]`.
- An empty variable in `["prop", "", property]`, and an empty name in
  `["ident", ""]` or `["param", ""]`.
- A JSON object in an expression position that has no `expr` key. Only the
  `exists`, `count` and `pattern_comp` payloads accept a nested query spec or
  path spec; every other object must be written as `{"expr": ...}` (the nested
  `spec_version` key is optional).
- A compile-time build or emission failure. These raise `VoyagerError` too:
  `INVALID_SPEC` at the document root (`path == ""`) when the AST cannot be
  built or emitted, or `UNSUPPORTED_FEATURE` / `UNSUPPORTED_DIALECT` when the
  core error identifies a dialect limitation. Messages name the AST node kind
  only, never a Rust debug dump.

Parameter names may be written with or without one leading `$`: `["param",
"$name"]` and `["param", "name"]` both bind `$name`, and an `unwinds` entry
`["$batch", "row"]` compiles to `UNWIND $batch AS row`, matching the Python
SDK's `param()`. Exactly one `$` is stripped. An `["alias", expression, name]`
expression is only meaningful in a projection: an `expr` projection uses the
name when its own alias is `null`, and the `alias` expression inside a
projection item is the same as carrying that name. In every other expression
position (`where`, `set`, function arguments, and so on) the name is validated
as a string and the inner expression is used; no alias is emitted there.

`INVALID_CLAUSE` remains the code for non-spec callers, such as the fluent
Python/Rust builder API.

---

## 2. Root Query Spec Structure

```json
{
  "spec_version": 1,
  "load_csv": ["https://data.example.com/nodes.csv", true, "row"],
  "unwinds": [["$batch", "item"]],
  "matches": [
    {
      "spec_version": 1,
      "optional": false,
      "path_mode": "trail",
      "path_variable": "p",
      "paths": [
        [
          ["node", "u", ["User"], {"city": ["lit", "Berlin"]}],
          ["edge", "outgoing", ["KNOWS"], "r", 1, 2, {"since": ["lit", 2020]}],
          ["node", "f", ["User"]]
        ]
      ],
      "where": [
        ["bin", "gt", ["prop", "f", "age"], ["lit", 21]]
      ]
    }
  ],
  "with_clauses": [
    {
      "spec_version": 1,
      "distinct": false,
      "projections": [
        ["field", "u", "name", "user_name"],
        ["agg", "f", "id", "count", "friend_count"]
      ],
      "where": [
        ["bin", "gte", ["ident", "friend_count"], ["lit", 5]]
      ],
      "order_by": [
        [["ident", "friend_count"], false]
      ],
      "limit": 10
    }
  ],
  "projections": [
    ["expr", ["ident", "user_name"], null],
    ["expr", ["ident", "friend_count"], "total"]
  ],
  "distinct": false,
  "order_by": [
    [["ident", "user_name"], true]
  ],
  "skip": 0,
  "limit": 50,
  "execution_mode": "normal"
}
```

### ORDER BY entries

`order_by` appears at the root and inside each `with_clauses[i]`. Every entry is one of two shapes:

| Shape | Description |
| :--- | :--- |
| `[variable, property, direction]` | Sort by a property of a bound variable, e.g. `["p", "name", true]`. |
| `[expression, direction]` | Sort by an expression descriptor, e.g. `[["fn", "size", [["prop", "p", "name"]]], "DESC"]`. |

`direction` accepts both a boolean and a string: `true` means ascending, `false` means descending, and the strings `"ASC"` / `"DESC"` are exact, uppercase spellings of the same two directions. A boolean and its string counterpart compile to the same statement, so `["p", "name", true]` and `["p", "name", "ASC"]` are equivalent. Any other value (for example `"asc"`, `"up"` or `1`) is rejected with an `INVALID_SPEC` error at the direction element (for example `/order_by/0/2`).

The `[variable, property, direction]` form reads `variable.property`, so it only
works for a variable bound by a pattern. To sort by a `WITH`/`RETURN` alias, use
the expression form with an identifier:
`[["ident", "friend_count"], false]`. Writing `["friend_count",
"friend_count", false]` compiles to `ORDER BY friend_count.friend_count`, which
is not the alias.

---

## 3. Path Step Tuples

Paths within `matches[i].paths` or `create`/`merge` mutations are sequences of steps:

| Step Type | Shape | Description |
| :--- | :--- | :--- |
| **Node** | `["node", variable, labels, properties]` | Node pattern, e.g. `["node", "u", ["User", "Admin"]]`. `variable` can be `null` for anonymous nodes. `properties` is optional: an object mapping property names to [expressions](#4-expression-ast-descriptors), for example `{"name": ["lit", "Alice"]}`. Omitted, `null`, and `{}` all mean no properties; any other non-object is rejected. |
| **Edge** | `["edge", dir, types, variable, min_hops, max_hops, properties]` | Directed or undirected relationship traversal. `dir` accepts exactly `"outgoing"` (`"out"`, `"->"`), `"incoming"` (`"in"`, `"<-"`), and `"undirected"` (`"both"`, `"-"`, `"--"`); the spelling is case-sensitive. `min_hops` has minimum 0. `max_hops` has minimum 0, or accepts `"unbounded"` to represent an unbounded upper bound (e.g. `*2..` in Cypher, `{2,}` in ISO GQL/PGQ). `max_hops: null` means exactly `min_hops` hops; when `min_hops` is omitted or `null` and `max_hops` is set, the lower bound is 1 (e.g. `*1..3` in Cypher, `{1,3}` in ISO GQL/PGQ). `variable`, `min_hops`, `max_hops` and `properties` may be omitted (`["edge", dir, types]`) and default to `null`. `properties` follows the same rules as the node step. A non-null item after `properties` is rejected. |

Property values follow the same strict rule as every other expression
position: each value must be a tagged expression such as `["lit", "Alice"]` or
`["param", "email"]`. A bare JSON scalar is rejected, for example
`{"name": "Alice"}` fails at `/matches/0/paths/0/0/3/name` with the
`["lit", value]` hint. Inline properties compile to the equivalent pattern in
openCypher, Apache AGE and ISO GQL; SQL:PGQ renders the same values as
`WHERE` predicates inside its node/edge pattern (for example
`(p IS Person WHERE p.name = $p0)`).

---

## 4. Expression AST Descriptors

Expression trees used in `where`, `set`, and projections support fine-grained operators:

| Operator | Tuple / Array Shape | Description |
| :--- | :--- | :--- |
| **Property Access** | `["prop", var, property]` | Access entity property: `u.age` |
| **Identifier** | `["ident", name]` | Variable or alias identifier: `friend_count` |
| **Parameter** | `["param", name]` | Query parameter reference: `$param` |
| **Literal** | `["lit", value]` | Any JSON value: a scalar (`42`, `"Alice"`, `true`, `null`), a list or an object |
| **Binary Op** | `["bin", op, left, right]` | Binary operators (canonical lowercase spellings; the consumer also accepts the uppercase word spellings): `eq` (`=`), `neq` (`!=`, `ne`, `<>`), `gt` (`>`), `gte` (`>=`, `ge`), `lt` (`<`), `lte` (`<=`, `le`), `and` (`&`), `or` (`\|`), `xor` (`^`), `add` (`+`), `sub` (`-`), `mul` (`*`), `div` (`/`), `mod` (`%`), `concat` (`\|\|`), `contains`, `starts_with` (`startswith`), `ends_with` (`endswith`), `regex` (`=~`), `in`, `not_in` (`not in`). Any other operator is `INVALID_SPEC` at the operator element. |
| **Unary Op** | `["unary", op, operand]` | `not` (`~`), unary minus as `["unary", "-", operand]` (the operator `neg` is an alias for `-`), `is_null` (`is null`), `is_not_null` (`is not null`). Negation has no top-level `neg` or `negate` tag. |
| **Function** | `["fn", name, [args...]]` | Function call: `["fn", "toUpper", [["prop", "u", "name"]]]` |
| **CASE WHEN** | `["case", operand, [[when, then]...], else]` | Conditional branching |
| **List Literal** | `["list", [items...]]` | Literal list of expressions |
| **Subquery Exists**| `["exists", subquery_spec]` | Existential graph subquery |
| **Subquery Count** | `["count", subquery_spec]` | Scalar subquery count |
| **Alias** | `["alias", inner, alias_name]` | Projection aliasing (only meaningful in projections) |

The recursive payload positions are fully typed:

- A `case` branch is exactly `[when, then]`; the `case` operand and else branch
  are an expression or `null`.
- `list_comp` is `["list_comp", variable, list_expr, where | null, map | null]`.
- `pattern_comp` is `["pattern_comp", path, where | null, projection]`, where
  `path` is a path-step list (the same shape as a `matches[i].paths` entry).
- An `exists` or `count` payload is either a nested query spec object (the same
  shape as the root document, with `spec_version` optional) or a path-step list.
- `["lit", value]` accepts any JSON value.
- A merge `ON CREATE SET` / `ON MATCH SET` entry is exactly
  `[variable, property, value_expression]`.
- A `bin` operator is one of the operators in the table above; anything else is
  rejected at the operator element.

### Expression arrays are always tagged

An expression position never accepts an untagged JSON array. Every array there
must be a tagged expression descriptor: its first element is a string that is a
known expression tag. The array spelling may be a Python `list` (the JSON form)
or a `tuple` (the form the Python SDK emits), and the tag rule is the same for
both.

Valid (a literal list, and a list whose items are expressions):

```json
["lit", [1, 2, 3]]
["list", [["lit", 1], ["lit", 2], ["lit", 3]]]
```

Invalid (`INVALID_SPEC` at the path of the array itself):

```json
[1, 2, 3]
```

An empty array, a non-string first element and an unknown first string (for
example `["propp", "p", "age"]` or `["red", "green"]`) are all rejected. Write a
literal list as `["lit", [...]]`; write a list whose items are expressions as
`["list", [...]]`.

### `where`: one expression or a list of expressions

The schema allows `where` to be either one `Expression` or an array of
`Expression`s. A single expression is itself a JSON array starting with its tag
string, so the form is chosen deterministically from the first element:

| `where` value | Meaning |
| :--- | :--- |
| `[]` | An empty predicate list; no predicate is emitted. |
| `["bin", ...]` (first element is a string) | One expression. |
| `[["bin", ...], ["bin", ...]]` (first element is an array or object) | A list of expressions. |
| anything else, such as `[1, 2, 3]` | `INVALID_SPEC` at the `where` path. |

These two forms compile to identical Cypher:

```json
"where": ["bin", "gt", ["prop", "f", "age"], ["lit", 21]]
"where": [["bin", "gt", ["prop", "f", "age"], ["lit", 21]]]
```

The rule is the same for `matches[i].where` and `with_clauses[i].where`, and for
the Python tuple spelling of either form. An empty array remains an empty
predicate list in both spellings.

### Projection items

Root `projections` and `with_clauses[i].projections` both contain projection
items with one of three exact shapes:

| Shape | Description |
| :--- | :--- |
| `["field", variable, property, alias]` | Project `variable.property` as `alias`. `alias` is a string or `null`. |
| `["expr", expression, alias]` | Project an expression as `alias`. `alias` is a string or `null`; when it is `null`, an outer `["alias", expression, name]` supplies it. |
| `["agg", variable, property, func, alias]` | Aggregate `variable.property`. `func` is one of `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `collect`. A `property` of `"*"` (or `""`) aggregates the variable itself, so `["agg", "p", "*", "count", "n"]` compiles to `COUNT(p) AS n`. |

An `["alias", expression, name]` name takes effect in an `expr` projection (or
as the projection's explicit alias); in any other expression position the name
is validated and the inner expression is used. Every shape above is exact: an
item past the documented length is `INVALID_SPEC` at the extra item's path.

### Aggregations are projection items only

`agg` is not an expression tag. It is accepted only as a projection item in
root `projections` and `with_clauses[i].projections`. An `agg` array anywhere
in an expression position (inside `where`, `order_by`, a `set` value, a
function argument, a binary or unary expression, or a path property map) is
rejected with `INVALID_SPEC` at the array's own path. The message names the
misuse directly: aggregations are only valid as projection items, so move the
aggregate into a projection list or a `WITH` clause. It is not the generic
unknown-tag hint about `["lit", [...]]`.

The Python fluent API raises the same rule at construction or at the call that
uses the aggregation. `p.id.count() > 2`, `~p.id.count()`, `fn.size(p.id.count())`,
`q.where(p.id.count())`, `q.order_by(p.id.count())` and `q.set(x=p.id.count())`
raise `VoyagerError` with code `INVALID_CLAUSE` and the message
"aggregations are only valid in WITH/RETURN projections".
`q.return_(count=p.id.count())` and `q.with_(count=p.id.count())` remain valid.

### Linear clauses

`linear_clauses` is an ordered list of clause entries emitted between the
`MATCH` clauses and the first `WITH`:

| Shape | Description |
| :--- | :--- |
| `["let", variable, expression]` | Bind `variable` to `expression` (`LET` in ISO GQL, a projected expression in openCypher-style output). |
| `["filter", predicate]` | Filter the current rows by `predicate` (a bare `WHERE`/`FILTER`). |

Neither entry accepts extra items.

---

## 5. Clause order and scope

The consumer emits clauses in a fixed order, whatever order the document's keys
use. Every variable reference must resolve at the point its clause is emitted;
a spec that depends on a different order is rejected with `INVALID_SPEC` at the
offending identifier (or at the expression containing it) instead of being
silently reordered.

The emission order is:

1. `load_csv`
2. `unwinds` (in list order)
3. `matches` (in list order)
4. `linear_clauses` (`let` and `filter`, in list order)
5. `with_clauses` (in list order)
6. `mutations` (in list order)
7. the final `RETURN`: `projections`, then `order_by`, then `skip`/`offset` and
   `limit`

The scope rules follow that order:

- A `load_csv` or `unwinds` alias is bound from its clause onward.
- A match path binds its node variables, edge variables and `path_variable` for
  the rest of the query. `matches[i].where` sees the variables of its own paths
  and everything bound before the match.
- A linear `let` binds its variable only from that entry onward. A `filter` sees
  the bindings of the entries before it.
- A `WITH` clause replaces the visible names with the ones it projects. A bare
  variable projection without an alias (`["expr", ["ident", "p"], null]`) keeps
  that variable. Every aliased projection keeps only its alias: a bare variable
  projected as `["expr", ["ident", "p"], "x"]` keeps `x`, and
  `["field", "p", "name", "n"]` keeps `n` and drops `p`. `load_csv`, `unwinds`,
  match and `let` names are dropped by a `WITH` unless the clause projects them.
- A `WITH` projection item that is not a bare variable must have an alias. The
  consumer rejects an unaliased `["field", "p", "name", null]`, `["agg", ...]`
  without an alias, or a non-identifier `expr` projection without an alias, at
  the projection item's pointer. Only a bare variable may be projected without
  one. `RETURN` projections keep working without aliases.
- `with_clauses[i].where` and `with_clauses[i].order_by` see the names that
  clause projects plus the names that were in scope before the `WITH`. When the
  `WITH` is `DISTINCT` or contains an aggregation, both see only the projected
  names, and a reference to a pre-`WITH` name is rejected with `INVALID_SPEC`:
  a `WITH` with `DISTINCT` or an aggregation only exposes the names it projects.
  The clauses after the `WITH` always see only the projected names.
- Under `DISTINCT` or an aggregation, `where` and `order_by` may still repeat a
  projected expression. The expression may match a projection exactly
  (`WITH DISTINCT p.city AS city ORDER BY p.city`) or appear inside a larger
  expression (`WHERE p.city IS NOT NULL`, `ORDER BY p.city + 1`,
  `ORDER BY toUpper(p.city)`), and property access on a projected variable is
  allowed (`RETURN DISTINCT p ORDER BY p.city`). Every other pre-`WITH` or
  pre-`RETURN` variable is still rejected, including a variable that appears
  next to a projected expression (`ORDER BY p.city + p.age`). The repeated
  expression is evaluated from the projected column, so an alias may also be
  used directly.
- Mutations and the final `projections` see the scope after the last `WITH`
  (plus variables the earlier mutations created). Root `order_by` may
  additionally use the names the `RETURN` projects. When the root `RETURN` is
  `DISTINCT` or contains an aggregation, root `order_by` sees only the names the
  `RETURN` projects, with the same repeat-a-projected-expression rule as a
  `WITH`: `RETURN DISTINCT p.city ORDER BY p.city` and
  `RETURN p.city, COUNT(p) AS c ORDER BY p.city` are accepted, and a repeated
  aggregate expression such as `RETURN COUNT(p) AS c ORDER BY count(p)` is
  accepted too.
- Variables bound inside an `exists`/`count` subquery or a `pattern_comp` path
  are local to that construct, though the construct may read outer variables.
  A `list_comp` iteration variable is visible only in that comprehension's
  `where` and map expressions.
- Parameters (`param`) and literals (`lit`) never need a binding.

### Filter after `WITH`

`linear_clauses` is emitted before `with_clauses`, so a filter cannot read an
alias that a later `WITH` defines. This document is rejected at
`/linear_clauses/0/1/2/1`:

```text
{"spec_version": 1,
 "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
 "with_clauses": [{"projections": [["field", "p", "name", "n"]]}],
 "linear_clauses": [["filter", ["bin", "gt", ["ident", "n"], ["lit", 0]]]],
 "projections": [["expr", ["ident", "n"], null]]}
```

The error says `variable 'n' is not in scope here: linear clauses are emitted
before WITH clauses; move the reference into with_clauses[0].where ...`. Express
the filter in the `WITH` that defines the alias:

```json
{
  "spec_version": 1,
  "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
  "with_clauses": [
    {
      "projections": [["field", "p", "name", "n"]],
      "where": ["bin", "gt", ["ident", "n"], ["lit", 0]]
    }
  ],
  "projections": [["expr", ["ident", "n"], null]]
}
```

That compiles to `MATCH (p:Person) WITH p.name AS n WHERE n > $p0 RETURN n`.

### Mutations after `WITH`

Mutations are emitted after `with_clauses`, so they see the last `WITH`'s
projected names. A mutation that targets a match variable the last `WITH` did
not project is rejected at the mutation's target:

```text
{"spec_version": 1,
 "matches": [{"paths": [[["node", "p", ["Person"]]], [["node", "q", ["Person"]]]]}],
 "with_clauses": [{"projections": [["field", "p", "name", "n"]]}],
 "mutations": [["set", "q", "age", ["lit", 30]]],
 "projections": [["expr", ["ident", "n"], null]]}
```

The error is at `/mutations/0/1`: `variable 'q' is not in scope here: the last
WITH clause (with_clauses[0]) does not project 'q'; add it to that clause's
projections`. Add the bare variable `["expr", ["ident", "q"], null]` to the
`WITH` projections (or move the mutation before the `WITH` by removing the
`WITH` from the pipeline) to make the target visible.

---

## 6. DML Mutations

Mutation tuples within `mutations`:

- **CREATE**: `["create", paths_list]`
- **MERGE**: `["merge", path_steps, on_create_sets, on_match_sets]`
- **SET**: `["set", variable, property, value_expression]` (property assignment) or `["set", variable, map_expression]` (map merge: `SET variable += map`)
- **DELETE**: `["delete", detach_bool, [target_vars...]]`
- **REMOVE**: `["remove", variable, property]`

---

## 7. Target Dialect Parity

Any valid `spec` compiled with `compile_query_from_spec(spec, dialect="cypher" | "iso_gql" | "sql_pgq" | "apache_age")` produces the exact same parameter-isolated SQL/Cypher statement as the fluent `Query` builder API.

### Dialect names

The `dialect` argument is matched case-insensitively and accepts these aliases:

| Emitter | Accepted names |
| :--- | :--- |
| openCypher | `cypher`, `opencypher`, `neo4j`, `memgraph`, `falkordb`, `falkor` |
| ISO GQL | `iso_gql`, `gql` |
| SQL:PGQ | `sql_pgq`, `pgq`, `duckpgq`, `sql` |
| Apache AGE | `age`, `apache_age`, `postgres_age` |

### Per-dialect limits

Not every construct is available in every dialect. Unsupported constructs raise
`UNSUPPORTED_FEATURE` instead of being silently dropped or emitting a partial
statement (for example `UNWIND`, `WITH`, `LET`/`FILTER` and DML mutations in
SQL:PGQ):

- `load_csv` renders only in openCypher and Apache AGE; SQL:PGQ and ISO GQL
  raise `UNSUPPORTED_FEATURE` for it.
- SQL:PGQ raises `UNSUPPORTED_FEATURE` for `optional: true` on a match,
  `exists`/`count` subqueries, `list_comp`, `pattern_comp` and the `=~` regex
  operator. Root `distinct: true` emits `SELECT DISTINCT`.
- ISO GQL raises `UNSUPPORTED_FEATURE` for `MERGE` whose `ON CREATE SET` and
  `ON MATCH SET` assignments differ, for `["delete", true, [...]]`
  (`DETACH DELETE`), and for the `=~` regex operator.
- ISO GQL quantifies a single edge on the edge itself (`-[r]->{1,3}`), the same
  per-edge form used by openCypher multi-edge paths.

A compile-time failure in any dialect still raises `VoyagerError` with
`UNSUPPORTED_FEATURE`/`UNSUPPORTED_DIALECT` or `INVALID_SPEC`, never a bare
`ValueError`.
