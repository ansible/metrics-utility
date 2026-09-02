# Event data Unicode escaping

## Conclusion

The `replace(event_data, '\u', '\u005cu')` expression was added to handle real
AWX analytics failures. It is not merely a metrics-utility artifact and is not
proven obsolete.

The workaround is specific to JSON stored as text and subsequently processed as
PostgreSQL `jsonb`. It should not be applied to every collector or hidden inside
the generic JSON validation function. However, the host collectors have a
related mismatch because they validate with `json` and subsequently cast to the
stricter `jsonb` type.

## Protected scenario

AWX stores `main_jobevent.event_data` as `text`. This is a deliberate legacy
choice intended to avoid converting millions of event rows during an upgrade;
see `JSONBlob` in `awx/main/fields.py` in the AWX repository.

Python can serialize values such as NUL and malformed UTF-16 surrogates into
syntactically valid JSON text:

```json
{"value": "\u0000"}
{"value": "\ud800"}
```

PostgreSQL accepts these representations as `json`, whose input validation only
checks that `\u` is followed by four hexadecimal digits. PostgreSQL `jsonb` is
stricter and rejects:

- `\u0000`, because NUL cannot be represented by PostgreSQL `text`
- lone or incorrectly paired UTF-16 surrogates
- escaped characters unavailable in the database encoding

The replacement used by AWX and metrics-utility changes every Unicode escape
into literal text before casting the document to `jsonb`:

```sql
replace(event_data, '\u', '\u005cu')::jsonb
```

For example, `"\u0000"` becomes a JSON string whose resulting value is the six
literal characters `\u0000`, rather than a NUL character that PostgreSQL cannot
materialize.

PostgreSQL documents the difference between `json` and `jsonb` Unicode handling
in [JSON Types](https://www.postgresql.org/docs/15/datatype-json.html).

## History

### Initial NUL failure

AWX PR [#9279](https://github.com/ansible/awx/pull/9279), opened in February
2021, describes job output containing NUL being represented in event JSON as
`\u0000`. Most AWX code loaded the text into Python, but analytics asked
PostgreSQL to process it as JSON and failed. Discussion measured approximately
15 percent overhead from applying `replace()` to 200,000 event rows.

AWX PR [#9821](https://github.com/ansible/awx/pull/9821), commit `f694cd14`,
landed in April 2021. It first attempted the ordinary cast and, on
`UntranslatableCharacter`, repeated the collection after removing `\u0000`.

### Move to JSONB

AWX commit `e3893b18` from August 2021 changed analytics processing from `json`
to `jsonb`. This was required for the JSONB `- 'artifact_data'` operator. The
fallback continued to remove `\u0000` before making the stricter cast.

### General Unicode neutralization

AWX PR [#12252](https://github.com/ansible/awx/pull/12252), commit `973faceb`,
landed in May 2022. It replaced the retry with unconditional neutralization of
every `\u` escape. Its stated reasons were:

- avoiding a complete retry when a null escape is encountered
- preventing other bad Unicode sequences from producing a confusing traceback

The resulting expression remains in AWX's event analytics collector at
`awx/main/analytics/collectors.py`.

Metrics-utility copied the same expression when its first `main_jobevent`
collector was introduced in commit `9272f1d` in May 2024. The original
metrics-utility PR [#17](https://github.com/ansible/metrics-utility/pull/17) did
not discuss the expression. The service event collector later copied it.

Current metrics-utility occurrences are:

- `metrics_utility/library/collectors/controller/main_jobevent.py`
- `metrics_utility/library/collectors/controller/main_jobevent_service.py`

## Relationship to `ensure_functions`

The event collectors do not call `ensure_functions()` and do not use either of
the functions it installs. The replacement and the functions proposed in AWX PR
[#16458](https://github.com/ansible/awx/pull/16458) are separate concerns.

`ensure_functions()` installs:

- `metrics_utility_is_valid_json(text)`
- `metrics_utility_parse_yaml_field(text, text)`

They are used only to read `main_host.variables` in these collectors:

- `job_host_summary`
- `main_host`
- `main_host_daily`
- `main_hostmetric`

The event-related comment on AWX PR #16458 prompted this investigation, but the
event data is not made readable by those functions.

## Side effects

The replacement is deliberately broad and does not preserve normal JSON
semantics. It affects valid escapes as well as invalid ones:

| Input JSON value | Value after replacement and JSONB parsing |
| --- | --- |
| `"\u0000"` | literal `\u0000` |
| `"\ud800"` | literal `\ud800` |
| `"\u00e9"` | literal `\u00e9`, not `é` |
| `"\\u0041"` | altered literal escape text |

Consequently, this should not be treated as generic JSON normalization or
incorporated into `metrics_utility_is_valid_json()`.

## Host collector mismatch

The shared validation path has a separate correctness issue. The helper checks
whether host variables can be cast to `json`, but callers then cast the same
value to `jsonb`:

```sql
metrics_utility_is_valid_json(main_host.variables)
main_host.variables::jsonb->>'ansible_host'
```

A value containing `\u0000` or a lone surrogate can therefore pass the first
expression and fail the second, aborting the complete collector query.

This case is reachable. AWX stores `main_host.variables` as unrestricted
JSON-or-YAML `text`, and API and inventory-import paths can serialize NUL or lone
surrogate values into escaped JSON. It is probably less common than arbitrary
event output, but it has the same underlying PostgreSQL limitation.

Changing the extraction from `::jsonb` to `::json` is not sufficient.
PostgreSQL JSON extraction operators materialize Unicode escapes and can raise
the same errors even when their input type is `json`.

## Assessment

- Event collectors should retain defensive handling for historical and current
  event data unless AWX prevents incompatible escapes at write time and old rows
  are no longer supported.
- The current broad replacement should remain event-scoped because it changes
  valid Unicode and literal `\u` strings.
- The replacement should not be incorporated into the generic validation
  function.
- Host-variable validation should match the actual JSONB operation, or use a
  safe extraction function that catches JSONB conversion failures without
  rewriting valid Unicode.
- PostgreSQL-backed tests should cover valid Unicode, `\u0000`, lone surrogates,
  valid surrogate pairs, and literal or double-escaped `\u`. Current
  metrics-utility event tests only assert that the generated SQL contains the
  replacement.
