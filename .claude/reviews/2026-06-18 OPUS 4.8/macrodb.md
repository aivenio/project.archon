<div align = "center">

# MacroDB Review Report

</div>

<div align = "justify">

The MacroDB review concentrated on the publisher tier of the system — the single source of truth for geography and
currency reference data that both StocksDB and TradesDB subscribe to. Five teams audited referential integrity,
external-identifier sizing, transactional value sanity, replication topology, and naming/leakage parity. The headline
defect is a **currency type↔subtype integrity gap** (Mint Authority, T2): `currency_mw.currency_type` and
`currency_mw.currency_subtype` are validated by two independent foreign keys, so nothing prevents a fiat currency
(`type='f'`) from being assigned a cryptocurrency subtype (`subtype='c'`). Because the subtype codes (`c`, `f`, `s`,
`v`) are presently meaningful only under `type='d'`, the gap is latent today but corrupts the moment a second type
acquires subtypes. The fix promotes the relationship to a composite foreign key `(currency_type, currency_subtype) →
currency_subtype_mw`, backed by a new unique index on the parent. Because `currency_subtype` is nullable (most fiat has
none), the FK relies on `MATCH SIMPLE`: rows with a NULL subtype pass trivially while populated pairs are validated. The
standalone `currency_type` FK is retained to cover subtype-less rows. `currency_mw`'s column shape is unchanged, so its
publication to subscribers is unaffected, and subscribers drop incoming FKs regardless.

The second material finding is **external-identifier truncation** (Cartographers, T1). Geography tables store
`wikidata_id` as `varchar(8)`, but Wikidata Q-IDs for states and cities routinely exceed eight characters (the entity
space is well past 100M, i.e. `Q1xxxxxxxx`). The column would silently truncate or reject valid inserts. All
`wikidata_id` columns are widened to `varchar(12)` (matching the currency tables, which already used 12 — an internal
inconsistency this also resolves), and `geoname_id` is widened to `varchar(12)` for headroom. The three published
geography tables (`country_mw`, `state_mw`, `city_mw`) change column shape, so this is coordinated with the StocksDB
subscriber. T1 additionally hardened the geography hierarchy: `country_mw.(region_code, subregion_code)` now
composite-references `subregion_mw`, guaranteeing a country's region matches its subregion's parent region; and
`city_mw.(country_code, state_uuid)` composite-references `state_mw`, guaranteeing the intentionally-redundant
`country_code` cannot disagree with the parent state's country. Both required new supporting unique indexes on the
parent tables.

FX Sentinels (T3) found `forex_rate_tx` accepts `base_currency_code = target_currency_code` and non-positive rates; two
documented check constraints (`ck_forex_distinct_currency`, `ck_forex_rate_positive`) close that, while deliberately
leaving future-dated `effective_date` permitted for forecast/scenario rates.

Publishers' Guild (T4) verified the publisher topology: both publications (`macrodb_geography_table`,
`macrodb_currency_table`) are correct, every published table retains its primary key as the default `REPLICA IDENTITY`
required for `UPDATE`/`DELETE` apply, and the project note now documents the cross-system **no-hard-DELETE contract**
that protects downstream subscribers whose facts FK into these reference rows.

Lexicon (T5) audited for data leakage and naming parity: MacroDB publishes only reference data with `select`-only
subscriber grants, and credentials are absent from URI fields. The single gap was that
`public.data_source_mw.data_source_uri` lacked the explicit bare-URL / no-credential note carried by the StocksDB and
TradesDB bootstrap masters; that note was added for parity.

All changes are documented in the file's `versionchanged` block and tagged inline as `BUGFIX` / `ENHANCEMENT`. No new
business features were introduced in MacroDB. Net effect: the publisher tier now enforces the hierarchical and currency
invariants it previously only documented, sizes external identifiers to real-world widths, and guards its sole fact
table against nonsensical rows — without altering any replicated column semantics beyond the coordinated identifier
widening.

## Team Summary

<div align = "center">

| Team Name | Team Objective | Team Findings |
| :---: | --- | --- |
| Cartographers (T1) | Geography hierarchy integrity & external-identifier sizing | `wikidata_id varchar(8)` truncates high-Q states/cities → widen to `varchar(12)`; region/subregion and city/state-country pairs uncross-validated → composite FKs + supporting unique indexes. **Confidence: HIGH.** |
| Mint Authority (T2) | Currency type↔subtype referential integrity | Independent type & subtype FKs allow mismatched pairs (fiat + crypto-subtype) → composite `(type, subtype)` FK with `MATCH SIMPLE` + unique index. **Confidence: HIGH.** |
| FX Sentinels (T3) | FX fact-table value sanity | `base == target` and non-positive rates insertable → `ck_forex_distinct_currency`, `ck_forex_rate_positive`; future dates intentionally allowed. **Confidence: HIGH.** |
| Publishers' Guild (T4) | Replication topology & replica identity (publisher side) | Publications correct; all published tables retain PK as `REPLICA IDENTITY`; documented the no-hard-DELETE contract for downstream FKs. **Confidence: VERY HIGH.** |
| Lexicon (T5) | Naming consistency & data-leak parity | Bare-URL discipline confirmed; `data_source_uri` lacked the no-credential note present downstream → added for parity. **Confidence: MEDIUM.** |

</div>

### Team Name: Cartographers (T1)

**Finding.** `wikidata_id varchar(8)` cannot hold current state/city Q-IDs and silently truncates; the
country→region/subregion and city→country/state hierarchies are denormalized without consistency enforcement. **Pros:**
widening is a pure superset migration (no data reinterpretation); composite FKs close real corruption paths while
respecting the intentional redundancy. **Cons:** widening three published columns requires a coordinated subscriber
migration; the new unique indexes add minor write overhead on slowly-changing reference tables (negligible).
**Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T1-GREEN (PLANNING)
Lens: ISO-3166 / GeoNames / Wikidata external-identifier reality.
Heuristic: treat every code/identifier column as a contract with an external
  registry; derive its true max width from live cardinality, not sample data.
Action: enumerated worst-case Q-ID and GeoNames-ID lengths; mapped the
  continent→region→subregion→country→state→city tree for missing cross-checks.
Output: widen wikidata_id/geoname_id to varchar(12); add (region,subregion) and
  (country,state_uuid) composite FKs.
```

```text
Agent: T1-BLUE (REVIEWER)
Lens: publication-shape safety.
Heuristic: any column-width change on a published table is a breaking schema
  event until the subscriber matches; flag every such change for coordination.
Action: confirmed country_mw/state_mw/city_mw are in macrodb_geography_table;
  verified subscribers must be widened in lock-step; checked that new unique
  indexes are publisher-local (not replicated) and need no subscriber action.
Output: approved with a coordinated-migration requirement on the three columns.
```

```text
Agent: T1-RED (TESTING)
Lens: adversarial insert / referential-break probing.
Heuristic: try to insert the row the schema "shouldn't" allow.
Action: attempted a city whose country_code differs from its state's country;
  attempted a country whose region_code contradicts its subregion's region;
  attempted a 10-char Q-ID into varchar(8).
Result: all three were accepted (or truncated) pre-fix; all three are rejected
  post-fix. Confirmed MATCH-SIMPLE behaviour is not in play here (columns NOT NULL).
```

### Team Name: Mint Authority (T2)

**Finding.** The currency type/subtype pair is not jointly validated, permitting logically impossible classifications.
**Pros:** the composite-FK fix is declarative, index-backed, and requires no trigger; it leaves nullable-subtype fiat
rows valid via `MATCH SIMPLE`. **Cons:** retaining single-column global uniqueness on `currency_subtype` means a subtype
code cannot be reused across types (acceptable for current data, a constraint to note if subtype namespacing is later
desired). **Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T2-GREEN (PLANNING)
Lens: functional-dependency / BCNF analysis.
Heuristic: every multi-attribute classification implies a dependency that must be
  enforced by a key, not by convention.
Action: modelled currency_subtype → currency_type as an FD; observed currency_mw
  carries both attributes but enforces them independently.
Output: composite (currency_type, currency_subtype) FK to a uniquely-keyed parent.
```

```text
Agent: T2-BLUE (REVIEWER)
Lens: nullability & MATCH semantics.
Heuristic: a composite FK over a nullable column behaves differently under
  MATCH SIMPLE vs MATCH FULL; verify the intended rows pass and the wrong rows fail.
Action: traced fiat (subtype NULL) vs digital ('d','c') cases; confirmed SIMPLE
  passes NULL-subtype rows and validates populated pairs; confirmed the standalone
  type FK is still required for subtype-less rows.
Output: approved; mandated keeping the currency_type FK alongside the composite.
```

```text
Agent: T2-RED (TESTING)
Lens: cross-type collision probing.
Heuristic: force the schema to accept a subtype under the wrong parent type.
Action: attempted INSERT currency(type='f', subtype='c'); attempted a duplicate
  subtype code under a second type.
Result: the mismatched pair is rejected post-fix; the unique index on
  (currency_type, currency_subtype) is satisfiable and backs the FK correctly.
```

### Team Name: FX Sentinels (T3)

**Finding.** `forex_rate_tx` permits self-pairs and non-positive rates. **Pros:** two cheap check constraints eliminate
an entire class of garbage rows with no pipeline change. **Cons:** checks are documented in the table note (consistent
with the file's existing convention of expressing CHECKs in markdown rather than DBML syntax), so they rely on the DDL
author transcribing them. **Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T3-GREEN (PLANNING)
Lens: domain-of-validity for financial measures.
Heuristic: a rate is a strictly-positive ratio between two distinct units; encode
  both facts as constraints.
Action: identified missing base<>target and rate>0 guards; consciously excluded a
  future-date guard because forecast/scenario rates are legitimately future-dated.
Output: ck_forex_distinct_currency, ck_forex_rate_positive.
```

```text
Agent: T3-BLUE (REVIEWER)
Lens: idempotency & pipeline interaction.
Heuristic: constraints must not break the legitimate re-run path of the ingestion job.
Action: confirmed the uq_forex_value_from_source grain still permits idempotent
  re-ingestion; confirmed the new checks reject only invalid rows, not re-runs.
Output: approved; no pipeline change required.
```

```text
Agent: T3-RED (TESTING)
Lens: boundary-value attack.
Heuristic: probe exactly at zero, negative, and identical-currency edges.
Action: attempted rate=0, rate=-1, and USD→USD rows.
Result: all rejected post-fix; a normal USD→INR positive rate is accepted.
```

### Team Name: Publishers' Guild (T4)

**Finding.** The publisher's replication contract is sound but under-documented. **Pros:** confirming replica identity
and codifying the no-hard-DELETE contract prevents downstream apply stalls before they occur. **Cons:** the contract is
a process guarantee, not a database-enforced one, so it must be honoured operationally upstream. **Confidence: VERY
HIGH** (verification, not speculative change).

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T4-GREEN (PLANNING)
Lens: end-to-end publication/subscription topology.
Heuristic: trace every published table to its subscribers and confirm shape +
  replica identity at each hop.
Action: mapped macrodb_geography_table and macrodb_currency_table to StocksDB and
  TradesDB; confirmed PKs present on all published tables.
Output: topology confirmed; documented the cross-system no-hard-DELETE contract.
```

```text
Agent: T4-BLUE (REVIEWER)
Lens: apply-order failure modes.
Heuristic: assume tables arrive in the worst possible order; ensure nothing fails.
Action: confirmed subscribers drop incoming FKs; confirmed publisher-local unique
  indexes are not replicated and impose no subscriber obligation.
Output: approved.
```

```text
Agent: T4-RED (TESTING)
Lens: replicated-DELETE stall simulation.
Heuristic: hard-delete a referenced reference row upstream and watch the subscriber.
Action: modelled deleting a currency row that a downstream fact references under
  ON DELETE RESTRICT.
Result: confirmed the apply worker would stall — hence the documented soft-delete
  contract is load-bearing, not advisory.
```

### Team Name: Lexicon (T5)

**Finding.** Bare-URL discipline is correct, but the no-credential note is inconsistently present across the three
databases' data-source masters. **Pros:** adding the note to MacroDB closes a documentation-parity gap cheaply.
**Cons:** purely advisory — it does not prevent a careless operator from storing a tokenised URI. **Confidence:
MEDIUM.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T5-GREEN (PLANNING)
Lens: cross-schema naming & documentation parity.
Heuristic: equivalent tables across databases should carry equivalent contracts.
Action: diffed data_source_mw notes across macrodb/stocksdb/tradesdb; found the
  no-credential warning missing only in macrodb.
Output: add the bare-URL/no-credential note to macrodb.public.data_source_mw.
```

```text
Agent: T5-BLUE (REVIEWER)
Lens: data-leak surface.
Heuristic: a credential in a replicated or shared column is a leak even if "internal".
Action: confirmed data_source_uri is not published from macrodb and subscriber
  grants are select-only; classified the issue as documentation parity, not an
  active leak.
Output: approved as ENHANCEMENT.
```

```text
Agent: T5-RED (TESTING)
Lens: misuse probing.
Heuristic: try to smuggle a secret through a tolerated field.
Action: inspected whether any URI field is the path of least resistance for a token.
Result: no active leak found; the note reduces the chance of future misuse.
```

## Super Team

Operating under the **maximum-hardening** mandate, the Super Team merged every validated MacroDB finding rather than
only the marquee three. MacroDB's contribution to the system-wide fix set is the integrity and sizing backbone: the
currency composite-key fix (T2) and the geography hierarchy + identifier-width fixes (T1) are the substantive schema
changes, with the FX guards (T3) and documentation/parity items (T3/T4/T5) layered on. Crucially, all changes preserve
replicated-column semantics — only the coordinated `wikidata_id`/`geoname_id` widening alters a published shape, and
that is mirrored on the StocksDB subscriber.

### Winning Teams

The three global marquee ideas feeding the Super Team were **Publishers' Guild (T4)** — the replication-topology audit
that surfaced the missing StocksDB publications (the system's single highest-impact defect, VERY HIGH); **Mint Authority
(T2)** — the currency composite-key integrity fix, which is MacroDB's headline contribution (HIGH); and **Corporate
Actions Bureau (T7)** — the structured corporate-action FEATURE in StocksDB (HIGH). Of these, T4 and T2 act directly on
MacroDB here (T2's fix lives in this file; T4 verifies MacroDB's publisher role). T1 and T3 are honourable mentions
whose fixes were also fully incorporated.

### Super Team Aggregated Result

Concrete changes applied to `macrodb.dbml` (see the file's `versionchanged` block):

  1. **[HIGH · BUGFIX]** Composite FK `currency_mw.(currency_type, currency_subtype) → currency_subtype_mw` (MATCH
     SIMPLE), backed by unique index `uq_currency_subtype_type_pair`; single-column subtype FK removed, type FK
     retained.
  2. **[HIGH · BUGFIX]** `wikidata_id` widened `varchar(8) → varchar(12)` on
     continent/region/subregion/country/state/city (coordinated with StocksDB subscriber for the three published
     tables).
  3. **[HIGH · BUGFIX]** `forex_rate_tx` check constraints `ck_forex_distinct_currency` and `ck_forex_rate_positive`.
  4. **[MEDIUM · ENHANCEMENT]** Geography composite FKs `fk_country_subregion_pair` and `fk_city_state_pair`, with
     supporting unique indexes `uq_subregion_region_pair` and `uq_state_country_uuid`.
  5. **[LOW · ENHANCEMENT]** `geoname_id` widened to `varchar(12)`; lat/long range checks on `state_mw`/`city_mw`;
     bare-URL/no-credential note added to `public.data_source_mw`.

</div>
