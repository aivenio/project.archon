<div align = "center">

# StocksDB Review Report

</div>

<div align = "justify">

StocksDB sits in the middle of the topology: it subscribes geography and currency from MacroDB and, in turn, must
publish securities, exchange, and derivative masters to TradesDB. The review's single most important finding — and the
highest-impact defect in the entire system — is that **StocksDB defined no publications at all** (Publishers' Guild,
T4). TradesDB's own schema documents three subscriptions it expects from StocksDB (`stocksdb_securities_table`,
`stocksdb_exchange_table`, `stocksdb_derivative_table`), yet none existed on the publisher side. Without them, the
entire TradesDB subscription tier is inert: no securities, no symbols, no exchanges, no contracts ever arrive. The fix
authors all three publications — `stocksdb_securities_table` over `securities_mw` + `securities_exchange_symbol_mw`,
`stocksdb_exchange_table` over `stock_exchange_mw`, and `stocksdb_derivative_table` over `derivative_contract_mw` —
documents them in a new replication section, requires `wal_level = logical`, and confirms each published table keeps its
primary key as `REPLICA IDENTITY`. MacroDB-origin tables are deliberately not re-published (TradesDB takes currency
directly from MacroDB and needs no geography), avoiding a redundant relay.

The second confirmed defect is the **deliberately-preserved column typo** `scock_exchange_acronym` on
`stock_exchange_mw` (Lexicon, T5). Per the decision to fix it everywhere, the column is renamed to
`stock_exchange_acronym`. Because logical replication maps columns by name, this is not a one-sided edit: the file now
carries a lock-step migration procedure (disable the TradesDB subscription, rename both publisher and subscriber,
re-enable and refresh) so the corrected name cannot desynchronise the stream.

Corporate Actions Bureau (T7) addressed the largest data-capture gap: `corporate_actions_mw` previously held only a
type, a date, and a free-text description — unusable for the price back-adjustment an algorithmic system requires. Per
the decision to add structured values as a FEATURE, the table gains `ex_date`, `record_date`, `ratio_old`/`ratio_new`
(splits and bonus issues), `dividend_amount` with a nullable `dividend_currency_code` FK, and a precomputed
`price_adjustment_factor`, governed by three check constraints (`ck_ca_ratio_pair`, `ck_ca_dividend`,
`ck_ca_adj_factor`) and an `(isin, ex_date)` lookup index. Because this table is not part of any publication, the new
columns are local to StocksDB and need no subscriber coordination — a clean place to add structure.

Derivatives Desk (T6) validated the polymorphic contract master and the time-series facts. The `derivative_contract_mw`
design is sound: the underlying is XOR-constrained between an equity ISIN and an index id, options-vs-futures fields are
check-guarded, and the natural key uses `UNIQUE NULLS NOT DISTINCT`. The one structural improvement is that the four
`private.*_tx` fact tables expressed their grain only as a `UNIQUE` index; each is promoted to an explicit composite
**PRIMARY KEY**, which is semantically identical for the heap today but supplies a replica identity if ever published
and remains compatible with the deferred TimescaleDB migration (the time column is part of every key). T6 also added
`ck_exchange_operational_dates` to `stock_exchange_mw` and a `base_value`/`base_date` consistency check to
`market_index_mw`.

Cartographers (T1) carried the subscriber half of MacroDB's identifier-widening: `wikidata_id` and `geoname_id` on the
subscribed `country_mw`/`state_mw`/`city_mw` copies are widened to `varchar(12)` to match the corrected publisher shape
— mandatory for replication apply to succeed.

On data leakage, the audit confirmed subscriber tables receive `select`-only grants, URIs are bare, and the
price/option-chain facts have `INSERT/UPDATE/DELETE` revoked from `PUBLIC`. The TradesDB-bound publications expose only
the columns those masters legitimately need. Every change is recorded in the file's `versionchanged` block and tagged
inline as `BUGFIX` / `FEATURE` / `ENHANCEMENT`. The net effect is that StocksDB now actually fulfils its publisher role
(previously broken), corrects the long-standing column typo under a safe rename protocol, and captures corporate actions
in a form the trading engine can compute against — while leaving its already-solid derivative and fact-table modelling
essentially intact apart from the PK promotion.

## Team Summary

<div align = "center">

| Team Name | Team Objective | Team Findings |
| :---: | --- | --- |
| Publishers' Guild (T4) | Replication topology & publication completeness | **StocksDB published nothing**, yet TradesDB subscribes to three of its tables → author `stocksdb_securities_table` / `stocksdb_exchange_table` / `stocksdb_derivative_table`; `wal_level=logical`; PKs as replica identity. **Confidence: VERY HIGH.** |
| Lexicon (T5) | Naming defect & coordinated rename | `scock_exchange_acronym` typo replicated into TradesDB → rename to `stock_exchange_acronym` with a lock-step migration (column mapping is by name). **Confidence: HIGH.** |
| Derivatives Desk (T6) | Polymorphic contract & fact-table integrity | Contract XOR/option checks sound; `*_tx` grains were UNIQUE-only → promote to composite PRIMARY KEY (Timescale-safe); add exchange/index date & base checks. **Confidence: MEDIUM.** |
| Corporate Actions Bureau (T7) | Corporate-action data completeness | Only type/date/free-text captured → add structured `ex_date`, `record_date`, split ratio, dividend (+currency FK), `price_adjustment_factor` + checks. Table is unpublished → no subscriber impact. **Confidence: HIGH (FEATURE).** |
| Cartographers (T1) | Subscriber identifier sizing | Subscribed `wikidata_id`/`geoname_id varchar(8)` must widen to `varchar(12)` to match the corrected MacroDB publisher. **Confidence: HIGH.** |

</div>

### Team Name: Publishers' Guild (T4)

**Finding.** The publisher half of the StocksDB→TradesDB link did not exist. **Pros:** authoring the three publications
is the difference between a working and a non-working downstream database; it is mechanically simple and fully
reversible. **Cons:** enabling `wal_level=logical` requires a server restart in some deployments and increases WAL
volume; replica-identity discipline must be maintained on every published table going forward. **Confidence: VERY
HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T4-GREEN (PLANNING)
Lens: contract-first topology reconciliation.
Heuristic: a subscriber's declared subscriptions are a binding contract; the
  publisher must satisfy every one of them.
Action: enumerated TradesDB's expected sources, diffed against StocksDB's
  (empty) publication set, and grouped the required tables into three publications
  matching the subscriber's expectations.
Output: stocksdb_securities_table (+ses), stocksdb_exchange_table,
  stocksdb_derivative_table; currency/geography NOT re-published.
```

```text
Agent: T4-BLUE (REVIEWER)
Lens: replica identity & column-set compatibility.
Heuristic: a publication fails at apply time unless the subscriber's columns are a
  compatible superset and the publisher has a usable replica identity.
Action: confirmed all four published tables have PKs; verified TradesDB's
  subscriber copies match column shapes (incl. the renamed acronym and the widened
  identifiers handled by T5/T1); checked indexes are not replicated.
Output: approved with wal_level=logical and PK-as-replica-identity noted.
```

```text
Agent: T4-RED (TESTING)
Lens: cold-start subscription simulation.
Heuristic: stand up the subscriber from empty and see what actually flows.
Action: modelled initial table copy + streaming for each publication; probed the
  securities↔ses ordering.
Result: pre-fix nothing replicates (no publication exists); post-fix all four
  tables populate. Confirmed dropping cross-table FKs on the subscriber avoids
  apply-order failure.
```

### Team Name: Lexicon (T5)

**Finding.** A misspelled column replicated verbatim into a second database. **Pros:** correcting it removes a permanent
naming wart and the documented protocol makes the rename safe across the replication boundary. **Cons:** it is a
coordinated, multi-database DDL operation with a brief subscription pause; downstream application code referencing the
old name must be updated in the same window. **Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T5-GREEN (PLANNING)
Lens: replication-aware refactoring.
Heuristic: a column rename on a published table is a distributed operation, not a
  local edit; design the ordering before touching either side.
Action: authored the disable→rename-both→enable→refresh procedure; verified
  logical replication's by-name column mapping makes lock-step renaming mandatory.
Output: corrected column + Migration Notes section.
```

```text
Agent: T5-BLUE (REVIEWER)
Lens: blast-radius containment.
Heuristic: every consumer of the old identifier is a breakage point.
Action: catalogued references to the column on both publisher and subscriber;
  confirmed the rename is the only schema change (type/width unchanged).
Output: approved; flagged application-code update inside the migration window.
```

```text
Agent: T5-RED (TESTING)
Lens: desync simulation.
Heuristic: rename one side only and observe the failure.
Action: modelled renaming the publisher without the subscriber.
Result: the stream errors on the missing target column — validating the
  lock-step requirement; the documented procedure prevents it.
```

### Team Name: Derivatives Desk (T6)

**Finding.** Derivative modelling is robust; the fact tables under-specify their grain as a non-PK unique index.
**Pros:** PK promotion is semantically free today and future-proofs replication and partitioning; the added date/base
checks are cheap correctness wins. **Cons:** if a future TimescaleDB hypertable is created, the PK must keep the time
column (it does); no other downside. **Confidence: MEDIUM.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T6-GREEN (PLANNING)
Lens: polymorphic / EAV modelling + check-constraint algebra.
Heuristic: a single table holding two instrument shapes (option/future, equity/index
  underlying) must encode the XOR and the conditional-NOT-NULL rules as checks.
Action: validated ck_dc_underlying_xor, ck_dc_option_fields, ck_dc_inst_underlying;
  found the fact grains expressed only as UNIQUE.
Output: promote each _tx grain to a composite PRIMARY KEY; add exchange/index checks.
```

```text
Agent: T6-BLUE (REVIEWER)
Lens: time-series storage strategy.
Heuristic: a fact table's identity must survive a future hypertable migration.
Action: confirmed every promoted PK includes effective_time/snapshot_time, keeping
  create_hypertable() viable; confirmed PKs are local (tables unpublished).
Output: approved PK promotion.
```

```text
Agent: T6-RED (TESTING)
Lens: contract-shape fuzzing.
Heuristic: attempt the impossible contract and the duplicate bar.
Action: tried a future with an option_type, an option with no strike, a contract
  with both an ISIN and an index underlying, and a duplicate (entity,timeframe,time)
  bar.
Result: malformed contracts rejected by existing checks; duplicate bars rejected by
  the new PK exactly as by the prior unique index.
```

### Team Name: Corporate Actions Bureau (T7)

**Finding.** Corporate actions were captured only as free text, blocking automated price adjustment. **Pros:** the
structured columns make splits, bonuses, and dividends machine-computable and the precomputed factor lets the pipeline
persist one canonical adjustment; the table is unpublished so there is zero subscriber coordination. **Cons:** it is a
genuinely new feature (correctly gated on explicit approval) and adds population responsibility to the ingestion job.
**Confidence: HIGH (FEATURE).**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T7-GREEN (PLANNING)
Lens: downstream-consumer data sufficiency (price back-adjustment).
Heuristic: a reference table is complete only if every downstream computation it
  feeds can be performed without re-parsing free text.
Action: derived the fields a split/bonus/dividend adjustment needs (ex/record dates,
  ratio, cash amount + currency, adjustment factor); designed all-or-nothing checks.
Output: ex_date, record_date, ratio_old/new, dividend_amount, dividend_currency_code
  (FK), price_adjustment_factor + ck_ca_ratio_pair/ck_ca_dividend/ck_ca_adj_factor.
```

```text
Agent: T7-BLUE (REVIEWER)
Lens: scope discipline & publication impact.
Heuristic: new features are added only where they cost least and leak least.
Action: confirmed corporate_actions_mw is in no publication, so the columns are
  StocksDB-local; confirmed the dividend currency FK targets the subscribed
  currency_mw without affecting replication.
Output: approved as a bounded FEATURE.
```

```text
Agent: T7-RED (TESTING)
Lens: partial-data probing.
Heuristic: supply half a corporate action and ensure it is rejected.
Action: tried a split with only ratio_old, a dividend with no currency, a negative
  adjustment factor.
Result: each partial/invalid combination is rejected by the new checks; a complete
  1-for-2 split and a cash dividend with currency are accepted.
```

### Team Name: Cartographers (T1)

**Finding.** The subscribed geography copies must match the widened publisher shape. **Pros:** a mechanical, mandatory
alignment that unblocks apply. **Cons:** none beyond being part of the coordinated MacroDB migration. **Confidence:
HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T1-GREEN (PLANNING)
Lens: subscriber-publisher shape parity.
Heuristic: a subscriber column must be at least as wide as the publisher's.
Action: matched the widened wikidata_id/geoname_id (varchar(12)) onto the
  subscribed country_mw/state_mw/city_mw copies.
Output: widen the three subscribed tables' identifiers to varchar(12).
```

```text
Agent: T1-BLUE (REVIEWER)
Lens: apply-time type compatibility.
Heuristic: width mismatches surface as apply errors, not insert errors.
Action: confirmed the subscriber widths now equal the publisher; confirmed no other
  geography column changed shape.
Output: approved.
```

```text
Agent: T1-RED (TESTING)
Lens: replicated-value overflow probing.
Heuristic: push a max-width publisher value through to the subscriber.
Action: modelled replicating a 10-char Q-ID into the (old) varchar(8) subscriber.
Result: pre-fix apply would fail/truncate; post-fix the value lands intact.
```

## Super Team

Under the maximum-hardening mandate the Super Team consolidated all five teams' validated work, anchored by the
publications fix (T4) — the change that makes the downstream database function at all. The acronym rename (T5) and the
corporate-action FEATURE (T7) are the next-most-consequential, followed by the structural PK promotion (T6) and the
mandatory subscriber identifier alignment (T1). The result restores StocksDB's publisher role, removes the naming defect
under a safe protocol, and closes the corporate-action capture gap — with no change to the security/leakage posture
(subscriber grants remain select-only, facts remain write-revoked from `PUBLIC`).

### Winning Teams

The global marquee trio is **Publishers' Guild (T4, VERY HIGH)** — the missing-publications discovery, which is
StocksDB's and the whole system's top finding; **Mint Authority (T2, HIGH)** — the MacroDB currency integrity fix; and
**Corporate Actions Bureau (T7, HIGH)** — the StocksDB structured corporate-action FEATURE. Two of the three (T4, T7)
act directly on this file. Lexicon (T5) and Derivatives Desk (T6) are honourable mentions fully incorporated here.

### Super Team Aggregated Result

Concrete changes applied to `stocksdb.dbml` (see the file's `versionchanged` block):

  1. **[VERY HIGH · BUGFIX]** Added publications `stocksdb_securities_table` (securities_mw +
     securities_exchange_symbol_mw), `stocksdb_exchange_table` (stock_exchange_mw), `stocksdb_derivative_table`
     (derivative_contract_mw); documented `wal_level=logical` and PK-as-replica-identity.
  2. **[HIGH · BUGFIX]** Renamed `scock_exchange_acronym → stock_exchange_acronym` on `stock_exchange_mw`, with a
     lock-step Migration Notes procedure for the TradesDB subscriber.
  3. **[HIGH · BUGFIX]** Subscriber `wikidata_id`/`geoname_id` widened `varchar(8) → varchar(12)` on country/state/city
     (matches corrected MacroDB publisher).
  4. **[HIGH · FEATURE]** `corporate_actions_mw` extended with `ex_date`, `record_date`, `ratio_old`/`ratio_new`,
     `dividend_amount`, `dividend_currency_code` (FK), `price_adjustment_factor`, three check constraints, and an
     `(isin, ex_date)` index.
  5. **[MEDIUM · ENHANCEMENT]** Promoted each `private.*_tx` grain to a composite PRIMARY KEY; added
     `ck_exchange_operational_dates` and `ck_market_index_base`; documented the no-hard-DELETE contract on
     `securities_mw`.

</div>
