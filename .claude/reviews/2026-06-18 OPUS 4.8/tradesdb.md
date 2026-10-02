<div align = "center">

# TradesDB Review Report

</div>

<div align = "justify">

TradesDB is the system's terminal subscriber and the only database that writes trading activity. Five teams examined its
replication safety, schema layering, and risk/audit integrity. The most operationally dangerous finding is the
**replication apply-stall risk** (Apply-Guard, T8): TradesDB's local fact tables foreign-key *into* the subscription
tables (e.g. `signal_tx.ses_primary_id → securities_exchange_symbol_mw`) with `ON DELETE RESTRICT`. If an upstream
system hard-deletes a referenced security or contract and that delete replicates down, the `apply` worker is blocked by
the `RESTRICT` constraint and the subscription stalls — silently halting all downstream reference updates. Critically,
`RESTRICT` is the *correct* behaviour for an audit-grade trading store (a security with live trade history must not
vanish), so the resolution is not to weaken the constraint but to codify the **publisher no-hard-DELETE contract**:
upstream must retire reference rows logically (via `stock_exchange_mw.operational_to` or a `DELISTING` corporate
action), never with a physical delete. This contract is now documented in the file, and the same review reworded the
previously self-contradictory note that simultaneously claimed subscription tables "carry no incoming foreign keys" yet
were "referenced by trading facts." The corrected note draws the real distinction: subscription tables carry no FKs
*among themselves* (those are dropped on the subscriber to avoid apply-order failures), while *local* facts legitimately
reference them via ordinary FKs.

Stratum (T9) found a **schema-layering inversion**: the enum `instrument_kind` was defined in the `private` schema, yet
`common.strategy_mw` and `common.strategy_universe_mw` — and the fact tables — depend on it. A `common` object depending
on a `private` type is a layering violation that breaks the schema's own encapsulation boundary. The enum is relocated
to `common.instrument_kind`. Because enum-typed columns are represented as `varchar` in the DBML, this is a
documentation-and-reference change in the diagram (the header enum table and every `// enum:` comment), but it encodes
the correct dependency direction for the real DDL. T9 also confirmed `timeframe_value` is already correctly in `common`
here (it sits in `private` only in StocksDB, which is an independent database, so no cross-DB issue arises).

Sentinel (T10) audited risk, audit, accounting, and deployment metadata. The **version/cluster contradiction** is the
notable defect: the original schema claimed TradesDB runs PostgreSQL v17.10 "on the same cluster" as the v18.1 MacroDB
and StocksDB — impossible, since one cluster runs exactly one major version. It is re-documented as a dedicated v18.1
Aiven service, which also resolves the replication-direction concern (publisher v18 → subscriber v17 is the
less-supported newer→older path; aligning to v18.1 keeps it publisher ≤ subscriber). This is flagged for operations to
confirm the genuinely deployed version. On accounting integrity, `pnl_daily_tx` had no constraint tying `net_pnl` to
`gross_pnl` and `total_charges`; the identity `ck_pnl_net` (`net_pnl = gross_pnl − total_charges`) plus non-negativity
guards on charges and trade count are added. Sentinel confirmed the audit log is correctly immutable (UPDATE/DELETE
revoked plus a trigger) and deliberately FK-free so a logged reference can never be blocked or cascaded.

Publishers' Guild (T4) and Lexicon (T5) handled the subscriber side of cross-database fixes: T4 confirmed TradesDB's
four subscriptions now have real publications upstream (the StocksDB fix) and that its subscriber column shapes match
the publishers; T5 applied the lock-step `scock_exchange_acronym → stock_exchange_acronym` rename on TradesDB's
subscribed `stock_exchange_mw` copy.

On data leakage, the audit found TradesDB is a private operational store with no onward publications; subscription
tables are write-revoked from `PUBLIC`, the audit log is tamper-evident, and `jsonb` payloads (strategy parameters,
model outputs) live in a private schema. The structural reconstruction note in the file header records that the local
trading tables were rebuilt from the design model and should be diffed against the canonical source. All changes are
captured in the `versionchanged` block and tagged `BUGFIX` / `ENHANCEMENT`. Net effect: TradesDB's replication is now
stall-resistant by contract, its schema layering is corrected, its deployment metadata is internally consistent, and its
P&L respects its own accounting identity — with the protective `RESTRICT` semantics and immutable audit trail preserved.

## Team Summary

<div align = "center">

| Team Name | Team Objective | Team Findings |
| :---: | --- | --- |
| Publishers' Guild (T4) | Subscriber-side topology verification | Confirmed the four subscriptions now resolve to real StocksDB/MacroDB publications; subscriber column shapes match publishers (incl. renamed acronym, widened identifiers). **Confidence: VERY HIGH.** |
| Lexicon (T5) | Coordinated subscriber rename | Applied `scock_exchange_acronym → stock_exchange_acronym` on the subscribed `stock_exchange_mw`, lock-step with the publisher. **Confidence: HIGH.** |
| Apply-Guard (T8) | Replication apply-stall risk | Local→subscription FKs with `ON DELETE RESTRICT` can stall apply on an upstream hard-delete → keep RESTRICT, document publisher no-hard-DELETE contract; reword the contradictory FK note. **Confidence: HIGH.** |
| Stratum (T9) | Schema layering & enum placement | `instrument_kind` enum in `private` but used by `common` tables → relocate to `common.instrument_kind`. `timeframe_value` already correct. **Confidence: MEDIUM.** |
| Sentinel (T10) | Risk, audit, accounting & deployment metadata | v17.10/"same cluster" is impossible → dedicated v18.1 service; `pnl_daily_tx` lacked the net identity → `ck_pnl_net` + non-negativity; audit immutability confirmed. **Confidence: MEDIUM–HIGH.** |

</div>

### Team Name: Publishers' Guild (T4)

**Finding.** TradesDB's subscriptions were dangling until the StocksDB publications were authored. **Pros:** verifying
shape parity end-to-end guarantees the stream will actually apply. **Cons:** correctness depends on the upstream
StocksDB fix landing first; the subscriber must be migrated in step with the publisher for the renamed/widened columns.
**Confidence: VERY HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T4-GREEN (PLANNING)
Lens: subscriber contract verification.
Heuristic: every declared subscription must resolve to a real upstream publication
  with a matching column set.
Action: matched TradesDB's four subscriptions to the (now-authored) StocksDB
  publications and the MacroDB currency publication; checked each subscriber table's
  columns against its publisher.
Output: confirmed resolvable; flagged the rename/widen coordination.
```

```text
Agent: T4-BLUE (REVIEWER)
Lens: subscriber FK-drop correctness.
Heuristic: subscription tables must not carry FKs among themselves.
Action: confirmed currency_mw/securities_mw/ses_mw/stock_exchange_mw/
  derivative_contract_mw expose cross-references as plain columns; confirmed local
  facts' FKs into them are intentional and separate.
Output: approved.
```

```text
Agent: T4-RED (TESTING)
Lens: end-to-end apply rehearsal.
Heuristic: replicate the full reference set and confirm local facts can bind.
Action: modelled initial copy of all four subscriptions, then inserting a signal/
  order that FKs into ses_mw and derivative_contract_mw.
Result: post-fix the references resolve; pre-fix (no upstream publications) nothing
  arrives and every local fact insert fails its FK.
```

### Team Name: Lexicon (T5)

**Finding.** The subscriber copy of the typo'd column must be renamed in step. **Pros:** keeps the subscriber
name-aligned with the publisher so replication continues. **Cons:** must occur inside the same maintenance window as the
publisher rename. **Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T5-GREEN (PLANNING)
Lens: subscriber-side of a distributed rename.
Heuristic: the subscriber column name must equal the publisher's at all times the
  stream is enabled.
Action: scheduled the subscriber RENAME between the publisher rename and the
  subscription re-enable per the StocksDB Migration Notes.
Output: corrected stock_exchange_acronym on the subscribed table.
```

```text
Agent: T5-BLUE (REVIEWER)
Lens: consistency with the publisher procedure.
Heuristic: one procedure, two sides; no divergence.
Action: verified the subscriber rename is the only TradesDB schema change for this
  item and that the type/width are unchanged.
Output: approved.
```

```text
Agent: T5-RED (TESTING)
Lens: ordering-failure probe.
Heuristic: re-enable the subscription before renaming the subscriber and observe.
Action: modelled the wrong ordering.
Result: apply errors on the name mismatch; the documented ordering avoids it.
```

### Team Name: Apply-Guard (T8)

**Finding.** Correct `RESTRICT` FKs from facts into subscription tables create an apply-stall exposure to upstream
hard-deletes. **Pros:** documenting the soft-delete contract preserves both referential safety and replication liveness
without weakening any constraint. **Cons:** the guarantee is operational, not database-enforced upstream; it must be
honoured by every producer that can delete reference rows. **Confidence: HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T8-GREEN (PLANNING)
Lens: replication liveness vs referential safety trade-off.
Heuristic: never resolve a liveness risk by discarding a correctness constraint if a
  contract can satisfy both.
Action: traced every local→subscription FK; confirmed RESTRICT is the right local
  semantics; identified the upstream hard-delete as the only stall trigger.
Output: keep RESTRICT; document the publisher no-hard-DELETE contract; reword the
  contradictory subscription-FK note.
```

```text
Agent: T8-BLUE (REVIEWER)
Lens: alternative-remedy elimination.
Heuristic: prove the rejected options are worse.
Action: evaluated ON DELETE CASCADE (destroys trade history — unacceptable),
  SET NULL (orphans facts and violates instrument XOR checks — unacceptable), and
  dropping the FK (loses local integrity). Confirmed the contract is the least-bad
  remedy.
Output: approved the contract-based resolution.
```

```text
Agent: T8-RED (TESTING)
Lens: stall reproduction.
Heuristic: replicate a hard-delete of a referenced row and watch apply.
Action: modelled deleting a ses/contract row upstream that a signal references.
Result: the apply worker stalls on RESTRICT pre-contract; under the soft-delete
  contract (logical retirement) no tombstone is produced and apply proceeds.
```

### Team Name: Stratum (T9)

**Finding.** A `common` table depends on a `private` enum — a layering inversion. **Pros:** relocating `instrument_kind`
to `common` restores a clean dependency direction and matches the placement of the other shared enums. **Cons:** in the
real DDL this is a type relocation requiring care if existing objects already depend on it; in the diagram it is a
comment/header change because enum columns render as `varchar`. **Confidence: MEDIUM.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T9-GREEN (PLANNING)
Lens: schema-as-module dependency hygiene.
Heuristic: lower-privilege/shared layers (common) must not depend on
  higher-restriction layers (private).
Action: enumerated dependents of instrument_kind (strategy_mw, strategy_universe_mw,
  signal_tx, order_tx, position_snapshot_tx); found the enum mis-placed in private.
Output: move instrument_kind to common; update header enum table + all enum comments.
```

```text
Agent: T9-BLUE (REVIEWER)
Lens: cross-database enum consistency.
Heuristic: compare enum placement across the three databases for systemic drift.
Action: confirmed timeframe_value already sits in common in TradesDB (private only in
  the independent StocksDB, so no shared-cluster conflict); confirmed strategy_stage/
  signal_source/allocation_policy already in common.
Output: approved; no change needed for timeframe_value.
```

```text
Agent: T9-RED (TESTING)
Lens: representation-fidelity check.
Heuristic: ensure the varchar representation does not hide a real column change.
Action: verified that relocating the enum changes only the type's schema, not the
  column's stored type/width/nullability.
Result: no column-shape change; replication unaffected (the enum is not a published
  table).
```

### Team Name: Sentinel (T10)

**Finding.** Deployment metadata is internally contradictory and daily P&L lacks its accounting identity. **Pros:**
correcting the version/cluster claim removes an impossible configuration and improves the replication direction;
`ck_pnl_net` makes a silent reconciliation error impossible. **Cons:** the version correction is an inference that
requires ops confirmation; the P&L identity assumes charges are fully captured in `total_charges`. **Confidence:
MEDIUM–HIGH.**

### Agent Details

The following agents were deployed for the team that ensures that the output of the teams are different even if working
in the same objective. The details are as follows:

```text
Agent: T10-GREEN (PLANNING)
Lens: deployment-topology realism + accounting invariants.
Heuristic: a stated infrastructure fact must be physically possible; a derived
  financial column must equal its definition.
Action: flagged v17.10-on-a-v18.1-cluster as impossible; chose a dedicated v18.1
  service to also fix replication direction; defined net = gross − charges.
Output: re-document deployment as v18.1 Aiven service (confirm with ops);
  add ck_pnl_net + non-negativity checks.
```

```text
Agent: T10-BLUE (REVIEWER)
Lens: audit immutability & non-destructive guarantees.
Heuristic: an audit trail that can be altered is not an audit trail.
Action: confirmed audit_log_tx revokes UPDATE/DELETE and adds a blocking trigger,
  and carries no FKs (so it cannot be blocked/cascaded); confirmed circuit_breaker
  release ordering and risk-scope targeting checks.
Output: approved; audit posture sound.
```

```text
Agent: T10-RED (TESTING)
Lens: invariant-violation probing.
Heuristic: try to break the accounting identity and tamper the log.
Action: attempted a pnl row where net != gross − charges, a negative total_charges,
  and an UPDATE against audit_log_tx.
Result: the P&L rows are rejected by ck_pnl_net/ck_pnl_charges; the audit UPDATE is
  blocked by the revoke + trigger.
```

## Super Team

Under the maximum-hardening mandate the Super Team incorporated all five teams' validated work. For TradesDB the
load-bearing change is operational rather than structural: the no-hard-DELETE contract (T8) keeps the protective
`RESTRICT` FKs from ever stalling replication, while the schema-layering fix (T9), the deployment-metadata correction
and P&L identity (T10), and the coordinated subscriber rename (T5) round out the set. The subscriber-topology
verification (T4) ties the database back to the upstream publications fix. None of these weakens the security posture:
subscription tables stay write-revoked from `PUBLIC` and the audit log stays immutable.

### Winning Teams

The global marquee trio — **Publishers' Guild (T4, VERY HIGH)**, **Mint Authority (T2, HIGH)**, and **Corporate Actions
Bureau (T7, HIGH)** — is dominated by upstream fixes, but T4 is the team that makes TradesDB viable at all by surfacing
the missing publications its subscriptions depend on. Within this file, **Apply-Guard (T8)** is the standout local
contribution (it converts a latent replication-halting hazard into a documented contract), with **Stratum (T9)** and
**Sentinel (T10)** as honourable mentions whose fixes were fully incorporated.

### Super Team Aggregated Result

Concrete changes applied to `tradesdb.dbml` (see the file's `versionchanged` block):

  1. **[HIGH · BUGFIX]** Renamed subscriber `scock_exchange_acronym → stock_exchange_acronym` on
     `common.stock_exchange_mw`, lock-step with the StocksDB publisher.
  2. **[MEDIUM · BUGFIX]** Corrected the impossible "v17.10 on the same cluster" claim to a dedicated v18.1 Aiven
     service (keeps replication publisher ≤ subscriber); flagged for ops confirmation.
  3. **[MEDIUM · ENHANCEMENT]** Relocated enum `instrument_kind` from `private` to `common`; updated the header enum
     table and all `// enum:` references.
  4. **[MEDIUM · ENHANCEMENT]** Added `ck_pnl_net` (`net_pnl = gross_pnl − total_charges`) with non-negativity checks;
     documented the publisher no-hard-DELETE contract protecting the `ON DELETE RESTRICT` FKs.
  5. **[LOW · ENHANCEMENT]** Reworded the contradictory subscription-FK note into two precise rules (no FKs among
     subscription tables; local facts reference them via ordinary FKs).

> Note: the local trading tables in `tradesdb.dbml` were reconstructed from the validated design model (the original
> upload was not recoverable post-compaction); the five subscription tables are reproduced at exact publisher
> column-shape fidelity. Please diff the local-table column set against your canonical source before applying.

</div>
