# Current state

This file is the concise current-orientation record. The authoritative
development plan is [docs/MILESTONES.md](MILESTONES.md).

```text
G0=COMPLETE
G1=COMPLETE
G2=COMPLETE
G3=COMPLETE
G4=COMPLETE
G5=COMPLETE
G6=COMPLETE
G7=COMPLETE
G8=COMPLETE
G9=COMPLETE
G10=COMPLETE
G11=COMPLETE
POST_G11_RUNTIME_PRODUCTIZATION=COMPLETE
LIVE_BRANCH=main
LATEST_BEHAVIOR_CHANGING_IMPLEMENTATION_MERGE=72961563912f08541b311c09f77f49af1e03fd41
IMPLEMENTATION_TREE_AT_THAT_MERGE=0c4df08b5bc06e49ab7f66d180b1aeea3f471d47
RECOVERY_OBSERVABILITY=COMPLETE
RECOVERY_OBSERVABILITY_MERGE=72961563912f08541b311c09f77f49af1e03fd41
RECOVERY_FAILURE_HISTORY_CAPACITY=7
PERFORMANCE_INSTRUMENTATION=COMPLETE
RECOVERY_OBSERVATION_CAMPAIGN=COMPLETE
TOTAL_VALID_RECOVERY_OBSERVATION_SECONDS=3600
POST_READY_TARGET_SPOT_RECOVERY_REPRODUCED=NO
HISTORICAL_SPOT_ROOT_CAUSE=NOT_ESTABLISHED
RCO03_AUTHORIZED=NO
RECOVERY_OBSERVATION_BUDGET=EXHAUSTED
BOUNDED_UNCERTAINTY_RETAINED=YES
PERF01=COMPLETE
POST_G11_PERFORMANCE_BASELINE=COMPLETE
ORIGINAL_INTERNAL_LATENCY_QUEUE_DELIVERY_EVIDENCE_REUSABLE=YES_REUSABLE_WITH_SCOPE_NOTE
PERF01_VALID_SECONDS_PER_ROW=300
PERF01_ROWS=A_B_C
PERF01_UNPLANNED_RECOVERY_DURING_CREDIT=NO
PERF01_HOST_CONTEXT_VALID=YES
PERF01_ORDERLY_SHUTDOWN=YES
CLASH_ROOT_CAUSE_ESTABLISHED=NO
NEXT_LIVE_PERFORMANCE_RUN_AUTHORIZED=NO
OLD_A_PROCESS_EXACT_NEW_HEAD_AUTHORITY=NO
OLD_B_PROCESS_EXACT_NEW_HEAD_AUTHORITY=NO
OLD_CONTAMINATED_C_PROCESS_REUSABLE=NO
EXACT_CURRENT_HEAD_PROCESS_A_B_C_COMPANION=COMPLETE
BASELINE_MILESTONE_COMPLETE=YES
PRODUCTION_QUALIFICATION_AUTHORIZED=NO
OPTIMIZATION_AUTHORIZED=NO
DOC_ALIGN_01=COMPLETE_ON_MERGE
NEXT_TECHNICAL_STAGE=NOT_YET_FROZEN
G2_SYNTHETIC_HOST_IMPLEMENTED=YES
G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES
REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES
GATEWAY_NETWORK=SPOT_BTCUSDT_AND_USD_M_PERPETUAL_BTCUSDT_IMPLEMENTED
USD_M_BTCUSDT=IMPLEMENTED
MULTI_MARKET_RUNTIME=IMPLEMENTED
G11_PRODUCT_COUNT=2
PRODUCTION_DAEMON=IMPLEMENTED
PRODUCTION_DAEMON_BINARY=bmd-gatewayd
PRODUCTION_PRODUCT_COUNT=2
PRODUCTION_GRPC_CONFIGURABLE=YES
PRODUCTION_REQUIRES_BOTH_INITIAL_LIVE=YES
PRODUCTION_SERVES_BEFORE_BOTH_INITIAL_LIVE=NO
SIGINT_SUPPORTED=YES
SIGTERM_SUPPORTED=YES
STARTUP_ROLLBACK=IMPLEMENTED
POST_START_SINGLE_MARKET_FAILURE_ISOLATION=IMPLEMENTED
INSTALLABLE_PRODUCTION_DAEMON=YES
REAL_PRODUCTION_DAEMON_ACCEPTANCE=PASS
RECONNECT=IMPLEMENTED
AUTOMATIC_RECOVERY=IMPLEMENTED
PLANNED_ROTATION=IMPLEMENTED
SUBSCRIBE_ORDER_BOOK=IMPLEMENTED
SUBSCRIBE_EVENTS=IMPLEMENTED
GET_GATEWAY_STATUS=IMPLEMENTED
BOUNDED_PUBLICATION=IMPLEMENTED
EVENT_PUBLICATION=IMPLEMENTED
GRPC=IMPLEMENTED
MAX_ACTIVE_SUBSCRIPTIONS=8
MAX_ACTIVE_EVENT_SUBSCRIPTIONS=8
ORDINARY_QUEUE_CAPACITY=64
TERMINAL_CONTROL_CAPACITY=1
EVENT_ORDINARY_QUEUE_CAPACITY=64
EVENT_TERMINAL_CONTROL_CAPACITY=1
PENDING_ADMISSION_CAPACITY=8
IDLE_CLIENT_CANCELLATION_CHECK_INTERVAL=250ms
MAKE_BEFORE_BREAK=NO
GATEWAY_GRPC_BUSINESS_FLOW=SUBSCRIBE_ORDER_BOOK_SUBSCRIBE_EVENTS_GET_GATEWAY_STATUS_IMPLEMENTED
RECORDER_DEPENDENCY=NO
GW-PREQ-002=COMPLETE
PROJECTION_M6_GATEWAY_INTEGRATION_ACCEPTANCE=COMPLETE
MAX_CONCURRENT_STATUS_RPCS=1
STATUS_USES_STREAM_CONTEXT_TRACKER=NO
STREAM_CONTEXT_LIMIT=48
STREAM_CONTEXT_LIMIT_G11_OFF=24
G7_ACTIVE_LIMIT_PER_MARKET=8
G7_PENDING_LIMIT_PER_MARKET=8
G9_ACTIVE_LIMIT_PER_MARKET=8
MAX_G7_ACTIVE_TOTAL=16
MAX_G7_PENDING_TOTAL=16
MAX_G9_ACTIVE_TOTAL=16
MAX_ACTIVE_TRANSPORTS_PER_MARKET=1
MAX_ACTIVE_TRANSPORTS_TOTAL=2
STATUS_MARKET_COUNT=2
NEXT=NOT_FROZEN
FIRST_RUNNABLE=G2
FIRST_REAL_NETWORK=G4
FIRST_GRPC=G7
PROJECTION_M6_START_GATE=G8
FIRST_MULTI_MARKET=G11
```

PERF-01 accepted resource summary (300 credited seconds per row):

| Row | Workload | CPU mean | CPU p95 | RSS median | RSS max |
|---|---|---:|---:|---:|---:|
| A | dual market, no subscribers | 5.5684% | 6.9745% | 42.8516 MB | 43.0469 MB |
| B | dual market, order book | 5.9757% | 7.9739% | 46.7012 MB | 47.0078 MB |
| C | dual market, book plus depth events | 6.6056% | 7.9803% | 45.9219 MB | 46.1367 MB |

These measurements are descriptive accepted baseline evidence. They are not a
hard SLA, capacity guarantee, exact causal A→B→C subscriber-cost result,
infinite-duration RSS proof, or zero-observer production truth.

## Post-G11 production daemon

The current `bmd-gatewayd` is the ordinary long-running production host for
fixed Spot BTCUSDT and USD-M perpetual BTCUSDT. It acquires authoritative
metadata, constructs the two accepted product runtimes, waits for both to reach
initial Live/Synchronized, and only then starts the configured synchronous gRPC
listener. It serves until SIGINT/SIGTERM. A startup failure rolls back the
partial graph; after startup, a failure of one market remains isolated. During
shutdown, server handlers are drained/cancelled before product owner
destruction. Production contains no acceptance-only controlled-recovery hook.

## What is implemented

Gateway `main` currently contains:

- typed, finite configuration;
- synchronous Foundation lifecycle;
- the historical/minimal Foundation CLI seam;
- deterministic Foundation tests;
- build/CI/sanitizer support;
- the explicit, opt-in G1 dependency proof;
- the deterministic in-memory G2 synthetic Spot BTCUSDT host with focused tests
  and a dedicated executable;
- the G3 Spot BTCUSDT `MarketRuntime` with one private `BookProjection`, one
  serialized owner thread, bounded ingress and bootstrap buffers, an injected
  clock, synthetic input/fault admission, copied owner-domain observations, and
  deterministic joined shutdown; and
- the opt-in G4 real Binance Spot BTCUSDT runtime with verified exchangeInfo
  metadata acquisition, `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize` NumericSpec
  derivation, verified TLS REST and raw diff-depth WebSocket transport, real
  receive timestamps, connection generation 1 identity, bounded bootstrap
  through `MarketRuntime`/Projection, server ping handling, and deterministic
  clean stop; and
- the opt-in G5 Spot BTCUSDT recovery coordinator, which preserves one
  `MarketRuntime`/private Projection owner across break-before-make connection
  generations, resets Projection only through the owner domain after the old
  transport joins, detects `NeedsResync` without polling, and applies bounded,
  interruptible, rate-limit-aware recovery; and
- the opt-in G6 planned-rotation policy integrated into that same coordinator,
  which uses monotonic generation age, rotates at the project-defined 23h50m
  mark, quiesces and joins the old source before an owner-domain healthy reset,
  then conservatively re-enters the existing bootstrap path with a new
  generation; and
- the opt-in G7 bounded order-book publication runtime and synchronous
  `SubscribeOrderBook` service. The existing `MarketRuntime` owner exclusively
  owns subscriber admission, snapshot cuts, registry mutation, Applied-update
  fanout, and recovery/rotation terminalization. Each accepted synchronous RPC
  handler exclusively owns its stream writer; and
- the G8 Projection M6 integration acceptance composition. It proves
  deterministic real-gRPC Projection `NeedsResync`, real consumer-visible G5
  controlled recovery, and real consumer-visible G6 planned rotation using the
  existing G3-G7 architecture. Old subscriptions terminate before full
  rebootstrap, fresh subscriptions restart `session_sequence`, and the
  acceptance adds no production runtime redesign.
- the G9 synchronous `SubscribeEvents` flow. V1 accepts exactly one selector
  for Spot BTCUSDT `DIFF_DEPTH`, `AGG_TRADE`, or `BOOK_TICKER` over one combined
  Binance Spot WebSocket; the legacy depth-only G4-G8 profile remains available.
  `DIFF_DEPTH` is pre-Projection normalized, while G7 `SubscribeOrderBook`
  remains Projection-Applied-only. Event publication is bounded with exact
  per-session `session_sequence`; sessions do not cross source-generation
  replacement and recovery/rotation use
  `CONNECTION_GENERATION_CHANGED`/`RESUBSCRIBE`. There is no second sequence
  classifier, generic event bus, or new publication thread.
- the G10 synchronous unary `GetGatewayStatus` flow for one Binance Spot BTCUSDT
  market. It reports the existing gateway instance identity, monotonic server
  uptime, mapped runtime lifecycle state, optional last normalized WebSocket
  market-event receive time, optional uniquely applicable connection generation,
  and G7 resident plus G9 active subscription count. It permits one expensive
  status collection at a time and adds no health, metrics, or telemetry subsystem.
- the G11 fixed two-product runtime for Binance Spot BTCUSDT and Binance USD-M
  perpetual BTCUSDT. It uses one isolated `MarketRuntime`, private
  `BookProjection`, serialized owner, and `RecoveryCoordinator` per product,
  with independent transports and one fixed non-owning two-entry registry.
  Gateway parses and forwards USD-M `pu` through
  `DepthUpdate.previous_final_update_id`; Projection's
  `SequencePolicyKind::UsdMPerpetual` remains the sole continuity authority.
  G7 routes both products, G9 exposes only USD-M `DIFF_DEPTH`, and status returns
  two deterministic market rows.

There is no make-before-break source stitching. G7 historically implemented only
`SubscribeOrderBook`; the current Gateway also implements G9 `SubscribeEvents`.
G11 implements exactly the two accepted products and is not arbitrary
multi-symbol support. G8 is
an acceptance/test composition and opt-in CMake wiring; it does not add a
production runtime layer. G4 remains independently usable as a one-shot transport.
Post-G11 runtime productization is complete: the ordinary `bmd-gatewayd` is
the installed long-running two-product daemon.
`POST_G11_PERFORMANCE_BASELINE=COMPLETE`; the bounded recovery-observation
campaign is complete. No G12 or further numbered Gateway milestone is currently
frozen.
G5 recovers transport, snapshot, malformed-input, bounded-admission, bootstrap
overflow, `serverShutdown`, and Projection `NeedsResync` failures through a new
connection and the same conservative bootstrap path. Internal adapter,
Projection-rejection, clock, and invariant failures remain terminal. Historical
G2/G3 implementation attempts are abandoned and are not authority for the
current implementation.

The completed G2 host remains a separate direct-Projection milestone proof. G3
remains independently testable without transport and accepts deterministic
in-memory Contracts messages and synthetic fault events. G4 is the first real
network milestone and drives G3 only through its bounded serialized boundary.
G5/G6 add no sequence classifier: Projection remains the sole owner of continuity
meaning. Recovery permits at most six consecutive attempts with deterministic
delays of 1, 2, 4, 8, 16, and 30 seconds. HTTP 429/418 additionally require a
strict valid `Retry-After`, which is never capped downward; stop interrupts the
wait. G6 clean rotations consume no recovery attempt or backoff; genuine
transport/runtime failure at the planned cut wins and remains ordinary G5
recovery.

G0 acceptance and the frozen G1 candidate dependency proof do not establish a
formal upstream release. G1 must not be continuously repinned. GW-PREQ-002 is
complete and adds no runtime behavior: the normal G2–G6/G7-disabled lane uses
the root `conanfile.py` with Contracts message/Protobuf and Projection
Core/ProtoAdapter, without Contracts gRPC or `grpc`. G7 conditionally adds the
current Contracts gRPC artifact and its `grpc` dependency while preserving one
Contracts message lineage. The historical four-target proof uses
`conanfile_g1.py` explicitly.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. G7 publishes only Projection `Applied` updates. Its limits are
eight resident accepted channels, 64 ordinary records per channel, one separate
terminal slot per channel, and eight pending admissions. Existing sessions
terminate and resubscribe rather than cross G5/G6 full Projection rebootstrap.
G9 event subscribers are separately bounded at eight active sessions, 64 ordinary
records, and one terminal control slot per session.

## Recovery observability

PR #25 merged the bounded internal recovery-failure diagnostics into the
production daemon. Each product retains the newest seven classified unplanned
failure cuts in chronological order, including generation, `RecoveryCause`,
optional `NetworkError`, runtime fault, adapter diagnostic, and
Projection/gap summary. The history is captured after source quiescence and
before reset/rebootstrap destroys attempt evidence. It does not change recovery
policy, retry/backoff, generation/reset semantics, subscribers, public
protobuf, Contracts, Projection, G10 status, or normal message processing.

Successful later Live recovery may clear the current terminal error while the
historical failure cuts remain. Startup failure and orderly shutdown may emit
bounded one-line diagnostics; an empty history emits none.

## Baseline closure and retained uncertainty

The original internal latency, queue, and delivery evidence remains reusable
with a scope note because PR #25 does not change the normal market-message
processing path. Old process CPU/RSS A/B measurements are historical context,
not exact-current-main authority; contaminated Row C evidence is not reusable.
The accepted recovery campaign comprised RCO-01 and RCO-02, each valid for
1,800 seconds, for 3,600 valid observation seconds total. The post-ready Spot
recovery target was not reproduced. RCO-02 retained one real Spot startup cut
(generation 1, pre-serving, `NeedsResync`, classification E,
`spot-bootstrap-forward-gap`), but historical post-ready causal equivalence was
not established. Therefore `HISTORICAL_SPOT_ROOT_CAUSE=NOT_ESTABLISHED`, the
observation budget is exhausted, bounded uncertainty is retained, and RCO-03 is
not authorized.

The accepted PERF-01 companion is complete. Its A/B/C CPU and RSS values are
descriptive baseline evidence only; the original internal latency, queue, and
delivery evidence remains reusable with its scope note. Production
qualification and optimization remain unauthorized. No new live performance
run is authorized or required by this closure; that is not a pending recovery
adjudication.

## Known nonblocking follow-up findings

- P2-1: The deterministic productionization test matrix is not exhaustive;
  some explicit start-result, exception, and combined-backpressure paths are
  not individually tested. No corresponding production source defect was found.
- P2-2: GitHub default sanitizer jobs do not enable the production-daemon graph;
  local productization-enabled sanitizer evidence was not independently
  authenticated.
- P2-3: SIGINT/SIGTERM cannot immediately cancel synchronous metadata HTTPS
  acquisition, although network stage deadlines make the delay bounded.
- P2-4: the Contracts Gateway service declares `SubscribeMarketState`, while
  the current Gateway production service does not implement that RPC. The
  contract and implementation surfaces require future reconciliation before
  formal product V1 contract acceptance; baseline closure is not blocked.

These findings are nonblocking and are not claimed to be resolved.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
