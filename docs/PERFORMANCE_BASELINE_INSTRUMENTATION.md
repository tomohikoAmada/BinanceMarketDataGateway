# Post-G11 performance-baseline instrumentation

This facility was introduced for the fixed two-product `bmd-gatewayd`. The
current configured daemon retains one measurement buffer per product; the
instrumentation facility is implemented, while multi-product performance
acceptance remains pending under [MILESTONES.md](MILESTONES.md). It does not add a benchmark host, an RPC,
a public API, a wire field, or an optimization. The original internal A/B/C
campaign has been run and independently reviewed; its latency, queue, delivery,
and overflow evidence remains reusable with a scope note because the recovery
observability change does not alter the normal market-message processing path.

`POST_G11_PERFORMANCE_BASELINE` is `COMPLETE`. The final exact-current-head
whole-process PERF-01 companion was accepted together with the original
internal latency, queue, delivery, and overflow evidence. These measurements
are descriptive baseline evidence, not a hard SLA, capacity guarantee, exact
causal subscriber-cost decomposition, infinite-duration RSS proof, or
zero-observer production truth. They are evidence for the fixed two-product
G11 daemon only, not capacity evidence for the authorized G12 configurable
multi-product daemon. The current milestone plan requires explicit performance
and resource budgets plus measured, regression-tested improvements where needed;
this historical baseline is not current multi-product qualification.

## Build and export

Instrumentation is compile-time opt-in and requires the production-daemon
graph:

```sh
cmake -S . -B build/performance-baseline \
  -DCMAKE_TOOLCHAIN_FILE=build/g7-conan/conan_toolchain.cmake \
  -DBMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON \
  -DBMD_GATEWAY_BUILD_TESTS=ON \
  -DBMD_GATEWAY_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION=ON
cmake --build build/performance-baseline --target bmd-gatewayd
```

Set `BMD_GATEWAY_PERFORMANCE_BASELINE_OUTPUT` to the desired artifact path
before starting the daemon. The process retains evidence in memory and writes
one JSON Lines artifact only after orderly shutdown has drained synchronous
gRPC handlers and stopped all configured product graphs. No path means no artifact; the
instrumented daemon reports that export was skipped. An explicitly requested
export failure makes the daemon exit unsuccessfully.

```sh
BMD_GATEWAY_PERFORMANCE_BASELINE_OUTPUT=build/evidence/baseline.jsonl \
  build/performance-baseline/bmd-gatewayd --config examples/gateway.json
```

## Measurement points

- T0: successful WebSocket read completion, sampled before payload
  materialization and JSON parsing.
- T1: successful canonical `DepthUpdate` construction, before G9 publication
  and before runtime submission.
- Q: successful bounded `MarketRuntime` ingress commit.
- T2: owner dequeue of that update, before ProtoAdapter adaptation.
- T3: return from inbound adaptation and Projection apply, including the
  Projection disposition or a failure-equivalent adapter/internal result.
- T4-event: successful queue commit into one matching G9 `DIFF_DEPTH`
  subscriber. This pre-Projection branch may precede Q, T2, and T3.
- T4-book: successful queue commit of an `Applied` update into one G7
  order-book subscriber.
- T5: successful return from the synchronous server-side
  `ServerWriter::Write`; it is not client application processing.

All subtraction timestamps use the same monotonic clock basis. The internal
trace ordinal is per product and never changes Binance identifiers, Projection
identity, `SourceProvenance`, public protobufs, connection generation,
subscription ID, or session sequence.

## Bounded evidence and format

Each product preallocates 16,384 trace rows and 32,768 delivery rows per branch.
There is no per-traced-event allocation or hot-path formatting. Exhausted trace
or delivery storage increments an overflow counter, stops recording the
affected new evidence, and marks `evidence_valid=false`; market data continues
through the existing production path.

The JSON Lines artifact contains:

- one `campaign` row per product with capacities, overflow/clock-error validity,
  ingress/order-book/event queue maxima, ingress Full count, and subscriber
  full-terminalization counts;
- one `trace` row per retained event with trace ID, T0/T1/Q/T2/T3,
  disposition, Q occupancy, delivery counts, and any missing-stage reason; and
- zero or more `delivery` rows per trace and branch with subscriber ordinal,
  session sequence, T4, optional successful T5, T4 occupancy, and missing-T5
  reason.

The first baseline deliberately omits internal CPU/RSS collection,
time-at-capacity tracking, probabilistic sampling, continuous aggregation, and
per-event logging. CPU and RSS remain external campaign measurements; occupancy
samples, maxima, and existing bounded-queue Full/terminalization transitions
are sufficient queue evidence for this first measurement facility.

## Projection P1 extension — 2026-10-18

The configured production exporter now writes every configured product after
shutdown, with its exact market/symbol identity. The legacy two-BTC restriction
is removed; normal builds still ignore the export environment variable.
Historical fixed-two baseline acceptance above is unchanged.

Additive trace fields: `adapted_ns` (successful inbound adaptation, immediately
before checked apply), `owner_done_ns` (after update publication/state work),
`owner_cpu_begin_ns`/`owner_cpu_end_ns` (CLOCK_THREAD_CPUTIME_ID at T2 and done).
`adapted_ns`→T3 includes `AdaptedDepthBatch::apply_to` binding/vector work and
Core; it is not a pure-Core span. CPU differences cover this update-processing
segment only, not all owner-thread CPU. Clock-read errors invalidate evidence.
Zero extension timestamps mean the stage was not observed.

Delivery `write_begin_ns` is sampled after envelope materialization immediately
before synchronous Write. T4→write_begin includes queue/wake/materialization;
write_begin→T5 measures server Write completion, not client processing.

The test-only `bmd_gateway_projection_p1_measurements DEPTH BATCH COUNT INTERVAL_US
OUTPUT` reuses the G2 fixture/production graph, seeds equal fixed-depth books,
warms up 32 events/product and exercises four owners plus eight real loopback
streams. It preconstructs JSON input and validates complete ordered deliveries,
market identity, final accepted IDs, no trace overflow and stopped owners.
`gateway_projection_p1_smoke` covers it in normal and instrumented builds.
No production acceptance hook/configuration, new library public API, network
host or dependency pin is introduced. CPU/RSS include in-process client/fixture
work; this driver does not establish production daemon capacity or VPS budgets.
Results and reproduction are shared with Projection's
`docs/PROJECTION_P1_RESULT.md`; Gateway G3 remains unqualified.

P1 local campaign completed: three scenarios × OFF/ON × three processes,
27,600 Applied traced updates and 55,200 traced successful server writes;
normal builds delivered the same update counts through both branches. Every
trial preserved all four identities, final IDs and ordered complete delivery;
no ingress Full, slow-client terminalization, trace overflow or clock error.
On the eight-core Apple M3 host (24 GiB), OFF whole-process CPU means were
0.057 / 0.925 / 0.983 cores for D1000/B10/10Hz, D5000/B10/100Hz and
D5000/B100/100Hz. These CPU values include embedded loopback clients and
fixtures. They are not a four-core VPS or Recorder co-load acceptance.

Local validation: OFF Release 24/24, ON Release 25/25; ASan+UBSan production
23/23 and final measurement/instrumentation 2/2; focused TSan 5/5; format and
diff checks pass. OFF/ON counts include historical G2 synthetic/G8 tests;
sanitizer graphs omit those two historical groups. A fresh independent agent
approved the implementation and measurement methodology; result review is
recorded with Projection's evidence. No Core or adapter production change.

### Twenty-key P1 increment (2026-10-19)

The same test-only measurement executable also accepts
`--profile tests/fixtures/projection-p1-twenty.json normal|deep|batch OUTPUT`.
The frozen profile uses selected official metadata and 24-hour trade-count activity
tiers for ten Spot and ten USD-M research symbols. It records per-key depth,
batch, count and interval; trade counts are a synthetic workload proxy, not
observed depth frequency or an approved deployment list. Each key has one Book
and one Depth Event stream (40 contexts); the global bound remains 48.

Three alternating OFF/ON pairs per scenario completed. Retained Projection P1
results identify exact measured commit `8ce41be`, binaries, runner and profile,
plus a local raw archive with 107 verified data-member hashes. Instrumentation
adds about 110–111 MiB peak RSS for twenty preallocated buffers; CPU ordering and
tail latency vary, so no zero-observer-cost or SLA claim is made. Per-key p99 and
maximum tails remain in the evidence. Projection Core/Adapter/pins are unchanged.
See [Projection PR #55](https://github.com/tomohikoAmada/BinanceMarketDataProjection/pull/55).
