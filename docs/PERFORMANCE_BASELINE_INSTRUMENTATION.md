# Post-G11 performance-baseline instrumentation

This facility measures the existing fixed two-product `bmd-gatewayd`. The
instrumentation facility is complete. It does not add a benchmark host, an RPC,
a public API, a wire field, or an optimization. The original internal A/B/C
campaign has been run and independently reviewed; its latency, queue, delivery,
and overflow evidence remains reusable with a scope note because the recovery
observability change does not alter the normal market-message processing path.

`POST_G11_PERFORMANCE_BASELINE` is `COMPLETE`. The final exact-current-head
whole-process PERF-01 companion was accepted together with the original
internal latency, queue, delivery, and overflow evidence. These measurements
are descriptive baseline evidence, not a hard SLA, capacity guarantee, exact
causal subscriber-cost decomposition, infinite-duration RSS proof, or
zero-observer production truth. Production qualification and optimization are
not authorized.

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
gRPC handlers and stopped both product graphs. No path means no artifact; the
instrumented daemon reports that export was skipped. An explicitly requested
export failure makes the daemon exit unsuccessfully.

```sh
BMD_GATEWAY_PERFORMANCE_BASELINE_OUTPUT=build/evidence/baseline.jsonl \
  build/performance-baseline/bmd-gatewayd --grpc-listen 127.0.0.1:50051
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
