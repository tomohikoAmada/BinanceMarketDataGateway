# BMDG performance baseline instrumentation

Opt-in, compile-time measurement support for the production graph. It adds no
RPC, public API, wire field or production behavior change. Normal builds compile
it out.

## Build and export

```sh
cmake -S . -B build/performance \
  -DCMAKE_TOOLCHAIN_FILE=build/production-deps/conan_toolchain.cmake \
  -DBMDG_BUILD_PRODUCTION_DAEMON=ON \
  -DBMDG_BUILD_TESTS=ON \
  -DBMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION=ON
cmake --build build/performance --target bmdg
```

Set `BMDG_PERFORMANCE_BASELINE_OUTPUT` to write one JSON Lines artifact
after orderly shutdown. Unset means no artifact. An explicitly requested export
failure exits unsuccessfully.

## Measurement points

- T0: successful WebSocket read completion, before payload materialization.
- T1: canonical `DepthUpdate` constructed, before G9 publication/runtime submit.
- Q: successful `MarketRuntime` ingress commit.
- T2: owner dequeue, before ProtoAdapter adaptation.
- T3: return from inbound adaptation + Projection apply.
- T4-event: matching G9 DIFF_DEPTH subscriber queue commit (pre-Projection).
- T4-book: Applied update committed to a G7 subscriber.
- T5: successful return from synchronous server-side `ServerWriter::Write`;
  not client application processing.

All subtraction timestamps use the same monotonic clock. Trace ordinals are
per product and never change Binance IDs, Projection identity, connection
generation, subscription ID or session sequence.

## Bounded storage

Each product preallocates 16,384 trace rows and 32,768 delivery rows per branch.
There is no per-traced-event allocation or hot-path formatting. Overflow stops
new evidence, marks `evidence_valid=false`, and market data continues. Twenty
instrumented products add about 110-111 MiB peak RSS; do not enable this in a
normal production deployment.

The artifact contains one `campaign` row per product, one `trace` row per
retained event, and zero or more `delivery` rows per trace/branch. CPU/RSS are
external campaign measurements, not internal collection.

## P1 results

Offline, Apple M3, 24 GiB, includes embedded fixture/clients. Not a VPS/SLA
result.

| 20-key scenario | CPU mean | RSS | Delivered updates/s |
| --- | ---: | ---: | ---: |
| normal | 0.115 cores | ~27 MiB | ~82 |
| deep | 1.192 cores | ~41 MiB | ~820 |
| batch | 1.242 cores | ~53 MiB | ~820 |

- 127,920 updates / 255,840 deliveries; 18 paired trials.
- Checked apply share of owner update wall time: 95.2% / 98.3% / 97.6%.
- Checked apply means: ~592 / ~1018 / ~991 µs per update.
- Projection allocations: ~2023 / 161 KB per update at D1000; ~10029 / 1 MB at
  D5000.
- P1 attributed the dominant cost to full-book Projection `apply_transaction`
  copy/rebuild. Projection M-P2.A/B/C have since replaced it with in-place
  accepted apply and the trusted adapter path; these P1 numbers are
  pre-optimization evidence.
- Largest observed Book maximum: 102.4 ms in one deep run; tail variability is
  retained and no SLA is claimed.
- Four-product P1: 0.057 / 0.925 / 0.983 cores and ~18.5 / ~28 / ~33 MiB for
  D1000/B10/10Hz, D5000/B10/100Hz, D5000/B100/100Hz.

Re-measure the complete BMDG path on the target VPS in G3-c before setting new
CPU/RSS budgets; do not reuse the pre-optimization attribution as a current
hotspot.

## Scope limits

The measurements are offline, include fixture/client work, are not a
twenty-product production capacity proof, and do not establish infinite-duration
RSS or latency guarantees. See [EVIDENCE](EVIDENCE.md) and the Projection
repository's `PROJECTION_P1_RESULT.md` / `PROJECTION_P1_TWENTY_RESULT.md`.
