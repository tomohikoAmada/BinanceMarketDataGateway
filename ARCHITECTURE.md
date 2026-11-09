# Gateway architecture

## Responsibilities and dependencies

```text
Binance public REST / WebSocket
              |
              v
     BinanceMarketDataGateway
        +--> Contracts Protobuf / opt-in gRPC artifacts
        +--> Projection ProtoAdapter --> Core
```

- **Contracts** owns messages, services and wire compatibility.
- **Projection** owns fixed-point numbers, order book and Spot/USD-M sequence
  semantics (including `pu`).
- **Gateway** owns transport, metadata, lifecycle, bounded publication and gRPC.
- Gateway does not depend on Recorder, copy proto files, or add a second
  sequence classifier.

## Domain layout

```text
src/foundation/     public config/lifecycle seam, no network or threads
src/protocol/       Spot/USD-M JSON parsers and normalized metadata
src/transport/      Binance REST/WebSocket transport, routes, API budget
src/runtime/        MarketRuntime, MarketKey, clock, source provenance
src/recovery/       RecoveryCoordinator and planned rotation policy
src/publication/    order-book/event subscriber channels
src/grpc/           gRPC service/server (namespace `rpc`)
src/status/         GetGatewayStatus assembler
src/composition/    ProductRuntime, configured set and immutable registry
src/production/     daemon config, metadata, startup/shutdown, signals
src/performance/    opt-in trace/delivery instrumentation
```

The old `G0`-`G11` labels are historical milestone names, not modules. See
[CODE_MAP](docs/CODE_MAP.md).

## Production composition

```text
bmd-gatewayd --config PATH
  -> strict JSON config (grpc_listen, spot_symbols, usdm_symbols)
  -> fetch exchangeInfo once per configured market
  -> resolve NumericSpec for every exact MarketKey
  -> ProductionGateway
       +-- ConfiguredProductRuntimeSet (1..20 stable owned products)
       |     +-- immutable MarketKey
       |     +-- MarketRuntime / private BookProjection / one owner thread
       |     +-- product-local order-book and event publication
       |     +-- independent RecoveryCoordinator / one active transport
       +-- immutable non-owning MarketRuntimeRegistry
       +-- one shared synchronous gRPC server
```

`MarketKey = (venue, market, exact symbol)`. The same symbol in Spot and USD-M
is two products. Configuration does not case-fold; the lowercase stream route is
only transport encoding. Binance routes currently accept uppercase ASCII
letters/digits; exchangeInfo validates existence, trading status and precision.

Configuration strictly rejects unknown fields, duplicate JSON keys, duplicate
MarketKeys, empty sets, more than twenty products and invalid endpoints. Any
missing/ineligible symbol or unsupported numeric spec fails startup. Validation
does no network I/O; metadata acquisition is a separate bounded phase. There is
no hot reload or runtime add/remove.

Insecure gRPC accepts numeric IPv4 `127/8` or IPv6 `::1` only. Hostnames,
wildcards and non-loopback endpoints are rejected before products start or the
server binds. Offline tests may use loopback port 0; the production CLI rejects
port 0.

Exchange JSON accepts unrelated extension fields but validates required fields,
types, exact identity and numeric semantics. Spot depth rejects `pu`; USD-M
`pu` remains a Projection/Adapter boundary. Gateway does not add a second
sequence classifier.

Twenty products is a resource policy, not a Binance limit or measured capacity.
The implementation uses small linear lookups and stable `unique_ptr` owners; it
does not add a generic registry framework or a shared multiplexed transport.

## Concurrency and lifecycle

- `MarketRuntime` receives complete events. Bounded ingress and bootstrap
  buffers are separate. Projection read/write, reset and publication
  registration all run on the same owner thread. Only owning/copy data crosses
  the boundary.
- The owner thread is published as started only after successful creation.
  Production start rolls back every started product on a returned failure or
  exception.
- All configured products share one absolute first-readiness deadline. gRPC
  binds only after every product is Live/Synchronized.
- Startup readiness observes the owner FIFO barrier without blocking. Normal
  observations wait at most one second; explicit deadline/stop-token
  observations return an owning copy or no result, never stale Live.
- Metadata and first synchronization have separate bounds. The first-sync
  deadline is absolute and is not reset by adding products or by a late Live.
- After startup, product failure is isolated. Recovery quiesces the old
  transport before reset/rebootstrap; at most one active transport exists per
  product.
- Planned rotation uses a 23h50m monotonic policy and break-before-make. Data is
  never stitched across source generations.
- An order-book session ends before a full Projection rebootstrap is needed.
  An event session ends when the actual source generation is replaced.
- SIGINT/SIGTERM uses an async-safe signal model. Shutdown closes admission,
  drains/cancels server handlers, then stops transports/owners and destroys
  products. Owners are joined, not detached.

## Publication and gRPC

`SubscribeOrderBook` publishes Projection-Applied data. `SubscribeEvents`
requires exactly one V1 selector; Spot supports DIFF_DEPTH / AGG_TRADE /
BOOK_TICKER, USD-M only DIFF_DEPTH. Depth events are
`PRE_PROJECTION_NORMALIZED`.

Per product: at most 8 resident order-book subscriptions and 8 active event
subscriptions; each ordinary queue is 64 with one terminal slot. Slow consumers
are terminalized per session. The process-wide TryCancel tracker is capped at
48 regardless of product count. Expensive unary status is limited to one
in-flight request and does not consume a streaming slot. There is no second
health framework.

`GetGatewayStatus` uses a `min(1 s, client deadline)` observation budget shared
across products; timeout returns `DEADLINE_EXCEEDED`. An infinite RPC deadline
is clamped in system-clock units before conversion. The service drains before
products are destroyed; an in-flight owner operation must return before join.

`SubscribeMarketState` remains unimplemented. The service is insecure gRPC on
numeric loopback only. TLS/auth is a separate delivery item if remote clients
become a real requirement.

`markets[].last_event_utc_ns` is the receive UTC of the last Projection-Applied
Depth in the current valid generation. It is absent when the runtime is not
Live/Synchronized, recovery is not Live, there is no single active transport,
the generation does not match, or the receive time is absent. Zero remains a
present timestamp. This is time evidence, not a freshness SLA; quiet markets do
not automatically Gap/resync.

USD-M depth `E` is event time, `T` is transaction time, trade time stays absent;
`T=0` and missing `T` keep their presence semantics.

## Build and history

Foundation is independently buildable with no network/threads. The production
graph is opt-in:

```sh
-DBMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON
```

Domain tests require the pinned Contracts/Projection/Boost/OpenSSL/Conan graph.
Historical live/acceptance executables are opt-in with
`-DBMD_GATEWAY_BUILD_ACCEPTANCE_TOOLS=ON` and are not linked into
`bmd-gatewayd`. The frozen upstream link smoke remains explicit
(`-DBMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE=ON`).

Dependencies do not use floating FetchContent. Historical G0-G11 evidence and
the old review/proof documents are in `docs/archive/` and
`scripts/archive/`; they describe the old layout and must not be used as current
build instructions.

## Performance and quality

High performance, low latency, low CPU/RSS and stable behavior are delivery
requirements, but no approved multi-product budget exists yet.

P1 evidence (offline, Apple M3, includes fixture/clients) for 20 keys:

| Scenario | CPU cores (mean) | RSS | Notes |
| --- | ---: | ---: | --- |
| normal (~82 updates/s) | 0.115 | ~27 MiB | shallow books, low rate |
| deep (~820 updates/s) | 1.192 | ~41 MiB | deep books, high rate |
| batch (~820 updates/s) | 1.242 | ~53 MiB | triple batch |

Checked apply is 95%+ of owner update wall time. At D1000 it is about 592 µs
and 2023 allocations/update; at D5000 about 1018 µs and 10029 allocations/update.
The dominant hotspot is the pinned Projection `apply_transaction` full-book
copy/rebuild. P2 remains conditional on a measured shared-host budget.

Gateway-side candidates (measure before/after, do not assume):

- avoid per-frame `observe()`/legacy counters/`notify_all`;
- notify only on state change and avoid per-update subscriber sweeps;
- skip event publication/allocation when there are no event subscribers;
- move depth updates into bootstrap/publication instead of deep-copying;
- replace large-object `std::deque` ingress/bootstrap with fixed rings;
- share one SSL context across transports (CA store + session resumption);
- consider a shared Asio reactor if thread/RSS measurements justify it.

Do not add lock-free queues, custom allocators, CPU affinity, busy polling, a
generic event bus/DI/plugin framework, a shared multiplexed transport or a
second sequence classifier without measured evidence.
