# Code map

The source tree is organized by domain. Historical `G0`-`G11` milestone labels
are kept only in archived documents and git history; they are not directory
names or namespaces.

## Historical stage to domain

| Historical stage | Domain | Current location | In production daemon |
| --- | --- | --- | --- |
| G2 | synthetic Spot test host | `tests/support/synthetic_spot_host.*` | no |
| G3 | runtime / clock / provenance | `src/runtime/` | yes |
| G4 | protocol + Binance transport | `src/protocol/`, `src/transport/` | yes |
| G5 | recovery coordinator | `src/recovery/` | yes |
| G6 | planned rotation policy | `src/recovery/planned_rotation.hpp` | yes |
| G7 | order-book publication + gRPC | `src/publication/`, `src/grpc/` | yes |
| G8 | Projection integration acceptance | `tests/integration/` | tests only |
| G9 | event publication | `src/publication/event_publication.*` | yes |
| G10 | gateway status | `src/status/` | yes |
| G11 | configured products / registry / USD-M | `src/composition/`, `src/protocol/usdm_protocol.*`, `src/transport/usdm_transport.*` | yes |
| production | daemon composition | `src/production/` | yes |

## Namespaces

| Namespace | Responsibility |
| --- | --- |
| `binance_market_data::gateway::v1` | public Foundation config/lifecycle API |
| `...::runtime` | `MarketRuntime`, `MarketKey`, clock, provenance |
| `...::protocol` | Spot/USD-M JSON parsers and wire metadata |
| `...::transport` | Binance REST/WebSocket transport and API budget |
| `...::recovery` | reconnect/resync coordinator and rotation policy |
| `...::publication` | order-book and event subscriber channels |
| `...::rpc` | gRPC service/server (`grpc` would collide with the gRPC library) |
| `...::status` | `GetGatewayStatus` assembler |
| `...::composition` | `ProductRuntime`, configured set and registry |
| `...::production` | daemon config, metadata, startup/shutdown and signals |
| `...::testing` | synthetic Spot host used by tests |

## Build targets

- `bmdg_foundation`
- `bmdg_protocol`
- `bmdg_publication`
- `bmdg_runtime`
- `bmdg_transport`
- `bmdg_recovery`
- `bmdg_composition`
- `bmdg_status`
- `bmdg_grpc`
- `bmdg_production`
- `bmdg`

Tests are named after the domain they cover, e.g.
`bmdg_protocol_tests`, `bmdg_transport_tests`,
`bmdg_recovery_tests`, `bmdg_grpc_service_tests`,
`bmdg_production_tests`.

Compatibility: old `bmd_gateway_*` library target names are aliases; old
`BMD_GATEWAY_*` CMake options are deprecated aliases; `bmd-gatewayd` is a
transition symlink/custom target for `bmdg`.

## Historical naming note

Old documents use `G3` for the serialized `MarketRuntime`. Current v0.3/v0.4
planning also calls its third phase `G3` (shared load, API budget and
delivery). The two meanings are unrelated. Use this map, not the old labels.
