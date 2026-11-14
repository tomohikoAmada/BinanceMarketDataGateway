# BMDG Python Client & Consumer Examples

This directory shows how downstream strategy or market-data consumers connect
to an already running `bmdg` daemon over gRPC.

## Dependencies

Install the pinned client libraries and add the generated protobuf/gRPC modules
from `BinanceMarketDataContracts` (`24a209b1a415c4f4dd56d864eee9132135bc7237`):

```sh
python3 -m venv .venv-client
.venv-client/bin/pip install -r ../../tools/requirements-client.txt
export PYTHONPATH=/path/to/BinanceMarketDataContracts/src
```

On the deployment host, `/opt/bmdg/current/client-venv` and
`/opt/bmdg/current/contracts-src/src` already provide these dependencies.

## 1. Minimal standalone client (`minimal_client.py`)

`minimal_client.py` demonstrates the core gRPC contract on one product
(default: `usdm` `BTCUSDT`):

- Queries `GetGatewayStatus` before subscribing.
- Subscribes to `SubscribeOrderBook` (`order-book-subscription-request.v1`),
  applies `snapshot` + incremental `depth_update` messages into a local L2 map,
  and clears/resubscribes on `consumer_gap` or `CLOSED` lifecycle status.
- Subscribes to `SubscribeEvents` (`event-subscription-request.v1`) for
  `AGG_TRADE`, `BOOK_TICKER`, and (on USD-M) `MARK_PRICE`, `LIQUIDATION`, and
  `REFERENCE_DATA`.
- Enforces typed `REFERENCE_DATA` semantics: validates `source_kind` vs
  `oneof payload`, distinguishes `VALID` / `EMPTY` / `ERROR` (never replacing a
  previously cached `VALID` value with `ERROR`, and never treating `ERROR`-only
  as fresh), and checks `600s` receive / `1200s` periodic source age and `48h`
  instrument definition age.

Run for 15 seconds against a local or SSH-forwarded `bmdg`:

```sh
PYTHONPATH=/path/to/BinanceMarketDataContracts/src \
  .venv-client/bin/python minimal_client.py \
  --target 127.0.0.1:50051 \
  --market usdm \
  --symbol BTCUSDT \
  --duration-seconds 15
```

## 2. Production sharded consumer (`tools/consume_gateway.py`)

For consuming the full five-pair, 45-stream profile (`BTCUSDT`, `ETHUSDT`,
`SOLUSDT`, `BNBUSDT`, `XRPUSDT` across Spot and USD-M) without slow-consumer
head-of-line blocking on bursty `book_ticker` streams, use the sharded
supervisor client in [../../tools/consume_gateway.py](../../tools/consume_gateway.py):

```sh
PYTHONPATH=/path/to/BinanceMarketDataContracts/src \
  .venv-client/bin/python ../../tools/consume_gateway.py \
  --target 127.0.0.1:50051 \
  --config ../gateway-production.json \
  --profile configured \
  --supervisor \
  --duration-seconds 0 \
  --output /tmp/bmdg-consumer-summary.json
```

See [../../tools/README.md](../../tools/README.md) for the 11-shard topology
(1 order-book process + 5 Spot symbol processes + 5 USD-M event+reference
processes = 45/48 streaming contexts). Remember to stop any test probe
(`bmdg-consumer.service`) before connecting an external strategy client so the
process-wide 48-stream cap is respected.
