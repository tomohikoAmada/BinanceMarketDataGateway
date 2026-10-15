# Shared Contracts C0 source oracle

`usdm-depth-time.v1.json` is a byte-identical copy of
`BinanceMarketDataContracts/fixtures/integration/usdm-depth-time.v1.json`,
fixture version `contracts-c0.usdm-depth-time.v1`, checked 2026-10-15 against
the sibling Contracts C0 working tree (base `8701c330`; C0 was uncommitted).
SHA-256: `9f56c86077b5636fd94634ab21e6d851a6a08922d951ca047c64c83714bf1a0b`.

This small test oracle keeps Gateway tests independent of a sibling checkout.
It contains no copied proto or runtime dependency. Update it explicitly with
Contracts if that oracle changes. Protocol tests consume all source cases and
negative patches; production integration continues its existing synthetic Live
baseline by changing sequence/price levels, preserving the oracle's E/T values
and presence, then compares the entire parsed DepthUpdate through both RPCs.
