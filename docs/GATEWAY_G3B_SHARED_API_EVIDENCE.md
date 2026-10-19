# G3-b / Recorder R3-b shared-IP offline evidence

2026-10-19 — implementation, joint production-policy proof and new scoped review
APPROVED. Final validation/PR status is recorded below. G3-c/live qualification
and BMD-4 remain open; there was no VPS or Binance API access.

## Minimal production change

The process-global public API budget assigns Gateway3000 Spot weight/60s,
600 USD-M weight/60s and60 WS connection attempts/300s per market. Recorder's
companion shares are2000/1200 weight,180 WS attempts per market,400 funding
requests/300s and800 aggregate statistics requests/300s. Combined ceilings are
5000/1800 weight and240 attempts; no IPC/coordinator, service or dependency pin
is introduced. Full snapshot limits remain Spot5000/USD-M1000.

Each HTTP write and each WS DNS/connect start rechecks a rolling window. Every
attempt counts, including failures; short Live or transport replacement cannot
reset it. The mutex spans product owners, while actual waiting uses the existing
cancellable Asio timer. HTTP/WS429/418 source status/Retry-After is retained and
installs a market deadline; a shorter response cannot reduce it. Metadata is
synchronous and finite: unavailable quota/cooldown returns RuntimeAdmission,
checked before DNS and again at the actual HTTP write. WS control-message policy
is unchanged: no client keepalive Ping or dynamic subscription traffic, only the
existing required server Ping response; incoming market frames are not controls.

## Evidence scope

`bmd_gateway_public_api_budget_tests` verifies rolling minute/5minute boundaries,
market isolation, concurrent admission and shared rejection deadlines. Its
`--trace` mode executes the same class and production route-weight function in
an independent C++ process. Recorder's companion Python process executes its real
budget/cooldown classes. The joint tool merges both admission traces and checks
all windows and cooldowns in five schedules: cold start,100 repeated single-key
recoveries,12 whole-set short-Live recovery rounds, heavy side catch-up and429/418.

Combined observed maxima:5000 Spot weight/min,1800 USD-M weight/min,240 connection
attempts/5min in each market,400 funding/800 statistics per5min. Cold start costs
3220 Spot/422 USD-M weight and40 Spot+60 USD-M connections. All operations remain
accounted for; surplus work waits. This is policy composition, not a real-network
transport campaign. Production entry/cancel tests and existing20-key actual
runtime recovery fixtures separately cover binding and recovery semantics.

Companion [Recorder proof and commands](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/codex/shared-api-budget/docs/milestone_acceptance/R3-b-G3-b-shared-api-20261019.md)
and [scoped review](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/codex/shared-api-budget/docs/reviews/2026-10-19-shared-api-budget-review.md)
record both repositories and the saved per-scenario result.

## Deployment prerequisites, not live credit

Exactly one instance of each role, no blue/green overlap, and a complete300s quiet
interval after stopping/replacing a process are required because counters are in
memory. Preserve any longer known server ban and set the eventual service restart
delay accordingly. Unknown consumers or restart loops are outside the proof;
other users must fit the explicit residual shares. Check current metadata limits
and used-weight before deployment:6000/2400 are reference minute limits, not
permanent constants. Spot's official300/5min attempt limit leaves60; the USD-M
240 local ceiling is conservative and does not assert an official300 limit.
Recorder REST rejection retains REST-only cooldown; its WS handshake rejection
blocks market WS+REST. Gateway uses its market gate for both.
No deployed R2 source/configuration or restart policy was changed.

## Validation and delivery

- [x] New scoped independent code review APPROVED; all concrete findings closed.
- [x] Full production Release27/27; format/diff PASS. Actual loopback-only
  transport budget-wait stop and metadata finite return are included.
- [x] Complete ASan27/27 and focused TSan4/4 (budget, real transport,
 20-product production integration, production regression) PASS.
- [ ] UBSan and final PR/CI/merge closeout.
