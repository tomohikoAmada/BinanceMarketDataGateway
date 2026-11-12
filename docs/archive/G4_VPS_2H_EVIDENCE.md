# G4 — Tokyo VPS co-located 2h acceptance

Date: 2026-11-12. Target: Ubuntu 24.04, x86_64, 4 vCPU, 5.7 GiB RAM.
Recorder C5 remained the existing production process; Gateway and the
consumer probe were added alongside it.

## Deployment identity

| Item | Value |
| --- | --- |
| Gateway commit | `5e47c59c0769de8b16b67f1aabdb095c3bacdf78` |
| CI run | [43195397457](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/43195397457), Linux Ubuntu 24.04 / GCC 13, CTest 27/27 |
| Release artifact | `bmdg-linux-x86_64-5e47c59c0769de8b16b67f1aabdb095c3bacdf78` |
| Archive tar SHA-256 | `defce965c1879efa4cbc4176b831bd5efd190c35fa136721cd1dd250b017e628` |
| `bmdg` SHA-256 | `f57c1a13670f18a59f34c882c98532934f242d596e9e138852a35a78599c105f` |
| `/etc/bmdg/gateway.json` SHA-256 | `b1fa45339f701f471b319bec55884b8f6313e591038b45c4c42a8781abb374a2` |
| `tools/consume_gateway.py` SHA-256 | `7e82154f7738cac18f74bedb95f8c4ea65e121eed5b3e9d48f074945c5e80a66` |
| Consumer monitor SHA-256 | `c29a505f0111a97db8bd335bdcec927971859fe941d20a01550ca8d439f24358` |
| Gateway unit / consumer unit SHA-256 | `b03fb49d…bfd9` / `10c057cd…fe63` |
| Recorder release | `/opt/sbmdr/releases/20261111-c4c-e0b7f684` |
| Recorder C5 verify-only | `PASS` (`recorder-c5-verify-only.json` SHA-256 `0518e5cf…0256`) |
| Recorder restart policy | `Restart=on-failure`, `RestartSec=300` (drop-in SHA-256 `1dd5db53…1b38`) |

The Gateway release is isolated under `/opt/bmdg` with configuration in
`/etc/bmdg`; the Recorder remained under `/opt/sbmdr` and
`/srv/recorder-data`. The Gateway stopped cleanly for the 300 s quiet window at
`09:49:45Z` (`gateway_state=stopped contexts=0 transports=0 subscriptions=0
owners_joined=yes`) and was started from the new release at `09:57:17Z`
(`gateway_state=serving products=20` at `09:57:19Z`). The 40-stream consumer
started at `09:57:33Z`.

## 2h observation

The monitor sampled every 30 s from `10:01:12.803Z` to `12:01:38.601Z`
(7,225.8 s, 240 samples). It did not start or stop either service.

| Check | Result |
| --- | --- |
| Streams | 40/40 accepted, 40/40 valid |
| Products | 20 keys, 20 order books, 20 snapshots |
| Data | 1,239,203 order-book updates; 3,306,529 Spot events |
| Status | 20/20 configured keys seen; 0 missing; 0 identity mismatches |
| Status errors/timeouts | 0 / 0 |
| Protocol / RPC / EOF errors | 0 / 0 / 0 |
| Active subscriptions | 40 |
| Product state | 20/20 Live at every sample |
| Consumer exit | `running`; no restart (`NRestarts=0`) |
| Gateway | active; PID 454133; no restart; 2 startup bootstrap recoveries, no later recovery |
| Consumer | active; PID 454351; no restart; 11 child shard processes |

The strict monitor verdict is **FAIL** because it required `valid == 40` at
every 30 s sample. Two samples reported 38/40 valid (`11:53:32Z` and
`12:00:37Z`) while two BookTicker subscriptions were between gap detection and
resubscription. The next sample was 40/40 both times. The plan's session
correctness clause requires gaps to invalidate the cache, force a
resubscription and recover; it does not require zero gaps. The functional
verdict below follows that clause and keeps the strict monitor output as
evidence.

## Slow-consumer gaps

All 11 gaps were `CONSUMER_GAP_REASON_SLOW_CONSUMER`; there were no
order-book gaps and no protocol, identity or sequence errors.

| Time (UTC) | Gap increase | Affected streams | Host load1 / CPU sample |
| --- | ---: | --- | --- |
| 11:31:19 | +2 | BTC, ETH BookTicker | 18.1 / 270.1% |
| 11:37:53 | +2 | BTC, ETH BookTicker | 19.3 / 291.7% |
| 11:51:30 | +1 | BTC/ETH BookTicker | 14.3 / 279.5% |
| 11:53:32 | +4 | BTC, ETH, DOGE BookTicker | 28.9 / 357.6% |
| 12:00:37 | +2 | BTC, ETH BookTicker | 23.8 / 311.6% |

Final per-stream gap totals: BTC 5, ETH 5, DOGE 1. Every affected stream ended
with `current_valid=true`; the maximum time below 40/40 valid was one 30 s
sample. The gaps correlate with host load/archive peaks, not with Gateway
identity or sequence failures.

The 64-deep event publication queue was raised to **1024** in this release
(order-book queues remain 64) after a live BTCUSDT BookTicker burst delivered
95 messages in 0.19 s and terminalized a 64-deep queue. The queue remains
bounded and still terminalizes on overflow. The consumer uses one order-book
process plus one process per Spot symbol (aggTrade + BookTicker) and an 8 MiB
gRPC receive lookahead.

## Resources

| Metric | Result |
| --- | --- |
| CPU, 10-minute mean | max **297.9%** of one core (line ≤300%); run mean 184.9% |
| CPU, individual 30 s sample | max 391.8% (archive/load burst) |
| MemAvailable | min **3.39 GiB** (line ≥1 GiB) |
| Root free space | min **26.87 GiB** (line ≥15 GiB) |
| Swap | 38 MiB used, stable; no swap-in/out growth observed |
| Consumer RSS | final 323.3 MiB / peak 326.9 MiB |
| Gateway RSS | final 203.3 MiB / peak 205.8 MiB |
| Recorder RSS | spot 180.3 MiB / peak 284.5 MiB; USD-M 305.0 MiB / peak 398.9 MiB |
| OOM | none |

`load1` peaked at 30.8. The high-load windows correlate with
`sbmdr-archive.service` runs (the timer fires roughly every 10 s; each run took
about 3–14 s and exited successfully). No archive failure, timeout or blocked
message was found in the journal. The CPU line is met, but headroom is only
about 2.1 percentage points at the 10-minute maximum; this is recorded as an
observation for the still-open G3-c resource budget, not as a G4 functional
failure.

## Latency

Supplemental snapshot at `12:36Z` (2 h 39 m of consumer data; the 2h monitor
itself stores totals, not per-stream histograms). Values are bucket-approximate
p50/p99, not an SLA.

| Stream class | receive→publish p50 / p99 | publish→client p50 / p99 | max publish→client |
| --- | --- | --- | ---: |
| Order book (20) | 0–1 ms / 10–50 ms | 1–5 ms / 500 ms+ | 27,310.6 ms |
| Spot events (20) | 0–1 ms / 1–5 ms | 10–50 ms / 500 ms+ | 33,422.6 ms |

The publish→client tail includes the host load/archive spikes and gap recovery
windows. No business latency SLA is defined; the tail is recorded for G3-c and
is not hidden by the functional pass.

## Verdict

**G4 functional acceptance: PASS with observations.**

- Identity/data: PASS — 20 keys, 40 accepted streams, 20 snapshots,
  continuous updates/events.
- Session correctness: PASS — 0 protocol/identity/sequence errors; 11
  SLOW_CONSUMER gaps invalidated and resubscribed; no order-book gap; no silent
  generation splice; all streams recovered.
- Recovery/visibility: PASS — gaps are visible in per-stream summaries and
  recovered within one 30 s sample.
- Recorder: PASS — PIDs 442874/442895 unchanged since 08:44, `NRestarts=0`,
  archive timer active and every observed archive run succeeded; C5 verify-only
  remains PASS.
- Resources: PASS against the plan's suggested lines, but with thin CPU
  headroom (297.9% vs 300%) during archive/load peaks. G3-c remains open for an
  approved resource/latency budget.
- Stop/rollback: Gateway SIGTERM shutdown was exercised before the new release
  started; the previous release remains under `/opt/bmdg/releases/` for a
  symlink rollback. A second full rollback run was not performed to avoid
  another 300 s shared-IP cooldown.

Raw evidence is retained on the VPS at
`/var/lib/bmdg-consumer/evidence-g4-20261112T1001Z/` (final verdict, 240-sample
JSONL, start identity, monitor log, unit/config copies and SHA-256 list).
