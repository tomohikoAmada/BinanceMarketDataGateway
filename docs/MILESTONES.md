# Gateway milestone plan

更新日期：2026-10-19。当前执行依据是同级 `Milestone Plan` 目录中的
`01_BinanceMarketData_总体_Milestone_Plan.md` 和 `03_Gateway_Milestone_Plan.md`
（v0.4，保留v0.3已完成事实）；已阅读该目录全部六份文档。本页记录本仓执行状态与边界。
历史 G0–G11、G12-A/B/C 已完成，新版 G0–G3 编号不代表重做历史阶段。
历史链接证明与审查记录保留，不因重新编号改写 PASS/FAIL。

## 当前四阶段

| v0.3 阶段 | 工作与验收 | 当前状态 |
| --- | --- | --- |
| G0：安全、协议兼容与可用性 | G01 数值 loopback；G03 无关 JSON 扩展兼容且身份/类型/序列不放宽；G05 owner 观察/Status/startup 有界且生命周期安全；G10 测试 RAII join/有界同步 | 完成；2026-10-15 独立复核补修 reset gate 观察，PR #35 已合并；[证据](GATEWAY_G0_V03_EVIDENCE.md) |
| G1：数据语义与最小新鲜度 | USD-M T transaction-time 映射按共同 fixture 修正；Status last_event 明确收窄为当前有效 Applied Depth receive UTC；不增加 Health 服务或安静市场强制重同步 | 完成实现、完整离线验证及独立复核（2026-10-16）；沿用 Wire/依赖 pin；[证据](GATEWAY_G1_V03_EVIDENCE.md) |
| G2：四产品生产路径与最小 CI | Spot BTC/ETH + USD-M BTC/ETH，JSON→metadata→spec→各自 transport/Projection→Book/Event/Status；故障与慢消费者隔离、48 contexts、活动 RPC 退出、失败回滚；Foundation 之外真实 production build/smoke | 完成（2026-10-18）；完整离线及 sanitizer、独立复核、Linux/main/PR CI 通过，PR #37 已合并；[证据](GATEWAY_G2_V03_EVIDENCE.md) |
| G3：共同负载、API 联合预算与交付 | v0.4先完成20keys G3-a离线配置/路由/启动恢复/48 contexts与P1；G3-b完成BMD-RL-01；G3-c冻结现实资源预算并在授权同VPS真实负载下验收 | G3-a完成离线验证、独立复核及production CI，PR #38交付；[证据](GATEWAY_G3_OFFLINE_EVIDENCE.md)。G3-b/G3-c与原M4/G12-E未关闭 |

2026-10-15 用户授权完成 G1，并独立复核待审分支。2026-10-18 用户授权 G2；四产品离线组合、production CI、独立复核与合并交付完成。G3 生产窗口仍单独安排。

## 保留的实现与设计边界

- Gateway 管 Binance transport、metadata、生命周期、有界 publication 和 gRPC；
  Contracts 管 wire；Projection 是定点数、order book 和 Spot/USD-M sequence（包括 pu）的唯一权威。无 Recorder 依赖。
- 复用 ConfiguredProductRuntimeSet 和不可变 registry。每个精确
  `(venue, market, exact symbol)` 产品独立 owner/private Projection/recovery/transport；
  1..20 是资源政策，二十产品不是已测生产容量。
- 启动配置 `bmd-gatewayd --config PATH`，全体首次 Live/Synchronized 才服务。
  返回失败或异常全部回滚；运行后产品故障隔离；server handler drain 后销毁产品。
- 保留有界队列、slow-client 隔离、取消 lifetime 握手、source quiescence 和 async-safe 信号。
  不加 hot reload、共享 multiplexed transport、event bus、DI/plugin、第二分类器或限流 daemon。
- Spot events 为 DIFF_DEPTH/AGG_TRADE/BOOK_TICKER；USD-M 为 DIFF_DEPTH。
  Book 为 Projection-Applied；depth Event 为 pre-Projection；session 不跨 source replacement 拼接。
  SubscribeMarketState 保持 UNIMPLEMENTED。
- 无认证 gRPC 只绑定数值 loopback；配置未知字段仍拒绝，交易所无关扩展容忍。
  owner 观察超时返回错误/无结果，不以陈旧状态伪报 Live；仍安全 join，不 detach。

## 按影响范围验证

日常使用一个 production Release build 和必要离线 smoke/重点回归；使用固定 Conan 二进制依赖。
涉及并发、生命周期、内存或数值边界时做对应 sanitizer/序列回归；里程碑或候选发布做一次完整离线组合与必要 sanitizer。
G0 在生产图执行 Release、ASan、UBSan、TSan；G1 执行 Release、ASan+UBSan、TSan 全部 22/22，重点四组各重复十次、格式通过，并保留独立 Foundation 1/1。

历史 G1 四包/link/message-lineage proof 只在依赖打包、ABI 或导出边界实质变化时复跑。
不要求每 PR 冷编全部 Protobuf/gRPC/Boost/Projection/Contracts，不把线上长跑放进普通 PR。
G2 已补单一 production Release CI；普通 PR 只读取固定二进制缓存，首次准备为手动入口。最终 GitHub/main 验证见 [G2 证据](GATEWAY_G2_V03_EVIDENCE.md)。

## 质量与交付仍需证明

高性能、安全稳定、低延迟、低 CPU/RSS 是交付要求；尚无批准的多产品数值预算。
历史固定双 BTC 性能不能替代四产品生产证据。G2 复用已有 instrumentation/replay 准备可重复负载；
G3 在固定硬件、簿深、批大小、产品/订阅负载下测延迟分位、CPU/RSS、队列和恢复后的资源清理。
T5 是 server Write 返回，不代表客户端处理完成；有限窗口不能证明无限时长无泄漏。

Projection accepted batch 的全簿复制/重建是待测热点，
[配套评估](PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)尚未证明它阻碍四产品预算。
多个产品不需要新增 Projection API；有实际性能/预算依据再在 Projection 单独实施局部优化。
本轮无跨仓改动或 package repin。

BMD-RL-01 属 G3 的联合交付要求：按 Spot/USD-M 各自窗口列实际 endpoint/参数/weight，
合并 Recorder 与 Gateway 同启、单断、四产品同时恢复和短 Live 后重复断线的预算。
保留 429/418/Retry-After 和 fail-closed；先用静态份额/现有进程内机制及必要错峰。
生产观察须有授权窗口，不主动在真实 Binance 打出限流，不干扰 Recorder 冻结资格。

历史依据：[2026-10-04 审查](CODE_REVIEW_2026-10-04.md)、
[旧阶段计划](MILESTONES_PRE_REVIEW_2026-10-04.md)、
[历史 G0 Foundation](G0_EVIDENCE.md)、[G1 proof](../scripts/g1-candidate-proof.sh)。

## Projection P1 measurement support — 2026-10-18

Owner-authorized offline attribution reuses the G2 configured production graph.
The opt-in trace/export extension and test-only fixed-depth/paired-campaign
driver are documented in [performance instrumentation](PERFORMANCE_BASELINE_INSTRUMENTATION.md).
This is Projection P1 evidence preparation, not completion of Gateway G3,
real Binance networking, Recorder co-deployment or resource/API budget approval.

## v0.4 twenty-key offline P1 increment — 2026-10-19

The owner authorized finite twenty-product support and mixed-load attribution.
Config, runtime-set preparation and registry now share the 20-product bound; 21
is rejected. The existing four-product integration source also runs with a frozen
10 Spot + 10 USD-M research profile: exact decimal precision, private owners,
Book/Event/Status identity, local recovery, all-products-live startup/rollback,
40 baseline streams and the unchanged global 48-context cap are verified offline.
Release OFF 26/26 and ON 27/27 pass; paired smoke passes. Fresh independent code/method review is APPROVED. ASan+UBSan 25/25 and focused
TSan 6/6 pass, including final metadata-error test validation. Full mixed-load
measurements are complete: eighteen heterogeneous OFF/ON trials, 127,920 updates
and 255,840 client deliveries, with independent quantitative/hash/archive review
APPROVED. OFF CPU means normal/deep/batch are 0.115/1.192/1.242 cores on the local
M3, including fixture/clients. Tail variability (Book maximum 102.404 ms) is retained;
this is not a latency SLA. Technical test-correction commit `7266c47` passed all
seven ordinary jobs in CI `39145975891`, including Linux production 24/24.
Measured commit `8ce41be` and binaries remain bound to the raw campaign; later
test-only corrections do not alter production/measurement code.
This increment does not complete BMD-RL-01, approve a deployment symbol list,
or qualify shared-VPS/Recorder production capacity.

## G3-a offline closeout — 2026-10-19

- [x] Reconciled all six v0.4 plans with the existing PR #38 implementation; kept G0/G1/G2 historical scope.
- [x] Verified 20 accepted / 21 rejected at configuration, preparation and registry; no new runtime framework or upstream pin.
- [x] Verified exact 20-key parser/Projection/Book/Event/Status routing, per-market metadata reuse and heterogeneous precision.
- [x] Kept the process cap at 48: 40 baseline streams, 48 accepted, 49th RESOURCE_EXHAUSTED, cancel/reuse and final zero contexts; per-product limits unchanged.
- [x] Added explicit startup elapsed bound and deterministic late-Live/shared-deadline coverage using existing seams; all 20 failure positions roll back.
- [x] Added two rounds of all-product recovery, short Live between rounds, old-session Gap and new-generation subscriptions; max one transport per key.
- [x] Fresh independent code review and final test increment review APPROVED.
- [x] Release OFF 26/26, ON 27/27, ASan+UBSan ON 25/25; independent Foundation 1/1 and format/diff checks pass.
- [x] TSan ON focused concurrency/lifecycle 6/6 (five groups plus production regression).
- [x] Final technical commit `b4d3625`: Linux production 24/24 and all seven ordinary jobs pass on both push/PR runs `39180480384` / `39180483378`; reviewed PR #38 delivers the increment (final head/merge record at the PR).

The bounded profile is one Book + one Depth Event per key (40 contexts). A 60-stream
all-supported-events profile is outside the retained 48 cap. BMD-RL-01/G3-b and
shared-VPS/API/live/resource qualification remain OPEN; see [G3-a evidence](GATEWAY_G3_OFFLINE_EVIDENCE.md).


## 2026-10-19 G3-b shared-IP offline implementation

- [x] Gateway fixed rolling shares3000 Spot/600 USD-M weight per minute and60
  WS attempts per5min per market; actual request/connection admission and shared
  429/418 gate, bounded metadata and clean cancellation implemented.
- [x] Recorder companion policy and five two-process composition scenarios PASS;
  production binding/stop tests and existing20-product recovery cover separate layers.
- [x] New scoped code review APPROVED; complete Release27/27, format/diff PASS.
- [ ] Final sanitizer/PR/CI/merge delivery.

[Evidence](GATEWAY_G3B_SHARED_API_EVIDENCE.md) states assumptions and validation.
G3-c/ordinary live observation, actual metadata/current IP users, target VPS
resources and BMD-4 remain OPEN. No production/VPS/R2 operation occurred.
