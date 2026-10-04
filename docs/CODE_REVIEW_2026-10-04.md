# Gateway code review — 2026-10-04

审查起点：`feat/g12-c-production-composition`，commit `9340b99f3cbb3ceb1d88bc083feae83ac5a1a760`，对应 PR #33；当时 main 尚未包含 G12-C。因此旧文档和分支代码的差异需要分开判断。

修复实现 commit：`5689c453661a8033e195afd18bd5fea819b8611c`。计划/状态文档随同一 PR 合并。

结论：已有多产品 owner/registry 架构适合当前目标，不需要重写。配置生产组合已经实现，不能再称为“只有固定双 BTC”；也不能据此称为“四产品真实交付已完成”。本次修复三个可复现问题，并以新 [milestone plan](MILESTONES.md) 统一后续推进。

## 审查范围

检查 Foundation/config、G2 synthetic、G3 owner/ingress/bootstrap、Spot/USD-M 协议与 transport、recovery/rotation、order-book/event publication、gRPC routing/context cancel、status、configured product set、生产配置/metadata/startup/signal/shutdown、性能 instrumentation、CMake/Conan/CI 及对应测试。此审查和离线测试不等于穷尽所有网络/平台行为。

参考 Recorder 的 [README](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/main/README.md) 和 [可配置产品 ADR](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/main/docs/adr/0032-configurable-product-set.md)：复用“启动时自选 Spot/USD-M 产品”的能力目标；其存储、collector/writer 组织不进入 Gateway。Gateway 保留精确 MarketKey 与当前有界产品政策。

## 错误与缺口

| 优先级 | 问题、触发与后果 | 处理 |
| --- | --- | --- |
| P1 | `MarketRuntime::start` 在创建 owner thread 前标记 started。pthread_create 返回 EAGAIN 时没有 owner，但 publication shutdown 等待它的确认，导致生产对象清理挂住；生产 start 也没有统一的异常回滚 | 已修复：成功创建 thread 后提交 started；ProductionGateway start 捕获异常、完整 rollback 后重新抛出。测试让第二产品创建 owner 失败，验证第一产品与失败产品全部停止 |
| P2 | 验收客户端启动 child 使用 `--grpc-listen`，配置生产 daemon 已只接受 `--config`；它还要求 serving 日志中的旧 spot/usdm generation 字段 | 已修复：传同一 config 文件、验证其双 BTC 验收 profile 与端口、使用当前 products/port/instance 日志字段，并从初始 status 取得真实 generation。补 CLI/profile/serving 解析测试 |
| P2 | Spot combined parser 只认三个产品 stream，官方 `!serverShutdown` envelope 被误报 WrongEvent，恢复虽然仍可发生，但丢失正确停机原因 | 已修复：显式识别官方 stream 与事件/时间类型；保留未知 stream、错误事件和无效类型拒绝测试 |
| P2 | GitHub CI 与 sanitizer jobs 均未启用 runtime/生产图，绿灯只证明 Foundation，不覆盖本 PR 的生产代码 | 本机完整图已验证；M3 必须补固定依赖的 production CI，尚未完成 |
| P2，兼容风险 | 上游事件使用 exact-key/allowlist 校验；合法事件增加无关字段也可能触发协议失败/恢复。离线扩展字段 probe 证实当前会拒绝，但没有证明真实币安已经发生这个问题 | M3 小范围修改为“必需字段严格、无关扩展容忍”。自身配置继续严格拒绝未知字段；身份、类型、数值、序列检查保留 |

官方 combined 停机格式来自 [Binance Spot WebSocket 文档](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)，本次核对过原始文档。协议格式测试通过不等于观察到真实服务端停机。

主要代码位置：`src/g3/market_runtime.cpp`、`src/production/production_gateway.cpp`、`app/production_daemon_acceptance/acceptance_config.hpp`、`app/production_daemon_acceptance/main.cpp`、`src/g4/spot_protocol.cpp`、`.github/workflows/ci.yml`。

## 明显安全的优化

- 已删除 combined depth 内层 JSON 的 `dump()` 再 parse。raw/combined 共用内部 Json 对象解析函数，保持同一验证规则，不暴露 Json 到公共头文件。少一次序列化和解析；没有据此声称具体延迟收益。
- `production_metadata.cpp` 每市场只获取一次 exchangeInfo，但每个 symbol 都重新解码整个 body。最多八个产品、只在启动发生；可在需要时改成每市场解码一次并逐 symbol 选择。不引入缓存框架，也不把它当作行情热路径瓶颈。
- CMake 的大量历史阶段条件重复有维护成本。后续涉及构建时提取少数目标设置 helper，并让生产 CI 覆盖实际图；不做与目标无关的大规模构建系统重写。

## 过度设计与必要防御

明显需要缩减的是文档中大量失效的固定产品/授权状态和分散的重复阶段描述，以及对外部 JSON 非必需字段的过严拒绝。已精简当前状态、架构、AGENTS 与计划，旧计划另存用于追溯。

现有稳定 heap owner、小规模线性 registry、单 owner Projection 边界和独立 transport 是简单的可行方案。有界队列、terminal slot、slow-client 隔离、TryCancel 生命周期握手、初始全体就绪、完整回滚和先 drain handler 的退出顺序解决真实问题，必须保留。

目前没有证据需要 lock-free、busy polling、CPU affinity、allocator、通用 worker/event bus/DI/plugin framework。共享多 symbol transport 会增加 source generation、恢复和故障隔离耦合，当前目标不需要它。future optimization 应从测量到的瓶颈出发。

## 验证与证据范围

使用本机 AppleClang / macOS arm64、CMake 4.3.2 和当前 SDK，新建 ignored build 目录，使用已有固定 recipe 的 Conan 依赖；旧 build 的失效 SDK 路径没有通过改代码绕过。

生产图开关：`BMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON`、`BMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST=ON`、`BMD_GATEWAY_BUILD_G8_INTEGRATION_ACCEPTANCE=ON`、`BMD_GATEWAY_BUILD_TESTS=ON`。具体配置/构建命令见 README。

- Release/offline：22 个 CTest 组，包括新增验收配置测试与线程创建异常回滚测试。
- ASan、UBSan、TSan：各启用对应 flag，在完整生产图各通过同一组 22 个离线测试。
- instrumentation ON：通过 23 个离线 CTest 组，验证 performance baseline 编译与测试；不采集新的 live 性能数据。
- `scripts/format-check.sh`、`git diff --check`：通过。

故障复现：在独立测试进程中用 DYLD interpose 让下一次 pthread_create 返回 EAGAIN。修复前在 gateway destruction 等待超过三秒，测试进程被有界终止；修复后同一 pthread_create EAGAIN probe 完整退出，exit code 0。提交的可重复回归使用现有内部 RuntimeTestOptions 增加创建前异常注入，不依赖 macOS interposer。

验收客户端另经 synthetic child startup-failure smoke 验证：child 收到新配置参数，报告预期 product-initial-failure，没有旧选项错误。此 smoke 不作为真实行情验收。

本机日志/probe 位于 ignored `build/review-g12-c*`。Sanitizer 编译覆盖 Gateway 源码；已有 Conan 上游静态库未重新 instrument，不能把结果写成整个依赖栈的 sanitizer 证明。本次没有进行真实 Binance 网络或四产品容量测试。

## 完成边界

本次关闭 M1 的已复现缺陷和 M2/G12-C 配置生产实现。下一步是 M3/G12-D 四产品生产组合验收与生产 CI；随后 M4/G12-E 真实有界验收。`SubscribeMarketState` 仍未实现，不宣称完整 V1 服务或生产资格全部完成。
