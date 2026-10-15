# Gateway G1 v0.3 — 2026-10-15

状态：实现完成，离线验证和独立复核进行中。已阅读同级 Milestone Plan 全部六份文档，
依据总计划 BMD-1 和 Gateway G1；新版 G2/G3 尚未启动。

基线：G0 PR #35 merge `e4de33b`（含复核修复 `29e7baa`）。G1 使用全新子代理
独立审查；移除其发现的同步日志阻塞路径后，最终复核无待修复发现。

## 实现与边界

- USD-M Diff Depth `T` → 已有 `exchange_transaction_time_ms`；`E` 仍为 event time，
  trade-time 留空。保留 `T=0` presence、缺失 T 的既有接受策略；非法 T 仍拒绝。
  REST snapshot 已正确映射 transaction-time，补回归；Spot trade 映射不变。
- `RuntimeObservation.last_applied_depth` 记录最近一次 Projection `Applied` 的源接收
  UTC/monotonic 时间与 connection generation。owner 在已有 observation mutex 内提交，
  无新时钟采样/分配/线程。bootstrap 排队、重复、非法、Gap 不推进；恢复和计划 reset 清除。
- 复用现有 `GetGatewayStatus.markets[].last_event_utc_ns`，从 v0.3 G1 起明确表示
  **当前有效 source generation 最后 Applied Depth 的 receive UTC**。仅 runtime
  Live/Synchronized、recovery Live、active transport=1、generation 匹配且 UTC 存在时赋值；
  没有 Applied、缺少来源/时间、Gap/故障/停止/重同步时 absent（unknown）。零时间仍 present。
  内部保留历史 Applied 记录供诊断，reset 清除；不会冒称失效簿仍有可用 freshness。
- 这是对历史 G10 的有意语义收窄：旧 accepted boundary 的“所有已 normalized WebSocket
  事件，跨恢复保留”时间不再由此 RPC 表达；其 transport 记录仍在 recovery.observe 内部保留。
  Contracts 当前 optional UTC 字段未限定事件类别，现行 G1 计划允许复用现有状态。
  当前文档明确迁移，历史记录保持。Wire/字段号/schema 不变，消费者需按本语义解释。
- 客户端可读取时间证据及状态/generation，按需与 observed UTC 对照；这不是免墙钟回退
  的 monotonic age 保证，遇 UTC 回退/未来时间应报告 unknown，不能做 unsigned 相减。
  本轮不发布 age/SLA，不进行自动新鲜度判定。没有同步日志 I/O、side-channel metadata、
  Health 服务或新线程。三个已实现 RPC 与 SubscribeMarketState UNIMPLEMENTED 不变。
- 无年龄阈值、自动 Gap、安静市场重同步、Health 服务或新配置；八产品容量、四产品正式
  组合、production CI、VPS/API/性能验收均未因此完成。Contracts/Projection/Recorder 源码与 pin 不变。

## 可复核测试

共同 oracle 是 Contracts C0 工作树的固定副本，来源、版本和 SHA-256 见
[fixture 说明](../tests/fixtures/README.md)。这份 C0 尚未提交，不能冒称为已发布 package。

- 协议测试消费全部三种 E/T presence case、五种非法 T patch 和错误 metadata 反例，
  检查 U/u/pu、尾零、删除量和 binary round-trip；REST transaction-time 保持。
- runtime 测试覆盖无订阅/未 Applied、buffer、Applied=0、duplicate、Gap、不变历史、
  recovery reset、缺失 receive time、generation=2 和 planned reset。
- Status 测试使用实际 Spot trade/ticker parser→EventPublication，transport.last_event 更新时
  RPC Depth 时间仍不动、相对 observed UTC 变旧；缺失/零时间/未知来源/Gap 的 presence 正确，
  安静期间仍 Live 且无 reset。现有 gRPC generation cut 回归检查换源后两市场各自时间。
- ProductionGateway 实际 parser→独立 ProductRuntime/Projection→loopback Book/Event RPC
  比较整个 DepthUpdate，含 T 值/零/缺失及 generation；另一市场记录保持不变。

## G0 独立复核补修

PR #35 的 `df8a7e2` 发现 recovery lifecycle gate 等 owner reset 时，旧 recovery.observe
仍可阻塞 Status/startup。`29e7baa` 增加 recovery deadline/cancel 观察，保持 reset/shutdown
锁顺序；Status 与 production observation 共享原 deadline，startup 非阻塞检查。
独立子代理使用原 20ms 探针确认不再挂起，五组重点 Release CTest 5/5 通过；
完整 production Release CTest 22/22，12.49s，格式通过。
本机证据：`build/v03-g0/review-fix-ctest.log`、
`build/v03-g0-independent-review-20261015/repro-after-fix.log`。

## 构建与结果（待完成后填写）

复用固定 `build/g7-conan/conan_toolchain.cmake` 二进制依赖，显式 production daemon、
历史 G2 synthetic 与 G8 integration 开关，测试全开；未冷构建上游、未重做历史 G1 proof。
Release、相关 sanitizer、Foundation 与格式检查的精确结果将在运行后记录。
所有 runtime 验证均为本机离线/loopback，未连接 Binance 行情或访问 VPS。

已完成：production Release 全图 22/22（4.11s），独立 Foundation 1/1（0.08s），
`scripts/format-check.sh` 和 `git diff --check` 通过。各日志在 ignored `build/v03-g1/`。
ASan+UBSan、TSan 全 production 图及重点重复检查进行中。

复现命令：

```sh
cmake -S . -B build/v03-g1/release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$PWD/build/g7-conan/conan_toolchain.cmake" \
  -DCMAKE_OSX_SYSROOT=macosx -DBMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON \
  -DBMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST=ON \
  -DBMD_GATEWAY_BUILD_G8_INTEGRATION_ACCEPTANCE=ON -DBMD_GATEWAY_BUILD_TESTS=ON
cmake --build build/v03-g1/release --parallel 4
ctest --test-dir build/v03-g1/release --output-on-failure
```

ASan+UBSan 使用同样开关、独立 `asan-ubsan` 目录并增加
`-DBMD_GATEWAY_ENABLE_ASAN=ON -DBMD_GATEWAY_ENABLE_UBSAN=ON`；TSan 使用独立 `tsan`
目录、`-DBMD_GATEWAY_ENABLE_TSAN=ON`；两者 build parallel 2。Foundation 独立目录
不启用 production/upstream 图。Sanitizer 覆盖本仓源码，固定上游静态库未重新 instrument。
message RREV `53713fb82c27cf6ea0395f6b8a853006`、gRPC RREV
`c3661aa735e1eb6450c1a93f635dbc20`、Projection RREV `d95fa71d6dca8d931e72fbb5b74114a9` 不变。

2026-10-15 再核对官方 [USD-M public Diff Depth 定义](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams)：
E 是 event time，T 是 transaction time，与 Contracts C0 oracle 一致。
