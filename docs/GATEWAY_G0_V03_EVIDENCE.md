# Gateway G0 v0.3 — 2026-10-12

状态：实现、本机离线验证及 2026-10-15 独立复核完成，PR #35 已合并。本记录的 G0 是 2026-10-12
BinanceMarketData v0.3 总计划/Gateway 模块计划中的安全与可用性修复，
不是 [历史 G0 Foundation](G0_EVIDENCE.md)。已阅读 `Milestone Plan` 全部六份文档。

2026-10-15 独立复核发现并补修 G05：恢复/轮换持 lifecycle gate 等待 owner
reset 时，原 `recovery.observe()` 仍可能阻塞 Status/startup。新增 recovery
`observe_until(deadline, stop_token)`，保留 reset/shutdown 的互斥顺序；Status
与 production 整组观察共用已有 deadline，startup 非阻塞检查 gate。
回归将 owner 暂停在 planned reset 中，确认 runtime FIFO barrier 已可通过时，
Status 仍遵守 20ms deadline，并可由 stop token 取消；RAII watchdog 保证失败清理。
补修 `29e7baa`：完整 production Release 22/22（12.49s）、格式/diff 检查通过；
独立子代理复核通过，五组重点测试 5/5，原探针现在遵守期限和取消。
日志见 `build/v03-g0/review-fix-ctest.log` 和
`build/v03-g0-independent-review-20261015/repro-after-fix.log`。PR #35 已合并。

## 基线与去重

开始时工作树干净；`git fetch origin main` 后 main 与 origin/main 均为
`fb4c8606a6023795d2f14dbe64426d5e863c2ca0`，与 v0.3 审查基线一致。
核对 [2026-10-04 修复证据](CODE_REVIEW_2026-10-04.md)及已合并
`5689c453`：线程创建失败回滚、验收客户端参数及 combined serverShutdown 已修，未重复开发。
已有 ignored `build/independent-review-20261011/probe-{config,protocol,stall}.log`
与当前源码一致：wildcard/公网配置可被接受、扩展字段被拒、owner 操作停顿可阻塞观察。

## 改动与对应正负例

| G0 项 | 最小改动 | 提交的回归证据 |
| --- | --- | --- |
| G01 | 复用已有 Boost 数值 IP parser；JSON 配置、直接 ProductionGateway 构造和 gRPC 绑定只接受 IPv4 127/8 或 IPv6 ::1 | `production_config_metadata_tests` 接受 127.0.0.1/127.4.3.2/::1/展开式 ::1，拒绝 wildcard、公网/私网、hostname、伪 loopback、CLI port 0；production 构造在产品启动前拒绝；gRPC server 拒绝后 selected_port=0，随后可正常绑定 loopback |
| G03 | exact/allowed keys 改为 required keys；未知无关扩展忽略，已有类型/身份验证保留 | Spot raw/combined depth、aggTrade、bookTicker、shutdown、snapshot，USD-M depth/shutdown/snapshot 扩展对象/数组通过；必需字段缺失/null、错误 symbol、坏数值类型仍拒；Spot pu 仍拒；USD-M 带扩展的倒置 U/u、缺 pu、错误 pu 经现有 Adapter/Core fault/NeedsResync，last_update_id 不前进 |
| G05 | observe_until 返回 optional owning copy，普通 observe 最多等一秒后抛错误；Status 共用 min(一秒, client deadline)，关闭以 stop token 取消；启动检查使用不等待的 barrier | owner 暂停/已在 clock 回调内停顿时观察可超时；取消可唤醒；超时后 owner 继续处理并安全 join；真实 loopback RPC 无客户端 deadline 也返回 DEADLINE_EXCEEDED，50ms deadline 被遵守；随后可成功取 Status；暂停 owner 的 production startup 30ms timeout/10ms stop 均回滚且 owner/transport/context 清零 |
| G10 测试清理 | 原两处 join 前 assert 改为 std::jthread；200 次 yield 改为按 steady_clock 的有界同步；并发 Status 测试改走真实 loopback RPC | 单 status slot、关闭 admission、status 取消及清理测试稳定；四个相关组各连续 20 次通过；实际 clock 停顿测试的 scope guard 在断言异常时先释放 gate 再 join |

保持既有 owner/private Projection/registry/独立 transport；不添加服务、状态机或后台观察线程。
超时等待无 owner mailbox 请求，也不保存已返回 RPC 的 context/response 指针。
无限 RPC deadline 先在 system_clock 单位截短，再转换 steady_clock，避免 macOS 单位转换溢出。
一秒是内部观察等待上限，不是已验收的性能 SLA。

## 构建与离线结果

macOS 27.0.1 arm64，AppleClang 21.0.0，CMake 4.3.2，clang-format 22.1.8。
使用已有 `build/g7-conan/conan_toolchain.cmake` 与缓存二进制，未构建第三方源码、未重新获取依赖。
依赖配方/版本不变：Contracts message RREV `53713fb82c27cf6ea0395f6b8a853006`，
Contracts gRPC RREV `c3661aa735e1eb6450c1a93f635dbc20`，
Projection RREV `d95fa71d6dca8d931e72fbb5b74114a9`；历史 G1 smoke 关闭且未复跑。

| 构建/检查 | 结果 | 本机日志（ignored） |
| --- | --- | --- |
| Release production 全图，含 bmd-gatewayd | 22/22 CTest，8.50s | `build/v03-g0/release-ctest.log` |
| ASan production 全图 | 22/22，17.59s | `build/v03-g0/asan-ctest.log` |
| UBSan production 全图 | 22/22，18.24s | `build/v03-g0/ubsan-ctest.log` |
| TSan production 全图 | 22/22，34.62s | `build/v03-g0/tsan-ctest.log` |
| G3 runtime / G10 status assembler / G10 gRPC / production 重复运行 | 每组 20 次，共 80 次，全部通过 | `build/v03-g0/repeat-ctest.log` |
| 独立 Foundation Release | 1/1，0.27s；未启用任何上游图 | `build/v03-g0/foundation-ctest.log` |
| scripts/format-check.sh / git diff --check | 通过 | `build/v03-g0/format-check.log` |

Sanitizer 覆盖本仓生产源码；缓存上游静态库未重新 instrument，不能声称整个依赖栈被 sanitizer 验证。
各构建的 configure/build 日志及 CTest LastTest.log 同在 ignored `build/v03-g0/`。

复核用命令（仓库根目录，toolchain 路径可替换为同配方的固定二进制依赖）：

```sh
cmake -S . -B build/v03-g0/release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$PWD/build/g7-conan/conan_toolchain.cmake" \
  -DCMAKE_OSX_SYSROOT=macosx \
  -DBMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON \
  -DBMD_GATEWAY_BUILD_G2_SYNTHETIC_HOST=ON \
  -DBMD_GATEWAY_BUILD_G8_INTEGRATION_ACCEPTANCE=ON \
  -DBMD_GATEWAY_BUILD_TESTS=ON
cmake --build build/v03-g0/release --parallel 4
ctest --test-dir build/v03-g0/release --output-on-failure
ctest --test-dir build/v03-g0/release --output-on-failure \
  -R 'gateway_(g10_gateway_status|g10_grpc_service|g3_market_runtime|production)_tests' \
  --repeat until-fail:20
scripts/format-check.sh
git diff --check
```

三类 sanitizer 使用相同生产开关，分别在独立目录加
`BMD_GATEWAY_ENABLE_ASAN=ON`、`BMD_GATEWAY_ENABLE_UBSAN=ON`、
`BMD_GATEWAY_ENABLE_TSAN=ON`；每个构建 parallel 2。Linux 复核省去 macOS sysroot 参数。
已有 G2/G8 CMake 开关只启用历史回归，不能据此将新版 G1/G2 标为已开展。

协议字段核对依据：2026-10-12 官方
[Spot WebSocket Streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md) 和
[USD-M Diff Depth](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Diff-Book-Depth-Streams)。
无关扩展容忍是本地兼容策略，不宣称观测到了真实交易所扩展/恢复事故。

## 完成边界

仅 G01/G03/G05/G10 测试清理。新版 G1/G2/G3 未启动；USD-M T 映射/depth freshness、
四产品正式 acceptance、production CI、API 联合预算和性能/VPS 资格仍留在相应后续阶段。
无跨仓修改、无 production VPS 访问、无 Recorder 操作、无真实 Binance 行情连接。
本轮网络仅 GitHub fetch/push/PR 和官方文档读取；所有 runtime 测试都是本机离线/loopback。

生命周期仍要求正在执行的 owner 操作返回后 join；不强制中断永久阻塞回调、不 detach，
不以提前销毁产品来伪装退出成功。回归证明可超时观察、有限暂停的启动回滚、迟到处理及安全清理，
不承诺任意线程永不返回时仍能有界完成进程退出。
