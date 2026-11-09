# Gateway G3-a v0.4 — twenty-product offline evidence

更新：2026-10-19。用户授权完成20 ProductKeys的离线配置、48 contexts、启动恢复与路由核对。
已通读同级 `Milestone Plan` 全部六份文档。复用
[PR #38](https://github.com/tomohikoAmada/BinanceMarketDataGateway/pull/38)
已有P1扩容实现，不重做G0/G1/G2，不改历史四产品资格。

状态：G3-a离线完成、独立复核及CI通过，PR #38交付；最终head与合并记录见该PR。

## 完成范围

- [x] JSON、ConfiguredProductRuntimeSet::prepare 和 immutable registry 共用有限20上限，21拒绝；原1..8配置兼容。
- [x] 10 Spot + 10 USD-M研究样本：每市场一次metadata，精确Key、不同数值精度、20私有owner/Projection/transport/recovery、真实parser及loopback Book/Event/Status。
- [x] 40基础流、48满额、第49条明确拒绝、取消后复用；产品级Book/Event各8和普通队列64不变。
- [x] 每个位置的初始失败、owner创建异常全部回滚；19 Live不能开放gRPC；统一启动deadline，迟到第20个Live仍超时回滚。
- [x] 单产品故障隔离、20产品集中Gap后两轮恢复（generation 1→2→3）、短Live后再次恢复、旧session以Gap结束、新session可消费。
- [x] 慢session有界终止、活动RPC关闭/drain、所有owners joined、contexts/transport/subscriptions归零。
- [x] P1按实际配置Key导出、OFF/ON实测及独立复核已交付，见[插桩证据](../PERFORMANCE_BASELINE_INSTRUMENTATION.md)。
- [x] 本轮完整Release/sanitizer、Foundation、format及独立增量复核通过。
- [x] 技术提交`b4d3625`的最终production CI及全部普通检查通过，reviewed PR #38交付。

G3-a关闭不等于G3整体关闭。G3-b / BMD-RL-01（Recorder+Gateway同IP分市场窗口、429/418/Retry-After联合恢复预算）、最终部署名单、授权实网与G3-c共享VPS资源预算仍OPEN。

## 消费画像与容量裁决

离线验收画像是一个客户端为每Key建立一条Book和一条DIFF_DEPTH Event，共20×2=40个streaming RPC。
`global_context_limit`在全部20 Key均有订阅后增加四对流至48；第49条返回
`RESOURCE_EXHAUSTED`，不占用新槽位；Status仍可调用。取消一对后降至46，再订阅回48。
最后关闭服务后tracker=0。进程共享同一个48 tracker，不随Key数量相乘。

| 画像 | contexts | 本轮结论 |
| --- | ---: | --- |
| 每Key Book + Depth Event | 40 | 已通过，保留8个全局余量 |
| 上述画像加四对Book/Event | 48 | 已通过，满额时拒绝新增流，释放可复用 |
| Spot Book + 三类Event，USD-M Book + Depth | 60 | 超出现行48上限；本轮没有该并发要求，不提高cap |

每产品Book与Event各8是另外两项限制；20 Key不扩大这些限制。Spot提供
DIFF_DEPTH/AGG_TRADE/BOOK_TICKER，USD-M仅DIFF_DEPTH；SubscribeMarketState仍UNIMPLEMENTED。
Book为Projection-Applied，Depth Event为pre-Projection。无Contracts Wire/API或依赖pin变更。

## 启动、恢复及身份断言

`tests/four_product_integration_tests.cpp`同时构建原四产品和20产品测试，复用G2 fixture。
JSON来自`examples/gateway-twenty.json`；冻结metadata和精度样本来自
`tests/fixtures/projection-p1-twenty.json`。这份清单是离线研究样本，运行时仍要验证当前metadata。

每个Key通过实际Spot combined / USD-M depth与snapshot parser、route builder、
NumericSpec和私有Projection；Book snapshot核对精确market/symbol/价格/数量，
后续Book/Event逐字节核对预期Depth及generation，Status列出全部20身份。
错误metadata的symbol/decimal明确返回产品与市场，不落到另一个同名Key。

原测试遍历20个失败位置并注入最后owner构造异常，确认整体回滚且端口未绑定。
一个NeverLive产品与其他19 Live共存时，整体保持Starting；一秒启动预算后回滚，
实测耗时另断言小于五秒（测试容差，不是生产SLA）。新增确定性测试在启动前捕获
一次deadline，暂停第20 owner；其他19已Live后使测试时钟到期，再释放最后owner并确认
全部Live，启动仍返回InitialStartupTimeout且端口为0。只使用已有内部测试seam。

单USD-M产品错误pu只进入Event，不发布为Book Applied，其余19产品不重启。
新增两轮集中恢复把所有20产品的sequence gap先入队，再等恢复；每轮旧Book/Event流
明确以ConsumerGap结束，不能靠客户端deadline通过；每Key重建到下一generation，
每代40条新流都可继续Applied更新。每产品始终最多一个active transport。
这是离线生命周期与重订阅证据；fixture跳过实际backoff等待，不证明API窗口预算。

慢消费者使用生产64容量的publication session；同产品快RPC和其他产品持续消费。
最终关闭顺序复用server admission cut/cancel/drain后停止products；验证每个owner已join，
transport、pending/resident订阅、Event订阅及全局contexts均清零。

## 验证与交付

本机macOS arm64 / AppleClang 21、CMake 4.3.2，显式启用production graph，复用固定Conan二进制。
上游静态依赖未重新instrument；没有访问Binance、SSH/VPS、修改Recorder或冷编依赖。

| 检查 | 本轮结果 |
| --- | --- |
| 完整Release OFF `build/p1-off` | 26/26，8.97s |
| 完整Release ON `build/p1-on` | 27/27，9.16s |
| 完整ASan+UBSan ON `build/post-g11-performance-asan` | 25/25，21.12s |
| TSan ON重点并发/生命周期 | 6/6；路由/多产品/四及20集成/信号5组55.70s，production组4.09s |
| 独立Foundation | 1/1，0.01s |
| format / diff检查 | 通过 |
| 新子代理复核`origin/main...9d92c96` | APPROVED，无阻断缺陷 |
| 新增deadline/集中恢复测试独立复核 | APPROVED，无剩余发现；独立重跑20集成六组PASS |
| Linux/GCC13 production | 技术提交`b4d3625`，24/24（7.17s），含20产品集成（2.43s） |
| 最终GitHub普通CI | [PR run 39180483378](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39180483378)及[push run 39180480384](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39180480384)全部7个普通job通过；prepare按设计跳过 |
| 交付 | [PR #38](https://github.com/tomohikoAmada/BinanceMarketDataGateway/pull/38)；本表后续只补文档收尾，技术受测提交不变；最终head/合并记录以PR为准 |

重现入口（使用已经配置并启用production的固定二进制build）：

```sh
cmake --build build/p1-off --parallel 4
ctest --test-dir build/p1-off --output-on-failure
cmake --build build/p1-on --parallel 4
ctest --test-dir build/p1-on --output-on-failure
cmake --build build/post-g11-performance-asan --parallel 4
ctest --test-dir build/post-g11-performance-asan --output-on-failure
cmake --build build/post-g11-performance-tsan --parallel 4
ctest --test-dir build/post-g11-performance-tsan --output-on-failure \
  -R '^gateway_(g11_multi_market_runtime|g11_grpc_routing|production|four_product_integration|twenty_product_integration|production_daemon_process)_tests$'
scripts/format-check.sh
```

日志留在各build目录的`g3-closeout-*.log`；历史18次P1测量及原始包继续绑定实测提交
`8ce41be`，本轮测试和文档补充不改写其身份或资源结论。
