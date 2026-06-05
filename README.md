# sandbox-orchestrator

计算节点上的**单实例编排 daemon**，对外提供一套 **e2b 兼容 API**，把节点上的 microVM 沙箱
以 e2b 协议暴露给客户端——未改造的 e2b SDK 可直接指向本机运行。一身兼 e2b 的
**api + orchestrator + proxy** 三角色，对接 guest 内**原版 envd**。

- 产物：二进制 **`orchestrator-ctl`**；`orchestrator-ctl serve` 启动服务。
- 两类沙箱：**e2b**（含 envd 数据面：fs/process/pty/runCode）与 **bare**（无 envd，仅 floatingip 网络）。
- 不感知资源仲裁（准入在 `sandbox-ctl` 内部）；经 CLI 驱动 `sandbox-ctl`/`vswitch-ctl`/`flatten-ctl`/`manifest-ctl`；
  经 UDS 反代 envd。叶子组件，`CGO_ENABLED=0`。

完整设计见 [`docs/orchestrator.md`](docs/orchestrator.md)。
