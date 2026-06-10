# sandbox-orchestrator

计算节点上的**单实例编排 daemon**，对外提供一套 **e2b 兼容 API**，把节点上的 microVM 沙箱
以 e2b 协议暴露给客户端——未改造的 e2b SDK 可直接指向本机运行。一身兼 e2b 的
**api + orchestrator + proxy** 三角色，对接 guest 内**原版 envd**。

- 产物：两个二进制——**`orchestrator-ctl`**（daemon；`orchestrator-ctl serve` 启动服务，
  子命令 `serve` / `proxy` / `run-sandbox` / `run-builder` / `config` / `manifest-key` / `version`）与
  **`e2b-key-ctl`**（纯派生凭据工具，无 DB/config/编排状态：`gen-apikey [MANIFEST_KEY]` →
  `e2b_` 前缀 api key、`gen-key` → 随机 32B/64-hex manifest key、`fingerprint [MANIFEST_KEY]` →
  24-hex 指纹、`seal-pull-token [MANIFEST_KEY] {--registry-username/-password | -token}` →
  不透明镜像拉取 token（经 SDK `api_headers` 传入，§11）、`version`）。
- 两类沙箱：**e2b**（含 envd 数据面：fs/process/pty/runCode）与 **bare**（无 envd，仅 floatingip 网络）。
- 不感知资源仲裁（准入在 `sandbox-ctl` 内部）；经 CLI 驱动 `sandbox-ctl`/`vswitch-ctl`/`flatten-ctl`；
  经 UDS 反代 envd。叶子组件，`CGO_ENABLED=0`。

完整设计见 [`docs/orchestrator.md`](docs/orchestrator.md)。
