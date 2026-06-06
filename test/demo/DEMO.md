# e2b 兼容沙箱主机 — 端到端演示

`demo_e2b.sh` 用**未改造的 e2b CLI** 把 `sandbox-orchestrator` 的全链路跑一遍：构建模板 →
启动真实 microVM → 在 guest 内执行命令 → 暂停/恢复（验证状态存活）→ 销毁。CLI 二进制零修改，
仅靠环境变量 + 本机 `/etc/hosts` + 自签 TLS 指向本节点（与指向 e2b.dev 的方式一致）。

## 演示了什么

| 步骤 | 命令（真实 e2b CLI） | 证明 |
|---|---|---|
| 1 | （编排起栈） | store-ctl + 本机 zot registry + orchestrator(TLS) + eBPF vswitch |
| 2 | `e2b-key-ctl` + `manifest-key add` | 密钥模型：manifest_key 根密钥 → 派生 e2b api_key → 白名单 |
| 3 | `e2b template build` | 客户端 `docker build` 推镜像 → 节点展平为 microVM 模板（v1 build system） |
| 4 | `e2b sandbox create --detach` | 从模板冷启真实 cloud-hypervisor microVM，guest 内 envd 就绪 |
| 5 | `e2b sandbox exec <id> -- …` | guest 内执行命令（默认用户 `user`，可写 `/home/user`）经 proxy→envd |
| 6 | `e2b sandbox pause` / `resume` | 快照入内容存储 → 恢复；**暂停前写入的文件在恢复后仍在** |
| 7 | `e2b sandbox list` / `kill` | 生命周期 |

## 前置条件

- 二进制（`make -C kuasar-sandbox build` 产出到 `kuasar-sandbox/bin/`）：`orchestrator-ctl`、
  `store-ctl`、`flatten-ctl`、`e2b-key-ctl`、`vswitch-ctl`、`cloud-hypervisor`、`vmlinux`、
  `sandbox-runtime-e2b.erofs`、`mkfs.erofs`。
- 主机：**systemd 为 PID1 + root**（编排经 D-Bus 驱动单元；TLS 监听 :443；KVM）；可读写 `/dev/kvm`。
- 工具：`e2b` CLI、`zot`、`docker`、`openssl`、`python3`、`iproute2(ip)`、`mkfs.ext4`。
- 一个本机已缓存的容器镜像作 base（默认 `test-app-a:latest`，可 `E2E_IMAGE=` 覆盖）。

## 运行

```bash
make demo                                         # 开发树：build + 跑 demo（推荐）
bash kuasar-sandbox/test/demo/demo_e2b.sh         # 或直接跑脚本（普通用户即可，自动 sudo 重入）
# 发布包内：解包后于根目录 `bash test/demo/demo_e2b.sh`（BIN 自动解析到 ../bin）
DEMO_PAUSE=1 bash …/test/demo/demo_e2b.sh         # 每步之间回车暂停（适合逐步讲解）
DEMO_KEEP=1  bash …/test/demo/demo_e2b.sh         # 保留工作目录用于排查
DOMAIN=my.demo E2E_IMAGE=python:3.12-slim bash …  # 自定义域名 / base 镜像
```

退出即自动清理：停沙箱单元、停 vswitch、删 `/etc/hosts` 临时项、删临时单元与工作目录。

**另开终端手动操作**：演示在『租户接入』步后打印一段**凭据 + CLI 配置**并写入
`/tmp/demo-e2b-cli-env.sh`。配合 `DEMO_PAUSE=1`，在暂停处**另开一个终端**
`source /tmp/demo-e2b-cli-env.sh` 即可用真实 e2b CLI 操作本节点（`e2b sandbox list/exec/…`、
`e2b template build`）。演示自己创建的沙箱其 hosts 已自动加好、可直接 `exec`。

> **交互 `create`/`connect` 与 `--detach`**：不带 `--detach` 的 `e2b sandbox create <tmpl>` 建好沙箱后会
> **立刻连一个交互终端**到数据面 `https://49983-<sid>.<domain>`。该 host 是 per-sandbox、建好才知道，而
> `/etc/hosts` **不支持通配**、且 create→连终端是原子的——来不及把它加进 `/etc/hosts`，于是终端 `fetch`
> 失败报 `SandboxError: fetch failed`（控制面 `create` 其实已成功）。`--detach` 只建沙箱、不连终端，故能成功。
> 在另一个窗口要交互用沙箱，二选一：
>
> - **`--detach` 流程**（无需额外组件）：
>   ```bash
>   SID=$(e2b sandbox create <tmpl> --detach | grep -oE '[0-9a-f-]{36}')
>   echo "127.0.0.1 49983-$SID.$E2B_DOMAIN 49999-$SID.$E2B_DOMAIN" | sudo tee -a /etc/hosts
>   e2b sandbox connect $SID        # 或 e2b sandbox exec $SID -- '...'
>   ```
> - **通配 DNS**（可直接用交互 `create`/`connect`）：让 `*.<domain>` 解析到 `127.0.0.1`，例如 dnsmasq
>   `address=/<domain>/127.0.0.1`。`/etc/hosts` 无法通配正是上面要逐个加 host 的根因。

## CLI 如何指向本节点（零改造）

e2b CLI 用 `E2B_DOMAIN` 推出控制面 `https://api.<domain>` 与数据面 `https://<port>-<sid>.<domain>`。
演示把这些指到本机：

- `E2B_DOMAIN`/`E2B_API_KEY` 指向本节点；构建走 `E2B_IMAGE_URI_MASK`（推镜像到本机 zot，绕过 token broker）。
- 自签 `*.<domain>` 证书 + `NODE_TLS_REJECT_UNAUTHORIZED=0` 让 CLI 接受 TLS。
- `/etc/hosts` 把 `api.<domain>` 与每个沙箱的 `49983-<sid>.<domain>` 解析到 `127.0.0.1`（创建后加、退出时删）。

## 预期输出（节选）

```
[5] Run commands in the guest (e2b sandbox exec)
  $ e2b sandbox exec <sid> -- 'id; uname -sm; …; cat /etc/demo-stamp'
    uid=1000(user) gid=1000(user) groups=1000(user)
    Linux x86_64
    2+2=4
    built-by-kuasar-demo (RUN step executed client-side)
  $ e2b sandbox exec <sid> -- 'echo … > /home/user/state.txt; cat …'
    hello from before the snapshot
[6] Pause → resume — state survives
  $ e2b sandbox pause <sid>      → paused
  $ e2b sandbox resume <sid>     → resumed
  $ e2b sandbox exec <sid> -- 'cat /home/user/state.txt'
    hello from before the snapshot      ← 暂停前写入，恢复后仍在
```

## 说明与注意

- **base 镜像须满足 e2b userland 约定**：有 `user` 账户、`/bin/bash`、`util-linux`/`coreutils`
  （envd 以默认用户、包一层 `ionice … nice …` 执行命令）。脚本对裸 alpine 现补这些（adduser +
  bash/ionice/nice shim）；真实 e2b base 镜像本就具备。
- **vswitch 会拆掉 docker 的 `docker0` 网桥**（eBPF/TC 操作所致，重启 docker 恢复）。脚本因此把
  所有 `docker build` 放在起 vswitch 之前，并对 base 用 `--network=none`、对模板用纯 `FROM`，
  全程不依赖 `docker0`。演示后如需再用 docker build，`systemctl restart docker` 重建 `docker0`。
- **自签 TLS** 仅为本机演示；生产用通配 `*.<domain>` 正式证书（见 `sandbox-orchestrator/docs/orchestrator.md` §12）。

自动化回归（断言版、非讲解版）见 `kuasar-sandbox/test/e2e/` 下的 `e2e_build_cli.sh`（构建）
与 `e2e_execute.sh`（启动+执行+暂停/恢复状态存活）。
