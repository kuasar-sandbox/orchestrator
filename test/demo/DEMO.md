# e2b 兼容沙箱主机 — 端到端演示

`demo_e2b.sh` 用**未改造的 e2b CLI** 把 `sandbox-orchestrator` 的全链路跑一遍：构建模板 →
启动真实 microVM → 在 guest 内执行命令 → **端口转发 + 出网** → 暂停/恢复（验证状态存活）→ 销毁。
CLI 二进制零修改，仅靠环境变量 + 本机 `/etc/hosts` + 自签 TLS 指向本节点（与指向 e2b.dev 的方式一致）。

base 镜像直接用真实的 **`e2bdev/code-interpreter:latest`**（本机 docker 已缓存即可）——它本就具备
e2b userland（`user` 账户、`bash`、`util-linux`/`coreutils`、`python3`、`curl`），无需任何临时适配。

## 演示了什么

| 步骤 | 命令（真实 e2b CLI） | 证明 |
|---|---|---|
| 1 | （编排起栈） | store-ctl + 本机 zot registry + orchestrator(TLS) + eBPF vswitch + host NAT |
| 2 | `e2b-key-ctl` + `manifest-key add` | 密钥模型：manifest_key 根密钥 → 派生 e2b api_key → 白名单 |
| 3 | `e2b template build` | 客户端 `docker build` 推镜像 → 节点展平为 microVM 模板（v1 build system） |
| 4 | `e2b sandbox create --detach` | 从模板冷启真实 cloud-hypervisor microVM，guest 内 envd 就绪 |
| 5 | `e2b sandbox exec <id> -- …` | guest 内执行命令（默认用户 `user`，可写 `/home/user`）经 proxy→envd |
| 6 | `curl http://<floatingip>:port` / `https://<port>-<sid>.<domain>` | **端口转发**（host 经 sw0m0 直连 + e2b 暴露端口）+ **沙箱出网**（NAT） |
| 7 | `e2b sandbox pause` / `resume` | 快照入内容存储 → 恢复；**暂停前写入的文件在恢复后仍在** |
| 8 | `e2b sandbox list` / `kill` | 生命周期 |

## 前置条件

- 二进制（`make -C kuasar-sandbox build` 产出到 `kuasar-sandbox/bin/`）：`orchestrator-ctl`、
  `store-ctl`、`flatten-ctl`、`e2b-key-ctl`、`vswitch-ctl`、`cloud-hypervisor`、`vmlinux`、
  `sandbox-runtime-e2b.erofs`、`mkfs.erofs`。
- 主机：**systemd 为 PID1 + root**（编排经 D-Bus 驱动单元；TLS 监听 :443；KVM）；可读写 `/dev/kvm`。
- 工具：`e2b` CLI、`zot`、`docker`、`openssl`、`python3`、`iproute2(ip)`、`curl`、`sqlite3`、`iptables`。
- base 镜像本机已缓存（默认 `e2bdev/code-interpreter:latest`，可 `E2E_IMAGE=` 覆盖为其他
  e2b-ready 镜像）。

## 运行

```bash
make demo                                         # 开发树：build + 跑 demo（推荐）
bash kuasar-sandbox/test/demo/demo_e2b.sh         # 或直接跑脚本（普通用户即可，自动 sudo 重入）
# 发布包内：解包后于根目录 `bash test/demo/demo_e2b.sh`（BIN 自动解析到 ../bin）
DEMO_PAUSE=1   bash …/demo_e2b.sh                 # 每步之间回车暂停（适合逐步讲解）
DEMO_KEEP=1    bash …/demo_e2b.sh                 # 保留工作目录用于排查
DEMO_NETDIAG=1 bash …/demo_e2b.sh                 # 网络步失败时不中止、打印 host/guest 网络诊断
DOMAIN=my.demo E2E_IMAGE=other/e2b-base:tag bash … # 自定义域名 / base 镜像
```

退出即自动清理：停沙箱单元、停 vswitch、删本脚本所加的 NAT 规则与 `/etc/hosts` 临时项、
删临时单元与工作目录。

## 网络配置（端口转发 + 出网）

沙箱 guest 网卡只有一个 link-local **inner IP**（`169.254.1.1/31`，所有沙箱相同——唯一身份是 floatingip）；host 与外网都不在该网段。
三处配置把「host↔沙箱」与「沙箱→外网」打通——脚本已自动完成，原理如下：

```
  host (root netns)                       vswitch (netns sw0)                 guest microVM
  curl <floatingip>:port ─route─► sw0m0 ─► ARP-proxy + DNAT floatingip->inner ─tap─► eth0 169.254.1.1/31
  reply ◄───────────────────────── SNAT inner->floatingip ◄──────── tap ◄──────────  http.server :port
                                                                                      (default via 169.254.1.0)

  egress: guest ─(default via 169.254.1.0)─► nx extract 0.0.0.0/0 ─► sw0m0
          ─► host NAT MASQUERADE (-s 100.100.96.0/20, ip_forward=1) ─► internet
```

1. **`vswitch-ctl start … --mgmt-extract=:sw0m0:169.254.169.254,0.0.0.0/0`**
   在 host(root netns) 建管理网卡 `sw0m0`(`169.254.169.254`)，并自动加路由
   `100.100.96.0/20 dev sw0m0`（floating-ip 段）。eBPF 在 sw0m0 侧 ARP 代答 + 把 host 发往
   floatingip 的包 **DNAT** 成沙箱 inner IP、重定向到对应 tap，沙箱回包再 **SNAT** 回 floatingip。
   `mgmt_cidrs` 含 `0.0.0.0/0`：沙箱发往任意目的的流量都被「提取」到 sw0m0（出网入口）。

2. **host NAT**（沙箱出网 / 互访）——脚本幂等添加、退出删除：
   ```bash
   sudo sysctl -w net.ipv4.ip_forward=1     # 多数装了 docker 的机器已为 1
   sudo iptables -A FORWARD -o sw0m0 -m state --state RELATED,ESTABLISHED -j ACCEPT
   sudo iptables -A FORWARD -i sw0m0 -j ACCEPT
   sudo iptables -t nat -A POSTROUTING -s 100.100.96.0/20 -j MASQUERADE
   ```

3. **guest inner IP + 默认路由**（orchestrator 自动下发）：guest 拿一个 link-local /31
   `169.254.1.1/31`，默认路由 nexthop = /31 的另一端 `169.254.1.0`；vswitch 对网关 ARP 代答，
   guest 由此能回复「非本网段」来源（proxy/mgmt）并经 NAT 出网。**缺它则 guest 只能访问自身 /31：
   `host→floatingip:port` 全部超时、且无法出网。** inner IP 对所有沙箱相同（唯一身份是 floatingip，
   由 slot 决定），故复用同一 /31、无需 inner-IP 池。

两条访问沙箱端口的路径（步骤 6 均验证）：

- **直连**：host 经 `sw0m0` 直达 `http://<floatingip>:<port>`（floatingip 见 `orchestrator.db`）。
- **e2b 暴露端口**：`https://<port>-<sid>.<domain>`（CLI 风格），proxy 校验 `X-Access-Token`
  后转发到 `floatingip:port`。

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
- `/etc/hosts` 把 `api.<domain>` 与每个沙箱的 `49983-<sid>.<domain>`、`<port>-<sid>.<domain>`
  解析到 `127.0.0.1`（创建后加、退出时删）。

## 预期输出（节选）

```
[5] Run commands in the guest (e2b sandbox exec)
  $ e2b sandbox exec <sid> -- 'id; uname -sm; python3 --version; grep ^PRETTY_NAME= /etc/os-release'
    uid=1000(user) gid=1000(user) groups=1000(user)
    Linux x86_64
    Python 3.x.x
    PRETTY_NAME="Debian GNU/Linux 13 (trixie)"
  $ e2b sandbox exec <sid> -- 'echo … > /home/user/state.txt; cat …'
    hello from before the snapshot
[6] 网络：端口转发 + 出网
  $ curl http://<floatingip>:8000/pf.txt                              → pf-ok-xxxx   (host 经 sw0m0 直连)
  $ curl -H 'X-Access-Token: …' https://8000-<sid>.<domain>/pf.txt    → pf-ok-xxxx   (proxy→floatingip)
    guest curl http://1.1.1.1                                         → egress HTTP 301  (NAT 出网)
[7] Pause → resume — state survives
  $ e2b sandbox pause <sid>      → paused
  $ e2b sandbox resume <sid>     → resumed
  $ e2b sandbox exec <sid> -- 'cat /home/user/state.txt'
    hello from before the snapshot      ← 暂停前写入，恢复后仍在
```

## 说明与注意

- **base 镜像须满足 e2b userland 约定**：有 `user` 账户、`/bin/bash`、`util-linux`/`coreutils`
  （envd 以默认用户、包一层 `ionice … nice …` 执行命令）。`e2bdev/code-interpreter` 本就具备，
  直接 `docker tag`+`push` 到本机 zot 作 base，模板 Dockerfile 仅 `FROM` 它，无任何 shim/改写。
- **envd 以 root 运行**：envd 是 e2b 基础设施，须 root 才能 setuid 到镜像默认用户执行负载命令
  （`sandbox-orchestrator` 对 e2b profile 固定 `launch.user=0:0`，不沿用镜像 `Config.User`）。
- **vswitch 会拆掉 docker 的 `docker0` 网桥**（eBPF/TC 操作所致）。脚本所有 docker 操作
  （`tag`/`push` 到 `127.0.0.1` 的 zot、CLI 的 `FROM`-only `build`）都走本机回环、不依赖 `docker0`，
  故顺序不受影响。演示后如需再用 `docker build` 联网，`systemctl restart docker` 重建 `docker0`。
- **自签 TLS** 仅为本机演示；生产用通配 `*.<domain>` 正式证书（见 `sandbox-orchestrator/docs/orchestrator.md` §12）。

自动化回归（断言版、非讲解版）见 `kuasar-sandbox/test/e2e/` 下的 `e2e_build_cli.sh`（构建）
与 `e2e_execute.sh`（启动+执行+暂停/恢复状态存活）。
