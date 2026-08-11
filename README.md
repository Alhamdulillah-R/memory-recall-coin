# memory-recall-coin

运行在 Tailnet 内的跨设备 MCP 记忆系统。不同设备上的 Codex、CLI 和 Agent 共享 PostgreSQL 中的 authoritative memory，同时保留 namespace、scope、device、evidence、TTL、revision 和 source provenance。

## 核心能力

- PostgreSQL 是唯一 authoritative store，不扫描大型 Markdown 完成查询。
- `pgvector` 提供 1024 维 semantic retrieval；`pg_trgm`、PostgreSQL FTS、JSONB GIN 和时间索引分别处理 substring、lexical、metadata 与 temporal retrieval。
- exact、substring、lexical、semantic 候选通过 hybrid ranking 融合，并返回可检查的 score breakdown。
- memory mutation 使用 optimistic concurrency；旧版本追加到 immutable revision history。
- memory、source 和 ingestion 支持 TTL；正常查询直接过滤过期记录，不等待后台 GC。
- 本地 stdio bridge 读取、hash、上传和 watch 本机 path；中央服务从不读取客户端 filesystem。
- MCP 使用官方 Go SDK `github.com/modelcontextprotocol/go-sdk v1.7.0`；本地 stdio bridge 保留 verified device identity，并通过中央 typed RPC 跨设备共享数据。

## 架构

```mermaid
flowchart LR
    C["Codex / CLI"] -->|"stdio MCP"| L["本机 memory-recall-coin mcp"]
    L -->|"Bearer /v1/rpc"| A["中央 memory-recall-coin serve"]
    L -->|"read / hash / watch"| F["本机 files"]
    A --> P["PostgreSQL + pgvector + pg_trgm"]
    A --> E["OpenAI-compatible embeddings"]
```

进程模式：

| 命令 | 作用 |
|---|---|
| `memory-recall-coin mcp` | 本地 stdio MCP bridge；无参数时也是此模式 |
| `memory-recall-coin serve` | 中央 authenticated HTTP RPC service |
| `memory-recall-coin migrate` | 单独执行 PostgreSQL migration |
| `memory-recall-coin version` | 输出 version、revision 和 build time |

`serve` 启动时也会执行 idempotent migration。

## 编译

要求：

- Go 1.26.2 或更高版本；container build 固定 Go 1.26.5。
- Go Task v3.50.0。

安装固定版本的 Task：

```powershell
go install github.com/go-task/task/v3/cmd/task@v3.50.0
```

本项目按要求使用非标准文件名 `Task.yml`。Go Task 自动发现的是 `Taskfile.yml`、`Taskfile.yaml` 等名称，**不会自动发现 `Task.yml`**，所以每次都必须显式传入：

```powershell
task --taskfile .\Task.yml check
task --taskfile .\Task.yml build
```

Windows 输出：

```text
dist\memory-recall-coin.exe
```

Cross compile：

```powershell
task --taskfile .\Task.yml build:linux-amd64
task --taskfile .\Task.yml build:linux-arm64
```

注入 build metadata：

```powershell
task --taskfile .\Task.yml build VERSION=v0.1.0 REVISION=0123456789abcdef BUILD_TIME=2026-08-10T23:30:00Z
```

## 配置

程序只读取 process environment 和向上查找的 `.memory-recall.json`，不会自动加载 `.env`。`D:\dev\memory-recall-coin\.env.example` 只是变量清单；本地用 PowerShell 设置 `$env:...`，K8s 使用 Secret/ConfigMap 注入。

| 变量 | 默认值 | 作用 |
|---|---:|---|
| `MEMORY_LISTEN_ADDRESS` | `:8080` | 中央 HTTP listener |
| `MEMORY_DATABASE_URL` | 无 | PostgreSQL DSN；`serve`/`migrate` 必填 |
| `MEMORY_API_TOKEN` | 无 | `/v1/rpc` Bearer token；`serve`/`mcp` 必填 |
| `MEMORY_API_TOKEN_FILE` | 无 | token 文件；`MEMORY_API_TOKEN` 为空时读取，适合 MCP controller 避免把 secret 写进 plugin registry |
| `MEMORY_API_URL` | 无 | 本地 stdio bridge 使用的中央 base URL；`mcp` 必填 |
| `MEMORY_SIGNAL_HMAC_SECRET` | 无 | hardware signal HMAC secret；`serve` 必填 |
| `MEMORY_DEFAULT_NAMESPACE` | workspace config | 本机默认 namespace |
| `MEMORY_WORKSPACE_CODE` | workspace config | 本机 workspace identity |
| `MEMORY_DEFAULT_SCOPE` | `workspace` | 默认 scope |
| `MEMORY_IDENTITY_FILE` | OS user config directory | installation identity 文件 |
| `MEMORY_AUTO_REGISTER` | `true` | identity 不存在时自动注册 |
| `MEMORY_EMBEDDING_PROVIDER` | `none` | `none` 或 `openai` |
| `MEMORY_EMBEDDING_URL` | 无 | OpenAI-compatible base URL；provider 为 `openai` 时必填 |
| `MEMORY_EMBEDDING_API_KEY` | 无 | 可选 Bearer credential |
| `MEMORY_EMBEDDING_MODEL` | `text-embedding-3-small` | embedding model |
| `MEMORY_EMBEDDING_DIMENSIONS` | `1024` | 固定为 1024，其他值直接拒绝启动 |
| `MEMORY_EMBEDDING_WORKERS` | `2` | embedding worker 数量 |
| `MEMORY_EMBEDDING_BATCH_SIZE` | `32` | 单批 input 数量 |
| `MEMORY_MAX_FILE_BYTES` | `2097152` | 本机单文件读取上限 |
| `MEMORY_MAX_RPC_BODY_BYTES` | `33554432` | `/v1/rpc` body 上限 |
| `MEMORY_CHUNK_CHARACTERS` | `1800` | source chunk 字符数 |
| `MEMORY_CHUNK_OVERLAP_CHARACTERS` | `200` | chunk overlap 字符数 |
| `MEMORY_WATCH_DEBOUNCE` | `750ms` | filesystem watch debounce |
| `MEMORY_REQUEST_TIMEOUT` | `30s` | 本地 HTTP 与 embedding request timeout |
| `MEMORY_SHUTDOWN_TIMEOUT` | `15s` | 中央 graceful shutdown timeout |

`MEMORY_EMBEDDING_PROVIDER=none` 时 exact、substring、lexical、metadata 和 temporal channel 仍可用，只有 semantic channel 被关闭。

## 启动中央服务

PowerShell：

```powershell
$env:MEMORY_DATABASE_URL = 'postgres://memory_user:replace-me@100.119.87.38:5432/memory_recall?sslmode=disable'
$env:MEMORY_API_TOKEN = 'replace-with-a-random-token'
$env:MEMORY_SIGNAL_HMAC_SECRET = 'replace-with-a-random-hmac-secret'
$env:MEMORY_EMBEDDING_PROVIDER = 'none'
.\dist\memory-recall-coin.exe migrate
.\dist\memory-recall-coin.exe serve
```

HTTP surface：

| Endpoint | Auth | 用途 |
|---|---|---|
| `GET /healthz` | 无 | process liveness |
| `GET /readyz` | 无 | PostgreSQL/embedding readiness |
| `POST /v1/rpc` | Bearer | 本地 stdio bridge 使用的 typed RPC |

## Codex 本地 stdio 配置

先让 Codex host process 可以读取中央地址与 token 文件：

```powershell
$env:MEMORY_API_URL = 'http://100.119.87.38:8080'
$env:MEMORY_API_TOKEN_FILE = "$env:LOCALAPPDATA\memory-recall-coin\api-token"
```

东京环境把 `Service/memory-recall-coin` 的 `externalIPs` 固定到 master 的 Tailscale IP `100.119.87.38`。链路由 Tailnet WireGuard 加密，HTTP RPC 仍强制 Bearer token；该地址不对公网路由。

在用户级 `config.toml` 或 trusted project 的 `.codex/config.toml` 中配置：

```toml
[mcp_servers.memory_recall_coin]
command = 'D:\dev\memory-recall-coin\dist\memory-recall-coin.exe'
args = ["mcp"]
env_vars = ["MEMORY_API_URL", "MEMORY_API_TOKEN_FILE"]
required = true
startup_timeout_sec = 10
tool_timeout_sec = 120
default_tools_approval_mode = "writes"

[mcp_servers.memory_recall_coin.env]
MEMORY_DEFAULT_NAMESPACE = "memory-recall-coin"
MEMORY_WORKSPACE_CODE = "github.com/Alhamdulillah-R/memory-recall-coin"
MEMORY_DEFAULT_SCOPE = "workspace"
```

stdio 的 `stdout` 只承载 MCP JSON-RPC；程序日志固定写入 `stderr`。配置后使用：

```powershell
codex mcp list
```

也可以在 Codex/ChatGPT desktop 的 `/mcp` 面板检查连接。配置字段参考 [OpenAI 官方 MCP 文档](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)。

## 22 个 MCP tools

| Tool | 作用 |
|---|---|
| `memory_put` | 创建带 scope、evidence、TTL 和 idempotency key 的 versioned memory |
| `memory_patch` | 使用 `expected_version` 修改 mutable fields，并追加 revision |
| `memory_get` | 按 ID 读取当前 memory 或指定历史 version |
| `memory_search` | 执行 exact、substring、lexical、semantic、temporal、metadata 和 hybrid retrieval |
| `memory_delete` | soft delete memory，保留 revision history |
| `memory_history` | 分页读取 append-only revisions |
| `memory_restore` | 将历史 snapshot 恢复为新的 current version |
| `memory_supersede` | 原子创建 replacement 并 supersede target memory |
| `memory_refute` | 标记 memory 为 refuted，并附 reason、evidence 或 refuting memory |
| `memory_touch` | 延长、替换或清除 TTL，使用 optimistic concurrency |
| `memory_pin` | 清除 expiration，并把 TTL 变化写入 history |
| `memory_ingest_path` | 在本机扫描、hash、增量上传并可选 watch 文件或目录；仅 stdio bridge 可读取 path |
| `memory_ingest_status` | 按 ingestion ID 查询 source 与 embedding 状态 |
| `memory_source_status` | 按 source/path 查询 hash、generation、parser、TTL 和 embedding 状态 |
| `memory_source_delete` | 只删除服务端 source index，绝不删除客户端源文件 |
| `memory_watch_list` | 列出本机 active filesystem watches 与最近同步结果；仅 stdio bridge |
| `memory_watch_stop` | 停止指定本机 filesystem watch；仅 stdio bridge |
| `device_register` | 使用 transient hardware signals 注册 installation 与 logical device |
| `device_claim` | 显式把当前 installation 绑定到已有 logical device |
| `device_migrate` | 把 source device 合并到 canonical target，同时保留 provenance |
| `device_whoami` | 查询当前 installation、device、workspace 与 verified caller identity |
| `memory_health` | 查询 PostgreSQL 与 embedding provider 状态及 server version |

默认检索行为是 `scope_mode=prefer_local`，同时排除 expired、refuted、superseded 和 deleted 记录。mutation 应携带 `expected_version`；可能重试的 write 应携带稳定 `idempotency_key`。

## Docker

Docker builder 固定：

- `golang:1.26.5-alpine3.24` immutable digest；
- `github.com/go-task/task/v3/cmd/task@v3.50.0`；
- 通过 `task --taskfile Task.yml build:container` 真实完成 binary 编译。

构建：

```powershell
docker build --build-arg VERSION=dev --build-arg REVISION=local --build-arg BUILD_TIME=unknown -t memory-recall-coin:local .
```

最终 image 基于 `scratch`、以 UID/GID `65532` 运行、包含 CA bundle、无 shell，默认执行 `serve`。

## CI 与 K8s 边界

`.mignon-ci.yaml` 只声明以下 contract：

- image repository：`mignon/memory-recall-coin`；
- build context 与 `Dockerfile`；
- rollout target：namespace `memory-recall-coin` 中的 `deployment/memory-recall-coin`、container `memory-recall-coin`。

`.github/workflows/deploy.yml` 在 `main` push 或手动 `workflow_dispatch` 时串行执行：

1. `Check`：在 `tokyo-test` Runner 上校验 exact checkout，只运行 `gofmt`、`go vet ./...` 与 `go build ./...`，不运行 regression tests；
2. `Build and push`：在 `production` Environment 与 `tokyo-build` Runner 上调用 `/usr/local/bin/mignon-build-push`，只接受 `rex-tokyo-serv.tail3078d0.ts.net:8443/mignon/memory-recall-coin@sha256:<digest>`；
3. `Deploy`：在 `tokyo-deploy` Runner 上使用项目专属 kubeconfig `/etc/mignon-ci/kubeconfigs/Alhamdulillah-R__memory-recall-coin.yaml` 调用 `/usr/local/bin/mignon-deploy`，按 immutable digest 更新既有 Deployment。

所有 job 都把 `actions/checkout` 固定到 reviewed commit，并验证 checkout `HEAD` 等于 `GITHUB_SHA`。Registry credential 只来自 GitHub `production` Environment；workflow 不接收 tag-only image，也不自行实现 rollout/rollback。

Image binary 由 `Dockerfile` 内的 `task --taskfile Task.yml build:container` 编译；`mignon-build-push` 只负责 clean checkout 校验、Docker build/push 与 digest verification，不绕过 `Task.yml` 直接编译。

Mignon CI 负责在 clean checkout 上 build/push immutable image digest，并把既有 Deployment 的目标 container 更新到该 digest；它不负责：

- 创建或维护 GitHub Runner、Registry、Docker Engine 与 CI credentials；
- 创建 namespace、RBAC、ServiceAccount、image pull Secret 或 Tailscale Serve；
- 生成 `MEMORY_API_TOKEN`、`MEMORY_SIGNAL_HMAC_SECRET`、database password 或 embedding key；
- 初始化、备份、扩容或恢复 PostgreSQL PVC；
- 把服务暴露到公网。

K8s manifests/cluster operator 负责 StatefulSet/PVC、Service、NetworkPolicy、Secret/ConfigMap、probe 与资源限制。应用 Deployment 可以滚动更新；PostgreSQL authoritative store 必须保持有状态并使用持久卷。服务只应通过 Tailnet/Tailscale ingress 访问，即使位于 Tailnet 内也仍需 Bearer token。`/healthz` 与 `/readyz` 只供 cluster probe，不应通过公网 ingress 暴露。

设计细节见 `D:\dev\memory-recall-coin\docs\Codex-跨设备记忆系统设计.md`。
