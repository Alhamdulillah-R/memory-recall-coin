# Codex 跨设备记忆系统设计

## 1. 目标

构建一套运行在 Tailnet 内的中央记忆服务，供不同设备上的 Codex、CLI 和其他 Agent 共同读写。

必须支持：

- 按项目隔离记忆；
- 默认优先召回当前设备产生的记忆；
- 精确字符串、substring、时间范围、metadata 和 semantic 检索；
- hybrid retrieval；
- 创建、修改、删除、覆盖、废弃与 revision history；
- memory、source、path 级 TTL；
- 指定本机文件或目录上传、增量更新和过期清理；
- 跨设备实时共享；
- 保留 evidence、来源文件、版本和 supersede/refute 关系；
- 足够低的查询延迟，不能依赖扫描大型 Markdown 文件。

本系统不是另一个笔记目录，也不是仅有 embedding 的 RAG。PostgreSQL 中的结构化状态是唯一 authoritative source。

## 2. 核心命名

### 2.1 namespace

`namespace` 表示项目，而不是设备。

示例：

```text
rex-mirror-realm
spider-aav
akamai-abck
personal-global
```

所有 memory、source、revision 和 chunk 必须属于一个 namespace。调用方必须显式传入 namespace，或者由 workspace 配置自动解析，服务端不能根据当前目录随意猜测。

### 2.2 device_code

`device_code` 表示长期存在的逻辑设备身份。

示例：

```text
dev_main_windows
dev_kali_wsl
dev_server_01
```

它不应直接等于 CPU ID、MachineGuid、hostname 或 Tailscale node ID，也不能因为更换 CPU、硬盘或重装系统而改变。

### 2.3 installation_code

`installation_code` 表示某台逻辑设备上的一次系统安装：

```text
inst_01K2A6M...
```

每次重装系统生成新的 installation_code，但仍可重新绑定原 device_code。这样既能跨刷机继承设备记忆，也能查询某次安装特有的问题。

## 3. 设备识别

### 3.1 身份层次

```text
Tailnet identity
    -> installation_code
        -> device_code
```

- Tailscale 负责确认请求来自哪个 Tailnet 设备；
- installation_code 标识当前系统安装；
- device_code 是 namespace 检索时使用的长期设备身份。

Tailscale node ID 可能在重装或重新注册后变化，因此只能作为当前连接信号，不能作为永久 device_code。

### 3.2 hardware signals

客户端可提交以下信号，用于刷机后重新识别设备：

| signal | 建议权重 |
|---|---:|
| TPM EK public hash | 100 |
| SMBIOS System UUID | 90 |
| Baseboard serial | 80 |
| BIOS serial | 70 |
| System disk serial | 30 |
| Tailscale node ID | 30 |
| hostname | 10 |
| CPU 信息 | 5 |

要求：

- CPU 只能作为弱信号，更换 CPU 不得创建新 device_code；
- 原始硬件编号不写入数据库，只上传 `HMAC(server_secret, signal_type + raw_value)` 或等价不可逆值；
- 高置信度匹配可自动把新 installation 绑定到旧 device_code；
- 中低置信度不得静默合并，必须显式执行 claim；
- 更换主板、清除 TPM 并重装系统后，不存在可靠的纯硬件自动识别方式，此时使用已认证账户手动 claim/migrate。

### 3.3 device_code 生命周期

- 首次注册：创建 device_code 和 installation_code；
- 普通启动：通过当前 Tailnet identity 找到 installation_code；
- 重装系统：创建新 installation_code，通过 signals 或 claim 重新绑定旧 device_code；
- 更换 CPU/硬盘：更新 signals，不改变 device_code；
- 整机迁移：显式 migrate 到原 device_code；
- 设备退役：标记 retired，不物理删除历史记忆。

## 4. Scope 与本机优先

namespace 和 scope 必须分开存储。

支持以下 scope：

| scope_type | 含义 |
|---|---|
| installation | 仅当前系统安装 |
| device | 当前逻辑设备长期有效 |
| workspace | 当前 workspace/repository |
| project | namespace 内所有设备共享 |
| global | 用户全局共享 |

默认查询模式为 `prefer_local`，候选结果建议增加以下 locality boost：

```text
current installation   +100
current device          +80
current workspace       +60
current project         +40
global                  +20
other devices             0
```

最终排序优先级：

```text
状态有效性 > exact match > evidence/confidence > locality > lexical/semantic score
```

不能让本机已证伪的记录压过 project scope 中已确认的 canonical memory。

查询应支持：

```text
prefer_local
local_only
project_only
all_devices
```

## 5. Memory 数据模型

一条 memory 至少包含：

```text
id
namespace
scope_type
scope_id
device_code
installation_code
workspace_code
type
title
content
metadata
tags
status
confidence
source_id
source_path
source_hash
source_range
created_at
updated_at
observed_at
expires_at
version
supersedes_id
created_by
updated_by
```

`type` 建议包含：

```text
fact
experiment
hypothesis
decision
artifact
procedure
incident
summary
```

`status` 建议包含：

```text
active
superseded
refuted
expired
deleted
```

## 6. 修改与历史

当前 memory 允许 PATCH，但不能破坏历史：

- `memories` 保存当前版本；
- `memory_revisions` append-only 保存每次修改前后的内容；
- PATCH 必须携带 `expected_version`，使用 optimistic concurrency；
- supersede/refute 使用显式关系，不通过覆盖正文模拟；
- 默认 soft delete；
- history 接口可以恢复任意 revision。

任何 Agent 写入结论时都应记录来源、evidence 和 created_by，避免无法追踪的“模型认为”。

## 7. TTL

TTL 支持相对时间和绝对时间：

```text
ttl_seconds
expires_at
```

要求：

- 查询 SQL 必须强制过滤已过期数据，不能只依赖后台清理；
- 后台 GC 再异步清理正文、chunk、embedding 和 blob；
- 支持 touch 延期；
- 支持 pin 取消过期；
- memory、source 和一次 path ingestion 可以分别设置 TTL；
- source 过期时，其派生 chunk 和未被其他 memory 引用的索引一起失效；
- 过期记录可进入 cold archive，默认检索不返回。

## 8. Path ingestion

中央服务无法直接读取其他设备的 `D:\codex`，因此读取、hash 和上传必须发生在调用设备的 MCP/CLI client 中。

接口需要支持：

```text
path
namespace
scope_type
ttl_seconds / expires_at
recursive
include
exclude
watch_mode
parser
```

服务端保存：

```text
device_code
installation_code
original_absolute_path
source_uri
content_hash
size
mtime
parser
generation
expires_at
```

source URI 示例：

```text
device://dev_main_windows/D:/codex/rex-notes.md
```

要求：

- 使用 content hash 去重；
- 文件未变化时不得重复解析和 embedding；
- 文件变化后只重建受影响的 source/chunk；
- 删除服务端索引不能删除客户端源文件；
- 上传目录时保留相对路径；
- 每个 chunk 必须能够回溯到原文件和字符/行范围。

## 9. 检索

### 9.1 检索模式

- exact：ID、完整字符串、tag、hash、path；
- substring：代码片段、报错、路径和中文任意子串；
- lexical：全文 BM25；
- temporal：created/updated/observed/expires 时间范围；
- semantic：向量近邻；
- hybrid：以上候选并行召回后融合。

### 9.2 Hybrid ranking

推荐并行执行：

```text
exact/path/tag
trigram substring
full-text BM25
vector ANN
```

再通过 RRF 或显式权重合并。exact、path、status 和 evidence 权重必须高于 semantic similarity。

每条结果必须返回：

- 命中片段；
- score 分解；
- namespace/scope/device；
- status/version；
- evidence；
- source path、hash 和 range；
- expires_at；
- 是否来自当前设备。

## 10. 服务与存储

建议首版使用：

```text
Memory API / MCP Server：Python 或 Go，禁止 Rust 依赖
Authoritative Store：PostgreSQL
Semantic：pgvector
Substring：pg_trgm
Metadata：JSONB + GIN
Temporal：B-tree/BRIN
Lexical：PostgreSQL FTS
Network：Tailscale only
```

不在首版引入 Elasticsearch、Qdrant、Redis 等额外 authoritative store。确有规模瓶颈后再拆分索引层。

服务只监听 Tailnet 地址或通过 Tailscale Serve 暴露，不开放公网端口。

## 11. MCP/API

最小工具集合：

```text
memory_put
memory_patch
memory_get
memory_search
memory_delete
memory_history
memory_supersede
memory_refute
memory_touch
memory_ingest_path
memory_source_status
device_register
device_claim
device_migrate
device_whoami
```

所有 memory 请求至少携带：

```text
namespace
device_code（通常由当前连接自动补充）
scope_type
```

`memory_search` 默认值：

```text
scope_mode = prefer_local
include_expired = false
include_refuted = false
include_superseded = false
```

## 12. 性能约束

- exact、path、tag 和 metadata 查询必须直接走索引；
- 禁止查询时扫描源文件或大型 Markdown；
- namespace、status、expires_at 和 scope 过滤应尽量在 vector candidate selection 前完成；
- 单条 memory 写入后应立即可被 exact/lexical 查询；
- bulk path ingestion 的 embedding 可以异步，但状态必须可查询；
- query embedding 和各检索通道并行执行；
- 使用 connection pool 和 prepared statement；
- identity resolution 结果按 Tailnet connection/device 缓存，不得每次重新计算硬件匹配。

建议性能目标以服务端 warm query 为准：

```text
exact/metadata p95 <= 30 ms
substring/lexical p95 <= 80 ms
hybrid search p95 <= 200 ms（包含本地 query embedding）
```

## 13. 实现顺序

1. PostgreSQL schema、revision、TTL 和 namespace；
2. device_code、installation_code 与 Tailscale identity mapping；
3. exact、substring、time 和 metadata query；
4. memory CRUD 与 MCP；
5. path ingestion、hash 去重和 source/chunk；
6. embedding 与 hybrid retrieval；
7. hardware signals、reinstall claim/migrate；
8. cold archive、watch mode 和管理界面。

## 14. 验收行为

- 设备 A 写入 project memory，设备 B 可以立即查询；
- 同时存在本机和其他设备相似结果时，`prefer_local` 优先本机有效记录；
- project scope 的 confirmed memory 可以压过本机 refuted memory；
- 修改 memory 后 version 增加，旧内容可从 history 获取；
- TTL 到期后无需等待 GC，正常查询立即不可见；
- 上传同 hash 文件不会产生重复 chunk；
- 重装系统后产生新 installation_code，但可以恢复原 device_code；
- 更换 CPU 后仍使用原 device_code；
- `local_only` 不返回其他设备私有 scope；
- 每条召回结果都能回溯 evidence 和 source range。
