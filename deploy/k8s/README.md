# Kubernetes 部署

`CloudNativePG 1.30.0` 由 cluster infra 单独安装；本目录只管理项目 namespace、StorageClass、PostgreSQL Cluster/Database、应用和 VictoriaMetrics 采集规则。Operator manifest 固定在 `deploy\vendor\cnpg-1.30.0.yaml`，provenance 与 SHA-256 见相邻 README。

应用 CI 只能更新 `Deployment/memory-recall-coin` 的 image digest，不负责改数据库、StorageClass、Secret 或 Service。

## Apply 顺序

```powershell
kubectl apply --server-side -f .\deploy\vendor\cnpg-1.30.0.yaml
kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=180s
kubectl apply --server-side -f .\deploy\k8s\00-namespace.yaml
kubectl apply --server-side -f .\deploy\k8s\10-storage-class.yaml
kubectl apply --server-side -f .\deploy\k8s\20-database.yaml
kubectl apply --server-side -f .\deploy\k8s\25-embedding.yaml
kubectl apply --server-side -f .\deploy\k8s\40-monitoring.yaml
kubectl apply --server-side -f .\deploy\k8s\30-application.yaml
```

应用 manifest 中的 image 必须先替换为 Registry 返回的 immutable `@sha256:` digest；禁止 tag-only 或 `latest` rollout。

`memory-recall-runtime` 和 `registry-pull` 必须由运维从 secret source 创建，仓库不保存 value。

## Embedding

`25-embedding.yaml` 在 `k3s-master` 上运行单副本 Hugging Face Text Embeddings Inference 1.9.3 CPU server。模型固定为 Apache-2.0 的 `Qwen/Qwen3-Embedding-0.6B` revision `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`，输出 1024 维向量，通过 cluster-internal `/v1/embeddings` 提供 OpenAI-compatible API。NetworkPolicy 只允许应用 Pod 调用 inference port，并允许 `monitoring` namespace 采集 metrics。

模型 cache 使用 5Gi `local-path` PVC。它只是可重新下载的 cache，不属于业务数据；应用不可将其当成 backup。TEI image 和 model revision 都必须 immutable pin，升级模型时必须同步触发全量 embedding requeue，禁止在同一 vector space 混用不同模型。

## 存储边界

`local-path-retain` 将 PV reclaim policy 改为 `Retain`，但数据仍绑定单 node 的根盘，且不支持 online expansion。三实例 CNPG 通过 PostgreSQL streaming replication 跨 node 冗余；它不是 backup。PITR 需要后续接入 Barman Cloud plugin 和独立 S3-compatible bucket。

`Cluster.spec.storage.resizeInUseVolumes` 固定为 `false`，避免 CNPG 向不支持 expansion 的 `local-path-retain` 提交无效扩容。需要扩容时必须按 CNPG local storage 重建流程逐实例迁移，不能直接增大 `storage.size`。

当前 VictoriaMetrics 的 kube-state-metrics 未采集 PVC collector，项目规则不引用 `kube_persistentvolumeclaim_*`。数据库可用性由 CNPG metrics target 数量与 streaming replica 数量告警；底层容量继续由集群级 node root filesystem 告警覆盖。
