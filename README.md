# market-event-pipeline-go

本地可运行的行情事件流处理组件：接收上游行情事件，按**事件时间**清洗、
去重、重排后有序输出，支持结构演进、断点恢复与背压。

## 运行

```bash
go build ./...
go run ./cmd/market-pipeline            # 运行演示（含乱序/重复/更正/迟到样例）
go test ./... -v                        # 运行全部单测，日志含业务场景与用例结果
```

## 架构

```
Source -> [schema.Normalize 结构校验] -> [dedup 去重判定]
       -> [watermark 迟到判定 + 乱序缓冲] -> 按事件时间输出 -> Sink
       -> 周期性提交 checkpoint（位点 + 水位线 + 去重状态 + 缓冲快照）
```

| 包 | 职责 |
|---|---|
| `internal/event` | 行情事件模型与处置结论枚举（`Disposition`） |
| `internal/schema` | 结构描述、演进兼容性校验、缺省值填充 |
| `internal/dedup` | 重复投递 / 过期版本 / 更正版本的判定 |
| `internal/watermark` | 事件时间水位线与乱序重排缓冲（最小堆） |
| `internal/checkpoint` | 断点原子持久化、过期与回退校验 |
| `internal/pipeline` | 流水线编排、背压、断点提交与恢复 |

## 事件时间处理语义

- **水位线** = 已见最大事件时间 − 最大乱序容忍（`MaxOutOfOrderness`）。
- **乱序事件**：事件时间 > 水位线 − 迟到宽限（`AllowedLateness`）的事件进入
  最小堆缓冲**等待**；水位线越过其事件时间后按事件时间升序输出。
- **迟到事件**：事件时间 ≤ 水位线 − 迟到宽限，结论 `LATE_DROPPED`，明确丢弃。
- **重复投递**：以事件 ID 为键、内容指纹（标的/价格/量/事件时间）比对——
  - 指纹一致：`DUPLICATE_DROPPED`，不重复计入；
  - 指纹不同且序号更旧/相等：`STALE_DROPPED`；
  - 指纹不同且序号更新：视为更正，`MERGED`——替换缓冲中的旧版本；
    若旧版本已输出，则按 `LATE_DROPPED`（correction after emit）处理。
- **结构非法**：`REJECTED`。
- 每条事件的处理都会生成带上述结论与原因字符串的回执
  （`Pipeline.Results()`），并累计到 `Pipeline.Stats()`，不会静默丢弃。

## 结构兼容规则

演进校验在注册新版本时强制执行（`schema.CheckCompatibility`）：

| 变更 | 结论 |
|---|---|
| 新增**可选**字段（带缺省值） | ✅ 兼容；旧数据清洗时填充缺省值，未声明缺省则用类型零值，语义稳定可复现 |
| 删除字段 | ❌ 拒绝 |
| 修改字段类型 | ❌ 拒绝 |
| 可选字段改为必填 | ❌ 拒绝 |
| 新增必填字段 | ❌ 拒绝 |
| 记录中出现本地未注册的字段 | 忽略（向前兼容读取），不进入清洗结果 |
| 必填字段缺失 / 类型无法转换 | 该条记录 `REJECTED` |

时间字段支持 `time.Time`、RFC3339 字符串、Unix 毫秒三种输入，统一转换为
`time.Time`，保证缺省与转换语义可复现。

## 断点与恢复

- 每处理 `CheckpointEvery` 条提交一次断点，内容包括：源位点、水位线基准、
  去重状态快照、**仍滞留在乱序缓冲中的事件**（防止恢复后重放被误判重复而丢失）。
- 写入原子化：先写临时文件再 rename，崩溃不会留下半截断点。
- 恢复（`Run` 自动 / `Resume` 显式）：
  - 断点不存在 → 从头开始；
  - 断点超过 `CheckpointTTL` → 返回 `ErrExpired`，**拒绝**从过旧状态接着算；
  - 请求位点早于已提交位点 → 返回 `ErrRollback`，**拒绝**回退。
- **不多也不少**：已提交位点之前的事件不会重读；崩溃时未入断点的少量事件
  会被重放，由 **Sink 幂等契约**（按事件 ID 去重）吸收，最终输出恰好一次。
  因此自定义 Sink 必须实现幂等 `Emit`。

## 背压

输入通道容量固定（`InputBufferSize`），突发流量时读取端阻塞而非无限堆积；
去重表容量有界（FIFO 淘汰）；处理回执使用定长环形缓冲。内存占用与流量
峰值解耦。

## 怎么验证

```bash
go test ./... -v
```

测试日志中每个用例都打印「业务场景」与「用例结果」，覆盖：

| 场景 | 测试 |
|---|---|
| 乱序等待与按事件时间输出 | `watermark.TestOutOfOrderDrain`、`pipeline.TestOutOfOrderAndDuplicates` |
| 重复投递 / 过期版本 / 更正合并 | `dedup.TestVerdicts`、`pipeline.TestOutOfOrderAndDuplicates` |
| 迟到丢弃 | `watermark.TestLateDropped`、`pipeline.TestOutOfOrderAndDuplicates` |
| 缺省值稳定可复现、未知字段忽略、必填缺失拒绝 | `schema.TestNormalize_*` |
| 兼容 / 不兼容演进 | `schema.TestCompatibility`、`schema.TestRegistry_RejectsIncompatible`、`pipeline.TestSchemaEvolution` |
| 崩溃恢复结果不多不少（含重放吸收） | `pipeline.TestCrashRecovery` |
| 断点过期 / 回退拒绝、原子写入 | `checkpoint.TestExpired`、`TestRollbackRejected`、`TestAtomicWrite`、`pipeline.TestExpiredCheckpointRejected`、`pipeline.TestRollbackRejected` |
| 突发流量背压与有界内存 | `pipeline.TestBurstBackpressure`（5 万条、输入缓冲仅 8） |
