# market-event-pipeline-go

本地可运行的行情事件流处理组件：从文件或标准输入按行接收带结构版本的行情
事件，按**事件时间**清洗、去重、重排后有序写入文件或标准输出，支持结构演进、
断点精确一次恢复、有界内存与背压。

## 运行

```bash
go build ./...

# 文件 -> 文件（可断点恢复，重复运行不会产生重复输出）
go run ./cmd/market-pipeline -input ticks.jsonl -output out.jsonl -checkpoint data/cp.json

# 标准输入 -> 标准输出（一次性运行，不做断点；汇总打到标准错误）
cat ticks.jsonl | go run ./cmd/market-pipeline > out.jsonl

# 内置演示（含乱序/重复/更正/迟到/多版本结构）
go run ./cmd/market-pipeline -demo

# 全部单测（日志含每个用例的业务场景与结果）
go test ./... -v
```

### 命令行参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `-input` | `-`（stdin） | 输入文件路径，`-` 为标准输入 |
| `-output` | `-`（stdout） | 输出文件路径，`-` 为标准输出 |
| `-checkpoint` | `data/checkpoint.json` | 断点文件路径 |
| `-no-checkpoint` | `false` | 显式禁用断点（一次性运行） |
| `-max-out-of-order` | `5s` | 允许的最大乱序时长 |
| `-allowed-lateness` | `1s` | 水位线之后仍允许的迟到宽限 |
| `-max-buffer` | `100000` | 乱序重排缓冲容量硬上界（内存上界，`<=0` 不限） |
| `-dedup-capacity` | `100000` | 去重表容量上界 |
| `-input-buffer` | `1024` | 输入通道容量（背压边界） |
| `-checkpoint-every` | `500` | 每处理多少行提交一次断点 |
| `-checkpoint-ttl` | `24h` | 断点有效期，`0` 不过期 |

> stdin 不可重读、stdout 不可截断；只要输入或输出是标准流，即为**一次性
> 运行**（自动不落断点）。要获得崩溃精确一次，请使用文件输入 + 文件输出。

## 输入 / 输出格式

输入**按行**给，每行标明自己对应的结构版本，两种写法等价：

```json
{"version":2,"data":{"id":"t1","symbol":"BTCUSDT","price":30000,"volume":1.5,"seq":1,"event_time":"2026-10-06T10:00:11Z"}}
{"version":3,"id":"t2","symbol":"ETHUSDT","price":1900,"volume":2,"seq":1,"event_time":"2026-10-06T10:00:12Z","is_maker":true}
```

- 顶层缺省 `version` 按 **1** 处理。
- 无法解析的行（非法 JSON / 空行 / 坏时间等）**不中断整批**，计为 `REJECTED`。
- 时间字段支持 RFC3339 字符串或 Unix 毫秒（见下）。

输出按**事件时间升序**，每行一条**扁平 JSON**：核心字段在前，该事件结构
版本声明的全部扩展字段（含按缺省值补齐的）紧随其后，并带 `schema_version`：

```json
{"schema_version":3,"id":"t2","symbol":"ETHUSDT","price":1900,"volume":2,"seq":1,"event_time":"2026-10-06T10:00:12Z","exchange":"UNKNOWN","is_maker":true,"tick_size":0,"venue_category":"SPOT","venue_id":0}
```

整批跑完在标准错误（demo 模式在标准输出）给出**汇总**：输入总行数、正常
接收、重复丢弃、过期丢弃、合并更正、迟到丢弃、明确拒绝各多少，以及最终
输出行数、强制输出数、乱序缓冲峰值与断点提交次数。

## 架构

```
文件/stdin(按行, 带版本)
   -> [jsonio 解析] -> [schema.Normalize 结构校验+缺省填充]
   -> [dedup 去重判定] -> [watermark 迟到判定 + 有界乱序缓冲]
   -> 按事件时间输出 -> 文件/stdout(按行 JSON)
   -> 周期性提交 checkpoint（源位点 + sink 位点 + 水位线 + 地板 + 去重状态 + 缓冲快照）
```

| 包 | 职责 |
|---|---|
| `internal/event` | 行情事件模型（核心字段 + `Attrs` 扩展字段）与处置结论枚举 |
| `internal/schema` | 结构描述、演进兼容性校验、缺省值填充 |
| `internal/dedup` | 重复投递 / 过期版本 / 更正版本的判定（指纹含扩展字段） |
| `internal/watermark` | 事件时间水位线与**有界**乱序重排缓冲（最小堆，增量索引） |
| `internal/checkpoint` | 断点原子持久化、过期/回退校验、扩展字段带类型序列化 |
| `internal/pipeline` | 流水线编排、背压、有界强制输出、断点提交与恢复 |
| `internal/jsonio` | 带版本号的按行 JSON 输入源与可截断/可 fsync 的文件输出 |
| `internal/e2e` | 外部文件 -> 流水线 -> 文件的端到端测试 |

## 事件时间处理语义

- **水位线** = 已见最大事件时间 − 最大乱序容忍（`MaxOutOfOrderness`）。
- **乱序事件**进入最小堆等待；水位线越过其事件时间后按事件时间升序输出。
- **迟到事件**（事件时间 ≤ 水位线 − `AllowedLateness`）：`LATE_DROPPED`。
- **重复投递**（ID 与内容指纹一致，指纹含核心字段与全部扩展字段）：
  `DUPLICATE_DROPPED`，不重复计入。
- **过期版本**（同 ID、指纹不同、序号更旧/相等）：`STALE_DROPPED`。
- **更正版本**（同 ID、指纹不同、序号更新）：`MERGED`，替换缓冲中旧版本；
  旧版本已输出则按 `LATE_DROPPED`（correction after emit）处理。
- **结构非法 / 未知版本 / 坏行**：`REJECTED`，不静默丢弃，汇总可见。

## 扩展字段流转规则与兼容范围

清洗后的事件由「核心强类型字段」+「`Attrs` 扩展字段」组成。**该事件结构
版本声明的所有字段**都会进入最终输出：上游新增的可选字段（无论显式赋值
还是取缺省）随事件一路透传，下游不会只剩几个核心字段。

演进校验在注册新版本时强制执行（`schema.CheckCompatibility`）：

| 变更 | 结论 |
|---|---|
| 新增**可选**字段（带稳定缺省值） | ✅ 兼容；旧数据清洗时填缺省，未声明缺省取类型零值，语义稳定可复现 |
| 删除字段 | ❌ 拒绝注册 |
| 修改字段类型 | ❌ 拒绝注册 |
| 可选字段改为必填 | ❌ 拒绝注册 |
| 新增必填字段 | ❌ 拒绝注册 |
| 记录出现本地未注册的字段 | **忽略**（向前兼容读取），不进入输出 |
| 必填缺失 / 类型无法转换 / 未知版本 / 坏行 | 该条 `REJECTED` |

当前内置版本：

- **v1**：`id,symbol,price,volume,seq,event_time`（全必填）。
- **v2**：v1 + 可选 `exchange`（缺省 `"UNKNOWN"`）。
- **v3**：v2 + 可选 `venue_category`（缺省 `"SPOT"`）、`is_maker`（缺省
  `false`）、`tick_size`（缺省 `0`）、`venue_id`（缺省 `0`）。

要点：

- **缺省语义稳定可复现**：同一旧记录无论何时重跑，补出的字段值都一致；
  断点 JSON 对 `time.Time` 与 `int64` 扩展字段使用带类型标记序列化，
  恢复后值类型不退化（int64 不会变成 float64）。
- **只改扩展字段的更正**也能被识别：内容指纹覆盖 `Attrs`，序号更新即
  `MERGED`，完全一致才 `DUPLICATE_DROPPED`。
- **不兼容的结构变更在注册期就被拒绝**，下游不会拿到错误字段语义。

## 容量与大输入约束（内存有上界）

数据文件可能比内存大、也可能突发涌入。内存占用与输入规模解耦：

- **输入通道**容量固定（`-input-buffer`），突发时上游读取端阻塞而非无限堆积。
- **去重表**容量有界（`-dedup-capacity`），FIFO 淘汰最旧条目。
- **处理回执**使用定长环形缓冲（`ResultRingSize`）。
- **乱序重排缓冲**有硬上界（`-max-buffer`）：缓冲超限时，最小事件时间的
  事件被**强制提前输出**（计入 `ForcedEmitted`），同时抬高「已输出地板」，
  之后更旧的事件按迟到丢弃。因此即使数据持续、高度乱序，缓冲也恒定在上界，
  且输出始终事件时间非降序、不丢已接受事件。
- 输入按行流式读取（带缓冲 reader），输出带缓冲 writer，不会整文件载入内存。

> 语义权衡：当乱序幅度超出「水位线窗口」或「缓冲上界」时，过旧事件会被
> 明确判迟到丢弃 / 强制提前输出，而不是无限占内存。这是有界内存下的既定
> 取舍；如需更长的重排能力，调大 `-max-out-of-order` 与 `-max-buffer`。

## 断点与精确一次恢复

- 每处理 `-checkpoint-every` 行提交一次断点：**源字节位点 + sink 字节位点
  + 水位线 + 已输出地板 + 去重状态快照 + 仍滞留缓冲的事件（含扩展字段与
  结构版本）**。源耗尽时再提交一次最终断点。
- 写断点原子化（临时文件 + rename）；输出在断点提交前先 `fsync`。
- 恢复（`Run` 自动）：无断点从头开始并把输出文件截断到 0；有断点则定位到
  源位点，并把输出文件**截断到断点记录的 sink 位点**，未入断点的尾部随之
  重放，最终输出与一口气跑完**逐字节一致**（不多不少）。
- 断点超过 `-checkpoint-ttl` 返回 `ErrExpired` 拒绝恢复；请求位点早于已
  提交位点返回 `ErrRollback` 拒绝回退。
- 已用 `kill -9` 在 20 万行、1000 行一断点的运行中途验证：重跑输出与参考
  逐字节一致。

## 怎么验证

```bash
go test ./... -v
```

测试日志中每个用例打印「业务场景」与「用例结果」。覆盖：

| 场景 | 测试 |
|---|---|
| 端到端：文件输入一路到文件输出、按事件时间有序、汇总计数 | `e2e.TestEndToEnd_FieldPassthrough` |
| 端到端：**新增字段（显式 + 缺省、多类型）完整到达输出**、未声明字段忽略、版本标注 | `e2e.TestEndToEnd_FieldPassthrough`、`jsonio.TestOutputRecordFlat` |
| 端到端：大输入 + 突发流量下**缓冲硬上界、堆不随输入增长、不丢事件且有序** | `e2e.TestEndToEnd_BoundedMemoryUnderBurst`、`watermark.TestBoundedBufferForcesOldest`、`pipeline.TestBoundedBufferForcesAndDropsLate` |
| 端到端：跑到一半被杀、重跑输出逐字节一致（精确一次 + sink 截断） | `e2e.TestEndToEnd_ExactlyOnceAfterRerun`、`jsonio.TestFileSinkSyncTruncate`、`pipeline.TestCrashRecovery` |
| 扩展字段随断点缓冲快照恢复（含 int64/time 类型不退化） | `pipeline.TestAttrsSurviveCheckpoint` |
| 只改扩展字段的更正判定（指纹含 Attrs） | `dedup.TestAttrsAffectVerdict` |
| 两种行格式 / 默认版本 / 坏行不中断 / 字节位点定位 | `jsonio.TestParseLineShapes`、`jsonio.TestFileSourceOffsets` |
| 乱序 / 重复 / 过期 / 更正 / 迟到 | `watermark.TestOutOfOrderDrain`、`dedup.TestVerdicts`、`pipeline.TestOutOfOrderAndDuplicates` |
| 缺省值可复现、未知字段忽略、必填缺失拒绝、兼容/不兼容演进 | `schema.TestNormalize_*`、`schema.TestCompatibility`、`pipeline.TestSchemaEvolution` |
| 断点过期 / 回退拒绝 / 原子写入 | `checkpoint.TestExpired`、`TestRollbackRejected`、`TestAtomicWrite`、`pipeline.TestExpired*`、`pipeline.TestRollbackRejected` |
