# market-event-pipeline-go

本地可运行的行情事件流处理组件：接收上游行情事件，按**事件时间**清洗、
去重、重排后有序输出，支持结构演进、断点恢复、背压与**有界内存的流式
外部数据处理**（文件 / 标准输入 → 文件 / 标准输出）。

## 运行

```bash
go build ./...

# 处理你自己的数据：JSONL，每行一条，可来自文件或标准输入，输出到文件或标准输出
market-pipeline -in data.jsonl -out out.jsonl
cat data.jsonl | market-pipeline                 # 标准输入 -> 标准输出
market-pipeline -in data.jsonl                   # 文件 -> 标准输出

# 跑到一半挂了，接着上次断点重跑（结果不多不少）
market-pipeline -in data.jsonl -out out.jsonl -resume

# 内置演示（乱序/重复/更正/迟到/多版本字段样例）
market-pipeline -demo

go test ./... -v                                  # 全部单测，日志含业务场景与用例结果
go test ./... -race                               # 含竞态检测
```

### 输入 / 输出格式

按行处理（JSONL，每行一个 JSON 对象），行与行之间相互独立。

输入每行用 `version` 标明本行对应的**结构版本**，字段放在 `payload` 里；
也接受顶层平铺形式（除保留键 `version` 外其余字段整体作为 payload）：

```jsonc
{"version": 2, "payload": {"id":"t1","symbol":"BTCUSDT","price":30000,"volume":1.5,"seq":1,"event_time":"2026-10-06T10:00:11Z","exchange":"BINANCE"}}
{"version": 1, "id":"t2","symbol":"BTCUSDT","price":30001,"volume":1.0,"seq":1,"event_time":"2026-10-06T10:00:12Z"}
```

- `event_time` 支持 RFC3339 字符串或 Unix 毫秒（数字）；JSON 数字按
  `float64` 解码，整数毫秒可无损接受，非整数毫秒明确拒绝。
- 空行跳过；无法解析的行（坏 JSON / 缺 `version` / 超长行）不进入流水线，
  计入汇总的“无法解析的输入行”，并给出行号、原因与截断预览，不影响其他行。

输出同样是 JSONL，每行一个对象，**严格按事件时间升序**。对象包含核心字段、
来源版本 `version` 以及该版本所属注册表里**所有已声明字段**（见下节）。

### 运行汇总

整批跑完向标准错误打印汇总：总处理记录数，以及每种结论各多少——
正常接收 `ACCEPTED`、重复丢弃 `DUPLICATE_DROPPED`、过期丢弃 `STALE_DROPPED`、
合并更正 `MERGED`、迟到丢弃 `LATE_DROPPED`、明确拒绝 `REJECTED`，
外加实际唯一输出数 `EMITTED`、断点提交次数与无法解析的输入行。
五类处置互斥，合计等于总处理记录数。

### 常用参数

| 参数 | 含义 | 默认 |
|---|---|---|
| `-in` / `-out` | 输入 / 输出 JSONL；缺省或 `-` 为标准输入 / 标准输出 | stdin / stdout |
| `-resume` | 从已有断点续跑（默认忽略旧断点、从头开始并覆盖它） | 关 |
| `-checkpoint` | 断点文件路径 | `data/checkpoint.json` |
| `-max-buffered` | 乱序缓冲硬上界；`0` 用默认值，负数不限制 | 100000 |
| `-dedup-capacity` | 去重表容量（FIFO 淘汰） | 100000 |
| `-input-buffer` | 输入通道容量（突发背压边界） | 1024 |
| `-checkpoint-every` | 每处理多少条提交一次断点 | 500 |
| `-checkpoint-ttl-hours` | 断点有效期（小时），负数不过期 | 24 |

## 扩展字段的流转规则与兼容范围

### 归一化到“当前所有已声明字段”

记录先按**自身版本**做必填/类型校验（不兼容记录在此被拒绝），再统一按
注册表中**最新版本结构**归一化。因此：

- 无论上游按哪个兼容版本发数，最终输出都包含当前注册表里的**全部已声明
  字段**，下游不会因为发数方是旧版本就缺字段；
- 老版本缺失的新增**可选字段**，一律补齐为该字段声明的稳定缺省值
  （未声明缺省则用类型零值），缺省语义稳定、可复现；
- 记录里出现但结构中**未声明**的字段在清洗阶段即被忽略，不会进入输出；
- 输出对象保留来源 `version` 以便追溯该条记录的原始结构版本。

内置结构：V1 为核心字段；V2 新增可选 `exchange`（缺省 `"UNKNOWN"`）；
V3 再新增可选 `venue_mic`（缺省 `"XXXX"`）与 `is_maker`（缺省 `false`）。
一条 V1 记录的输出因此也会带上这三个字段的缺省值。

### 扩展字段参与去重/更正判定

内容指纹除核心行情字段外，**也包含全部扩展字段**（按字段名排序后稳定哈希，
时间统一用 RFC3339Nano）。所以“核心字段不变、只更新新增字段”的版本更新：

- 序号更新 → `MERGED`，合并进缓冲中的旧版本，新字段值到达下游；
- 序号相同/更旧 → `STALE_DROPPED`；
- 内容完全一致 → `DUPLICATE_DROPPED`。

扩展字段随乱序缓冲一起进入断点快照；其中的时间值用显式类型标记序列化，
崩溃恢复后类型不退化，内容指纹保持一致，不会把同一事件误判成更正或重复。

### 兼容范围（演进规则）

| 变更 | 结论 |
|---|---|
| 新增**可选**字段（带稳定缺省值） | ✅ 兼容；老数据补缺省值，新字段完整到下游 |
| 删除字段 / 修改字段类型 / 可选改必填 / 新增必填字段 | ❌ 注册时拒绝 |
| 记录出现本地未注册的字段 | 忽略，不进入输出（向前兼容读取） |
| 必填字段缺失 / 类型无法转换 / 未知 `version` | 该条 `REJECTED`，不让错误语义流到下游 |

## 容量约束（大输入与突发流量）

设计目标：**内存占用有上界，不随输入规模增长；不丢事件、不重复计入。**

- **流式读写**：按行读取、逐条写出，读取与写出缓冲固定（64KiB）；整个文件
  从不需要一次性进内存，可处理大于内存的文件。
- **单行上界**：单行超过 8MiB 整条记为坏行并跳过（字节仍读出以保证位点连续），
  坏行只保留 256 字节预览，单条异常输入不会撑爆内存。
- **输入背压**：输入通道容量固定（`-input-buffer`），突发洪峰时上游阻塞而非
  无限堆积。
- **去重表有界**：容量固定（`-dedup-capacity`），FIFO 淘汰最旧条目。
- **乱序缓冲硬上界**：缓冲超过 `-max-buffered` 时，按事件时间升序**强制提前
  输出**最旧事件，缓冲长度恒定在上界内；被提前越过的最大事件时间记为输出
  下限，此后到达且不晚于该下限的事件按 `LATE_DROPPED` 处理，从而保证已输出
  序列始终全局有序。
- **回执环形缓冲**：处理回执只保留最近 `ResultRingSize` 条，观测内存有界。
- **断点**：每处理固定条数原子落盘一次（临时文件 + rename）。

权衡：在极端乱序（事件时间落后超过缓冲窗口）下，为保证有界内存会提前输出
最旧事件，过晚到达者按迟到丢弃——即用“有界、确定的丢弃规则”换取内存上界，
所有丢弃都有明确回执与计数，绝不静默。

## 断点与恢复（不多不少）

- 文件输入按**字节位点**定位；断点记录“下一行首字节偏移”，`-resume` 时
  `seek` 到该位置，已提交位点之前的行不会重读。
- 标准输入/管道不可定位：只能从头顺序读；对非零恢复位点会**明确报错**，
  不会从错误位置接着算。
- 崩溃时已输出但尚未入断点的少量行会被重放，由接收端幂等契约吸收
  （文件输出按事件 ID 去重；向同一文件追加时，需下游/作业侧保证幂等）。
- 断点超过 TTL 返回 `ErrExpired`、请求位点早于已提交位点返回 `ErrRollback`，
  均明确拒绝。

## 架构

```
文件/stdin -> streamio.LineSource(按行/字节位点)
          -> [schema 校验 + 归一化到最新已声明字段]
          -> [dedup 去重/更正判定（指纹含扩展字段）]
          -> [watermark 迟到判定 + 有界乱序缓冲/溢出提前输出]
          -> 按事件时间输出 -> streamio.LineSink(JSONL, 幂等) -> 文件/stdout
          -> 周期性原子 checkpoint（位点 + 水位线 + 去重状态 + 缓冲快照含扩展字段）
```

| 包 | 职责 |
|---|---|
| `internal/event` | 行情事件模型（含扩展字段容器 `Attrs`）与处置结论枚举 |
| `internal/schema` | 结构描述、演进兼容性校验、缺省值填充、类型转换 |
| `internal/dedup` | 重复 / 过期 / 更正判定（指纹含全部已声明字段） |
| `internal/watermark` | 事件时间水位线、有界乱序缓冲（溢出按序提前输出） |
| `internal/checkpoint` | 断点原子持久化（含缓冲扩展字段）、过期/回退校验 |
| `internal/pipeline` | 流水线编排、归一化、背压、断点提交与恢复、汇总 |
| `internal/streamio` | 按行流式读取（文件/stdin）与 JSONL 幂等写出（文件/stdout） |

## 怎么验证

```bash
go test ./... -v
```

测试日志中每个用例都打印「业务场景」与「用例结果」。本轮新增/相关覆盖：

| 场景 | 测试 |
|---|---|
| 外部文件端到端：多版本混发、新字段（含缺省）完整到输出、未知字段忽略、按事件时间有序 | `pipeline.TestE2EFileNewFieldsFlowToOutput` |
| 仅更新扩展字段的更正被识别为 MERGED 并到下游 | `pipeline.TestAttrsOnlyCorrectionMerged` |
| 大输入（20 万条）有界内存、不丢不重、有序、每行字段完整 | `pipeline.TestE2ELargeInputBoundedMemory` |
| 极端乱序下缓冲硬上界、溢出按序提前输出、不丢且全局有序 | `pipeline.TestE2EBufferedOverflowBounded`、`watermark` 用例 |
| 真实文件崩溃→按字节位点续跑，结果不多不少、扩展字段随缓冲快照往返 | `pipeline.TestE2EFileCrashResume`、`checkpoint.TestBufferedEventAttrsRoundtrip` |
| 信封/平铺输入、坏行、超长行、字节位点定位、非可定位输入拒绝 | `streamio.TestLineSource*` |
| JSONL 幂等写出、全部已声明字段（含时间类型）完整 | `streamio.TestLineSinkEmitsAllDeclaredFields` |
| JSON 数字毫秒时间戳转换 | `schema.TestNormalize_TimeCoerce` |
| 乱序/重复/过期/更正/迟到、结构演进、断点过期/回退、突发背压 | 既有 `watermark/dedup/schema/checkpoint/pipeline` 用例（无回归） |
