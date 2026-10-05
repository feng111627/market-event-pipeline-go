# 行情事件流处理组件（market-event-pipeline-go）

一个可在本地运行的行情事件流处理组件：**接收**上游 JSONL 行情记录，**清洗/规范化**数据结构，
并严格**按事件时间（event time）**输出结果。针对乱序、重复投递、结构演进、崩溃恢复都给出
**可区分的明确结论**——不静默丢数据，也不重复计入结果。

## 目录结构

| 包 | 职责 |
|---|---|
| `internal/event` | 行情事件模型、处理原因（Reason）、可注入时钟 |
| `internal/ingress` | 逐行读取输入并跟踪**精确字节偏移**（断点定位） |
| `internal/schema` | 结构版本注册、兼容性判定、缺省值物化 |
| `internal/pipeline` | watermark、乱序缓冲、去重/合并状态机、背压、编排 |
| `internal/checkpoint` | 断点原子持久化与恢复校验（过期/回退/跨代拒绝） |
| `internal/sink` | 按事件时间排序输出 JSONL、序号幂等、崩溃尾部截断 |
| `cmd/demo` | 本地可运行的端到端示例 |

## 一、事件时间处理语义

组件维护一个**水位 watermark**，含义为“事件时间小于该水位的数据视为已到齐”。

- **水位来源**
  - 输入中的控制记录：`{"type":"watermark","watermark":"2026-10-06T10:00:05Z"}`；
  - 已建立显式基准后，墙钟按 `watermark = 已观测最大事件时间 - 容忍窗口` 兜底推进；
  - **水位只进不退**：倒退的控制记录以 `control_rejected` 明确拒绝。
- **乱序到达 → 等待（`buffered`）**：`event_time >= watermark` 的新记录先进入按事件时间排序的
  缓冲，到点后按 `(event_time, symbol, seq_id)` 确定序输出（`accepted`）。容忍窗口由
  `-lateness` 配置。
- **重复投递 → 丢弃且不计数（`duplicate`）**：以 `seq_id` 为幂等键，对内容（symbol/event_time/
  price/qty/schema 版本/演进字段）计算指纹；`seq_id` 相同且指纹相同即重复，**绝不第二次计入结果**，
  但会以独立原因落日志/统计，不是静默丢弃。已输出记录的判重条目会再保留一个完整容忍窗口，
  因此“输出后才重投”也能被识别为重复。
- **同键不同内容 → 拒绝（`conflict`）**：`seq_id` 相同但指纹不同，说明上游幂等键被复用/数据损坏，
  明确拒绝，绝不当作重复覆盖，也不让错误语义流向下游。
- **同 symbol+event_time 部分字段到达 → 合并（`merged`）**：互补字段（first-non-zero wins）
  合并为**一条**结果，输出时标记 `coalesced from seq_ids=...`；若同一字段两边都是非缺省值且不一致，
  记为 `conflict` 拒绝。
- **真正晚到 → 丢弃（`late_drop`）**：一条**从未见过**的新记录，其 `event_time < watermark`，
  已超过可纳入的时间范围，明确丢弃并记录原因。
- **判定优先级**：先判重（`duplicate`/`conflict`），再判晚到（`late_drop`）。
  这样“旧记录重投”与“另一条真正迟到的新数据”结论可区分，不会把重投误记成晚到。

每种结论都有独立 `Reason` 与 `Stats` 计数：
`accepted / buffered / merged / duplicate / conflict / late_drop / schema_rejected /
control_rejected / backpressure / control_watermark`。

## 二、结构兼容（Schema Evolution）规则

只允许**“加法且带缺省值”**的演进，版本号必须 `+1` 递增（`internal/schema`）：

- ✅ **新增可选字段**：必须声明缺省值。缺省采用 **sticky 缺省**——记录到达时即物化成具体值，
  因此无论之后 schema 如何演进，这条旧记录的取值都**稳定且可复现**。
- ✅ **可选 → 必填**：允许，但必须为未携带该字段的旧记录声明缺省值。
- ❌ 以下变更在 `Register` 阶段直接返回 `ErrIncompatible`，**必须被明确拒绝**：
  删除字段、改字段类型/字段名/语义、必填改可选、**修改已有缺省值**、新增字段不给缺省值、
  重定义核心字段、版本号跳跃或重复注册。
- ❌ 数据侧：未注册的 `schema_v`（`ErrUnknownVersion`）、携带未声明字段、缺少必填核心字段
  （`schema_v/seq_id/symbol/price/qty/event_time`）、坏 JSON/坏时间，一律以 `schema_rejected`
  拒绝，绝不带着错误字段语义继续往下跑。

## 三、断点与恢复（崩溃/重启后“不多不少”）

- **提交点不超过输出位置**：每条结果写入 sink 并 fsync、sidecar 确认序号推进后，才保存 checkpoint
  （临时文件 + fsync + rename，原子覆盖）。
- **断点内容**：`input_offset`（下一行起始字节偏移，精确到字节、含换行）、`watermark`、
  `sink_seq`（下游已确认序号）、`epoch`，以及处理器在途状态（排序缓冲 `pending` 与去重表 `seen`）。
  “已接收但未输出”的在途记录随断点保留，恢复后不会漏算。
- **输出端幂等**：每条结论带全局单调 `Seq`，sink 只接受 `ack+1`；重放产生的旧序号返回
  `ErrAlreadyAcked`，不重复计入。重启时若输出文件残留“已写未确认”的尾部，会被截断回确认点。
- **恢复前强制校验**（`checkpoint.Validator`），任一不满足都**明确拒绝、绝不从中间悄悄接着算**：
  - 断点格式版本不支持 → `ErrBadVersion`；
  - `epoch` 与本次启动不一致（跨代/陈旧断点）→ `ErrStale`；
  - 断点年龄超过 TTL → `ErrStale`；
  - 断点 `sink_seq` 落后于 sink 已确认位置（回退）→ `ErrRegressed`。

## 四、突发流量与内存有界（背压）

- 读取协程 → 处理协程之间是**有界通道**，通道满读取自然阻塞，不会无限堆积。
- 处理协程串行维护全部状态（watermark/去重/缓冲），因此无需加锁。
- 排序缓冲达到 `-buffer-limit` 时触发**背压腾挪**（`backpressure`）：把水位强制推进到在途记录的
  **最小事件时间**，提前到点输出最旧的一批记录以释放内存。保证：**内存有界、不丢数据、不死等**；
  代价是持续高压下缩短乱序等待（最旧数据提前输出）。

## 五、如何运行与验证

### 运行示例

```bash
go run ./cmd/demo -input testdata/sample.jsonl -out /tmp/out.jsonl \
    -checkpoint /tmp/cp.json -epoch 1 -lateness 2s
```

输出 `/tmp/out.jsonl` 每行一条结论（含 `seq/reason/detail/...`），日志逐条打印
`decision reason=... offset=... seq_id=... detail=...`，结尾打印各原因统计。

中断后用**相同 `-epoch`** 重跑即从断点恢复（结果不增不减）；换 `-epoch` 或断点超过 `-ttl` 会被拒绝。

### 跑单测（日志可见业务覆盖与每个用例结果）

```bash
go test ./... -v
go test -race ./...     # 并发竞态检测
```

测试用 `t.Logf` 以 `【业务覆盖】...` 标注场景、`✔` 标注断言结果，覆盖：

- **乱序**：到达序 s1,s3,s2，水位后按事件时间 s1<s2<s3 输出；
- **重复投递**：相同内容重投判 `duplicate` 不计数；同键不同内容判 `conflict`；
- **合并**：互补字段合并为一条，冲突字段拒绝；真正晚到判 `late_drop`；
- **结构演进**：加法演进缺省稳定可复现；删字段/改类型/改缺省/必填可选互换等被拒绝；
  未注册版本、未声明字段、坏记录被拒绝；
- **中断恢复**：在冲刷前“崩溃”（在途记录入断点）→ 同 epoch 恢复，最终逐条结果与一次跑完完全一致；
  错误 epoch / 超 TTL / 序号回退被明确拒绝；输出文件未确认尾部被截断；
- **突发流量**：1000 条 + 极小缓冲，背压腾挪后零丢失、零重复；
- **偏移精确性**：行首/行末字节偏移精确，支持按偏移恢复定位。
