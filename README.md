# 临床数据交换链路（本地可运行，无外部依赖）

一条端到端的本地临床数据流水线：接收多版本 **HL7 v2** 消息与 **FHIR 风格** 就诊数据，
统一成患者 / 就诊 / 检验结果模型，支持乱序、重复、跨版本更新，按调用方权限受控导出，
并对所有判定与导出完整留痕。纯 Go 标准库实现，不依赖数据库、网络或任何外部服务。

本轮在以上基础上新增三块**可信、可回溯**能力：

1. **整批原子提交（`IngestBatch` / `Store.ApplyBatch`）**：一次提交几十上百条消息，
   任意一条不被接受则整批不落库；同批原样重跑要么整批去重、要么整批拒绝；
   失败批次修正后可原批次号重发并全部生效。
2. **历史与时间点回溯（`Store.SnapshotAt` / `Timeline`）**：按“业务时间点”重建
   当时的结论，迟到的旧消息不能改写已定稿的历史；更正与撤回在时间线上保留而不是被抹掉。
3. **一致性分页导出（`Service.ExportPage`）**：翻页固定在同一时间点视图上，并发写入
   不会让页间漂移；混入不存在或越权的患者时整次请求拒绝，拒绝不带任何原始敏感字段。


## 快速开始

```bash
# 运行所有测试（含竞态检测）
go test -race ./...

# 覆盖率
go test -cover ./...

# 端到端演示：接收 → 标准化 → 幂等存储 → 拒绝路径 → 受控导出 → 审计校验
go run ./cmd/demo

# 同一批数据跑两轮（独立内存库），对比状态序列与最终状态完全一致
go run ./cmd/demo -repeat
```

演示内置了正常消息、迟到的旧版本消息、版本不支持 / 缺字段 / 坏 JSON / 未知格式 /
数值类型冲突等拒绝样本，以及普通用途脱敏、审计用途选择性保留、越权访问被拒等导出场景；
另含本轮新增的整批原子提交、按业务时间点回溯、一致性分页导出三段演示。

## 架构与处理流程

```
wire payload (HL7 v2 / FHIR JSON)
   │  internal/ingest    解析 + 版本/类型/重复 ID 校验（绝不静默忽略）
   ▼
RawDocument（指针字段区分“缺失”与“空值”）
   │  internal/normalize 必填校验、词表统一、消息内去重、版本排序
   ▼
ChangeSet（canonical Patient/Encounter/Observation + VersionRank）
   │  internal/store     幂等表 → 新旧判定 → 类型冲突判定 → 字段级合并（单分片锁原子提交）
   ▼
内存分片库（64 分片，按患者 MRN 哈希）
   │  internal/export    角色 scope + 患者白名单 + 脱敏边界
   ▼
受控导出结果（普通用途全脱敏 / 审计用途按白名单保留）

每个阶段都写 internal/audit（SHA-256 哈希链，仅含标识，不含原始字段）
```

| 包 | 职责 |
|---|---|
| `internal/domain` | 统一模型、`Status` 枚举（含 `rejected_batch`）、唯一错误形态 `RejectError` |
| `internal/ingest` | HL7 v2 ER7 解析器、FHIR 版本化 Bundle 解析器 |
| `internal/normalize` | 必填/类型校验、词表映射、消息内去重、版本 rank |
| `internal/store` | 分片并发、幂等表、新旧版本判定、字段级合并、**整批原子提交**、**只追加修订日志 / 时间点回溯 / 时间线**、确定性快照 |
| `internal/audit` | 仅追加、SHA-256 哈希链、可校验防篡改 |
| `internal/export` | 角色 scope、患者白名单、脱敏/审计目的边界、**一致性分页导出** |
| `internal/pipeline` | 编排（单条 + **整批**）、结构化日志、统一错误收口 |

## 支持的输入

- **HL7 v2**：`2.3.1`、`2.5.1`；事件类型 `ADT`（患者/就诊）、`ORU`（检验结果）。
  支持 `\r`、`\n`、`\r\n` 行分隔，`|^~` 分隔规则。
- **FHIR**：顶层 `Bundle` + `fhirVersion: "R4"`，条目资源为
  `Patient` / `Encounter` / `Observation`。就诊/患者引用支持 Bundle 内部
  `Patient/xxx` 引用，也支持跨 Bundle 的 `subject.identifier.value`（业务 MRN）。
- 版本不支持（如 HL7 `2.2`、FHIR `STU3`）、未知事件类型、坏 JSON、缺少消息 ID
  都会以明确状态拒绝。

跨版本兼容范围在三条链路上保持一致：HL7 `2.3.1` / `2.5.1` 与 FHIR `R4` 不仅共享同一份
规范化模型、词表与字段级合并规则，也共享同一套版本 rank 全序——在线新旧判定、整批干跑
判定、以及 `SnapshotAt` 时间点取值都用 `(UpdatedAt, rank, writer)` 这一条全序，因此在
任一时间点、任一投递顺序下，三个版本的取值规则统一且可解释。

## 数据映射规则

### 标识（稳定去重的键）

| 模型 | 稳定标识来源 |
|---|---|
| Patient | HL7 PID-3 / FHIR Patient.identifier[0].value（业务 MRN） |
| Encounter | HL7 PV1-19 / FHIR Encounter.identifier[0]（无则用资源 id） |
| Observation | HL7 OBX-21（2.5.1）；缺失时合成 `OBR-3-OBX-1-检验代码`（同组多条结果稳定且唯一）；FHIR identifier[0]（无则资源 id） |

同一消息内出现重复的观察结果 ID、重复 Patient/Encounter 资源，直接判
`rejected_duplicate_id`。

### 字段映射与词表统一

- **性别**：HL7（M/F/O/A/U）与 FHIR（male/female/...）统一为
  `male | female | other | unknown`。
- **就诊类别**：HL7 patient class（I/O/P/E/R）与 FHIR ActEncounterCode 统一为
  `IMP | AMB | EMER | OBS`。
- **结果状态**：HL7 OBX-11（F/P/R/C/X/D 等）与 FHIR Observation.status 统一为
  `registered | preliminary | final | amended | cancelled | entered-in-error`。
- **数值类型**：HL7 OBX-2=`NM` 与 FHIR `valueQuantity` 解析为数值；
  `ST/TX/CWE` 与 `valueString/valueCodeableConcept` 解析为字符串。
  声明类型与实际值不符（如 `NM|not-a-number`）、同一 ID 新旧值类型不一致
  （numeric vs string），判 `rejected_type_conflict`，绝不静默丢弃。
- **日期时间**：HL7 DTM（`YYYYMMDDHHMMSS[.S][±ZZZZ]`）归为 UTC；
  FHIR instant 归 UTC RFC3339，日期保留 `YYYY-MM-DD`。

### 必填字段（缺失即拒 `rejected_missing_field`，不静默忽略）

- Patient：`mrn`、`family_name`、`birth_date`、`gender`
- Encounter：`visit_id`、`mrn`、`class`
- Observation：`id`、`mrn`、`test_code`、`status`、且必须有且仅有一种值

可选字段（GivenName、TestName、Unit、EffectiveTime、就诊起止时间）在更新缺失时
不覆盖已有值（字段级合并）。

## 幂等、乱序与跨版本兼容规则

### 幂等键与内容摘要

- 幂等键为**消息 ID**（HL7 MSH-10 / FHIR Bundle.identifier.value）。
- 每条消息对其规范化内容计算 SHA-256 `Digest`（时间取自消息业务时间，因此与投递时刻无关）。
- 同消息 ID + 同摘要重放 → `deduplicated`（稳定去重，无副作用）。
- 同消息 ID + 不同内容 → `conflict`（拒绝，且不改动任何已存数据）。

### 新旧判定（旧结论永不覆盖新结论）

每个实体记录来源三元组 `(UpdatedAt, VersionRank, WriterMsgID)`，候选写入按
以下顺序比较，**与投递顺序无关**：

1. 业务时间 `UpdatedAt` 较新者胜；
2. 时间相同，线格式版本 rank 较高者胜（rank：HL7 2.3.1=10 < 2.5.1=20 < FHIR R4=30）；
3. 仍相同，消息 ID 字典序较小者胜（确定性兜底，保证可重复收敛）。

候选结论更旧 → 整条消息判 `stale`，拒绝写入。由于比较是全序的，乱序、重复、
跨版本投递在重复运行下都会收敛到**同一个最终状态**（见
`store.TestApply_OrderIndependentConvergence` 与 `go run ./cmd/demo -repeat`）。

字段级合并：必需身份/结论字段用新值覆盖；可选字段仅在新消息实际携带时更新。

### 并发安全（无重复、无丢失、无死锁）

- 存储按患者 MRN 哈希分 64 片；一条消息的所有实体属于同一患者，故**只需持有
  一把分片锁**即完成整消息的原子检查与合并。
- 全局幂等锁始终在“分片锁之后”获取，加锁顺序全局一致，因此不可能形成锁环（无死锁）。
- 拒绝路径（stale/type-conflict）不写数据、不登记幂等，修正后可重发。
- `go test -race` 验证；并发用例覆盖同一患者大量重复投递（仅一条患者记录、
  无丢失更新）与大量不同患者（无丢失插入）。

### 处理结果状态

`accepted` · `updated` · `deduplicated` · `conflict` · `stale` ·
`rejected_malformed` · `rejected_message_type` · `rejected_missing_field` ·
`rejected_type_conflict` · `rejected_version` · `rejected_duplicate_id` ·
`rejected_permission` · `rejected_invalid_mask` · `rejected_batch`。

## 整批原子提交

除单条 `Ingest` 外，管线提供 `Pipeline.IngestBatch(batchID, []BatchMessage)`
（底层 `store.Store.ApplyBatch`），语义为“**全有或全无**”：

- **先全部解析、规范化**：任何一条版本不支持 / 坏格式 / 必填缺失 / 类型不符，
  在触碰存储之前整批拒绝，已通过的消息也不会先落库。
- **再一次性事务式校验**：整批涉及的分片按**分片索引升序**加锁、全局幂等锁最后取，
  加锁顺序全局唯一，因此并发批次不可能形成锁环（无死锁）。在持锁状态下，把批内消息按
  消息 ID 排序，对“已存状态 + 批内暂存覆盖层”做一次**干跑（dry-run）**：
  stale、跨消息类型冲突、批内非单调结论等都在这一阶段发现。
- **提交阶段才写入**：干跑通过后才真正合并、登记幂等；因此**不存在半截状态**。
- **失败不留痕、不毒化**：被拒批次既不写业务数据，也不登记批次/消息幂等。把问题消息
  改好后，**用同一个批次号**整批重发即可全部生效，上一次失败不会拖住它。
- **可重复验证的重放**：
  - 同批次号 + 同一组消息内容（**与提交顺序无关**，批次摘要对单消息摘要排序后再哈希）
    → 整批 `deduplicated`，结果与首次完全一致；
  - 同批次号 + 不同内容，或批内某消息已在本批之外提交过（会造成不可复现的部分重叠）
    → `rejected_batch`，整批拒绝、零改动。

关键用例见 `store/batch_test.go`（原子性、修正重发、乱序重放去重、与已提交消息重叠、
批内类型冲突、批内重复 ID、40 个跨分片批次并发无死锁无丢失）与
`pipeline/batch_test.go`（真实 HL7 2.3.1/2.5.1 + FHIR R4 报文、端到端拒绝路径不泄露
`BATCHSECRET/BATCHSUR` 等原始字段、失败后同批次号修正重发）。

## 历史与时间点回溯

存储在每次**实际获胜的写入**时，向该实体的只追加修订日志（revision log）追加一条
`EntityRevision`，记录：业务结论实体、业务时间 `UpdatedAt`、线格式版本 `Rank`、
获胜消息 ID（`Writer`），以及该结论真正进入系统的**记录时间 `RecordedAt`**。

- `Store.SnapshotAt(at, HistoryOptions{MRN? Kind? ID?})` 重建“截至业务时间点 `at`”的
  患者 / 就诊 / 检验结果视图；
- `Store.Timeline(kind, id)` 返回单个实体按业务时间升序的**全部获胜修订**，
  更正（`amended`）与撤回（`cancelled` / `entered-in-error`）都作为独立条目保留。

### 一条修订在时间点 `at` 可见，当且仅当两个条件同时成立

1. `UpdatedAt <= at`：结论所述业务时间不晚于 `at`（面向未来的结论不计入）；
2. `RecordedAt <= at`：结论在 `at` 时**确实已经送达**。

第二条是“**迟到的旧消息不能改写已经定下来的历史**”的关键：一条很旧业务时间、但更晚
才送到的更正，在更早的观察者看来当时并不存在，因此不会出现在那个历史视图里；它只会
出现在送达之后的视图中。当前时刻的视图仍由原有“旧结论永不覆盖新结论”规则保证
（迟到且更旧的消息本来就判 `stale`）。

### 跨版本统一取值规则（HL7 2.3.1 / 2.5.1 / FHIR R4 完全一致）

同一实体在 `at` 时点的多条合格修订，按与在线写入**完全相同的全序**取唯一胜者：

1. 业务时间 `UpdatedAt` 新者胜；
2. 时间相同，线格式版本 rank 高者胜（2.3.1=10 < 2.5.1=20 < FHIR R4=30）；
3. 仍相同，写入消息 ID 字典序小者胜（确定性兜底）。

因为是全序、且只依赖不可变修订，同一患者同一时间点反复查询得到**逐字节一致**的结果。
字段级合并规则也与在线一致（必需结论字段覆盖、可选字段仅在新消息携带时更新），修订
日志存的是每次合并后的结论快照，时间点重建即“按时间重放获胜快照”。

`export.Request.AsOf` 与 `export.PageRequest.AsOf` 非零时，受控导出/分页导出直接读取
该不可变历史视图，权限、脱敏、白名单规则与当前导出完全一致。

关键用例见 `store/history_test.go`：逐时间点重建、重复查询一致、迟到记录的更正不改写
过去、同业务时间跨版本 rank 取胜者、更正后撤回在时间线上全程可见、按 MRN 过滤。

## 一致性分页导出

`Service.ExportPage(PageRequest)` 在受控导出上提供分页：

- **固定一致视图**：不带 `Cursor` 的请求会对一次快照（当前态或 `AsOf` 历史态）打开一个
  只读会话，返回不透明游标；后续每页带上该游标。会话中的快照**不可变**，因此翻页期间
  即使有并发写入，同一次读取的每一页都来自同一个时间点，**页与页之间不漂移、不重不漏**。
- **稳定可复现顺序**：全局行序为 `(资源种类 patient<encounter<observation, 稳定 ID)`，
  页内与跨页顺序确定；同一读取重放得到相同分页。中途改变 `PageSize`、游标损坏/过期
  都会被拒绝。
- **名单式导出整体拒绝**：`PatientMRNs` 里只要有一个患者**不存在**或**超出白名单**，
  整次请求直接 `rejected_permission`，不返回任何部分结果。两种情况使用同一句不含数据的
  原因（“unknown or outside actor allow-list”），既不泄露患者是否存在，也不回显原始
  MRN；未知调用方即便持有合法游标也照样拒绝。
- 每页写一条审计（allow/deny、offset/count/total 或拒绝依据），脱敏与伪名规则沿用
  现有导出，保证分页结果仍可做患者—就诊—结果关联而不暴露身份。

关键用例见 `export/page_test.go`：翻页过程中持续并发写入仍固定 6 患者×3 行=18 行、
之后新开的读取才看到新写入；名单含不存在患者/越权患者整体拒绝且错误不含原始 ID；
游标损坏、页大小变化、未知调用方持游标、历史 `AsOf` 空视图等。


## 权限与脱敏边界

### 角色

角色由 `scope` 集合 + 患者白名单（`"*"` 为全部）构成：

- scope：`patient:read`、`encounter:read`、`observation:read`、`audit:unmask`。
- 导出时逐患者核对白名单：**只要有一个 MRN 越权，整次请求拒绝**，不产生部分结果。
- 未知调用方、无任何可读 scope，一律 `rejected_permission`。

### 脱敏组合（合法性先行）

| 用途 | 身份字段 | 要求 |
|---|---|---|
| `normal` | 始终脱敏 | 任何 `UnmaskedFields` 都判 `rejected_invalid_mask` |
| `audit` | 默认脱敏，可按白名单选择性保留 | 必须具备 `audit:unmask` scope，且字段在允许清单内 |

可保留字段白名单仅含：`family_name`、`given_name`、`birth_date`、`gender`；
请求清单外字段（如 ssn）判 `rejected_invalid_mask`。

### 脱敏方式与关联性

- 姓名 → `***`，出生日期 → `****-**-**`，性别 → `*`。
- MRN / VisitID → HMAC-SHA256 伪名（`pid-xxxx`，按标识种类加前缀，
  `mrn:` / `visit:` 命名空间隔离，防碰撞）。伪名**确定性**：同一患者跨次导出
  伪名一致，因此患者—就诊—检验结果在脱敏后仍可关联，但无法反推身份。
- 检验结果数值、代码、状态、就诊时间等非身份数据照常导出。

### 审计与错误防泄露

- 每次导出（允许或拒绝）都恰好写一条审计记录，包含 actor、purpose、判定依据、
  命中的资源标识；审计导出额外记录保留了哪些字段。
- 审计日志为 SHA-256 哈希链（每条含前一条哈希），`Log.Verify()` 可检测任何篡改。
- **所有错误响应只含调用方提供的 ref、状态码与不含数据的原因**；测试显式断言
  错误、日志、审计资源中都不会出现原始姓名等敏感字段。

## 日志

管线使用结构化日志（可注入任意实现），每条输入都打印：
输入标识（ref、msg_id、format、version）、判定依据（basis）与最终状态（status）、
内容摘要（digest）。拒绝路径同样记录 ref、所处阶段与状态，但不打印任何原始载荷字段。

## 本地验证方法

```bash
go vet ./...                 # 静态检查
go test -race ./...          # 全部单测 + 并发/竞态（含整批、回溯、分页并发）
go test -cover ./...         # 覆盖率
go test -v ./...             # 逐条用例结果；新增用例额外打印 business=... case=... result=...，
                             # 可直接看出覆盖了哪些业务、每个业务有哪些用例与结果
go run ./cmd/demo            # 端到端演示，新增“整批提交 / 历史回溯 / 分页导出”三段
go run ./cmd/demo -repeat    # 同批两轮，确认状态序列与最终状态一致
```

演示新增三段：

1. **atomic batch ingestion**：含一条 2.2 坏消息的批次被整批拒绝（0 条落地）→
   改成 2.5.1 后同批次号重发全部生效 → 打乱顺序原样重放得到 `deduplicated`。
2. **history & point-in-time reconstruction**：一条检验结果先 `final`、后更正
   `amended`、再撤回 `cancelled`，分别查询各业务时间点看到对应结论，并打印保留全部
   三个版本的时间线（记录时间与业务时间刻意错开，演示迟到数据不改写历史）。
3. **paginated consistent export**：以每页 2 行翻完全部数据，展示稳定的
   patient/encounter/observation 行序；并用一个不存在的患者演示名单式请求被整体拒绝、
   错误不回显原始 ID。

关键测试矩阵：

| 关注点 | 测试 |
|---|---|
| 解析/版本/类型/重复 ID 拒绝 | `internal/ingest/parse_test.go` |
| 必填、词表、类型冲突 | `internal/normalize/normalize_test.go` |
| 幂等、冲突、乱序、跨版本、并发 | `internal/store/store_test.go` |
| **整批原子性、修正重发、乱序重放去重、重叠/类型冲突、批次并发** | `internal/store/batch_test.go` |
| **时间点重建、重复一致、迟到更正不改写历史、跨版本同点取值、更正/撤回时间线** | `internal/store/history_test.go` |
| 权限、脱敏、非法组合、部分失败、错误防泄露 | `internal/export/export_test.go` |
| **翻页并发一致性、名单不存在/越权整体拒绝、游标校验、历史视图分页** | `internal/export/page_test.go` |
| 审计哈希链、篡改检测 | `internal/audit/audit_test.go` |
| 乱序批次重复运行一致性、并发整链、**端到端整批原子性/不泄密/修正重发** | `internal/pipeline/pipeline_test.go`、`internal/pipeline/batch_test.go` |

时钟可注入（`pipeline.New(..., nowFunc)`），演示与测试使用固定时钟，保证
同一批数据任意时刻、任意投递顺序重复运行都得到一致结果。
