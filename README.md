# 临床数据交换链路（本地可运行，无外部依赖）

一条端到端的本地临床数据流水线：接收多版本 **HL7 v2** 消息与 **FHIR 风格** 就诊数据，
统一成患者 / 就诊 / 检验结果模型，支持乱序、重复、跨版本更新，按调用方权限受控导出，
并对所有判定与导出完整留痕。纯 Go 标准库实现，不依赖数据库、网络或任何外部服务。

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
数值类型冲突等拒绝样本，以及普通用途脱敏、审计用途选择性保留、越权访问被拒等导出场景。

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
| `internal/domain` | 统一模型、`Status` 枚举、唯一错误形态 `RejectError` |
| `internal/ingest` | HL7 v2 ER7 解析器、FHIR 版本化 Bundle 解析器 |
| `internal/normalize` | 必填/类型校验、词表映射、消息内去重、版本 rank |
| `internal/store` | 分片并发、幂等表、新旧版本判定、字段级合并、确定性快照 |
| `internal/audit` | 仅追加、SHA-256 哈希链、可校验防篡改 |
| `internal/export` | 角色 scope、患者白名单、脱敏/审计目的边界 |
| `internal/pipeline` | 编排、结构化日志、统一错误收口 |

## 支持的输入

- **HL7 v2**：`2.3.1`、`2.5.1`；事件类型 `ADT`（患者/就诊）、`ORU`（检验结果）。
  支持 `\r`、`\n`、`\r\n` 行分隔，`|^~` 分隔规则。
- **FHIR**：顶层 `Bundle` + `fhirVersion: "R4"`，条目资源为
  `Patient` / `Encounter` / `Observation`。就诊/患者引用支持 Bundle 内部
  `Patient/xxx` 引用，也支持跨 Bundle 的 `subject.identifier.value`（业务 MRN）。
- 版本不支持（如 HL7 `2.2`、FHIR `STU3`）、未知事件类型、坏 JSON、缺少消息 ID
  都会以明确状态拒绝。

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
`rejected_permission` · `rejected_invalid_mask`。

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
go test -race ./...          # 全部单测 + 并发/竞态
go test -cover ./...         # 覆盖率
go run ./cmd/demo -repeat    # 同批两轮，确认状态序列与最终状态一致
```

关键测试矩阵：

| 关注点 | 测试 |
|---|---|
| 解析/版本/类型/重复 ID 拒绝 | `internal/ingest/parse_test.go` |
| 必填、词表、类型冲突 | `internal/normalize/normalize_test.go` |
| 幂等、冲突、乱序、跨版本、并发 | `internal/store/store_test.go` |
| 权限、脱敏、非法组合、部分失败、错误防泄露 | `internal/export/export_test.go` |
| 审计哈希链、篡改检测 | `internal/audit/audit_test.go` |
| 乱序批次重复运行一致性、并发整链 | `internal/pipeline/pipeline_test.go` |

时钟可注入（`pipeline.New(..., nowFunc)`），演示与测试使用固定时钟，保证
同一批数据任意时刻、任意投递顺序重复运行都得到一致结果。
