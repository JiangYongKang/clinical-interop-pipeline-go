# clinical-interop-pipeline-go

本地可运行的临床数据交换链路：接收 HL7 v2 / FHIR 风格消息，归一化为统一模型，
做幂等、按版本序号裁决的并发安全更新，并按调用方权限受控导出。不依赖任何外部服务。

```
cmd/pipeline        本地端到端演示（接收 -> 归一化 -> 幂等更新 -> 受控导出）
internal/model      统一模型：Patient / Encounter / Observation / Update
internal/ingest     HL7 v2 与 FHIR 风格 JSON 的解析与归一化，带原因码的错误
internal/store      并发安全、幂等、按序号裁决新旧版本的内存存储
internal/export     按角色受控导出：强制脱敏 / 审计留痕 / 整体拒绝
```

## 数据映射

统一主键（跨来源稳定去重）：

| 实体 | 主键规则 |
| --- | --- |
| Patient | `PAT:<大写MRN>`，HL7 取 PID-3 首组件，FHIR 取 `identifier[0].value` |
| Encounter | `ENC:<来源系统>:<就诊号>`，HL7 取 MSH-3 + PV1-19，FHIR 取 `x-source` + `Encounter.id` |
| Observation | HL7：`OBS:<MSH-3>:<OBR-3>:<OBX-1>`；FHIR：`OBS:<x-source>:<Observation.id>` |

字段映射要点：

| 统一字段 | HL7 v2 | FHIR 风格 JSON |
| --- | --- | --- |
| 消息标识（幂等键） | MSH-10 | `id`（Bundle） |
| 版本序号（排序键） | 自定义段 `ZVR\|<整数>` | `x-sequence`（整数） |
| 来源版本 | MSH-12（支持 2.3 / 2.5） | `fhirVersion`（支持 R4 / 4.0.x） |
| 姓名 | PID-5（姓+名） | `name[0].family/given` |
| 出生日期 | PID-7（YYYYMMDD） | `birthDate`（YYYY-MM-DD） |
| 性别 | PID-8（M/F/O→male/female/other） | `gender`（原样校验） |
| 就诊状态 | ADT^A03→finished，其余 in-progress | `Encounter.status`（白名单校验） |
| 检验值 | OBX-5（OBX-2=NM 时必须为数值） | `valueQuantity.value`（必须数值）或 `valueString` |
| 检验状态 | OBX-11（F/P/C→final/preliminary/corrected） | `status`（amended→corrected，registered→preliminary） |

## 幂等与兼容规则

- **幂等去重**：消息标识（MSH-10 / Bundle id）首次裁决结果被记录，重复投递返回首次结果，
  状态为 `deduped`，不产生重复记录。
- **版本裁决**：每个实体按 `x-sequence` / `ZVR-1` 的整数序号裁决——序号更大者胜出；
  序号相等视为同一逻辑版本的重复投递（跳过）；序号更小为过期数据（`stale`），
  **旧版本绝不覆盖新版本结论**。该规则对 HL7 与 FHIR 来源使用同一序号轴，天然支持跨版本更新。
- **乱序/并发安全**：写入路径单互斥锁、读快照读锁，锁内不调用外部代码，无锁序、无死锁；
  同一患者并发处理不会产生重复记录或丢失更新（`go test -race` 验证）。
- **可重复验证**：同一批消息以任意顺序、任意次数重放，最终快照一致（快照按主键排序输出）。
- **不静默忽略**：字段缺失（`missing_field`）、类型冲突（`type_conflict`）、
  版本不支持（`unsupported_version`）、编码非法（`invalid_encoding`）都会以带原因码的
  错误显式拒绝，错误中只含字段路径与原因，不含原始敏感值。

## 权限与脱敏边界

| 用途 | 未脱敏请求 | 脱敏请求 |
| --- | --- | --- |
| `general`（普通） | 拒绝：`illegal_masking_combination` | 允许，强制脱敏 |
| `audit`（审计） | 允许，完整留痕 | 允许，完整留痕 |
| 其他角色 / 空调用方 | 拒绝：`unauthorized` | 拒绝：`unauthorized` |

- 脱敏规则：姓名、病历号置空；出生日期仅保留年份（`1980-**-**`）；
  患者主键经加盐哈希假名化，且就诊/检验中的关联键同步替换，连接关系保持一致。
- 指定患者清单导出时，任何一个不存在即**整体拒绝**（`partial_failure`），不允许部分成功。
- 每一次导出（含被拒绝的）都写入审计留痕：调用方、角色、是否未脱敏、结果与原因。
- 所有错误响应只携带原因码与计数，不回显任何原始敏感字段（有测试保证）。

## 本地验证

```bash
go build ./...          # 编译
go test -race ./...     # 全部测试（含并发竞态检测）
go run ./cmd/pipeline   # 端到端演示：应用/乱序/重复/拒绝/脱敏/审计
```

演示与测试日志均打印输入标识（message_id）、判定依据（reason）与最终状态（status）；
同一批数据重复运行输出一致（演示脚本输出已对时间戳归一后可 diff 验证）。
