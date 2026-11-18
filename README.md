# 解析器插件系统 (Parser Plugin System)

数据源可绑定不同版本的解析器（解析配置 + 字段映射 + 校验规则）。插件发布前生成兼容报告；
发布后监控错误率与空结果比例，超阈值自动回滚到上一版本，并保留已处理批次与差异样本。
页面支持版本对比、灰度范围调整、手动回滚与批次重放。

## 快速开始

```bash
pip install fastapi uvicorn pytest httpx
python3 seed.py                                # 生成演示数据（可选）
python3 -m uvicorn app.main:app --port 8377    # 打开 http://localhost:8377
python3 -m pytest tests/ -q                    # 37 个测试
```

## 架构

```
app/
  plugins.py   解析引擎：声明式规格（parser / field_mapping / validation），纯函数
  pipeline.py  批次处理：run 不可变、确定性灰度路由、单事务写入
  compat.py    发布前兼容报告（基线 vs 候选，内存中双跑，不写生产表）
  monitor.py   发布 / 灰度 / 指标评估 / 自动与手动回滚
  main.py      FastAPI HTTP API + 静态页面
  static/index.html  管理界面（来源、版本、对比、批次重放、监控图表）
tests/         37 个测试：解析器、流水线、兼容报告、回滚、隔离性、API 全流程
```

### 核心概念

- **插件版本**：`{parser, field_mapping, validation}` 的声明式 JSON 规格。
  `field_mapping` 决定 canonical 输出字段；未映射的原始字段只进入 `extras` 侧车，
  绝不混入 canonical —— 这是新字段隔离边界。
- **来源 (source)**：绑定一个 active 版本，可选一个 gray 版本 + 灰度比例，
  以及 `error_threshold` / `empty_threshold` / `min_sample`。
- **run**：一次处理过程的不可变记录（batch × version × kind）。
  重放产生新 run，永不改写旧 run。
- **灰度路由**：`sha256(source_id:record_key) % 100 < gray_percent`，
  按记录确定性分流，重放可精确复现。

### 发布流程

1. `POST /api/plugins/versions` 注册版本（draft，先做规格校验）
2. `POST /api/plugins/versions/{id}/compatibility?source_id=` 生成兼容报告：
   用该来源近期原始记录双跑基线与候选，统计状态翻转、字段差异、新增/移除字段，
   按来源阈值给出 `pass / warn / fail`
3. `POST /api/plugins/versions/{id}/publish`（`fail` 报告默认拒绝，`force=true` 可强制；
   `gray_percent>0` 为灰度发布）
4. 每次批次处理后 `evaluate_source` 检查 live 指标：错误率或空结果率超阈值
   （且样本量 ≥ min_sample）即自动回滚到上一个 active 版本，
   故障版本标记 `rolled_back`（不可再发布），保留其全部 run，
   并抓取「故障版本 vs 回滚目标」的差异样本

### 隔离性保证（tests/test_isolation.py）

- **新字段**：v1 的 run 结果中绝不出现 v2 的字段；旧版本遇到新形态数据
  （列数不符）安全报错，而不是把未知字段吸进输出
- **旧数据**：升级与重放后，历史 run 的结果逐字节不变；批次原始负载不可变
- **并行批次**：每个 run 在独立事务中写入、按 run_id 隔离，
  8 线程并发处理互不串扰（record_key 严格按批次分区）

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/sources` | 创建来源（含阈值） |
| POST | `/api/plugins/versions` | 注册插件版本 |
| POST | `/api/plugins/versions/{id}/compatibility` | 生成兼容报告 |
| POST | `/api/plugins/versions/{id}/publish` | 发布（支持 gray_percent / force） |
| GET  | `/api/plugins/compare?a=&b=` | 版本对比（规格 diff + 指标 + 差异样本） |
| POST | `/api/sources/{id}/gray` | 调整灰度比例 |
| POST | `/api/sources/{id}/rollback` | 手动回滚 |
| POST | `/api/batches` | 上传批次并处理（返回 run ids 与是否触发自动回滚） |
| POST | `/api/batches/{id}/replay` | 用指定版本重放 |
| GET  | `/api/runs/{id}/results` | 查看 run 结果（canonical / extras 分列） |
| GET  | `/api/sources/{id}/metrics` | 分版本指标 |
| GET  | `/api/rollback-events` | 回滚事件（含保留的 run 与差异样本） |
