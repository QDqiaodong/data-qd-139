# data-qd-139 · 流式成员过滤索引

Deterministic bounded Bloom membership with exact audit evidence。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/bloom-index --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/bloom_index/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 持久成员路由索引（v2）

在单次审计之外，`bloom-index` 提供多上游数据清单的持久成员路由索引，仅用标准库与SQLite：

```sh
bloom-index init --db run/idx.db --bits 1000000 --hashes 16     # 建空库，参数永久固定
bloom-index sync --db run/idx.db --source fixtures/index/control_gen1.json
bloom-index sync --db run/idx.db --source fixtures/index/control_gen2.json
bloom-index query --db run/idx.db --source fixtures/index/query.json --output run/audit.json
```

- `init`：创建空库；路径已存在即拒绝（退出2），不损伤已有库。
- `sync`：按控制文件把各来源的完整归属替换为对应代清单；整批一个事务，含新代时 commit_seq 加1；完全相同的已应用代是无副作用重放；任何行错误、摘要不符、跳代或同代异内容都整批拒绝且无部分变更。
- `query`：在一个已提交快照中同时读归属与计数位图，输出 schema_version=2 审计报告（含 commit_seq、active_items、set_bits、bitmap_hex、逐查询 item/maybe_present/actual_present/source_count 及误报统计）。

清单为UTF-8 JSONL，每行严格 `{"item":"标识符"}`（<=4096字节、<=100万行）；sha256绑定实际读到的原始字节。计数索引表示所有来源有效item的并集：每item对其不同哈希位置各贡献1次，仅在全局来源数跨越0与1边界时调整，归零才清位。并发上允许一个写者加多个读者（WAL）；第二写者5秒内拿不到写锁退出2，读者不等待写事务结束。写事务中途被终止后重开，只见完整旧态或完整新态。完整语义见 [数据契约](docs/contract.md)。

## 可核验快照与隔离恢复（v3）

把单个已提交数据库状态导出为自描述快照包，离线核验后恢复到隔离分析节点；导出只读、不阻塞并发 sync，恢复采用同目录暂存+原子发布：

```sh
bloom-index snapshot --db run/idx.db --output run/bundle        # 导出：index.sqlite3 + manifest.json
bloom-index snapshot-check --bundle run/bundle                  # 只读全量核验，失败退出2
bloom-index restore --bundle run/bundle --db run/analytic.db    # 仅核验完整通过才发布到新路径
bloom-index query --db run/analytic.db --source fixtures/index/query.json --output run/audit.json
```

- `snapshot`：在只读事务内固定一个已提交状态，用SQLite在线备份复制，清单记录 format_version、bits、hashes、commit_seq、数据库字节数与SHA256，全部字段与包内同一快照交叉核对；目标目录已存在、路径冲突或符号链接一律拒绝，不改源库。
- `snapshot-check`：严格拒绝缺失/额外/错误类型字段，校验文件大小、摘要、`integrity_check`、日志模式、清单与库内元信息一致，并核对来源代次凭据连续、commit_seq/active_items 关系及重算的位置计数器；只读且不改包内字节，损坏给可定位错误、不做部分修复。
- `restore`：只接受完整通过检查的包，恢复到不存在的新路径；同目录暂存、全部写入和校验成功后原子改名，任何失败都保留既有目标并清除暂存，修正后可重试；不跟随符号链接、不接受路径穿越、不覆盖现有数据库、不旁路改写源包。

`fixtures/snapshot/` 下的自制 SQLite 快照夹具（含 `fixture-bundle/`、查询输入与期望报告/核验摘要）由真实CLI确定性生成，重建方式：

```sh
PYTHONPATH=src python3 -I tools/build_snapshot_fixture.py fixtures/snapshot
```

完整包格式、核验项与故障语义见 [数据契约](docs/contract.md) 的 v3 章节。

## 规模测量

流式读取清单、库内去重与差集，峰值内存有界。可重复的测量方法（默认100万item建库+100万行替换，上限128MiB，失败退出1）：

```sh
.venv/bin/python tools/measure_scale.py                      # 完整规模
.venv/bin/python tools/measure_scale.py --items 20000 --lines 20000   # 快速冒烟
```

输出JSON含各阶段耗时、子进程峰值RSS（ru_maxrss）与一致性核对；`--keep` 保留工作目录便于检查。

## 当前业务与样例
# Deterministic Bloom audit v1

为数据摄取者构建有界成员预过滤位图，并以同一快照精确集合审计误报。输入bits=1..1000000、hashes=1..16，items/queries各<=20000个非空UTF8标识符。重复插入不增加独立数；重复查询按每次探测统计。
第i个位置=SHA256(十进制i的ASCII + 一个零字节 + UTF8 item)大端整数 mod bits，i从0起。位图hex按字节，小端位索引；多余尾位为0。输出精确独立数、置位数、每查询maybe_present/actual_present、误报次数与负例分母，分母0时误报率null；假阴性必须0。
理论近似误报率(1-exp(-kn/m))^k只作理想均匀独立哈希近似，不能代替实测；小位图或相关哈希不保证吻合。为可核验基线保留精确集合做审计，因此不是声称端到端常数空间；过滤阳性不能视作已存在，不能用于删除原始数据。当前不做可删除Bloom、增量落盘或跨进程并发。

# Persistent membership routing index v2

多上游清单的持久成员路由：init建库固定bits/hashes；sync按代次凭据原子替换各来源归属并维护并集计数位图；query在单一已提交快照输出精确+Bloom审计。代次重放无副作用，错误整批拒绝，崩溃只见完整旧/新态。流式清单处理，100万item规模下峰值RSS<=128MiB。

# Verifiable snapshot export and restore v3

snapshot把单个已提交WAL状态只读备份为单文件DELETE日志库加严格manifest（format_version/bits/hashes/commit_seq/字节数/SHA256）；snapshot-check只读核验摘要、完整性、代次凭据连续性、active_items与重算计数器；restore在目标同目录暂存并在校验全过后原子发布，失败不留暂存、不改既有目标。
