# Deterministic Bloom audit v1（单次审计，保留兼容）

为数据摄取者构建有界成员预过滤位图，并以同一快照精确集合审计误报。输入bits=1..1000000、hashes=1..16，items/queries各<=20000个非空UTF8标识符。重复插入不增加独立数；重复查询按每次探测统计。
第i个位置=SHA256(十进制i的ASCII + 一个零字节 + UTF8 item)大端整数 mod bits，i从0起。位图hex按字节，小端位索引；多余尾位为0。输出精确独立数、置位数、每查询maybe_present/actual_present、误报次数与负例分母，分母0时误报率null；假阴性必须0。
理论近似误报率(1-exp(-kn/m))^k只作理想均匀独立哈希近似，不能代替实测；小位图或相关哈希不保证吻合。为可核验基线保留精确集合做审计，因此不是声称端到端常数空间；过滤阳性不能视作已存在，不能用于删除原始数据。当前不做可删除Bloom、增量落盘或跨进程并发。

`bloom-index --source 输入.json --output 报告.json` 保持v1字节稳定输出；输入/输出不得相同，输出符号链接拒绝，失败退出2并保留旧报告。

# Persistent membership routing index v2（多上游持久索引）

在v1哈希与位序契约之上，把多个上游供应方的数据清单固化为SQLite持久成员路由索引。仅使用Python标准库与SQLite。

## init —— 建库

`bloom-index init --db 库路径 --bits B --hashes K` 创建空库。B沿用1..1000000、K沿用1..16，写入库内元信息并永久固定，之后不可更改。库路径已存在（含悬空符号链接）即拒绝，退出2，不损伤已有库。库使用WAL日志。

## sync —— 归属替换

`bloom-index sync --db 库路径 --source 控制文件.json`。控制文件为严格JSON（沿用8MB上限、拒绝重复键与非有限数值）：

```json
{"updates":[{"source":"供应方","generation":1,"base_generation":0,"manifest":"清单路径","sha256":"小写64位摘要"}]}
```

- 相对清单路径相对控制文件所在目录解析；一批最多64个不同source。
- 清单为UTF-8 JSONL：每行严格为 `{"item":"标识符"}`，标识符沿用v1 ident规则（非空、去空白后不变、<=128字符、无控制字符）；禁止空行、重复字段、非有限数值与未知字段；每行内容最多4096字节；每份最多1000000行。
- sha256绑定实际读到的完整原始字节，不符即拒绝；清单内重复item只形成一份归属。

语义：每个列出的source的完整归属被该代清单替换；未列出的来源保持原样；空清单清空该来源。新来源当前代为0；未应用的代必须满足 generation=当前代+1 且 base_generation=当前代。与已应用代的 source/代/前代/原始摘要完全相同的更新是无副作用重放——即使已有更晚代，也不恢复旧归属。相同代的其他内容、跳代、摘要错误或任何清单行错误都拒绝整批。库内保留各来源已应用代的凭据（generation、base_generation、sha256）用于重放判定。

整批在一个事务中提交：归属替换、代次凭据与计数索引同生同灭；只有本批含有新应用代时，全局 commit_seq 才加1（纯重放不变）。进程在写事务中途失败或被终止后重开，只能看到完整旧态或完整新态。

## 计数索引

计数索引表示所有来源当前有效item的并集。哈希与位序沿用 `filtering.positions`：每个活跃item对其不同位置各贡献1次；来源重叠、清单内重复、单item多哈希落同位均不重复计数。只有归属同步使某item的全局来源数跨越0与1边界时才改变其计数贡献，否则位图贡献保持不变。删除最后归属后，仅当位置计数归零才清位，不抹掉碰撞者的位。

## query —— 快照审计

`bloom-index query --db 库路径 --source 查询.json --output 报告.json`。查询输入为 `{"queries":[...]}`，沿用v1的20000条上限与严格JSON限制。报告字段：

- `schema_version`=2、`commit_seq`、`bits`、`hashes`、`active_items`（当前有归属的精确item数）、`set_bits`、`bitmap_hex`；
- `queries` 按原顺序给出 `item`、`maybe_present`、`actual_present`、`source_count`（精确归属来源数）；重复查询逐次计数；
- `negative_queries`、`false_positives`、`false_negatives`、`observed_false_positive_rate`（精确分数字符串，分母0时为null）。

归属与计数在同一个已提交快照中读取（单个只读事务），审计不为查询重建全体item集合：精确判断按查询逐条查归属索引，Bloom判断来自计数位图。任何已提交状态不得有假阴性。报告发布沿用原子替换与旧报告保护。

## 并发与故障

允许一个写者和多个读者（WAL）。写者以 BEGIN IMMEDIATE 取写锁，忙等5秒；第二写者5秒内拿不到写锁即退出2。读者读取已提交快照，不因正常写事务而等待其结束。写事务中途崩溃由SQLite回滚，重开只见完整旧态或完整新态。

## 资源与错误

生产路径流式读取清单，去重与归属差集在库内（临时表）完成；不把完整清单或全库item放入Python集合/列表。库中100万个不同item、单次替换100万行时，进程峰值RSS不超过128MiB（测量方法见README）。除SQLite事务故障所需文件（-wal/-shm）外，不遗留临时中间产物。输入/状态错误退出2且无部分变更。库与输入/输出的路径冲突、数据库符号链接在写入前拒绝。

# 可核验快照与隔离恢复 v3（snapshot / snapshot-check / restore）

把单个**已提交**数据库状态打包为自描述、可离线核验、可恢复到隔离分析节点的快照包（bundle）。仅用标准库与SQLite；新增命令不改变 init/sync/query 与v1入口的任何输出和行为，输入或状态错误一律退出2。

## 包格式（format_version=1）

bundle 是一个目录，且**只能**包含两个普通文件（多、少、符号链接、子目录均拒绝）：

- `index.sqlite3`：单一文件的一致性SQLite快照，journal_mode=DELETE（rollback journal），不含 -wal/-shm 侧车；
- `manifest.json`：严格JSON（拒绝重复键、非有限数值），字段集合精确为且类型必须为：
  - `format_version`：整数，当前固定 1；
  - `bits`、`hashes`：整数，沿用 1..1000000 与 1..16 边界；
  - `commit_seq`：非负整数，快照所固定的已提交状态序号；
  - `db_size_bytes`：正整数，包内 `index.sqlite3` 的精确字节数；
  - `db_sha256`：64个小写十六进制字符，包内 `index.sqlite3` 原始字节的SHA256。

缺失字段、额外字段、错误类型（如字符串型bits、浮点hashes）、未知format_version、非法摘要均直接拒绝，不做部分修复。清单中的每个字段都必须描述包内同一个数据库快照，不允许拼接复制前后的状态。

## snapshot —— 只读导出

`bloom-index snapshot --db 库路径 --output 新目录`：

- 目标目录已存在（含悬空符号链接、同名文件）、其父目录是符号链接或路径冲突时拒绝；导出全程不写源库。
- 以 `mode=ro` 只读打开WAL源库，在一个只读事务内固定一个已提交状态，并从该同一状态读出 bits、hashes、commit_seq、active_items、来源代次与位计数；随后用SQLite在线备份API把该事务所见状态复制到同目录隐藏暂存区。导出期间另一个进程的 `sync` 可以正常提交：读者不等待写事务结束，备份也不持有写锁，因此不会因长时间写锁使 sync 失败；若 sync 在备份期间提交，包仍完整对应导出开始时固定的那一个提交状态。
- 复制后在暂存库上 checkpoint（TRUNCATE）并切换为 DELETE 日志，执行 `PRAGMA integrity_check`、结构核对，并重新读出全部元信息与固定状态逐项比对，不一致即放弃整包；然后计算字节数与SHA256写入清单，最后以目录原子改名发布。
- 只读连接可能在源目录产生空的 -wal/-shm；导出结束时仅清理**本次连接新建**且当前没有活动写事务（WAL为空）的侧车，任何预先存在或已被写者采用的侧车都不触碰。

## snapshot-check —— 只读核验

`bloom-index snapshot-check --bundle 目录`，成功打印含 `status:"ok"` 的摘要，任何缺陷退出2。全程以 `immutable=1` 只读打开包内数据库，绝不创建侧车或修改任何字节（重复核验前后包内文件SHA256不变）。核验顺序与内容：

1. 目录与条目契约：真实目录、无符号链接、恰为两个普通文件；
2. 清单严格解析：字段集合、类型、边界、摘要格式；
3. `index.sqlite3` 字节数等于 `db_size_bytes`、SHA256等于 `db_sha256`；
4. `PRAGMA integrity_check` 返回 ok，数据库对象与v2模式完全一致且日志模式为 delete；
5. 库内关系（单一只读事务内）：
   - `meta` 恰含 bits/hashes/commit_seq/active_items 四个非负整数，bits/hashes 在界内；
   - 每个 source 的当前代次与 `generations` 凭据一一对应，代次从1连续无缺口，且每行 `base_generation = generation - 1`，sha256为合法摘要；
   - commit_seq 与代次历史相容（不低于最高来源代次，不高于已应用代次总数）；
   - `active_items` 等于 ownership 中不同 item 数，ownership 不引用未知来源，无重复 (source,item)；
   - 按 ownership 的不同 item 用 `filtering.positions` 重新推导每个位置的计数器，与 `counters` 表逐行相等（位置在界内、计数为正、无缺失无多余）；
6. 清单的 bits、hashes、commit_seq 与库内读出值逐一相等。

错误信息带表名/来源/位置等定位线索；发现任何损坏即拒绝，不生成部分修复。

## restore —— 暂存发布

`bloom-index restore --bundle 目录 --db 新库路径`：

- 发布条件完全由**包内数据库与清单的实际核验结果**决定：先完整运行 snapshot-check 等价核验；例如清单 commit_seq 与库内不一致时直接拒绝，绝不会从源库另读元数据放行，目标路径保持不存在。
- 目标路径及其 -wal/-shm 侧车已存在（含符号链接）即拒绝，已有目标字节保持不变；不覆盖现有数据库，不回写源包。
- 在目标**同目录**的隐藏暂存目录中流式复制包内数据库，重新计算大小/摘要并做完整性与全量关系校验；全部写入和校验成功后，才用原子改名发布，发布后再读回确认状态未变。
- 任一步失败：删除本次暂存目录，既有目标（若有）原样保留，目标路径不会半创建。故障排除后可对同一路径重试。
- 全程不跟随符号链接；暂存名不含可逃逸目录的相对段，发布改名不会越过目标父目录；对包的读取保持只读。

## 成功语义

恢复出的新库可直接被现有 `query` 审计；对同一查询输入，其报告与导出时源库报告在 commit_seq、bits、hashes、active_items、set_bits、bitmap_hex 及逐查询 maybe_present/actual_present/source_count 上逐项相同，来源数与活跃集合一致。

