本目录为原创虚构合成数据，使用MIT许可。没有真实个人、线上实验、生产日志、交易、项目进度或搜索用户信息；不来自外部平台。

`snapshot/` 下的快照夹具由 `tools/build_snapshot_fixture.py` 通过真实CLI（init/sync/query/snapshot/snapshot-check）离线确定性生成：`fixture-bundle/` 是自描述SQLite快照包，`query.json` 为查询输入，`expected-report.json` 与 `expected-check.json` 是对应期望输出；重建产物字节稳定。

`resolve/` 下的记录归并夹具由 `tools/build_resolve_fixture.py` 通过真实CLI（init/sync/resolve）离线确定性生成：`membership.sqlite3` 为既有成员索引，`control.json` 列出多个JSONL主数据文件（含一个空文件），`expected-result.jsonl` 为字节稳定的期望归并输出；重建产物字节稳定。
