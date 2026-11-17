本目录为原创虚构合成数据，使用MIT许可。

由 tools/build_resolve_fixture.py 通过真实CLI（init/sync/resolve）离线确定性生成：membership.sqlite3 为既有成员索引，control.json 列出三个JSONL主数据文件（含一个空文件），expected-result.jsonl 为 resolve 的字节稳定期望输出。重建：

PYTHONPATH=src python3 -I tools/build_resolve_fixture.py fixtures/resolve
