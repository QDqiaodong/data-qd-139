本目录为原创虚构合成数据，使用MIT许可。

由 tools/build_window_fixture.py 通过真实CLI（window-init/window-apply/window-recover）离线确定性生成：control.json 列出两个严格JSONL变更文件，p.jsonl 与 q.jsonl 为多分区变更（含更正与水位），expected-report.jsonl 为字节稳定的期望报告，replay.json 仅重放已接受的首个文件（纯重放不改变epoch或报告）。重建：

PYTHONPATH=src python3 -I tools/build_window_fixture.py fixtures/window
