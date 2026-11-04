# 1.0.0 Validation Record

Release preparation was checked on 2026-11-04 after prerequisite PRs #54-#61.
The final exact commit is recorded by the annotated release tag and published
validation asset, avoiding a self-referential commit identifier in this file.

## Local Baseline Evidence

- Debian 11 container, Qt 5.15.2, GCC 10.2.1, C++11.
- RabbitMQ 3.13 with freshly generated, verified localhost TLS certificates.
- Full suite: parser 102, flow 37, client 61, exchange 27, queue 60, channel 6:
  **293 checks passed, zero failures**.
- Git-free shared/static CMake installation and external consumers passed.
- Git-free qmake in-source regeneration/rebuild, installation and external
  consumer passed; version 1.0.0 and SONAME `libqamqp.so.1` verified.
- Required license/provenance/migration documents verified in both install trees.
- Source nested inside an unrelated parent repository retained release metadata.
- ASan/UBSan parser and flow suites passed with leak detection enabled. An
  allocated thread-local depth counter initially leaked four bytes; it was
  replaced by a C++11 `thread_local` value and the gate was rerun successfully.
- Reconnect soak: 100 cycles, six exchange/queue pairs, 12 stable channel numbers,
  1800 messages of at least 8192 payload bytes, 20501 ms measured loop time.
  Final buffered bytes: 0. Object counts and payload/control/writer accounting
  returned to their expected baseline on every cycle.

## Publication Evidence

CI uploads package, sanitizer and broker/soak logs. Both Qt matrix entries must
pass on the merged commit and on `v1.0.0`. Source archives and logs are published
with SHA-256 checksums. A successful old PR run is not substituted for those gates.

The inherited root `LICENSE` Git blob remains
`fe891b45db3971d4f9956d83afc78c4410b229ca`. The additional LGPL 2.1 text is copied
from GNU, not rewritten. Existing source grants and original authors are retained;
see `LICENSING.md` for the unresolved inherited version discrepancy.

These results establish the documented build and regression baselines. They are
not a blanket guarantee of every platform, broker, allocator RSS pattern, or
downstream deployment's distribution compliance.