# Release Verification

Required baseline: Qt 5.15.2, GCC 10.2.1, C++11. Qt 6.5.3 is a separate CI
build/test/package target using Qt 6's language requirements. RabbitMQ 3.13 is
the live broker baseline. Do not equate local admission with broker confirmation.

## Reproducible Package Gate

```bash
bash tests/files/ci/check-release.sh /tmp/qamqp-package-check
```

Use an empty output path outside the source tree. The gate copies the source
without Git metadata, builds and installs shared/static CMake libraries, executes
clean external consumers, checks SONAME 1 and packaged license/provenance files,
and builds/regenerates/installs qmake twice without generated-source duplication.
The release CI runs this gate against an extracted source archive, not just a
developer checkout. Version 1.0.0 is declared independently of Git tags.

## Runtime Gates

- Run every parser/flow case on Qt 5 and Qt 6.
- Run all four broker suites on the disposable TLS-enabled broker. The client
  suite restarts that broker; do not point it at a shared service.
- Run parser/flow with AddressSanitizer and UndefinedBehaviorSanitizer, including
  leak detection. A sanitizer report is a failure, not a suppressible test result
  unless a narrowly documented third-party issue is independently established.
- Run at least 100 multi-channel connect/redeclare/rebind/reconsume/delivery
  cycles. Record elapsed time, cycle count, traffic and final buffer/object
  counters. Queued payload, write and control accounting must return to baseline;
  retained channel objects and channel numbers must not grow per cycle.

These are reproducible regression/soak gates, not a claim that allocator RSS must
be perfectly flat or that every deployed broker/platform is certified.

## Licensing and Source Distribution

The root `LICENSE` is retained unchanged. `COPYING.LESSER` supplies the referenced
LGPL 2.1 text; `LICENSING.md` records the inherited discrepancy and provenance.
`LICENSES/MIT-historical.txt` preserves the old notice without electing MIT for
the current project. All are installed by CMake/qmake and included in archives.
Existing copyright and warranty notices remain intact. New fork attribution is
limited to files actually changed by this fork, with a dated modification notice.

## Publication

1. Merge all prerequisite changes and the release-preparation PR through review.
2. Record the exact merged commit and passing core, package, sanitizer and soak CI.
3. Generate and test the final source archive from that commit; record SHA-256.
4. Create an annotated `v1.0.0` tag on that exact commit, without force-moving it.
5. Wait for the tag-triggered CI gates to pass.
6. Publish the release with source archive, checksums and validation evidence.

The maintainer account must author publication actions. An unavailable signing
key is not a reason to fabricate a signed tag; an annotated tag is the minimum.
Do not silently rewrite an already published release or claim license ambiguity
has been resolved by including multiple license texts.