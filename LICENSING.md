# Licensing and Attribution

This fork preserves inherited license grants. It is not a wholesale MIT
relicensing and does not claim that every file has one newly clarified license.

## Preserved Notices

| Material | Evidence and treatment |
| --- | --- |
| `LICENSE` | Preserved verbatim: GNU Library General Public License version 2, June 1991, with the inherited project preamble. |
| `COPYING.LESSER` | Verbatim GNU Lesser General Public License version 2.1 text referenced by inherited source headers. |
| Source headers carrying LGPL 2.1-or-later notices | Their existing grant, copyright and warranty notices remain intact. |
| Other inherited source, tests, tutorials and build files | Preserve existing notices and the repository-level license context; absence of a header is not a permissive-license grant. |
| `LICENSES/MIT-historical.txt` | Preserves the earlier MIT notice as historical provenance, not a current whole-project license election. |
| Original release scripts and documents added by this fork | LGPL-2.1-or-later, subject to preserved notices for any inherited material. |
| Certificates and keys under `tests/files/certs` | Legacy test fixtures, not production credentials. Active TLS tests generate fresh fixtures instead. |

Upstream commit `c4524b5204638f27d7028610c0631edf404f1227` (2015-02-09)
replaced `LICENSE.html` with the Library GPL v2 text while adding headers that
refer to LGPL 2.1-or-later. The previous revision's HTML contained Alexey
Shcherbakov's 2012 MIT notice. Including both LGPL texts preserves the published
evidence; it does not resolve that inherited version discrepancy or create
additional permissions. No unverified uniform SPDX expression is assigned to
the project or to ambiguous inherited files. An existing explicit per-file
grant continues to apply to that file.

## Attribution

- Alexey Shcherbakov: original implementation and inherited 2012-2014 notices.
- Matt Broadstone: upstream maintenance and inherited 2014-2015 notices.
- Fabian Suhr (YearsLaterFixed): 2026 fork maintenance and release hardening.
- Other contributors retain the rights and notices recorded in their files and
  upstream history; this list is not an exclusive ownership assertion.

Fork attribution is added only to files changed by the fork. Existing notices
are not removed, and the copyright inside a GNU license document is not replaced
with the project's authors. New modification notices identify their actual date.

Upstream: <https://github.com/mbroadst/qamqp>

Maintained fork/contact: <https://github.com/YearsLaterFixed/qamqp>

## Commercial Distribution

Commercial use is permitted under the applicable inherited LGPL grants; it is
not unrestricted proprietary redistribution of the library itself. Preserve
copyright, license and warranty notices, provide the exact corresponding
library source and modifications together with interface/build scripts, and
include the applicable license texts with distributed packages.

A suitable shared-library mechanism must allow use of an interface-compatible
modified library. Distribution terms must preserve the modification and
reverse-engineering rights required by the applicable LGPL provisions. Static
linking needs the applicable relinking materials or another compliant route;
shipping only a static binary is not equivalent to compliant shared linking.
Dependencies, including Qt, have their own licensing obligations.

Prepare distribution materials to satisfy the applicable obligations reflected
in both inherited LGPL texts rather than assuming either cancels the other.
A public repository or dynamic linking alone is not a complete distribution
compliance package. This document records provenance and practical packaging
requirements, not a legal clearance opinion. No upstream response is required
to continue under existing grants, and no new permission from upstream is claimed.