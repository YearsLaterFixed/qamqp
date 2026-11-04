#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Fabian Suhr (YearsLaterFixed)
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
work=${1:?Specify a scratch output directory outside the source tree}
mkdir -p "$work"
flags='-fsanitize=address,undefined -fno-omit-frame-pointer -g'
cmake -S "$root" -B "$work/library" -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="$flags" \
  -DCMAKE_SHARED_LINKER_FLAGS=-fsanitize=address,undefined
cmake --build "$work/library" -j2
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
export LD_LIBRARY_PATH="$work/library${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
for suite in qamqpparser qamqpflow; do
  mkdir -p "$work/$suite"
  (
    cd "$work/$suite"
    qmake "$root/tests/auto/$suite/$suite.pro" "LIBS+=-L$work/library" \
      "QMAKE_CXXFLAGS+=$flags" "QMAKE_LFLAGS+=-fsanitize=address,undefined"
    make -j2
    "./tst_$suite"
  )
done
echo 'AddressSanitizer/UndefinedBehaviorSanitizer parser and flow gates: PASS'