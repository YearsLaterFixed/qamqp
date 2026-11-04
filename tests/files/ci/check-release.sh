#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Fabian Suhr (YearsLaterFixed)
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
work=${1:?Specify an empty scratch output directory outside the source tree}
mkdir -p "$work"
test ! -e "$work/source"
cp -a "$root" "$work/source"
rm -rf "$work/source/.git"
source_dir="$work/source"
echo "Package validation: ${GITHUB_SHA:-local-source-copy}"
cmake --version | head -n 1
c++ --version | head -n 1
qmake -v
for shared in ON OFF; do
  build="$work/cmake-$shared"
  prefix="$work/install-$shared"
  cmake -S "$source_dir" -B "$build" -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS="$shared" -DCMAKE_INSTALL_PREFIX="$prefix"
  cmake --build "$build" -j2
  cmake --install "$build"
  for file in LICENSE COPYING.LESSER LICENSING.md README.md CHANGELOG.md LICENSES/MIT-historical.txt docs/RELEASE.md; do
    test -s "$prefix/share/doc/QAMQP/$file"
  done
  cmake -S "$source_dir/tests/cmake-consumer" -B "$work/consumer-$shared" \
    -DCMAKE_PREFIX_PATH="$prefix${CMAKE_PREFIX_PATH:+;$CMAKE_PREFIX_PATH}"
  cmake --build "$work/consumer-$shared" -j2
  "$work/consumer-$shared/qamqp-consumer"
  if [ "$shared" = ON ]; then
    readelf -d "$build/libqamqp.so.1.0.0" | grep -F 'libqamqp.so.1]'
  fi
done
(
  cd "$source_dir/src"
  qmake "$source_dir/src/src.pro" "PREFIX=$work/qmake-install"
  make -j2
  qmake "$source_dir/src/src.pro" "PREFIX=$work/qmake-install"
  make -j2
  make install
  test -s libqamqp.so.1.0.0
  readelf -d libqamqp.so.1.0.0 | grep -F 'libqamqp.so.1]'
)
for file in LICENSE COPYING.LESSER LICENSING.md README.md CHANGELOG.md LICENSES/MIT-historical.txt docs/RELEASE.md; do
  test -s "$work/qmake-install/share/doc/QAMQP/$file"
done
mkdir "$work/qmake-consumer"
(
  cd "$work/qmake-consumer"
  qmake "$source_dir/tests/cmake-consumer/qmake-consumer.pro" "QAMQP_PREFIX=$work/qmake-install"
  make -j2
  ./qamqp-qmake-consumer
)
echo 'Git-free shared/static consumers, qmake repeat build, SONAME and notices: PASS'