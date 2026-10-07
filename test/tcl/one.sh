#!/bin/bash
# one.sh test [bin] : run in a fresh dir, show the first failure lines
d=$(mktemp -d /tmp/tcl/work/o_XXXX); cd $d
MW_DEFAULT_MODE=${MODE:-2} ${2:-/tmp/sqlite-src/bld/testfixture_mw} /tmp/sqlite-src/sqlite-src-3530400/test/$1.test 2>&1 | grep -v "^$" | head -${N:-14} | cut -c1-300
