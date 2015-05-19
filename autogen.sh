#! /bin/sh

if test ! -d .git && test ! -f src/aft_init.c; then
    echo You really need to run this script in the top-level aft directory
    exit 1
fi

set -x

if test ! -d config; then
    mkdir config
fi

autoreconf -ivf
