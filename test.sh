#!/bin/bash
PATH=$PWD/clang/bin:$PATH
rm -f native/src/cpp/test/*.o
scons && ./cql_tests

