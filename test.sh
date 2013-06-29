#!/bin/bash
PATH=$PWD/clang/bin:$PATH
scons && ./cql_tests

