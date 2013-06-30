from glob import glob
import os

test_code = glob("native/src/cpp/gtest/*.cc") + \
            glob("native/src/cpp/test/*.cpp")

env = Environment(CXX="clang++",
                  CXXFLAGS="-g -std=c++11 -Inative/src/cpp",
                  LIBS=["pthread"])

env['ENV']['TERM'] = os.environ['TERM']
env['ENV']['PATH'] = os.environ['PATH']


env.Program(target="cql_tests", source=test_code)
