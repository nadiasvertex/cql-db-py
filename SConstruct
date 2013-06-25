import os

include_dirs = ["native/src/cpp"]

cwd = os.getcwd()
compression_base = "native/src/cpp/compression"
compression_src = [os.path.join(cwd, compression_base, filename) for filename in [
			"wkdm.cpp",
		  ]]

env = Environment(CPPFLAGS="-std=c++11 -I%s" % ("-I".join(include_dirs)))
env.SharedLibrary(target="compression", source=compression_src)

