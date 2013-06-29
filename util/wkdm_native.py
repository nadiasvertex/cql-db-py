'''
Created on Jun 16, 2013

@author: Christopher Nelson
'''
import os

from cffi import FFI

ffi = FFI()
ffi.cdef("""
typedef unsigned int WK_word;
unsigned int
WKdm_compress (WK_word* sourceBuffer,
               WK_word* destinationBuffer,
               unsigned int words);
void
WKdm_decompress (WK_word* sourceBuffer,
                 WK_word* destinationPage,
                 unsigned int words);
""")

__lib = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib", "libwkdm.so.1.0")
wkdm = ffi.dlopen(__lib)

def compress(src, num_words):
   src_arg = ffi.new("unsigned int[1024]", src)
   dst_arg = ffi.new("unsigned int[1024]")

   wkdm.WKdm_compress(src_arg, dst_arg, num_words)
   del src_arg
   return dst_arg

def decompress(src, num_words):
   src_arg = ffi.new("unsigned int[]", src)
   dst_arg = ffi.new("unsigned int[1024]")

   wkdm.wkdm_decompress(src_arg, dst_arg, num_words)
   del src_arg
   return dst_arg

if __name__ == "__main__":
   import time
   from copy import copy
   src = []

   j = 0
   for _ in range(0, 1024):
      src.append(j)
      j += 1
      if j > 9:
         j = 0

   out2 = compress(copy(src), 1024)

   ntimes = []
   limit = 1 << 18
   for i in range(0, limit):
      print i
      start = time.time()
      out2 = compress(copy(src), 1024)
      end = time.time()
      ntimes.append(end - start)
      del out2

