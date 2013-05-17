'''
Created on May 16, 2013

@author: christopher
'''

import os

from util import varint

class ValueStore(object):
   """
   :synopsis: Manages a store of values.

   The value store has several modes. The most basic mode involves simple binary value storage. The value is assumed
   to be a string, or at least to be something on which len() works and can be written to a file. The length of the
   object is taken, and then the object is written into the file store.

   The second mode is just like the first, except that zlib compression is applied to the object. By default we use
   a low compression setting for speed, however the compression level is settable. Compression is only applied to
   objects which are larger than some fixed size. By default this size is 32 bytes.

   The third mode is one in which the size of the data is kept externally. Presumably the caller has some information
   which allows them to know how large the object ought to be. In this case we just store the object. Note that if the
   object is larger than the caller says no corruption will occur. Space may be wasted.

   The fourth mode is just like the third, except that zlib compression is applied to the object. The compression
   details are just like mode 2.

   The final mode stores 64-bit signed integers using zigzag base128 encoding. This allows numbers to be stored in a
   very space efficient way, assuming that the numbers are mostly smaller than 8 bytes.
   """

   header_fmt = "<"

   __slots__ = ["filename", "f"]

   def __init__(self, base_name):
      self.filename = base_name + ".values"
      self.f = self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

   def append(self, value):
      self.f.seek(0, 2)
      offset = self.f.tell()
      varint.encode_stream(len(value), self.f)
      self.f.write(value)
      return offset

   def get(self, offset):
      self.f.seek(offset)
      size = varint.decode_stream(self.f)
      return self.f.read(size)

   def flush(self):
      self.f.flush()
