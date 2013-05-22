'''
Created on May 16, 2013

@author: christopher
'''

import os
import struct
import zlib

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

   header_fmt = "<bb"

   DATA_MODE_PACKED = 1
   DATA_MODE_COMPRESSED = 2
   DATA_MODE_USER = 3
   DATA_MODE_USER_COMPRESSED = 4
   DATA_MODE_PACKED_INT = 5

   __slots__ = ["filename", "f", "mode", "compression_level"]

   def __init__(self, base_name, mode=1, compression_level=zlib.Z_BEST_SPEED):
      self.filename = base_name + ".values"
      self.f = self._load_existing() if os.path.exists(self.filename) else self._initialize(mode, compression_level)

   def _load_existing(self):
      f = open(self.filename, "r+b")
      self.mode, self.compression_level = struct.unpack(self.header_fmt,
                                              f.read(struct.calcsize(self.header_fmt)))
      return f

   def _initialize(self, mode, compression_level):
      f = open(self.filename, "w+b")
      self.mode = mode
      self.compression_level = compression_level
      f.write(struct.pack(self.header_fmt, mode, compression_level))
      f.flush()
      return f

   def append(self, value):
      self.f.seek(0, 2)
      offset = self.f.tell()

      # Write the packed integer format
      if self.mode == self.DATA_MODE_PACKED_INT:
         varint.encode_stream(value, self.f)
         return offset

      # Compress the data before measuring it.
      if self.mode in (self.DATA_MODE_COMPRESSED, self.DATA_MODE_USER_COMPRESSED):
         value = zlib.compress(value, self.compression_level)

      # Measure the length
      if self.mode in (self.DATA_MODE_COMPRESSED, self.DATA_MODE_PACKED, \
                       self.DATA_MODE_USER_COMPRESSED):
         varint.encode_stream(len(value), self.f)

      # Write the value
      self.f.write(value)
      return offset

   def get(self, offset, size=0):
      self.f.seek(offset)

      # Read the packed integer format
      if self.mode == self.DATA_MODE_PACKED_INT:
         return varint.decode_stream(self.f)

      # Read the varint size of the data
      if self.mode in (self.DATA_MODE_COMPRESSED, self.DATA_MODE_PACKED, \
                       self.DATA_MODE_USER_COMPRESSED):
         size = varint.decode_stream(self.f)

      value = self.f.read(size)
      if self.mode in (self.DATA_MODE_COMPRESSED, self.DATA_MODE_USER_COMPRESSED):
         return zlib.decompress(value)

      return value

   def flush(self):
      self.f.flush()
