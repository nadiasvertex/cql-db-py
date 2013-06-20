'''
Created on May 16, 2013

@author: christopher
'''

import os
import struct

class Map(object):
   """
   :synopsis: Stores a mapping between a segment storage-key and the offset of
   the item in its data file. In other words, it is a large array of fixed-size
   elements pointing to the actual location of the element's value in some
   other file.
   """
   fmt = "<Q"
   fmt_size = struct.calcsize(fmt)

   __slots__ = ["filename", "f"]

   def __init__(self, base_name):
      self.filename = base_name + ".map"
      self.f = self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

   def append(self, offset):
      self.f.seek(0, 2)
      self.f.write(struct.pack(self.fmt, offset))

   def delete(self, index):
      self.f.seek(self.fmt_size * index)
      self.f.write(struct.pack(self.fmt, 0))

   def update(self, index, offset):
      self.f.seek(self.fmt_size * index)
      self.f.write(struct.pack(self.fmt, offset))

   def get(self, index):
      self.f.seek(self.fmt_size * index)
      return struct.unpack(self.fmt, self.f.read(self.fmt_size))[0]

   def count(self):
      self.f.seek(0, 2)
      offset = self.f.tell()
      return offset / self.fmt_size

   def flush(self):
      self.f.flush()
