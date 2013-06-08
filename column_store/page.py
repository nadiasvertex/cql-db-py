__author__ = 'Christopher Nelson'

import struct

BLOCK_SIZE = 2048
MBLKSZ = 524288
INVALID_PAGE = (1 << 64) - 1


class PageHead(object):
   fmt = "<QQLLQQQ"
   fmt_size = struct.calcsize(fmt)

   __slots__ = ["next", "prev", "level", "flag", "num", "parent", "page_id"]

   def load(self, f):
      self.next, self.prev, self.level, self.flag, self.num, self.parent, self.page_id = struct.unpack_from(self.fmt, f, 0)

   def store(self, f):
      struct.pack_into(self.fmt, f, 0, self.next, self.prev, self.level, self.flag, self.num, self.parent, self.page_id)

class Entry(object):
   __slots__ = ["key", "ptr"]

   def __init__(self, key, ptr):
      self.key = key
      self.ptr = ptr


class EntryFactory(object):
   __slots__ = []

   def get_entry_fmt(self):
      return "<QQ"

   def get_entry_size(self):
      return struct.calcsize(self.get_entry_fmt())

   def get_entry_count_max(self):
      return (BLOCK_SIZE - PageHead.fmt_size) / self.get_entry_size()

   def load(self, f):
      k, v = struct.unpack_from(self.get_entry_fmt(), f)
      return Entry(k, v)

   def store(self, f, entry):
      struct.pack_into(self.get_entry_fmt(), f, entry.key, entry.ptr)


