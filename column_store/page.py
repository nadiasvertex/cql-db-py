__author__ = 'Christopher Nelson'

import struct
from functools import total_ordering

BLOCK_SIZE = 2048
MBLKSZ = 524288
INVALID_PAGE = 0x0fffffffffffffff
MAX_KEY_VALUE = (1 << 64) - 1

class PageHead(object):
   fmt = "<QQLLQQQ"
   fmt_size = struct.calcsize(fmt)

   __slots__ = ["next", "prev", "level", "flag", "num", "parent", "page_id"]

   def load(self, f):
      self.next, self.prev, self.level, self.flag, self.num, self.parent, self.page_id = struct.unpack_from(self.fmt, f, 0)

   def store(self, f):
      struct.pack_into(self.fmt, f, 0, self.next, self.prev, self.level, self.flag, self.num, self.parent, self.page_id)

class Page(object):
   __slots__ = ["page", "header", "entry_factory", "entries", "exploded"]

   MASK = 0x0fffffffffffffff
   FENCE = 0x8000000000000000
   FILTER = 0x4000000000000000
   INTERNAL = 0x2000000000000000
   FENCE_CLEAR = 0x7fffffffffffffff
   FILTER_CLEAR = 0xbfffffffffffffff
   INTERNAL_CLEAR = 0xdfffffffffffffff
   FENCE_SET = 0x8fffffffffffffff
   FILTER_SET = 0x4fffffffffffffff
   INTERNAL_SET = 0x2fffffffffffffff

   def __init__(self, page, entry_factory):
      self.page = page
      self.entry_factory = entry_factory
      self.header = PageHead()
      self.header.load(self.page)
      self.entries = []
      self.exploded = False

   def _explode_entries(self):
      self.exploded = True
      offset = PageHead.fmt_size
      entry_size = self.entry_factory.get_entry_size()
      for _ in range(0, self.header.num):
         self.entries.append(self.entry_factory.load(self.page, offset))
         offset += entry_size

   def flush(self):
      self.header.store(self.page)

   def compact(self):
      self.exploded = False
      self.header.num = len(self.entries)
      self.flush()
      self.entries = []

   def search(self, key):
      if not self.exploded:
         self._explode_entries()

      high = len(self.entries) - 1
      low = 0
      while high > low:
         mid = low + ((high - low) / 2)
         entry = self.entries[mid]
         if key < entry.key:
            high = mid
         else:
            low = mid + 1

      return low

   def insert(self, entry):
      index = self.search(entry.key)

      # Deal with duplicate keys
      while index < self.header.num and entry.key > self.entries[index].key:
         index += 1

      # Put the entry at the end. We need to make
      # space on the list, and it's possible that
      # the item will end up there anyway.
      self.entries.append(entry)

      # Figure out where to put the new entry, honoring
      # fences. Prepare for the insertion by moving data
      # around.
      for i in range(self.header.num, index, -1):
         cur_entry = self.entries[i - 1]
         self.entries[i] = cur_entry
         if cur_entry.key == entry.key and \
            entry.ptr & self.FENCE and \
            (not cur_entry.ptr & self.INTERNAL):
            index = i
            break;

      # If we don't end up putting the item at the
      # end, go ahead and write it wherever it
      # should go.
      if index != self.header.num:
         self.entries[index] = entry
      self.header.num += 1

   def delete(self, entry):
      if len(self.entries) == 1:
         self.entries.pop()
         self.header.num = 0
         return True

      # Check the previous position first
      index = self.search(entry.key) - 1
      cur_entry = self.entries[index]

      # If that doesn't work, try the specified
      # position
      if cur_entry.key != entry.key:
         index += 1
         cur_entry = self.entries[index]

      while cur_entry.key == entry.key:
         if (not cur_entry.ptr & self.FENCE) and \
          cur_entry == entry:
            self.entries.pop(index)
            self.header.num -= 1
            return True

         index += 1
         cur_entry = self.entries[index]

      return False

   def split_to(self, new_page, new_page_id):
      """
      :synopsis: Performs a btree split of the current page, moving half
      the entries to 'new_page'.

      Adjusts all of the counters on both pages to be correct.

      :param new_page: The new page to move the entries to.
      """
      mid = self.header.num / 2
      new_page.exploded = True
      new_page.entries = self.entries[mid:]
      new_page.header.num = len(new_page.entries)

      self.entries = self.entries[0:mid]
      self.header.num = len(self.entries)

      new_page.header.next = self.header.next
      new_page.header.level = self.header.level
      new_page.header.parent = self.header.parent
      self.header.next = new_page_id

      # If we don't have a fence at the end of the entry list, we
      # need to insert a new internal fence.
      last_entry = self.entries[self.header.num - 1]
      if not last_entry.ptr & Page.FENCE:
         e = Entry(last_entry.key, last_entry.ptr | Page.INTERNAL_SET)
         self.entries.append(e)
         self.header.num += 1






@total_ordering
class Entry(object):
   __slots__ = ["key", "ptr"]

   def __init__(self, key, ptr):
      self.key = key
      self.ptr = ptr

   def __repr__(self):
      return "Entry(%s,%s)" % (str(self.key), str(self.ptr))

   def __eq__(self, other):
      return ((self.key, self.ptr) == (other.key, other.ptr))

   def __lt__(self, other):
      return ((self.key, self.ptr) < (other.key, other.ptr))

class EntryFactory(object):
   __slots__ = []

   def get_entry_fmt(self):
      return "<QQ"

   def get_entry_size(self):
      return struct.calcsize(self.get_entry_fmt())

   def get_entry_count_max(self):
      return (BLOCK_SIZE - PageHead.fmt_size) / self.get_entry_size()

   def load(self, f, offset=0):
      k, v = struct.unpack_from(self.get_entry_fmt(), f, offset)
      return Entry(k, v)

   def store(self, f, entry, offset=0):
      struct.pack_into(self.get_entry_fmt(), f, offset, entry.key, entry.ptr)


