'''
Created on Jun 7, 2013

@author: Christopher Nelson
'''

import bisect
import time
from collections import OrderedDict

from column_store import page

DEFAULT_BUFFER_LIMIT = 1024 * 1024 * 100

class LruNode(object):
   __slots__ = ["page_id", "file_id", "block_number", "dirty",
                "file_handle", "last_used", "page"]
   def __init__(self, page_id, file_id, file_handle, page):
      self.page_id = page_id
      self.file_id = file_id
      self.file_handle = file_handle
      self.page = page
      self.last_used = 0
      self.dirty = False

class Lru(object):
   def __init__(self):
      self.lru = OrderedDict()

   def get_lru(self):
      return self.lru.popitem(False)[1]

   def append(self, p):
      self.lru[p.page_id] = p

   def touch(self, p):
      del self.lru[p.page_id]
      self.lru[p.page_id] = p

class Manager(object):
   def __init__(self):
      self.pages = {}
      self.lru = Lru()

      self.stat_read = 0
      self.stat_hit = 0
      self.max_pages = 0
      self.set_buffer_limit(DEFAULT_BUFFER_LIMIT)

   def set_buffer_limit(self, max_bytes):
      self.max_pages = max_bytes / page.BLOCK_SIZE

   def at_capacity(self):
      return len(self.pages) >= self.max_pages

   def clear_dirty(self, file_id, page_id):
      p = self.pages.get((file_id, page_id), None)
      if not p:
         return
      p.dirty = False

   def mark_dirty(self, file_id, page_id):
      p = self.pages.get((file_id, page_id), None)
      if not p:
         return
      p.dirty = True

   def clear_all_dirty_bit(self):
      for v in self.pages.values():
         v.dirty = False

   def flush_dirty(self):
      for v in self.pages.values():
         self.write_page(v.file_handle, v.file_id, v.page_id, v.page)

   def _evict_page(self, file_handle, file_id, page_id):
      p = self.lru.get_lru()
      if p.dirty:
         self.write_page(p.file_handle, p.file_id, p.page_id, p.page)

      fp = (p.file_id, p.page_id)
      del self.pages[fp]

      p.dirty = False
      p.file_id = file_id
      p.file_handle = file_handle
      p.page_id = page_id
      p.last_used = time.time()
      self.lru.append(p)

      return p

   def new_page(self, file_handle, file_id, page_id):
      fp = (file_id, page_id)

      # If we're at capacity, re-use the LRU page.
      if self.at_capacity():
         p = self._evict_page(file_handle, file_id, page_id)
      else:
         p = LruNode(page_id, file_id, file_handle, bytearray(page.BLOCK_SIZE))

      # New pages are always dirty
      p.dirty = True
      p.last_used = time.time()

      # Update header
      header = page.PageHead()
      header.next = header.prev = header.parent = page.INVALID_PAGE
      header.level = 0
      header.num = 0
      header.page_id = page_id
      header.flag = 0
      header.store(p.page)

      # Store page
      self.pages[fp] = p
      self.lru.append(p)

      return p.page

   def read_page(self, file_handle, file_id, page_id):
      fp = (file_id, page_id)
      self.stat_read += 1

      p = self.pages.get(fp, None)
      # Page was found resident
      if p != None:
         self.stat_hit += 1
         self.lru.touch(p)
         return p.page

      # Page is not resident

      # If we're at capacity, re-use the LRU page.
      if self.at_capacity():
         p = self._evict_page(file_handle, file_id, page_id)
      else:
         p = LruNode(page_id, file_id, file_handle, bytearray(page.BLOCK_SIZE))

      file_handle.seek(p.page_id, 0)
      file_handle.readinto(p.page)

      return p.page

   def write_page(self, file_handle, file_id, page_id, page):
      file_handle.seek(page_id, 0)
      file_handle.write(page)
