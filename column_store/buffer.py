'''
Created on Jun 9, 2013

@author: Christopher Nelson
'''

from column_store.mq import Cache
from column_store.page import factory

import io

DEFAULT_BUFFER_SIZE = 1024 * 1024 * 100  # 100mb
PAGE_SIZE = 4096

class Manager(object):
   '''
   :synopsis: Manages pages for a particular file. The buffer constrains all
   pages from all associated files to a particular limit. It manages buffers
   from all files associated with a particular fdtree structure.
   '''

   def __init__(self, size=DEFAULT_BUFFER_SIZE, page_size=PAGE_SIZE, page_factory=factory,
                filename_base="data", filename_ext="db"):
      self.cache = Cache(capacity=size / page_size, on_evict=self.evict_page)
      self.size = size
      self.page_size = page_size
      self.page_factory = page_factory
      self.file_handles = []
      self.block_cache = []

      self.filename_base = filename_base
      self.filename_ext = filename_ext

   def allocate_file(self):
      file_id = len(self.file_handles)
      file_name = "%s.%d.%s" % (self.filename_base, file_id, self.filename_ext)
      self.file_handles.append(io.open(file_name, "a+b", buffering=0))
      return file_id

   def read_page_uncached(self, page_id, file_id):
      f = self.file_handles[file_id]
      f.seek(page_id)
      data = bytearray(self.page_size)
      f.readinto(data)
      return data

   def flush(self):
      for k, v in self.cache.iteritems():
         if v.is_dirty():
            v.persist(self, k[0], k[1])
            v.mark_dirty(False)

   def read_page(self, page_id, file_id):
      page = self.cache.get((page_id, file_id))
      if page is None:
         data_page = self.read_page_uncached(page_id, file_id)
         page = self.page_factory(page_id, file_id, data_page)
         self.cache.put((page_id, file_id), page)
      return page

   def evict_page(self, key, page):
      if page.is_dirty():
         page.persist(self, key[0], key[1])
