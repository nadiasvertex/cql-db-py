'''
Created on Jun 9, 2013

@author: Christopher Nelson
'''

from arc import Cache

DEFAULT_BUFFER_SIZE = 1024 * 1024 * 100  # 100mb
PAGE_SIZE = 4096

class Manager(object):
   '''
   :synopsis: Manages pages for a particular file. The buffer constrains all
   pages from all associated files to a particular limit. It manages buffers
   from all files associated with a particular fdtree structure.
   '''

   def __init__(self, size=DEFAULT_BUFFER_SIZE, page_size=PAGE_SIZE):
      self.cache = Cache(size / page_size)
      self.size = size
      self.page_size = page_size
      self.read_page = self.cache(self.read_page_uncached)
      self.file_handles = {}

   def read_page_uncached(self, page_id, file_id):
      f = self.file_handles[file_id]
      f.seek(page_id)
      return bytearray(f.read(self.page_size))



