'''
Created on May 17, 2013

@author: Christopher Nelson

'''



import os
import struct

from column_store import page
from column_store import buffer

class TreeState(object):
   __slots__ = ["state_type", "fdtree", "file_id", "file_handle", "buffer", "mbuffer",
                "current_page", "current_page_head", "current_entries", "current_offset",
                "page_id", "level", "force_write", "force_read", "tree", "next_state"]
   def __init__(self):
      self.state_type = 0
      self.fdtree = None
      self.file_id = 0
      self.file_handle = None
      self.buffer = None
      self.mbuffer = None
      self.current_page = None
      self.current_page_head = None
      self.current_entries = None
      self.current_offset = 0
      self.page_id = 0
      self.level = 0
      self.force_read = False
      self.force_write = False
      self.tree = None
      self.next_state = None

class BTree(object):
   __slots__ = ["file_id", "file_handle", "level_block", "max_block", "max_entry",
                "entry_count", "block_count", "level_count", "root", "init_entry", "buffer",
                "entry_factory", "page_buffer"]
   def __init__(self, entry_factory=page.EntryFactory):
      self.file_id = []
      self.file_handle = []
      self.level_block = []
      self.max_block = 0
      self.max_entry = 0
      self.block_count = 0
      self.entry_count = 0
      self.level_count = 0
      self.root = 0
      self.init_entry = 0
      self.buffer = None
      self.entry_factory = entry_factory()
      self.page_buffer = buffer.Manager()

   def insert(self, entry):
      # int offset1;
      # Entry * data1;
      key = entry.key
      page_id = self.root
      trace = []

      for i in range(0, self.level_count):
         current_level = self.level_count - i - 1
         trace[current_level] = page_id
         cur_page = self.read_page(self.file_handle[current_level], self.file_id[current_level], page_id)
         cur_entry = self.in_page_search(page, key)
         page_id = cur_entry.ptr

      final_page = self.read_page(self.file_handle[0], self.file_id[0], page_id)
      self.in_page_insert(final_page, entry)
      self.mark_dirty(self.file_id[0], page_id)

      if final_page.entry_count >= self.entry_factory.get_entry_count_max():
         self.split(final_page, 0, trace)

      self.entry_count += 1
      return True
