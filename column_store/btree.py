'''
Created on May 17, 2013

@author: Christopher Nelson

'''



import os
import struct

from column_store import page
from column_store import buffer
from column_store import file_manager

HEADTREE_PAGES_BOUND = 256

class TreeState(object):
   __slots__ = ["state_type", "tree", "file_id", "file_handle", "buffer", "mbuffer",
                "current_page", "current_entries", "current_offset",
                "page_id", "level", "force_write", "force_read", "tree", "next_state"]

   STATE_TYPE_BTREE = 1

   def __init__(self, tree, file_id, file_handle):
      self.state_type = self.STATE_TYPE_BTREE
      self.tree = tree
      self.file_id = file_id
      self.file_handle = file_handle
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
                "entry_factory", "page_buffer", "file_manager"]
   def __init__(self, entry_factory=page.EntryFactory):
      self.file_manager = file_manager.Manager()
      self.entry_factory = entry_factory()
      self.page_buffer = buffer.Manager()
      file_id, file_handle = self.file_manager.allocate_file()
      self.file_id = [file_id]
      self.file_handle = [file_handle]
      self.level_block = [0]
      self.max_block = HEADTREE_PAGES_BOUND
      self.max_entry = self.max_block * self.entry_factory.get_entry_count_max()
      self.block_count = 1
      self.entry_count = 1
      self.level_count = 1
      self.root = 0
      self.init_entry = 1
      self.buffer = None

      page_data = self.page_buffer.new_page(file_handle, file_id, self.level_block[0])
      p = page.Page(page_data, self.entry_factory)

      # Insert the sentinel element
      e = page.Entry(page.MAX_KEY_VALUE, page.INVALID_PAGE)
      p.insert(e)
      e.ptr |= (page.Page.FENCE | page.Page.INTERNAL)

      self.page_buffer.mark_dirty(file_id, self.level_block[0])
      self.level_block[0] += page.BLOCK_SIZE

   def _read_page(self, level, page_id):
      data = self.page_buffer.read_page(self.file_handle[level],
                                        self.file_id[level], page_id)
      return page.Page(data, self.entry_factory)

   def _split(self, page_to_split, level, trace):
      new_page_id = self.level_block[level]
      page_data = self.page_buffer.new_page(self.file_handle[level],
                                            self.file_id[level],
                                            new_page_id)
      self.level_block[level] += page.BLOCK_SIZE
      new_page = page.Page(page_data, self.entry_factory)
      page_to_split.split_to(new_page, new_page_id)


   def insert(self, entry):
      key = entry.key
      page_id = self.root
      trace = []

      for i in range(0, self.level_count - 1):
         current_level = self.level_count - i - 1
         trace[current_level] = page_id
         cur_page = self._read_page(current_level, page_id)
         index = cur_page.search(key)
         entry = cur_page.entries[index]
         page_id = entry.ptr & page.Page.MASK

      final_page = self._read_page(0, page_id)
      final_page.insert(entry)
      self.page_buffer.mark_dirty(self.file_id[0], page_id)

      if final_page.header.num >= self.entry_factory.get_entry_count_max():
         self._split(final_page, 0, trace)

      self.entry_count += 1
      return True
