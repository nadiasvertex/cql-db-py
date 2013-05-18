'''
Created on May 17, 2013

@author: Christopher Nelson
'''

import os
import struct

class Page(object):
   # key, value
   entry_fmt = "<QQ"
   entry_fmt_size = struct.calcsize(entry_fmt)

   # page_type, number of keys, largest_child_pointer
   page_header_fmt = "<BHQ"
   page_header_fmt_size = struct.calcsize(page_header_fmt)

   PAGE_TYPE_NODE = 1
   PAGE_TYPE_LEAF = 0

   def __init__(self, data, offset):
      self.data = data
      self.offset = offset
      self.dirty = False
      self.page_type, self.count, self.last_pointer = self._get_header()

   def dump(self):
      print "page at", self.offset, "(dirty)" if self.dirty else ""
      print "entries: ", self.count
      print "last pointer: ", self.last_pointer
      for i in range(0, self.count):
         print i, ":", str(self.get_entry(i))

   def _get_header(self):
      return struct.unpack_from(self.page_header_fmt, self.data)

   def _put_header(self):
      self.dirty = True
      struct.pack_into(self.page_header_fmt, self.data, 0,
                       self.page_type, self.count, self.last_pointer)

   def get_entry(self, index):
      offset = self.page_header_fmt_size + (self.entry_fmt_size * index)
      return struct.unpack_from(self.entry_fmt, self.data, offset)

   def put_entry(self, index, key, pointer_or_value):
      self.dirty = True
      offset = self.page_header_fmt_size + (self.entry_fmt_size * index)
      print "write entry: ", index, offset, len(self.data)
      struct.pack_into(self.entry_fmt, self.data, offset, key, pointer_or_value)

   def append_entry(self, key, pointer_or_value):
      self.dirty = True
      self.put_entry(self.count, key, pointer_or_value)
      self.count += 1

   def upsert_entry(self, new_key, new_offset, existing_offset):
      self.dirty = True
      for i in range(0, self.count):
            current_key, current_pointer = self.get_entry(i)
            if new_key < current_key:
               print "move ", current_key, "->", new_offset
               print "insert ", new_key, "->", current_pointer
               assert(existing_offset == current_pointer)

               # Update the current entry with the new offset.
               self.put_entry(i, current_key, new_offset)
               # Insert a new entry with the old offset
               self.insert_entry_before(i, new_key, current_pointer)
               self.count += 1
               return

      # If we got here then the new key must be the largest on the page. We
      # will take the current last pointer and assign it to the new key. Then
      # assign the new page to the last pointer.
      assert(existing_offset == self.last_pointer)
      self.append_entry(new_key, self.last_pointer)
      self.last_pointer = new_offset

   def _insert_entry_before(self, index, key, value):
      start_offset = self.page_header_fmt_size + (self.entry_fmt_size * index)
      end_offset = self.page_header_fmt_size + (self.entry_fmt_size * (index + 1))
      el_move_count = self.count - index
      self.data[start_offset:start_offset + el_move_count] = self.data[end_offset:end_offset + el_move_count]
      self.put_entry(index, key, value)
      self.count += 1

   def flush(self):
      self._put_header()

class BPlusTree(object):

   def __init__(self, base_name):
      self.page_size = 8192
      self.root_page = 0
      self.filename = base_name + ".idx"
      self.b = (self.page_size - Page.page_header_fmt_size) / Page.entry_fmt_size
      if os.path.exists(self.filename):
         self.f = open(self.filename, "r+b")
      else:
         self.f = open(self.filename, "w+b")
         self._allocate_page()

   def _set_root(self, offset):
      self.root_page = offset

   def _allocate_page(self):
      self.f.seek(0, 2)
      offset = self.f.tell()
      self.f.write("\x00" * self.page_size)
      return offset

   def _get_page(self, offset):
      self.f.seek(offset)
      return Page(memoryview(bytearray(self.f.read(self.page_size))), offset)

   def _put_page(self, page):
      page.flush()
      self.f.seek(page.offset)
      self.f.write(page.data)

   def _split_node(self, path):
      raise "not implemented"

   def _split_leaf(self, path):
      """
      :synopsis: Split this leaf into two leaves. If there is no parent, then
      this leaf is the root. Create a new parent. Take the middle key and
      insert it into the parent. This leaf becomes the pointer.

      (1,2,3,4,5)

      (3)------+
       |       |
      (1,2)  (3,4,5)

      (3)---------------+
       |                |
      (-2,-1,0,1,2)  (3,4,5)

      (0,      3)--------+
       |       |         |
      (-2,-1) (0,1,2)  (3,4,5)

      (0,      3)--------+
       |       |         |
      (-2,-1) (0,1,2)  (3,4,5,6,7)

      (0,      3,       5)-----+
       |       |        |      |
      (-2,-1) (0,1,2)  (3,4)  (5,6,7)

      (0,      3,               5)-----+
       |       |                |      |
      (-2,-1) (0,1,2,2.1,2.2)  (3,4)  (5,6,7)

      (0,      2,    3,          5)-----+
       |       |     |           |      |
      (-2,-1) (0,1) (2,2.1,2.2)  (3,4)  (5,6,7)

      """
      # Find the middle entry
      page_to_split = path[-1]
      page_to_split.dump()
      middle_index = page_to_split.count / 2
      middle_entry = page_to_split.get_entry(middle_index)
      print "splitting at: ", middle_index, str(middle_entry)

      # Move the middle entry and everything to the right of it into a new
      # page.
      new_page = self._get_page(self._allocate_page())
      new_page.page_type = Page.PAGE_TYPE_LEAF
      new_page.count = self.b - middle_index
      new_i = 0
      for i in range(middle_index, self.b):
         key, value = page_to_split.get_entry(i)
         new_page.put_entry(new_i, key, value)
         new_i += 1

      # Update the previous page to point to the new page, and to have fewer
      # items in its inventory
      page_to_split.page_type = Page.PAGE_TYPE_LEAF
      page_to_split.count = middle_index
      page_to_split.last_pointer = new_page.offset

      self._put_page(new_page)
      self._put_page(page_to_split)

      # Insert a new key in the parent
      if len(path) == 1:
         # We are the root, make a new one
         new_root_offset = self._allocate_page()
         new_root = self._get_page(new_root_offset)
         new_root.page_type = Page.PAGE_TYPE_NODE
         new_root.count = 2
         new_root.last_pointer = new_page.offset
         new_root.put_entry(0, *middle_entry)
         self._put_page(new_root)
         self._set_root(new_root_offset)
      else:
         # We have a parent, insert a new key into it to point to the existing
         # page, and update the existing key to point to the new page.
         parent_page = path[-2]
         parent_page.upsert_entry(middle_entry[1],
                                  new_page.offset, page_to_split.offset)

      return middle_entry, new_page, page_to_split

   def insert(self, key, value):
      offset = self.root_page
      path = []

      print "insert", key, value

      while True:
         page = self._get_page(offset)
         print "page at", str(offset) + ":", page.page_type, page.count, page.last_pointer

         # Save the current page and offset in a path stack, since
         # it may be needed to perform splitting.
         path.append(page)

         if page.page_type == Page.PAGE_TYPE_NODE:
            if page.count >= self.b:
               self._split_node(path)

            for i in range(0, page.count):
               current_key, pointer = page.get_entry(i)
               if key < current_key:
                  offset = pointer
                  break
            else:
               # This is the largest key on the page. Go to the last_child.
               offset = page.last_pointer

            continue
         else:
            if page.count == 0:
               # This must be the root page of a new index.
               page.put_entry(0, key, value)
               page.count += 1
               self._put_page(page)
               return

            if page.count >= self.b:
               # Split the page, then figure out which new page to use
               middle_entry, new_page, old_page = self._split_leaf(path)
               if key < middle_entry[1]:
                  page = new_page
               else:
                  page = old_page

            for i in range(0, page.count):
               current_key, _ = page.get_entry(i)
               if current_key > key:
                  # insert the new value right before this.
                  page.insert_entry_before(i, key, value)
                  break
            else:
               page.append_entry(key, value)

            self._put_page(page)
            return

   def find(self, key):
      offset = self.root_page

      while True:
         page = self._get_page(offset)

         if page.page_type == Page.PAGE_TYPE_NODE:
            for i in range(0, page.count):
               current_key, pointer = page.get_entry(i)
               if key < current_key:
                  offset = pointer
                  break
            else:
               # This is the largest key on the page. Go to the last_child.
               offset = page.last_pointer

            continue
         else:
            for i in range(0, page.count):
               current_key, value = page.get_entry(i)
               if current_key == key:
                  return value

            return None

   def flush(self):
      self.f.flush()

