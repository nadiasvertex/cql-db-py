import hashlib
import os
import struct
import unittest

from glob import glob

class TestBuffer(unittest.TestCase):
   def setUp(self):
      for filename in glob("test_data*"):
         os.unlink(filename)

   def test_can_create(self):
      from column_store.buffer import Manager
      _ = Manager()

   def test_can_read(self):
      from column_store.buffer import Manager
      mgr = Manager(page_size=2048, filename_base="test_data")
      file_id = mgr.allocate_file()
      for page_id in range(0, 2048 * 100, 2048):
         mgr.read_page(page_id, file_id)

   def test_can_write(self):
      from column_store.buffer import Manager
      mgr = Manager(page_size=2048, size=2048 * 10, filename_base="test_data")
      file_id = mgr.allocate_file()
      crcs = []
      for page_id in range(0, 2048 * 1000, 2048):
         p = mgr.read_page(page_id, file_id)
         struct.pack_into("BBBBB", p.data, 0, 1, 2, 3, 4, 5)
         p.mark_dirty(True)
         crcs.append(hashlib.md5(p.data).digest())

      for i, page_id in enumerate(range(0, 2048 * 1000, 2048)):
         p = mgr.read_page(page_id, file_id)
         crc = hashlib.md5(p.data).digest()
         self.assertEqual(crc, crcs[i])


