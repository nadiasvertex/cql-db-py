import io
import os
import unittest

class TestBtree(unittest.TestCase):
   filename = "test.idx"
   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_create(self):
      from column_store import btree
      b = btree.BTree()
