import os
import unittest

class TestBtree(unittest.TestCase):
   filename = "0.idx"
   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_create(self):
      from column_store import btree
      b = btree.BTree()

   def test_can_insert(self):
      from column_store import btree, page
      b = btree.BTree()
      e = page.Entry(100, 50)
      b.insert(e)
