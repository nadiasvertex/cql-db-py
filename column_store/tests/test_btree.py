import os
import unittest

class TestBtree(unittest.TestCase):
   filename = "test"
   def setUp(self):
      if os.path.exists(self.filename + ".idx"):
         os.unlink(self.filename + ".idx")

   def test_can_create(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)

   def test_can_insert(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)
      b.insert(1, 100)

   def test_can_find(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)
      b.insert(1, 100)
      self.assertEqual(b.find(1), 100)

   def test_can_insert_many(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)
      for i in range(1, 1000):
         b.insert(i, i + 100)

   def test_can_insert_many_small_page(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename, page_size=512)
      for i in range(1, 1000):
         b.insert(i, i + 100)

   def test_can_find_many(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)
      for i in range(1, 1000):
         b.insert(i, i + 100)

      for i in range(1, 1000):
         self.assertEqual(b.find(i), i + 100, "should find key %d = %d" % (i, i + 100))

   def test_can_persist(self):
      from column_store.btree import BPlusTree
      b = BPlusTree(self.filename)
      for i in range(1, 1000):
         b.insert(i, i + 100)

      b.flush()
      del b
      b = BPlusTree(self.filename)
      for i in range(1, 1000):
         self.assertEqual(b.find(i), i + 100, "should find key %d = %d" % (i, i + 100))
