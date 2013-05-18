import os
import unittest

from glob import glob

class TestPass(unittest.TestCase):
   def test_can_run_test(self):
      self.assertEqual(True, True)

class TestColumn(unittest.TestCase):
   def setUp(self):
      files = glob("test_table.test_col.*")
      for f in files:
         os.unlink(f)

   def test_can_open_column(self):
      from column_store.column import Column
      _ = Column("test_table", "test_col")

   def test_can_write_value(self):
      from column_store.column import Column
      c = Column("test_table", "test_col")
      c.append(1, "this is a test value")

   def test_can_read_value(self):
      from column_store.column import Column
      test_val = "this is a test value"
      c = Column("test_table", "test_col")
      c.append(1, test_val)
      self.assertEqual(c.get(1), test_val)

   def test_can_persist_value(self):
      from column_store.column import Column
      test_val = "this is a test value"
      c = Column("test_table", "test_col")
      c.append(1, test_val)
      c.flush()
      del c

      c = Column("test_table", "test_col")
      self.assertEqual(c.get(1), test_val)

   def test_can_write_many_values(self):
      from column_store.column import Column
      test_val_templ = "this is test value %d"
      c = Column("test_table", "test_col")
      limit = 100
      row_id = 1
      for i in range(1, limit):
         test_val = test_val_templ % i
         for j in range(1, limit):
            c.append(row_id, test_val)
            row_id += 1

      row_id = 1
      for i in range(1, limit):
         test_val = test_val_templ % i
         for _ in range(1, limit):
            self.assertEqual(c.get(row_id), test_val)
            row_id += 1


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

def get_suite():
   "Return a unittest.TestSuite."
   import column_store.tests

   loader = unittest.TestLoader()
   suite = loader.loadTestsFromModule(column_store.tests)
   return suite

