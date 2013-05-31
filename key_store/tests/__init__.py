import os
import unittest

class TestPass(unittest.TestCase):
   def test_can_run_test(self):
      self.assertEqual(True, True)

class TestDataFile(unittest.TestCase):
   filename = "test.db"
   wal_filename = filename + ".wal"

   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)
      if os.path.exists(self.wal_filename):
         os.unlink(self.wal_filename)

   def test_can_create(self):
      from key_store import datafile

      _ = datafile.DataFile(self.filename)

   def test_can_load(self):
      from key_store import datafile

      df = datafile.DataFile(self.filename)
      del df
      df = datafile.DataFile(self.filename)

class TestKeyPage(unittest.TestCase):
   filename = "test.key_page"

   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_create(self):
      from key_store import datafile
      with open(self.filename, "w+b") as f:
         _ = datafile.KeyPage(f, 8192)

   def test_can_set(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         self.assertTrue(kp.set(100, 500))

   def test_can_get(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         self.assertEqual(kp.get(100), 500)

   def test_can_delete(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         kp.delete(100)
         self.assertEqual(kp.get(100), None)

   def test_can_flush(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         f.seek(0)
         kp.flush(f)
         self.assertFalse(kp.dirty)

   def test_set_persists(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         f.seek(0)
         kp.flush(f)

      with open(self.filename, "r+b") as f:
         kp = datafile.KeyPage(f, 8192)
         self.assertEqual(kp.get(100), 500)

   def test_commit(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         kp.commit()
         self.assertEqual(kp.get(100), 500)

   def test_rollback(self):
      from key_store import datafile

      with open(self.filename, "w+b") as f:
         kp = datafile.KeyPage(f, 8192)
         kp.set(100, 500)
         kp.commit()
         kp.set(100, 250)
         kp.rollback()
         self.assertEqual(kp.get(100), 500)

class TestFreePage(unittest.TestCase):
   filename = "test.free_page"

   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_create(self):
      from key_store import datafile
      with open(self.filename, "w+b") as f:
         _ = datafile.FreePage(f, 8192)

   def test_can_release(self):
      from key_store import datafile
      with open(self.filename, "w+b") as f:
         fp = datafile.FreePage(f, 8192)
         self.assertTrue(fp.release(datafile.Extent(100, 500)), "release should succeed")

   def test_can_allocate(self):
      from key_store import datafile
      with open(self.filename, "w+b") as f:
         fp = datafile.FreePage(f, 8192)
         fp.release(datafile.Extent(100, 500))
         start = fp.acquire(50)
         self.assertEqual(start, 100)
         start = fp.acquire(50)
         self.assertEqual(start, 150)
         self.assertTrue(fp.release(datafile.Extent(100, 149)))
         start = fp.acquire(50)
         self.assertEqual(start, 100)

   def test_allocation_persists(self):
      from key_store import datafile
      with open(self.filename, "w+b") as f:
         fp = datafile.FreePage(f, 8192)
         fp.release(datafile.Extent(100, 500))
         fp.flush(f)

      with open(self.filename, "r+b") as f:
         fp = datafile.FreePage(f, 8192)
         start = fp.acquire(50)
         self.assertEqual(start, 100)




def get_suite():
   "Return a unittest.TestSuite."
   import key_store.tests

   loader = unittest.TestLoader()
   suite = loader.loadTestsFromModule(key_store.tests)
   return suite

