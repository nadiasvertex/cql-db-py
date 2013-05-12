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
      
      df = datafile.DataFile(self.filename)
      
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
         kp = datafile.KeyPage(f, 8192)
      
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
        
def get_suite():
    "Return a unittest.TestSuite."
    import key_store.tests
    
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(key_store.tests)
    return suite

