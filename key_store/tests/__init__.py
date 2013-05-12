import os
import unittest

class TestPass(unittest.TestCase):
   def test_can_run_test(self):
      self.assertEqual(True, True)
        
class TestCreate(unittest.TestCase):
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
      
        
def get_suite():
    "Return a unittest.TestSuite."
    import key_store.tests
    
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(key_store.tests)
    return suite

