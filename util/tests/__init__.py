import os
import unittest

class TestPass(unittest.TestCase):
   def test_can_run_test(self):
      self.assertEqual(True, True)

class TestFreePage(unittest.TestCase):
   filename = "test.free_page"

   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_encode(self):
      from util import varint
      _ = varint.encode(900)

   def test_can_decode(self):
      from util import varint
      s = varint.encode(900)
      _ = varint.decode(s)

   def test_can_roundtrip_positive_int(self):
      from util import varint
      s = varint.encode(900)
      self.assertEqual(varint.decode(s), 900)

   def test_can_roundtrip_negative_int(self):
      from util import varint
      s = varint.encode(-900)
      self.assertEqual(varint.decode(s), -900)


def get_suite():
   "Return a unittest.TestSuite."
   import util.tests

   loader = unittest.TestLoader()
   suite = loader.loadTestsFromModule(util.tests)
   return suite

