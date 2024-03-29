import os
import unittest

from glob import glob

from test_value_store import TestValueStore
from test_column import TestColumn
from test_mq_cache import TestMqCache
from test_buffer import TestBuffer
#from test_page import TestPage

class TestPass(unittest.TestCase):
   def test_can_run_test(self):
      self.assertEqual(True, True)


def get_suite():
   "Return a unittest.TestSuite."
   import column_store.tests

   loader = unittest.TestLoader()
   suite = loader.loadTestsFromModule(column_store.tests)
   return suite

