import os
import unittest
import zlib

from glob import glob

class TestValueStore(unittest.TestCase):
   def setUp(self):
      self.filename = "test_value_store"
      files = glob(self.filename + ".*")
      for f in files:
         os.unlink(f)

   def test_can_create(self):
      from column_store.value_store import ValueStore
      _ = ValueStore(self.filename)

   def test_keeps_open_mode(self):
      from column_store.value_store import ValueStore
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_COMPRESSED)
      del v

      v = ValueStore(self.filename)
      self.assertEqual(v.mode, ValueStore.DATA_MODE_COMPRESSED)
      self.assertEqual(v.compression_level, zlib.Z_BEST_SPEED)

