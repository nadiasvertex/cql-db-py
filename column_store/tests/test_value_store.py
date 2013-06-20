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

   def test_can_write_compressed(self):
      from column_store.value_store import ValueStore
      test_string = "this is a test 123"
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_COMPRESSED)
      o = v.append(test_string)
      self.assertEqual(v.get(o), test_string)

   def test_can_write_many_compressed(self):
      from column_store.value_store import ValueStore
      test_string = "this is a test 123"
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_COMPRESSED)
      ol = []
      for i in range(0, 100):
         ol.append(v.append(test_string + str(i)))
      for i, o in enumerate(ol):
         self.assertEqual(v.get(o), test_string + str(i))

   def test_can_write_user(self):
      from column_store.value_store import ValueStore
      test_string = "this is a test 123"
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_USER)
      o = v.append(test_string)
      self.assertEqual(v.get(o, size=len(test_string)), test_string)

   def test_can_write_user_compressed(self):
      from column_store.value_store import ValueStore
      test_string = "this is a test 123"
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_USER_COMPRESSED)
      o = v.append(test_string)
      self.assertEqual(v.get(o, size=len(test_string)), test_string)

   def test_can_write_int(self):
      from column_store.value_store import ValueStore
      test_value = 256
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_PACKED_INT)
      o = v.append(test_value)
      self.assertEqual(v.get(o), test_value)

   def test_can_write_many_int(self):
      from column_store.value_store import ValueStore
      v = ValueStore(self.filename, mode=ValueStore.DATA_MODE_PACKED_INT)
      ol = []
      for i in range(0, 10000):
         ol.append(v.append(i))
      for i, o in enumerate(ol):
         self.assertEqual(v.get(o), i)






