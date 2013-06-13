import os
import unittest
import zlib

from glob import glob

class TestBuffer(unittest.TestCase):
   def test_can_create(self):
      from column_store.buffer import Manager
      _ = Manager()
