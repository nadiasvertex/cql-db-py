'''
Created on Jun 8, 2013

@author: Christopher Nelson
'''

import io
import os
import random
import unittest

class TestPage(unittest.TestCase):
   BLOCK_SIZE = 2048

   def test_can_create(self):
      from column_store import page
      _ = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())

   def test_can_insert(self):
      from column_store import page
      p = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())
      e = page.Entry(10, 100)
      p.insert(e)

      self.assertEqual(1, p.header.num)

   def test_can_insert_many_backwards(self):
      from column_store import page
      p = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())

      for i in range(100, 0, -1):
         e = page.Entry(i, 100 + i)
         p.insert(e)

      self.assertEqual(100, p.header.num)
      for i in range(1, 101):
         self.assertEqual(i, p.entries[i - 1].key)

   def test_can_insert_many_forwards(self):
      from column_store import page
      p = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())

      for i in range(1, 101):
         e = page.Entry(i, 100 + i)
         p.insert(e)

      self.assertEqual(100, p.header.num)
      for i in range(1, 101):
         self.assertEqual(i, p.entries[i - 1].key)


   def test_can_insert_many_random(self):
      from column_store import page
      p = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())

      keys = [i for i in range(1, 101)]
      random.shuffle(keys)

      for i in range(0, 100):
         e = page.Entry(keys[i], 101 + i)
         p.insert(e)

      self.assertEqual(100, p.header.num)
      for i in range(1, 101):
         self.assertEqual(i, p.entries[i - 1].key)


   def test_can_search(self):
      from column_store import page
      p = page.Page(bytearray(self.BLOCK_SIZE), page.EntryFactory())

      for i in range(100, 0, -1):
         e = page.Entry(i, 100 + i)
         p.insert(e)

      index = p.search(50)
      self.assertEqual(50, index)

