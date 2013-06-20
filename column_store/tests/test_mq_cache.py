import os
import unittest
import zlib

from glob import glob

class TestMqCache(unittest.TestCase):
   def test_can_create(self):
      from column_store.mq import Cache
      _ = Cache()

   def test_can_put(self):
      from column_store.mq import Cache
      c = Cache(capacity=16)
      for i in range(0,1024):
         c.put(i, i<<16)

   def test_can_get(self):
      from column_store.mq import Cache
      c = Cache(capacity=1024, queue_count=8)
      for i in range(0,1024):
         c.put(i, i<<16)

      for i in range(0,1024):
         v = c.get(i)
         self.assertEqual(i<<16,v)

   def test_migration(self):
      from column_store.mq import Cache
      c = Cache(capacity=128, queue_count=8)
      for i in range(0,64):
         c.put(i, i<<16)

      # This should migrate each element into
      # the second level (index 1)
      for i in range(0,64):
         v = c.get(i)
         self.assertEqual(i<<16,v)

      self.assertEqual(64, len(c.queues[1]))

      # This should migrate each element into
      # the third level (index 2)
      for i in range(0,64):
         for j in range(0,2):
            v = c.get(i)
            self.assertEqual(i<<16,v)

      self.assertEqual(64, len(c.queues[2]))

      # These should be in the first level (index 0)
      for i in range(64,128):
         c.put(i, i<<16)

      self.assertEqual(64, len(c.queues[0]))
      self.assertEqual(64, len(c.queues[2]))

   def test_eviction(self):
      class Evictor(object):
         def __init__(self):
            self.evictions = 0

         def evict(self, key, value):
            self.evictions+=1

      from column_store.mq import Cache
      e = Evictor()
      c = Cache(capacity=128, queue_count=8, on_evict=e.evict)
      for i in range(0,1024):
         c.put(i, i<<16)

      expected_eviction_count = 1024 - 128
      self.assertEqual(expected_eviction_count, e.evictions)

      for i in range(1024-128, 1024):
         v = c.get(i)
         self.assertEqual(i<<16,v)










