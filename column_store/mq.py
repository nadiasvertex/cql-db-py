from collections import OrderedDict
import math

class Cache(object):
   """
   :synopsis: Stores key/value references using a particular caching policy.

   The policy used is basically covered in the MQ algorithm description at
   http://opera.ucsd.edu/paper/TPDS-final.pdf

   In short, there are a configurable number of queues. Each queue is an LRU. In
   addition to tracking the LRU, we track how many accesses an item has had,
   ever. Items are migrated from higher queues down to lower queues based on
   capacity or expiration. When an item reaches the bottom element of queue 0,
   the item is evicted from the cache.

   As items are accessed, they move up levels in the queue. The current level
   function is a simple log2 of the number of accesses. So, for example, on the
   8th access, an item will move from the second level to the third level.
   """
   def __init__(self, on_evict=None, capacity=1024, queue_count=8, life_time=32):
      self.current_time = 0
      self.life_time = life_time
      self.capacity = capacity
      self.queue_count = queue_count
      self.on_evict = on_evict
      self.cache = {}
      self.queues = [OrderedDict() for _ in range(0, queue_count)]

   def _check_for_demotion(self):
      for i, q in enumerate(self.queues):
         # If we are over capacity, or if a block has expired, move it to the
         # next level down.
         if len(q) > self.capacity or (len(q) and q[q.keys()[0]] < self.current_time):
            key, value = q.popitem(False)
            level_down = i - 1
            # If we are not at the very bottom, then just move it down a level
            if level_down >= 0:
               self.queues[level_down][key] = value
            # Otherwise we must evict the value. Inform the user.
            elif self.on_evict:
               self.on_evict(key, self.cache[key][1])

   def iteritems(self):
      for k,v in self.cache.iteritems():
         yield (k,v)

   def get(self, key, default=None):
      """
      :synopsis: Tries to return the value associated with 'key'. If the
                  key is not found, a default value may be specified. If
                  that value is not None, a new key will be inserted into
                  the cache with that value. Otherwise 'None' will be returned.

      :param key: The key for the value to fetch.
      :returns: The value or None on a cache miss.
      """
      self.current_time += 1
      level, value = self.cache.get(key, (None, None))
      if level is None:
         if default is not None:
            self.put(key, default)
         return default

      _, access_count = self.queues[level][key]
      expire_time = self.current_time + self.life_time
      access_count += 1

      requested_level = int(min(math.log(access_count, 2), self.queue_count - 1))
      if requested_level > level:
         del self.queues[level][key]
         level = requested_level
         self.cache[key] = (level, value)

      self.queues[level][key] = (expire_time, access_count)
      return value

   def put(self, key, value):
      """
      :synopsis: Stores 'value' into the cache using 'key'. Uses the 'MQ'
                 algorithm to maintain cache size.
      :param key: The key to associate with 'value'.
      :param value: The value to store.
      """
      self.queues[0][key] = (self.current_time + self.life_time, 1)
      self.cache[key] = (0, value)
      self._check_for_demotion()
