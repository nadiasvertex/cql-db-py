import os
import struct
import zlib

from util import varint
from map import Map
from value_store import ValueStore



class Column(object):
   def __init__(self, table_name, column_name, store_factory=RleColumnStore):
      self.base_name = "%s.%s" % (table_name, column_name)
      self.store = store_factory(self.base_name)
      self.store_map = Map(self.base_name + ".store")
      self.values = ValueStore(self.base_name)
      self.value_map = Map(self.base_name + '.value')

      self.previous_value = None
      self.previous_value_offset = None

   def append(self, row_id, value):
      if value == self.previous_value:
         if self.store.merge(self.previous_value_offset, row_id):
            return

      # Find the matching value, or create a new value entry
      for i in range(0, self.value_map.count()):
         v_offset = self.value_map.get(i)
         v = self.values.get(v_offset)
         if v == value:
            value_index = i
            break
      else:
         v_offset = self.values.append(value)
         self.value_map.append(v_offset)
         value_index = self.value_map.count() - 1

      # Append a new column tuple
      s_offset = self.store.append(value_index, row_id, 0)
      self.store_map.append(s_offset)

      self.previous_value = value
      self.previous_value_offset = s_offset

   def get(self, row_id):
      # Note: We may be able to early-out of this loop if we make the assumption
      # that this store is ordered by row_id. That's not a valid assumption for
      # some column stores, so there needs to be a mechanism to encode that
      # information.

      # Note: We may be able to do a binary search of this ordered array, again
      # assuming that it is ordered by row_id.

      for i in range(0, self.store_map.count()):
         s_offset = self.store_map.get(i)
         value_idx, start_row_id, row_count = self.store.get(s_offset)
         if row_id >= start_row_id and row_id <= start_row_id + row_count:
            v_offset = self.value_map.get(value_idx)
            return self.values.get(v_offset)

      # At this point we have found that there is no value for the specified row_id.
      return None

   def flush(self):
      self.store_map.flush()
      self.store.flush()
      self.value_map.flush()
      self.values.flush()




