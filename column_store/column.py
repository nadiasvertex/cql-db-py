from map import Map
from value_store import ValueStore
from rle import RleColumnStore


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

   def _get_tuple_at_index(self, index):
      s_offset = self.store_map.get(index)
      return self.store.get(s_offset)

   def _get_value_at_index(self, index):
      v_offset = self.value_map.get(index)
      return self.values.get(v_offset)

   def _cmp_row_with_range(self, row_id, index):
      value_idx, start_row_id, row_count = self._get_tuple_at_index(index)
      if row_id < start_row_id:
         return -1, value_idx

      if row_id > start_row_id + row_count:
         return 1, value_idx

      return  0, value_idx

   def _get_linear_search(self, row_id):
      for i in range(0, self.store_map.count()):
         range_result, value_idx = self._cmp_row_with_range(row_id, i)
         if range_result == 0:
            return self._get_value_at_index(value_idx)

      # At this point we have found that there is no value for the specified
      # row_id.
      return None

   def _get_binary_search(self, row_id):
      min_index = 0
      max_index = self.store_map.count() - 1

      while True:
         # We have gone all the way down, the row must not exist.
         if max_index < min_index:
            return None

         center = (max_index + min_index) / 2

         range_result, value_idx = self._cmp_row_with_range(row_id, center)
         if range_result == 0:
            return self._get_value_at_index(value_idx)

         if range_result == -1:
            max_index = center - 1
         else:
            min_index = center + 1

   def get(self, row_id):
      if self.store.is_row_ordered():
         return self._get_binary_search(row_id)

      return self._get_linear_search(row_id)

   def flush(self):
      self.store_map.flush()
      self.store.flush()
      self.value_map.flush()
      self.values.flush()

