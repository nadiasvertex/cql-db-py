'''
Created on May 16, 2013

@author: christopher
'''
import os
from util import varint

class RleColumnStore(object):
   """
   :synopsis: Implementation of a run-length-encoding projection store for a
   column. Stores a connection between the column value and the row_id for the
   column. Rows with identical values are compressed together.

   For columns with low cardinality and repetitive natures, this is a highly
   effective store format. If the column has low cardinality, but the values
   tend to alternate regularly it may not be effective.
   """
   def __init__(self, base_name):
      self.filename = base_name + ".column.rle"
      self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

   def is_row_ordered(self):
      return True

   def append(self, value_index, start_row_id, row_count):
      """
      :synopsis: Appends a tuple that represents an encoded version of a column.

      Each entry contains a reference to the value store, a row id, and a count
      of how many contiguous rows have the same value. The tuples are varint
      compressed.

      :param value_index: The index into the value store for the value.
      :param start_row_id: The row id that this column belongs to.
      :param row_count: The number of consecutive rows that have the same value.

      :returns: The offset in the column store where the tuple was written.
      """
      self.f.seek(0, 2)
      offset = self.f.tell()
      varint.encode_stream(value_index, self.f)
      varint.encode_stream(start_row_id, self.f)
      varint.encode_stream(row_count, self.f)

      return offset

   def get(self, offset):
      self.f.seek(offset)
      return varint.decode_stream(self.f), varint.decode_stream(self.f), varint.decode_stream(self.f)

   def merge(self, offset, row_id):
      """
      :synopsis: Merges this row with the existing RLE at offset.

      :param offset: The offset where the existing tuple lives.
      :param row_id:

      :returns: False if the row cannot be merged here.

      :notes: This only works when the tuple is the last element. Trying to merge to any other element may
      cause corruption in the store.
      """
      value_index, start_row_id, row_count = self.get(offset)
      if start_row_id - 1 == row_id:
         start_row_id -= 1
         row_count += 1
      elif start_row_id + row_count + 1 == row_id:
         row_count += 1
      else:
         return False

      self.f.seek(offset)
      varint.encode_stream(value_index, self.f)
      varint.encode_stream(start_row_id, self.f)
      varint.encode_stream(row_count, self.f)

      return True

   def flush(self):
      self.f.flush()
