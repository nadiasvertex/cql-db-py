import os
import struct
import zlib

from util import varint


class Map(object):
   fmt = "<Q"
   fmt_size = struct.calcsize(fmt)

   __slots__ = ["filename", "f"]

   def __init__(self, base_name):
      self.filename = base_name + ".map"
      self.f = self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

   def append(self, offset):
      self.f.seek(0, 2)
      self.f.write(struct.pack(self.fmt, offset))

   def delete(self, index):
      self.f.seek(self.fmt_size * index)
      self.f.write(struct.pack(self.fmt, 0))

   def update(self, index, offset):
      self.f.seek(self.fmt_size * index)
      self.f.write(struct.pack(self.fmt, offset))

   def get(self, index):
      self.f.seek(self.fmt_size * index)
      return struct.unpack(self.fmt, self.f.read(self.fmt_size))[0]

   def count(self):
      self.f.seek(0, 2)
      offset = self.f.tell()
      return offset / self.fmt_size

   def flush(self):
      self.f.flush()


class ValueStore(object):
   """
   :synopsis: Manages a store of values.

   The value store has several modes. The most basic mode involves simple binary value storage. The value is assumed
   to be a string, or at least to be something on which len() works and can be written to a file. The length of the
   object is taken, and then the object is written into the file store.

   The second mode is just like the first, except that zlib compression is applied to the object. By default we use
   a low compression setting for speed, however the compression level is settable. Compression is only applied to
   objects which are larger than some fixed size. By default this size is 32 bytes.

   The third mode is one in which the size of the data is kept externally. Presumably the caller has some information
   which allows them to know how large the object ought to be. In this case we just store the object. Note that if the
   object is larger than the caller says no corruption will occur. Space may be wasted.

   The fourth mode is just like the third, except that zlib compression is applied to the object. The compression
   details are just like mode 2.

   The final mode stores 64-bit signed integers using zigzag base128 encoding. This allows numbers to be stored in a
   very space efficient way, assuming that the numbers are mostly smaller than 8 bytes.
   """

   header_fmt = "<"

   __slots__ = ["filename", "f"]

   def __init__(self, base_name):
      self.filename = base_name + ".values"
      self.f = self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

   def append(self, value):
      self.f.seek(0, 2)
      offset = self.f.tell()
      varint.encode_stream(len(value), self.f)
      self.f.write(value)
      return offset

   def get(self, offset):
      self.f.seek(offset)
      size = varint.decode_stream(self.f)
      return self.f.read(size)

   def flush(self):
      self.f.flush()


class RleColumnStore(object):
   def __init__(self, base_name):
      self.filename = base_name + ".column.rle"
      self.f = open(self.filename, "r+b") if os.path.exists(self.filename) else open(self.filename, "w+b")

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
      # Note: We may be able to early-out of this loop if we make the assumption that
      # this store is ordered by row_id. That's not a valid assumption for some column
      # stores, so there needs to be a mechanism to encode that information.

      # Note: We may be able to do a binary search of this ordered array, again assuming
      # that it is ordered by row_id.

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




