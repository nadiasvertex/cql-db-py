"""
Data file theory of operation
=============================

The structure of the data file is quite straightforward. There are two files:
one file is the actual data content, the other file is the write-ahead log. All
operations are first journaled, and then later written to disk. The journal
itself only contains metadata operations. The actual data is always written to
a free spot in a data page. In the case of updates, the data is written to a
new empty area, and the old area is deallocated. This way data can never be
lost one we commit to a write.

The datafile has a header on the first page which indicates how many virtual
pages exist in the datafile. There is also a directory that has an entry for each
virtual page. The directory provides a mapping from virtual pages to actual
disk pages, and is the mechanism by which the extensible hashing algorithm is
implemented.

The first 32 bytes of the header are composed of an sha256 digest of the
directory and the directory entry count. After that is the free extent count,
a list of start/end ranges that indicate free _page_ ranges.

0                       8
+-----------------------+
| sig                   |
+-----------------------+
| sig                   |
+-----------------------+
| sig                   |
+-----------------------+
| sig                   |
+-----------------------+
| directory_entry_count |
+-----------------------+
| entry 0               |
+-----------------------+
| ..entry n..           |
+-----------------------+
| free_extent_count     |
+-----------------------+
| entry 0               |
+-----------------------+
| ..entry n..           |
+-----------------------+

"""

import array
import hashlib
import os
import struct

# signature + directory entry count
header_read_fmt = "<32sQ"
# directory entry count + directory
header_write_fmt = "<Q%ss"
# extent header
extent_header_fmt = "<Q"
# extent entry
extent_fmt = "<QQ"
# key value + data pointer
key_fmt = "<QQ"
# number of bytes in value
value_header_fmt = "<I"
# the actual value
value_fmt = "<%ss"

class IntegrityError(Exception):
   def __init__(self):
      pass
   def __str__(self):
      return "integrity failure"

class KeyPage(object):
   """
   synopsis: Maintains a key page.

   The key page does not contain values. Rather, it contains 64-bit value
   pointers.
   """
   __slots__ = ["dirty", "keys", "undo_keys", "page_size", "max_entries"]
   def __init__(self, d, page_size):
      entry_size = struct.calcsize(key_fmt)

      self.dirty = False
      self.keys = {}
      self.undo_keys = {}
      self.page_size = page_size
      self.max_entries = self.page_size / entry_size
      for _ in range(0, self.max_entries):
         data = d.read(entry_size)
         if len(data) < entry_size:
            break
         k, v = struct.unpack(key_fmt, data)
         if k == 0 and v == 0:
            break
         self.keys[k] = v

   def get(self, key, default=None):
      """
      :synopsis: If the key exists, return the value, otherwise return default.
      :param key: The key to lookup
      :param default: The value to return if the key does not exist.
      :returns: The value if key exists, otherwise 'default'

      The key is the actual user's key, however the value is merely a pointer
      to the value data somewhere in the data file.
      """
      return self.keys.get(key, default)

   def set(self, key, value):
      """
      :synopsis: Write a new key, if and only if we have space to write it.

      :param key: The key to write.
      :param value: The value pointer to write. If value is None the key is deleted.
      :returns: False if we were unable to write the new value, True otherwise.

      Writes a new key. If we have too many keys, we will fail. The old value is
      saved before we write the new value.
      """

      old_value = self.keys.get(key)
      if old_value == None and len(self.keys) >= self.max_entries:
         return False

      if not self.undo_keys.has_key(key):
         self.undo_keys[key] = old_value

      if value == None:
         del self.keys[key]
      else:
         self.keys[key] = value

      self.dirty = True

      return True

   def delete(self, key):
      self.set(key, None)

   def commit(self):
      """
      :synopsis: Commits to the new state.
      """
      self.undo_keys = {}

   def rollback(self):
      """
      :synopsis: Restores the keypage to the values it had before the current
                 transaction began.
      """
      for k, v in self.undo_keys.iteritems():
         if v == None:
            del self.keys[k]
         else:
            self.keys[k] = v

   def flush(self, d):
      """
      :synopsis: Writes the key page to disk. The file object must be positioned
                where the data should be written. Unused space on the page will
                be cleared.
      :param d: A file object.

      :notes: A flush implicitly causes a commit operation to be performed.
      """
      self.commit()
      bytes_to_clear = self.page_size
      for k, v in self.keys.iteritems():
         data = struct.pack(key_fmt, k, v)
         d.write(data)
         bytes_to_clear -= len(data)

      if bytes_to_clear > 0:
         d.write("\x00" * bytes_to_clear)
      self.dirty = False

class Extent(object):
   __slots__ = ["start", "end"]
   def __init__(self, start, end):
      self.start = start
      self.end = end

   def __lt__(self, o):
      if o.start < self.start:
         return True

      if o.start > self.start:
         return False

      return o.end < self.end

   def __gt__(self, o):
      if o.start > self.start:
         return True

      if o.start < self.start:
         return False

      return o.end > self.end

   def __eq__(self, o):
      return self.start == o.start and\
             self.end == o.end

   def merge(self, o):
      merged = False
      # Extend left
      if o.start <= self.start and\
         o.end <= self.end:
         self.start = min(o.start, self.start)
         merged = True

      # Extend right
      if o.start >= self.start and\
         o.end >= self.end:
         self.end = max(o.end, self.end)
         merged = True

      # Early out, we know complete containment
      # is not a possibility if merge is true.
      if merged:
         return True

      # Complete containment
      if o.start >= self.start and\
         o.end <= self.end:
         merged = True

      return merged


class FreePage(object):
   """
   :synopsis: Manages a list of extents that are free in the data file. The
   extents are stored in ascending order, and are merged when possible.
   """
   __slots__ = ["extents", "page_size", "dirty", "max_entries"]
   def __init__(self, d, page_size):
      entry_size = struct.calcsize(extent_fmt)
      self.extents = []
      self.dirty = False
      self.page_size = page_size
      self.max_entries = self.page_size / entry_size
      for _ in range(0, self.max_entries):
         data = d.read(entry_size)
         if len(data) < entry_size:
            break
         s, e = struct.unpack(extent_fmt, data)
         if s == 0 and e == 0:
            break
         self.extents.append(Extent(s, e))

      self.extents.sort()

   def acquire(self, count):
      """
      :synopsis: Acquires an extent.
      :param count: The number of pages desired.
      :returns: A page number that has the requested range free,
                or None on error.
      """
      self.dirty = True
      for i, e in enumerate(self.extents):
         if e.end - e.start > count:
            v = e.start
            e.start += count
            return v

         if e.end - e.start == count:
            v = e.start
            self.extents.pop(i)
            return v

      return None

   def release(self, e):
      """
      :synopsis: Releases an extent back into the free pool.
      :param e: The extent to release.
      :returns: True if the release worked, False otherwise.

      :notes: The free pool may run out of space for free extents. That is, the
      pool may be so fragmented that there is not enough space to hold all of
      the free ranges. Fragmentation is dealt with on each release by trying to
      merge blocks. However, it may become fragmented simply due to certain
      acquire and release patterns. release() tries very hard to make space in
      the free pool, but may need to give up.
      """
      self.dirty = True
      for o in self.extents:
         if o.merge(e):
            return True
         if o > e:
            break

      if len(self.extents) >= self.max_entries:
         if self.defragment() == False:
            return False

      self.extents.append(e)
      self.extents.sort()
      return True

   def defragment(self):
      """
      :synopsis: Tries to defragment the free pool by creating a new free pool
                 and then releasing the old free pool into the new free pool one
                 extent at a time.
      :returns: True if space was recovered by the defragmentation process.
      """
      old_extents = self.extents
      self.extents = []
      space_recovered = False
      for e in old_extents:
         if self.release(e):
            space_recovered = True

      return space_recovered

   def flush(self, d):
      """
      :synopsis: Writes the free page to disk. The file object must be positioned
                where the data should be written. Unused space on the page will
                be cleared.
      :param d: A file object.
      """
      bytes_to_clear = self.page_size
      for e in self.extents:
         data = struct.pack(extent_fmt, e.start, e.end)
         d.write(data)
         bytes_to_clear -= len(data)

      if bytes_to_clear > 0:
         d.write("\x00" * bytes_to_clear)
      self.dirty = False

class DataFile(object):
   """
   :synopsis: Manages the data file header and large-scale operations of the data file.

   A datafile is a disk-based hash, with certain elements kept in memory for fast access. The elements
   can be paged out of memory one flushed to disk, which means that we don't have to take up a lot of
   RAM in order to store a lot of data.
   """
   __slots__ = [ "file_size_limit", "page_size", "mask", "cache", "d", "l", "a" ]
   def __init__(self, filename, page_size=8192, file_size_limit=100 * 1024 * 1024):
      self.page_size = page_size
      self.file_size_limit = file_size_limit
      self.cache = {}
      self.a = array.array("L")

      if not os.path.exists(filename):
         self.d = open(filename, "w+b")
         self.l = open(filename + ".wal", "w+b")
         self._create()
      else:
         self.d = open(filename, "r+b")
         self.l = open(filename + ".wal", "r+b")
         self._load()

      self.mask = (1 << (len(self.a))) - 1

   def _create_header(self):
      """
      :synopsis: Generates a signature and header blob from the directory.
      """
      directory = self.a.tostring()
      header = struct.pack(header_write_fmt % len(directory), len(self.a), directory)
      m = hashlib.sha256()
      m.update(header)
      return (m.digest(), header)

   def _create(self):
      """
      :synopsis: Initializes the database file with a default directory.
      """
      self.a.append(2)
      self.a.append(3)
      signature, header = self._create_header()
      self.d.seek(0)
      # print "signature=%d, header=%d" % (len(signature), len(header))
      self.d.write(signature)
      self.d.write(header)
      self.d.flush()

   def _load(self):
      """
      :synopsis: Loads the database header and directory.
      """
      self.d.seek(0)
      header = self.d.read(struct.calcsize(header_read_fmt))
      # print "signature + entry_count = %d" % len(header)
      signature, num_entries = struct.unpack(header_read_fmt, header)
      self.a.fromfile(self.d, num_entries)
      check, header = self._create_header()
      if check != signature:
         raise IntegrityError()

   def set(self, key, value):
      index = key & self.mask
      page = self.a[index]



