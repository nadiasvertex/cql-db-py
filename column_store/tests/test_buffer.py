import io
import os
import unittest

class TestBuffer(unittest.TestCase):
   filename = "test.buffer.dat"
   def setUp(self):
      if os.path.exists(self.filename):
         os.unlink(self.filename)

   def test_can_create(self):
      from column_store import buffer
      m = buffer.Manager()

   def test_can_create_page(self):
      from column_store import buffer
      m = buffer.Manager()
      m.set_buffer_limit(buffer.page.BLOCK_SIZE * 100)

      file_id = 1
      file_handle = io.FileIO(self.filename, 'a+')
      header = buffer.page.PageHead()

      for i in range(0, buffer.page.BLOCK_SIZE * 500, buffer.page.BLOCK_SIZE):
         p = m.new_page(file_handle, file_id, i)
         header.load(p)
         self.assertEqual(buffer.page.INVALID_PAGE, header.next)
         self.assertEqual(buffer.page.INVALID_PAGE, header.prev)
         self.assertEqual(buffer.page.INVALID_PAGE, header.parent)
         self.assertEqual(0, header.level)
         self.assertEqual(0, header.num)

   def test_can_read_page(self):
      from column_store import buffer
      m = buffer.Manager()
      m.set_buffer_limit(buffer.page.BLOCK_SIZE * 10)

      file_id = 1
      file_handle = io.FileIO(self.filename, 'a+')
      header = buffer.page.PageHead()

      for i in range(0, buffer.page.BLOCK_SIZE * 50, buffer.page.BLOCK_SIZE):
         _ = m.new_page(file_handle, file_id, i)

      for i in range(0, buffer.page.BLOCK_SIZE * 50, buffer.page.BLOCK_SIZE):
         p = m.read_page(file_handle, file_id, i)
         header.load(p)
         msg = "page:(%d,%s)" % (i / buffer.page.BLOCK_SIZE, hex(i))
         self.assertEqual(buffer.page.INVALID_PAGE, header.next, msg)
         self.assertEqual(buffer.page.INVALID_PAGE, header.prev, msg)
         self.assertEqual(buffer.page.INVALID_PAGE, header.parent, msg)
         self.assertEqual(0, header.level, msg)
         self.assertEqual(0, header.num, msg)

   def test_can_write_page(self):
      from column_store import buffer
      m = buffer.Manager()
      m.set_buffer_limit(buffer.page.BLOCK_SIZE * 10)

      file_id = 1
      file_handle = io.FileIO(self.filename, 'a+')

      for i in range(0, buffer.page.BLOCK_SIZE * 50, buffer.page.BLOCK_SIZE):
         _ = m.new_page(file_handle, file_id, i)

      for i in range(0, buffer.page.BLOCK_SIZE * 50, buffer.page.BLOCK_SIZE):
         p = m.read_page(file_handle, file_id, i)
         m.write_page(file_handle, file_id, i, p)


