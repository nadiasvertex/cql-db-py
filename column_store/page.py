__author__ = 'cnelson'

class Page(object):
   def __init__(self, page_id, file_id, data):
      self.data = data
      self.dirty = False

   def persist(self, mgr, page_id, file_id):
      f = mgr.file_handles[file_id]
      f.seek(page_id)
      f.write(self.data)

   def mark_dirty(self, state):
      self.dirty = state

   def is_dirty(self):
      return self.dirty

def factory(page_id, file_id, data):
   return Page(page_id, file_id, data)