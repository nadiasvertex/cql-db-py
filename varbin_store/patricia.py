'''
Created on May 31, 2013

@author: Christopher Nelson

This module provides for an on-disk representation of a patricia trie. The
store can be efficiently updated, and is designed to be useful for any
variable length binary data, not just text strings.

The basic design principle involves storing the keys in a value-store external
to the index. Each entry in the index contains two fields indicating the start
and length data in a value store for the key. For example, if the value
store contains 'happy' at offset 0, then the entry for 'happy' would look like
this:

entry 0: (0,5,LEAF)

If we then add the word 'happens' into the value store at offset 5, and insert
its key into the index we would adjust the first entry to:

entry 0: (0,4,[1,2])

and then add new entries:

entry 1: (5,1,LEAF)
entry 2: (9,3,LEAF)

If we next enter the word 'haphazard' at offset 12, we will again need to emend
the zeroth entry:

entry 0: (0,3,[3,4])

Create two new entries:

entry 3: (4,1,[1,2])
entry 4: (15,6,LEAF)

Finally, we may want to enter the word 'manna' at offset 21. This is done by
inserting a 'next sibling' pointer into the zeroth entry. Siblings are always
sorted lexicographically:

entry 0: (0,4,[1,2],->5)
entry 5: (21,5,LEAF)

If 'manna' had instead been 'apple' we would have adjusted the root pointer for
the store to point to '5', and entry 5 would have pointed to entry 0.

Deleting an entry in the store is done by changing the leaf marker to a
tombstone marker.

As an additional optimization, keys can be entered into the value store using
only the fragments that are not already stored. We can know that by simply
searching the trie for the key and appending the unmatched portion.

The store can be paged trivially by creating two kinds of entry pointers:
on-page entries, and off-page entries. An entry-list might be kept per-page. My
current intention is simply to cache frequently used entries in memory instead
of trying to manage pages.

The trie store may tend to become fragmented over time due to the need to move
entries around as their contents change. In addition, the order of insertions
may cause items to be allocated in a less-than optimal fashion. Consequently,
it may be useful to occasionally re-write the store like this:

1. Starting at the root, write all sibling entries in lexicographical order.
2. For each node, write all of its immediate children.
3. For each of the children, write their sibling lists in lexicographical order.
4. Repeat at step 2 until you have exhausted all children.

This organization makes it more likely that an element with a high probability
of being used next (a sibling or immediate child) will be close by in memory.
'''

from util import varint

class Entry(object):
   __slots__ = ["size", "key_frag_start", "key_frag_length", "children", "next"]
   def __init__(self, key_frag_start, key_frag_length, next=None):
      self.size = None
      self.children = []
      self.next = next
      self.key_frag_start = key_frag_start
      self.key_frag_length = key_frag_length

   def load(self, f):
      entry_start = f.tell()
      self.key_frag_start = varint.decode_stream(f)
      self.key_frag_length = varint.decode_stream(f)
      child_count = varint.decode_stream(f)
      for i in range(0, child_count):
         pass

