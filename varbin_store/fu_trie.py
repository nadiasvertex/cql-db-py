__author__ = 'Christopher Nelson'

from varbin_store.bitstring import ConstBitArray, BitArray

class FuTrie(object):
   left = BitArray(bin="10")
   right = BitArray(bin="01")
   both = BitArray(bin="11")
   leaf = BitArray(bin="00")

   def __init__(self):
      self.levels = []
      self.keys = []

   def append(self, key):
      self.keys.append(key)

   def encode(self):
      def _append_to_level(level, node):
         if level>=len(self.levels):
            self.levels.append(node.copy())
         else:
            self.levels[level].append(node)

      for kv in sorted(self.keys):
         #print(kv)
         k = ConstBitArray(bytes=kv)
         append_only = False
         for i, b in enumerate(k):
            new_node = self.right if b else self.left
            if append_only or i>=len(self.levels):
               _append_to_level(i, new_node)
               continue

            current = self.levels[i]
            old_node = current[-2:]

            #print "%d?:" %i, old_node.bin, new_node.bin
            if old_node == self.both:
               continue

            if new_node != old_node:
               current[-2:] = self.both
               append_only = True
               #print "%d:" %i, self.levels[i].bin
         _append_to_level(i+1, self.leaf)

   def write(self):
      output = BitArray()
      #print("levels:", len(self.levels))
      for s in self.levels:
         output.append(s)
      #print(len(output), output)

   def dump(self):
      for i,l in enumerate(self.levels):
         print "%d:" %i, l.bin

   def find(self, key):
      k = ConstBitArray(bytes=key)
      skip = 0
      for i,b in enumerate(k):
         l = self.levels[i]
         print "%d:"%i,skip, l.bin,

         # Read skip bits to find out how many bits we will
         # need to skip on the next row just to stay in the
         # same tree branch.
         new_skip = skip
         if skip>0:
            for bi in range(0, skip, 2):
               count_node = l[bi:bi+2]
               if count_node == self.both:
                  new_skip+=2
               elif count_node == self.leaf:
                  new_skip-=2

         node = l[skip:skip+2]
         print node.bin, "right" if b else "left"

         # For the next row, we need to skip at least 'new_skip' bits.
         skip = new_skip

         # If we have reached a leaf, good!
         if node == self.leaf:
            return True

         # If the path bit is set, then we're good. Decide
         # what our index level is.
         if node[1 if b else 0]:
            skip+=2 if b else 0
         else:
            return False

      return True

if __name__ == "__main__":
   f = FuTrie()
   f.append("\x01")
   f.append("\x02")
   f.append("\x03")
   f.append("\x04")
   f.append("\x10")
   f.encode()
   f.dump()
   print "pass" if f.find("\x04") == True else "fail"
   print  "pass" if f.find("\x10") == True else "fail"
   print  "pass" if f.find("\x05") == False else "fail"
   print "pass" if f.find("\x03") == True else "fail"
   print  "pass" if f.find("\x02") == True else "fail"
   print  "pass" if f.find("\x01") == True else "fail"

   f= FuTrie()
   f.append("apple")
   f.append("orange")
   f.append("cat")
   f.append("cap")
   f.append("calf")
   f.encode()
   f.dump()
   print "pass" if f.find("orange") == True else "fail"


