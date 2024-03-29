'''
Created on Jun 15, 2013

@author: Christopher Nelson

  Implements the WKdm compression algorithm:

  direct-mapped partial matching compressor with simple 22/10 split

  Compresses buffers using a dictionary based match and partial match
   (high bits only or full match) scheme.

   Paul Wilson -- wilson@cs.utexas.edu
   Scott F. Kaplan -- sfkaplan@cs.utexas.edu
   September 1997

 compressed output format, in memory order
  1. a four-word HEADER containing four one-word values:
      i.   a one-word code saying what algorithm compressed the data
      ii.  an integer WORD offset into the page saying
           where the queue position area starts
      iii. an integer WORD offset into the page saying where
           the low-bits area starts
      iv.  an integer WORD offset into the page saying where the
           low-bits area ends

   2. a 64-word TAGS AREA holding one two-bit tag for each word in
      the original (1024-word) page, packed 16 per word
    3. a variable-sized FULL WORDS AREA (always word aligned and an
      integral number of words) holding full-word patterns that
      were not in the dictionary when encoded (i.e., dictionary misses)

   4. a variable-sized QUEUE POSITIONS AREA (always word aligned and
      an integral number of words) holding four-bit queue positions,
      packed eight per word.

   5. a variable-sized LOW BITS AREA (always word aligned and an
      integral number of words) holding ten-bit low-bit patterns
      (from partial matches), packed three per word.
'''

import math

PAGE_COMPRESS_WORDS_PER_PAGE = 1024
DICTIONARY_SIZE = 16

# values defining the basic layout of stuff in a page

HEADER_SIZE_IN_WORDS = 4
TAGS_AREA_OFFSET = 4
TAGS_AREA_SIZE = 64

BITS_PER_WORD = 32
BYTES_PER_WORD = 4
NUM_LOW_BITS = 10
LOW_BITS_MASK = 0x3FF
ALL_ONES_MASK = 0xFFFFFFFF

TWO_BITS_PACKING_MASK = 0b11
FOUR_BITS_PACKING_MASK = 0b1111
TEN_LOW_BITS_MASK = 0x000003FF
TWENTY_TWO_HIGH_BITS_MASK = 0xFFFFFC00

# Tag values.  NOTE THAT CODE MAY DEPEND ON THE NUMBERS USED.
# Check for conditionals doing arithmetic on these things
# before changing them

ZERO_TAG = 0x0
PARTIAL_TAG = 0x1
MISS_TAG = 0x2
EXACT_TAG = 0x3

BITS_PER_BYTE = 8

# these are the constants for the hash function lookup table.
# Only zero maps to zero.  The rest of the table is the result
# of appending 17 randomizations of the multiples of 4 from
# 4 to 56.

HASH_LOOKUP_TABLE_CONTENTS = [
   0, 52, 8, 56, 16, 12, 28, 20, 4, 36, 48, 24, 44, 40, 32, 60,
   8, 12, 28, 20, 4, 60, 16, 36, 24, 48, 44, 32, 52, 56, 40, 12,
   8, 48, 16, 52, 60, 28, 56, 32, 20, 24, 36, 40, 44, 4, 8, 40,
  60, 32, 20, 44, 4, 36, 52, 24, 16, 56, 48, 12, 28, 16, 8, 40,
  36, 28, 32, 12, 4, 44, 52, 20, 24, 48, 60, 56, 40, 48, 8, 32,
  28, 36, 4, 44, 20, 56, 60, 24, 52, 16, 12, 12, 4, 48, 20, 8,
  52, 16, 60, 24, 36, 44, 28, 56, 40, 32, 36, 20, 24, 60, 40, 44,
  52, 16, 32, 4, 48, 8, 28, 56, 12, 28, 32, 40, 52, 36, 16, 20,
  48, 8, 4, 60, 24, 56, 44, 12, 8, 36, 24, 28, 16, 60, 20, 56,
  32, 40, 48, 12, 4, 44, 52, 44, 40, 12, 56, 8, 36, 24, 60, 28,
  48, 4, 32, 20, 16, 52, 60, 12, 24, 36, 8, 4, 16, 56, 48, 44,
  40, 52, 32, 20, 28, 32, 12, 36, 28, 24, 56, 40, 16, 52, 44, 4,
  20, 60, 8, 48, 48, 52, 12, 20, 32, 44, 36, 28, 4, 40, 24, 8,
  56, 60, 16, 36, 32, 8, 40, 4, 52, 24, 44, 20, 12, 28, 48, 56,
  16, 60, 4, 52, 60, 48, 20, 16, 56, 44, 24, 8, 40, 12, 32, 28,
  36, 24, 32, 12, 4, 20, 16, 60, 36, 28, 8, 52, 40, 48, 44, 56
]

#***********************************************************************
# *                   THE PACKING ROUTINES

def pack_2bits(source_buf, source_start, source_end, dest_buf, dest_start):
   """
   Pack some multiple of four words holding two-bit tags (in the low
   two bits of each byte) into an integral number of words, i.e.,
   one fourth as many.
   NOTE: Pad the input out with zeroes to a multiple of four words!

   :param source_buf: A list containing the source words to pack
   :param source_start: The offset into source_buf where we should start
   :param source_end: The offset to the end of the source buffer.
   :param dest_buf: An array of 32-bit ints.
   :param dest_start: The offset into the starting point of the dest buffer
   :returns: The index to the next available spot in dest_buf
   """
   src_next = source_start
   dest_next = dest_start

   while src_next < source_end:
      temp = source_buf[src_next]
      temp |= (source_buf[src_next + 1] << 2)
      temp |= (source_buf[src_next + 2] << 4)
      temp |= (source_buf[src_next + 3] << 6)

      temp |= (source_buf[src_next + 4] << 8)
      temp |= (source_buf[src_next + 5] << 10)
      temp |= (source_buf[src_next + 6] << 12)
      temp |= (source_buf[src_next + 7] << 14)

      temp |= (source_buf[src_next + 8] << 16)
      temp |= (source_buf[src_next + 9] << 18)
      temp |= (source_buf[src_next + 10] << 20)
      temp |= (source_buf[src_next + 11] << 22)

      temp |= (source_buf[src_next + 12] << 24)
      temp |= (source_buf[src_next + 13] << 26)
      temp |= (source_buf[src_next + 14] << 28)
      temp |= (source_buf[src_next + 15] << 30)

      src_next += 16

      dest_buf[dest_next] = temp
      dest_next += 1

   return dest_next


def pack_4bits(source_buf, source_start, source_end, dest_buf, dest_start):
   """
    Pack an even number of words holding 4-bit patterns in the low bits
    of each byte into half as many words.
    note: pad out the input with zeroes to an even number of words!

   :param source_buf: A list containing the source words to pack
   :param source_start: The offset into source_buf where we should start
   :param source_end: The offset to the end of the source buffer.
   :param dest_buf: An array of 32-bit ints.
   :param dest_start: The offset into the starting point of the dest buffer
   :returns: The index to the next available spot in dest_buf
   """
   src_next = source_start
   dest_next = dest_start

   while src_next < source_end:
      temp = source_buf[src_next]
      temp |= (source_buf[src_next + 1] << 4)
      temp |= (source_buf[src_next + 2] << 8)
      temp |= (source_buf[src_next + 3] << 12)
      temp |= (source_buf[src_next + 4] << 16)
      temp |= (source_buf[src_next + 5] << 20)
      temp |= (source_buf[src_next + 6] << 24)
      temp |= (source_buf[src_next + 7] << 28)

      dest_buf[dest_next] = temp
      dest_next += 1
      src_next += 8
   return dest_next;


def pack_3_10bits(source_buf, source_start, source_end, dest_buf, dest_start):
   """
    Pack a sequence of three ten bit items into one word.
    note: pad out the input with zeroes to an even number of words!

   :param source_buf: A list containing the source words to pack
   :param source_start: The offset into source_buf where we should start
   :param source_end: The offset to the end of the source buffer.
   :param dest_buf: An array of 32-bit ints.
   :param dest_start: The offset into the starting point of the dest buffer
   :returns: The index to the next available spot in dest_buf
   """
   src_next = source_start
   dest_next = dest_start

   while src_next < source_end:
      temp = source_buf[src_next];
      temp |= (source_buf[src_next + 1] << 10)
      temp |= (source_buf[src_next + 2] << 20)

      dest_buf[dest_next] = temp;
      dest_next += 1
      src_next += 3
   return dest_next;

#***************************************************************************
#          THE UNPACKING ROUTINES

def unpack_2bits(input_buf, input_start, input_end, output_buf, output_start):
   """
   Takes any number of words containing 16 two-bit values
   and unpacks them into four times as many words containing those
   two bit values as bytes (with the low two bits of each byte holding
   the actual value.
   """
   input_next = input_start
   packing_mask = TWO_BITS_PACKING_MASK

   # loop to repeatedly grab one input word and unpack it into
   # 16 output words.
   while input_next < input_end:
      temp = input_buf[input_next];

      output_buf.append(temp & packing_mask)
      output_buf.append((temp >> 2) & packing_mask)
      output_buf.append((temp >> 4) & packing_mask)
      output_buf.append((temp >> 6) & packing_mask)
      output_buf.append((temp >> 8) & packing_mask)
      output_buf.append((temp >> 10) & packing_mask)
      output_buf.append((temp >> 12) & packing_mask)
      output_buf.append((temp >> 14) & packing_mask)
      output_buf.append((temp >> 16) & packing_mask)
      output_buf.append((temp >> 18) & packing_mask)
      output_buf.append((temp >> 20) & packing_mask)
      output_buf.append((temp >> 22) & packing_mask)
      output_buf.append((temp >> 24) & packing_mask)
      output_buf.append((temp >> 26) & packing_mask)
      output_buf.append((temp >> 28) & packing_mask)
      output_buf.append((temp >> 30) & packing_mask)

      input_next += 1

def unpack_4bits(input_buf, input_start, input_end, output_buf, output_start):
   """
   Consumes any number of words (between input_start
   and input_end) holding 8 4-bit values per word, and unpacks them
   into twice as many words, with each value in a separate byte.
   (The four-bit values occupy the low halves of the bytes in the
   result).
   """
   input_next = input_start
   output_next = output_start
   packing_mask = FOUR_BITS_PACKING_MASK

   # loop to repeatedly grab one input word and unpack it into
   # 4 output words.
   while input_next < input_end:
      temp = input_buf[input_next];

      output_buf.append(temp & packing_mask)
      output_buf.append((temp >> 4) & packing_mask)
      output_buf.append((temp >> 8) & packing_mask)
      output_buf.append((temp >> 12) & packing_mask)
      output_buf.append((temp >> 16) & packing_mask)
      output_buf.append((temp >> 20) & packing_mask)
      output_buf.append((temp >> 24) & packing_mask)
      output_buf.append((temp >> 28) & packing_mask)

      output_next += 8
      input_next += 1

   return output_next;

def unpack_3_10bits(input_buf, input_start, input_end, output_buf, output_start):
   """
   Unpacks three 10-bit items from (the low 30 bits of)
   a 32-bit word
   """
   input_next = input_start
   output_next = output_start
   packing_mask = LOW_BITS_MASK

   # loop to repeatedly grab one input word and unpack it into
   # 4 output words.  This loop should probably be unrolled
   # a little---it's designed to be easy to do that.
   while input_next < input_end:
      temp = input_buf[input_next];

      output_buf.append(temp & packing_mask)
      output_buf.append((temp >> 10) & packing_mask)
      output_buf.append(temp >> 20)

      output_next += 3
      input_next += 1

   return output_next;

#***************************************************************************
# * THE COMPRESSOR

def compress (src_buf, src_start, dest_buf, dest_start, num_input_words):
   """
   :param src_buf: An array of 32-bit words
   :param src_start: Where to start reading in src_buf
   :param dest_buf: An array of 32-bit words
   :param dest_start: Where to start writing in dest_buf
   :param num_input_words: The number of 32-bit words in src_buf
   """
   dictionary = [1 for _ in range(0, DICTIONARY_SIZE)]
   hash_lookup_table = HASH_LOOKUP_TABLE_CONTENTS

   # lists that hold output data in intermediate form during modeling
   # and whose contents are packed into the actual output after modeling

   # sizes of these arrays should be increased if you want to compress
   # pages larger than 4KB

   temp_tags = []  # tags for everything
   temp_qpos = []  # queue positions for matches
   temp_low_bits = []  # low bits for partial matches

   # boundary_tmp will be used for keeping track of what's where in
   # the compressed page during packing

   # boundary_tmp = 0

   # Fill pointers for filling intermediate arrays (of queue positions
   # and low bits) during encoding.
   # Full words go straight to the destination buffer area reserved
   # for them.  (Right after where the tags go.)

   next_input_word = src_start
   end_of_input = src_start + num_input_words

   next_full_patt = dest_start + TAGS_AREA_OFFSET + (num_input_words / 16);

   while next_input_word < end_of_input:
      input_word = src_buf[next_input_word]

      # compute hash value, which is a byte offset into the dictionary,
      # and add it to the base address of the dictionary. Cast back and
      # forth to/from char * so no shifts are needed
      dict_location = hash_lookup_table[(input_word >> 10) & 0xFF]
      dict_word = dictionary[dict_location]

      if input_word == dict_word:
         # exact match
         temp_tags.append(EXACT_TAG)
         temp_qpos.append(dict_location)

      elif input_word == 0:
         # zero match
         temp_tags.append(ZERO_TAG)

      else:
         # partial match
         input_high_bits = input_word >> NUM_LOW_BITS
         if input_high_bits == dict_word >> NUM_LOW_BITS:
            input_low_bits = input_word & LOW_BITS_MASK
            temp_tags.append(PARTIAL_TAG)
            temp_qpos.append(dict_location)
            temp_low_bits.append(input_low_bits)
            dictionary[dict_location] = input_word

         else:
            # miss
            temp_tags.append(MISS_TAG)
            dest_buf[next_full_patt] = input_word
            next_full_patt += 1
            dictionary[dict_location] = input_word

      next_input_word += 1
   # end of modeling loop

   # print "modeled: tag words:", len(temp_tags) / 16, " qpos words:", len(temp_qpos) / 8,
   # print " low_bits_words:", len(temp_low_bits) / 3

   # Record (into the header) where we stopped writing full words,
   # which is where we will pack the queue positions.  (Recall
   # that we wrote the full words directly into the dest buffer
   # during modeling.
   dest_buf[1] = next_full_patt

   # Pack the tags into the tags area, between the page header
   # and the full words area.  We don't pad for the packer
   # because we assume that the page size is a multiple of 16.
   boundary_tmp = pack_2bits(temp_tags, 0, len(temp_tags), dest_buf, HEADER_SIZE_IN_WORDS)

   # Pack the queue positions into the area just after
   # the full words.  We have to round up the source
   # region to a multiple of two words.

   num_bytes_to_pack = len(temp_qpos)
   padding = 8 - (num_bytes_to_pack % 8)

   # Pad out the array with zeros to avoid corrupting real packed
   # values.
   for _ in range(0, padding):
      temp_qpos.append(0)

   boundary_tmp = pack_4bits(temp_qpos, 0, len(temp_qpos), dest_buf, boundary_tmp)

   # Record (into the header) where we stopped packing queue positions,
   # which is where we will start packing low bits.
   dest_buf[2] = boundary_tmp

   # Pack the low bit patterns into the area just after
   # the queue positions.  We have to round up the source
   # region to a multiple of three words.

   num_tenbits_to_pack = len(temp_low_bits)
   padding = 3 - (num_tenbits_to_pack % 3)

   # Pad out the array with zeros to avoid corrupting real packed
   # values.
   for _ in range(0, padding):
      temp_low_bits.append(0)

   boundary_tmp = pack_3_10bits(temp_low_bits, 0, len(temp_low_bits), dest_buf, boundary_tmp)

   dest_buf[3] = boundary_tmp
   return boundary_tmp - dest_start

def decompress (src_buf, src_start, dest_buf, dest_start, num_input_words):
   """
   :param src_buf: An array of 32-bit words
   :param src_start: Where to start reading in src_buf
   :param dest_buf: An array of 32-bit words
   :param dest_start: Where to start writing in dest_buf
   :param num_input_words: The number of 32-bit words in src_buf
   """
   dictionary = [1 for _ in range(0, DICTIONARY_SIZE)]
   hash_lookup_table = HASH_LOOKUP_TABLE_CONTENTS

   # lists that hold output data in intermediate form during modeling
   # and whose contents are packed into the actual output after modeling

   # sizes of these arrays should be increased if you want to compress
   # pages larger than 4KB

   temp_tags = []  # tags for everything
   temp_qpos = []  # queue positions for matches
   temp_low_bits = []  # low bits for partial matches

   unpack_2bits(src_buf, TAGS_AREA_OFFSET, TAGS_AREA_OFFSET + TAGS_AREA_SIZE, temp_tags, 0)
   unpack_4bits(src_buf, src_buf[1], src_buf[2], temp_qpos, 0)
   unpack_3_10bits(src_buf, src_buf[2], src_buf[3], temp_low_bits, 0)

   # print "unpacked: tag words:", len(temp_tags) / 16, " qpos words:", len(temp_qpos) / 8,
   # print " low_bits_words:", len(temp_low_bits) / 3

   next_qpos = 0
   next_low_bits = 0
   next_full_word = src_start + TAGS_AREA_SIZE

   for tag in temp_tags:
      if tag == ZERO_TAG:
         dest_buf.append(0)
      elif tag == EXACT_TAG:
         dict_location = temp_qpos[next_qpos]
         next_qpos += 1
         dest_buf.append(dictionary[dict_location])
      elif tag == PARTIAL_TAG:
         dict_location = temp_qpos[next_qpos]
         next_qpos += 1
         temp = dictionary[dict_location]

         # strip out low bits
         temp = ((temp >> NUM_LOW_BITS) << NUM_LOW_BITS)
         temp |= temp_low_bits[next_low_bits]
         next_low_bits += 1

         dictionary[dict_location] = temp
         dest_buf.append(temp)
      elif tag == MISS_TAG:
         missed_word = src_buf[next_full_word]
         next_full_word += 1
         dict_location = hash_lookup_table[(missed_word >> 10) & 0xFF]
         dictionary[dict_location] = missed_word
         dest_buf.append(missed_word)

   return len(dest_buf)

if __name__ == "__main__":
   import zlib
   import wkdm_native
   import time
   import struct

   src = []
   dst = []
   rtr = []

   for _ in range(0, 1024):
      dst.append(0)

   j = 0
   for _ in range(0, 1024):
      src.append(j)
      j += 1
      if j > 9:
         j = 0

   compressed_len = compress(src, 0, dst, 0, 1024)
   print compressed_len, int((float(compressed_len) / 1024.0) * 100.0), "%"
   print decompress(dst, 0, rtr, 0, compressed_len)
   # for i in range(0, 1024):
   #   print hex(rtr[i]), ":", hex(src[i])

   print src == rtr

   bsrc = struct.pack("1024L", *src)

   times = []; ztimes = []; ntimes = []
   limit = 1 << 18
   for i in range(0, limit):
      start = time.time()
      compressed_len = compress(src, 0, dst, 0, 1024)
      end = time.time()
      times.append(end - start)

      start = time.time()
      out = zlib.compress(bsrc, zlib.Z_BEST_SPEED)
      end = time.time()
      ztimes.append(end - start)

      start = time.time()
      out2 = wkdm_native.compress(src, 1024)
      end = time.time()
      ntimes.append(end - start)

   data_compressed = 4096 * limit
   total_time = sum(times)
   ztotal_time = sum(ztimes)
   ntotal_time = sum(ntimes)
   print "total:", data_compressed / 1024 / 1024, "MB"
   print "compression rate: ", (data_compressed / total_time) / 1024 / 1204, "MBps"
   print "zlib compression rate: ", (data_compressed / ztotal_time) / 1024 / 1204, "MBps"
   print "wkdm C compression rate: ", (data_compressed / ntotal_time) / 1024 / 1204, "MBps"

   times = []; ztimes = []
   for i in range(0, limit):
      rtr = []
      start = time.time()
      decompress(dst, 0, rtr, 0, compressed_len)
      end = time.time()
      times.append(end - start)

      start = time.time()
      zlib.decompress(out)
      end = time.time()
      ztimes.append(end - start)

   total_time = sum(times)
   ztotal_time = sum(ztimes)
   print "total:", data_compressed / 1024 / 1024, "MB"
   print "decompression rate: ", (data_compressed / total_time) / 1024 / 1204, "MBps"
   print "zlib decompression rate: ", (data_compressed / ztotal_time) / 1024 / 1204, "MBps"
