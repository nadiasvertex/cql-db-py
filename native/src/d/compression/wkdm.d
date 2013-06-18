/*
  Date: June 15, 2013
  Authors: Christopher Nelson

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
*/

import std.stdio;
import std.datetime;

/* At the moment we have dependencies on the page size.  That should
 * be changed to work for any power-of-two size that's at least 16
 * words, or something like that
 */

immutable PAGE_SIZE_IN_WORDS = 1024;
immutable PAGE_SIZE_IN_BYTES = 4096;
immutable DICTIONARY_SIZE = 16;

/*
 * Constants defining the basic layout of stuff in a page
 */
immutable HEADER_SIZE_IN_WORDS = 4;
immutable TAGS_AREA_OFFSET = 4;
immutable TAGS_AREA_SIZE = 64;

immutable BITS_PER_WORD = 32;
immutable BYTES_PER_WORD = 4;
immutable NUM_LOW_BITS = 10;
immutable LOW_BITS_MASK = 0x3FF;
immutable ALL_ONES_MASK = 0xFFFFFFFF;

immutable TWO_BITS_PACKING_MASK = 0x3;
immutable FOUR_BITS_PACKING_MASK = 0xF;
immutable TEN_LOW_BITS_MASK = 0x000003FF;
immutable TWENTY_TWO_HIGH_BITS_MASK = 0xFFFFFC00;

/* Tag values.  NOTE THAT CODE MAY DEPEND ON THE NUMBERS USED.
 * Check for conditionals doing arithmetic on these things
 * before changing them
 */

immutable ZERO_TAG = 0x0;
immutable PARTIAL_TAG = 0x1;
immutable MISS_TAG = 0x2;
immutable EXACT_TAG = 0x3;

immutable BITS_PER_BYTE = 8;

/*
 * These are the constants for the hash function lookup table.
 * Only zero maps to zero.  The rest of the table is the result
 * of appending 17 randomizations of the multiples of 4 from
 * 4 to 56.
 */

immutable HASH_LOOKUP_TABLE = [
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
					];

 /**
  * Pack some multiple of four words holding two-bit tags (in the low
  * two bits of each byte) into an integral number of words, i.e.,
  * one fourth as many.
  * NOTE: Pad the input out with zeroes to a multiple of four words!
  *
  * Params:
  *  source_buf = A list containing the source words to pack
  *  source_start = The offset into source_buf where we should start
  *  source_end = The offset to the end of the source buffer.
  *  dest_buf = An array of 32-bit ints.
  *  dest_start = The offset into the starting point of the dest buffer
  *
  * Returns: The index to the next available spot in dest_buf
  */
pure uint 
pack_2bits(uint[] source_buf, uint source_start, uint source_end,
	   uint[] dest_buf, uint dest_start) {

  auto src_next = source_start;
  auto dest_next = dest_start;

  while (src_next < source_end) {
    uint temp = source_buf[src_next];
    temp |= (source_buf[src_next + 1] << 2);
    temp |= (source_buf[src_next + 2] << 4);
    temp |= (source_buf[src_next + 3] << 6);

    temp |= (source_buf[src_next + 4] << 8);
    temp |= (source_buf[src_next + 5] << 10);
    temp |= (source_buf[src_next + 6] << 12);
    temp |= (source_buf[src_next + 7] << 14);

    temp |= (source_buf[src_next + 8] << 16);
    temp |= (source_buf[src_next + 9] << 18);
    temp |= (source_buf[src_next + 10] << 20);
    temp |= (source_buf[src_next + 11] << 22);

    temp |= (source_buf[src_next + 12] << 24);
    temp |= (source_buf[src_next + 13] << 26);
    temp |= (source_buf[src_next + 14] << 28);
    temp |= (source_buf[src_next + 15] << 30);

    src_next += 16;

    dest_buf[dest_next] = temp;
    dest_next += 1;
    }

   return dest_next;
}

/**
 *   Pack an even number of words holding 4-bit patterns in the low bits
 *   of each byte into half as many words.
 *   note: pad out the input with zeroes to an even number of words!
 *
 *   Params:
 *    source_buf = A list containing the source words to pack
 *    source_start = The offset into source_buf where we should start
 *    source_end = The offset to the end of the source buffer.
 *    dest_buf = An array of 32-bit ints.
 *    dest_start = The offset into the starting point of the dest buffer
 *  
 *  Returns: The index to the next available spot in dest_buf
 */
pure uint
pack_4bits(uint source_buf[], uint source_start, uint source_end, uint dest_buf[], uint dest_start) {
  uint src_next = source_start;
  uint dest_next = dest_start;

  while (src_next < source_end) {
    uint temp = source_buf[src_next];
    temp |= (source_buf[src_next + 1] << 4);
    temp |= (source_buf[src_next + 2] << 8);
    temp |= (source_buf[src_next + 3] << 12);
    temp |= (source_buf[src_next + 4] << 16);
    temp |= (source_buf[src_next + 5] << 20);
    temp |= (source_buf[src_next + 6] << 24);
    temp |= (source_buf[src_next + 7] << 28);

    dest_buf[dest_next] = temp;
    dest_next += 1;
    src_next += 8;
   }
   return dest_next;
}

/**
 *    Pack a sequence of three ten bit items into one word.
 *    note: pad out the input with zeroes to an even number of words!
 *
 *   Params:
 *    source_buf = A list containing the source words to pack
 *    source_start = The offset into source_buf where we should start
 *    source_end = The offset to the end of the source buffer.
 *    dest_buf = An array of 32-bit ints.
 *    dest_start = The offset into the starting point of the dest buffer
 *  
 *  Returns: The index to the next available spot in dest_buf
 */
pure uint 
pack_3_10bits(uint source_buf[], uint source_start, uint source_end, uint dest_buf[], uint dest_start) {
  uint src_next = source_start;
  uint dest_next = dest_start;

  while (src_next < source_end) {
    uint temp = source_buf[src_next];
    temp |= (source_buf[src_next + 1] << 10);
    temp |= (source_buf[src_next + 2] << 20);

    dest_buf[dest_next] = temp;
    dest_next += 1;
    src_next += 3;
  }
  return dest_next;
}

/**
 * Params:
 *  src_buf = An array of 32-bit words
 *  src_start = Where to start reading in src_buf
 *  dest_buf = An array of 32-bit words
 *  dest_start = Where to start writing in dest_buf
 *  num_input_words = The number of 32-bit words in src_buf
 *
 * Returns:
 *  The number of words written to dest_buf.
 */
pure uint
compress(in uint src_buf[], in uint src_start, uint dest_buf[], in uint dest_start, in uint num_input_words) {

  uint[DICTIONARY_SIZE] dictionary;
  dictionary[0 .. $] = 1;

  // arrays that hold output data in intermediate form during modeling
  // and whose contents are packed into the actual output after modeling

  uint[1024] temp_tags;      //tags for everything
  uint[1024] temp_qpos;      // queue positions for matches
  uint[4096] temp_low_bits; // low bits for partial matches

  uint next_tag = 0,
       next_qpos = 0,
       next_low_bits = 0;

  // boundary_tmp will be used for keeping track of what's where in
  // the compressed page during packing

  uint boundary_tmp = 0;

  // Fill pointers for filling intermediate arrays (of queue positions
  // and low bits) during encoding.
  // Full words go straight to the destination buffer area reserved
  // for them.  (Right after where the tags go.)

  uint next_input_word = src_start;
  immutable end_of_input = src_start + num_input_words;
  uint next_full_patt = dest_start + TAGS_AREA_OFFSET + (num_input_words / 16);

  while(next_input_word<end_of_input) {
    auto input_word = src_buf[next_input_word];
    
    // compute hash value, which is a byte offset into the dictionary,
    // and add it to the base address of the dictionary. Cast back and
    // forth to/from char * so no shifts are needed
    auto dict_location = HASH_LOOKUP_TABLE[(input_word >> 10) & 0xFF];
    auto dict_word = dictionary[dict_location];

    if (input_word == dict_word) {
      // exact match
      temp_tags[next_tag++] = EXACT_TAG;
      temp_qpos[next_qpos++] = dict_location;
    } else if (input_word == 0) {
      temp_tags[next_tag++]= ZERO_TAG;
    } else {
      auto input_high_bits = input_word >> NUM_LOW_BITS;
      if (input_high_bits == dict_word >> NUM_LOW_BITS) {
	auto input_low_bits = input_word & LOW_BITS_MASK;
	temp_tags[next_tag++] = PARTIAL_TAG;
	temp_qpos[next_qpos++] = dict_location;
	temp_low_bits[next_low_bits++] = input_low_bits;
	dictionary[dict_location] = input_word;
      }
      else {
	// miss
	temp_tags[next_tag++]= MISS_TAG;
	dest_buf[next_full_patt++] = input_word;
	dictionary[dict_location] = input_word;
      }

    }
    next_input_word += 1;
  } // end of modeling loop

  // Record (into the header) where we stopped writing full words,
  // which is where we will pack the queue positions.  (Recall
  // that we wrote the full words directly into the dest buffer
  // during modeling.
  dest_buf[1] = next_full_patt;

  // Pack the tags into the tags area, between the page header
  // and the full words area.  We don't pad for the packer
  // because we assume that the page size is a multiple of 16.
  boundary_tmp = pack_2bits(temp_tags, 0, next_tag, dest_buf, HEADER_SIZE_IN_WORDS);

  // Pack the queue positions into the area just after
  // the full words.  We have to round up the source
  // region to a multiple of two words.
    
  auto num_bytes_to_pack = next_qpos;
  auto padding = 8 - (num_bytes_to_pack % 8);

  foreach(i; 0 .. padding) {
    temp_qpos[next_qpos++] = 0;
  }

  boundary_tmp = pack_4bits(temp_qpos, 0, next_qpos, dest_buf, boundary_tmp);

  // Record (into the header) where we stopped packing queue positions,
  // which is where we will start packing low bits.
  dest_buf[2] = boundary_tmp;

  // Pack the low bit patterns into the area just after
  // the queue positions.  We have to round up the source
  // region to a multiple of three words.

  auto num_tenbits_to_pack = next_low_bits;
  padding = 3 - (num_tenbits_to_pack % 3);

  // Pad out the array with zeros to avoid corrupting real packed
  // values.
  foreach(i; 0 .. padding) {
    temp_low_bits[next_low_bits++] = 0;
  }

  boundary_tmp = pack_3_10bits(temp_low_bits, 0, next_low_bits, dest_buf, boundary_tmp);
  dest_buf[3] = boundary_tmp;

  return boundary_tmp - dest_start;
}

void main() {
  writeln("wkdm test harness");

  uint[1024] src_buf;
  uint[1024] dst_buf;

  uint j = 0;
  foreach(i; 0 .. 1024) {
    src_buf[i] = j;
    j+=1;
    if (j>9) {
      j=0;
    }
  }

  Duration times;
  uint rounds = 1<<18;
  foreach(i; 0 .. rounds) {
    auto start = Clock.currTime();
    uint compressed_size = compress(src_buf, 0, dst_buf, 0, 1024);
    auto end = Clock.currTime();
    
    times += end-start;
  }

  auto total_size = 4096 * rounds;
  auto rate = total_size / times.seconds();
  writeln("processed: ", total_size/1024/1024, "MB");
  writeln("speed: ", rate/1024/1024, "MBs");

}
