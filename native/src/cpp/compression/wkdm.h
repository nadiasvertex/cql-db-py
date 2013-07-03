/*
 * File:   wkdm.h
 * Author: cnelson
 *
 * Created on June 25, 2013, 9:56 AM
 */

#ifndef CQL_WKDM_H
#define CQL_WKDM_H

class wkdm {
public:
    typedef unsigned long data_element;

    /* A structure to store each element of the dictionary. */
    typedef data_element dictionary_element;

protected:

    /*
     * Pack some multiple of four words holding two-bit tags (in the low
     * two bits of each byte) into an integral number of words, i.e.,
     * one fourth as many.
     * NOTE: Pad the input out with zeroes to a multiple of four words!
     */
    data_element * pack_2bits(data_element * source_buf,
            data_element * source_end,
            data_element * dest_buf);

    /*
     * Pack an even number of words holding 4-bit patterns in the low bits
     * of each byte into half as many words.
     * note: pad out the input with zeroes to an even number of words!
     */
    data_element * pack_4bits(data_element * source_buf,
            data_element * source_end,
            data_element * dest_buf);

    /*
     * Pack a sequence of three ten bit items into one word.
     * note: pad out the input with zeroes to an even number of words!
     */
    data_element * pack_3_tenbits(data_element * source_buf,
            data_element * source_end,
            data_element * dest_buf);

    /*  unpack_2bits takes any number of words containing 16 two-bit values
     *  and unpacks them into four times as many words containg those
     *  two bit values as bytes (with the low two bits of each byte holding
     *  the actual value.
     */
    data_element*
    unpack_2bits(data_element *input_buf,
            data_element *input_end,
            data_element *output_buf);

    /* unpack four bits consumes any number of words (between input_buf
     * and input_end) holding 8 4-bit values per word, and unpacks them
     * into twice as many words, with each value in a separate byte.
     * (The four-bit values occupy the low halves of the bytes in the
     * result).
     */
    data_element*
    unpack_4bits(data_element *input_buf,
            data_element *input_end,
            data_element *output_buf);

    /* unpack_3_tenbits unpacks three 10-bit items from (the low 30 bits of)
     * a 32-bit word
     */
    data_element*
    unpack_3_tenbits(data_element *input_buf,
            data_element *input_end,
            data_element *output_buf);

public:
    unsigned int
    compress(data_element* src_buf,
            data_element* dest_buf,
            unsigned int num_input_words);

    unsigned long int
    decompress(data_element* src_buf,
            data_element* dest_buf,
            unsigned int words);
};
#endif   /* CQL_WKDM_H */

