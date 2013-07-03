/*
 * index.h
 *
 *  Created on: Jun 30, 2013
 *      Author: christopher
 */

#ifndef CQL_DATAFILE_INDEX_H_
#define CQL_DATAFILE_INDEX_H_

#include "cache/mq.h"
#include <fstream>

/**
 * This index is intended only to be a fixed size entry index into the file. The
 * index does not correspond to columns or any other data concept. It is merely
 * used to provide a disk-based array over which a binary search can be
 * performed on the variable-length data store.
 *
 * The index entries are 64-bits apiece. There is one index entry for each
 * datafile entry. The index entry contains the offset in bytes for the entry
 * from the beginning of the file.
 */

namespace cql {

struct entry_position {
private:
	uint64_t position;
	entry_position() {
	}
	entry_position(uint64_t _position) :
			position(_position) {
	}
	;
public:
	static entry_position from_uint64(uint64_t _position) {
		return entry_position(_position);
	}

	uint64_t get_file_position() const {
		return position;
	}
};

class index {
	std::fstream index_file;
	uint64_t entry_count;
	int page_size;
private:
	mq<uint64_t, uint64_t*> cache;

	uint64_t* _get_index_page_from_disk(uint64_t offset) {


		index_file.seekg(offset, ios_base::beg);
		char* buffer = new char[page_size];
		index_file.read(buffer, page_size);

		auto data = (uint64_t*)buffer;
		cache.put(offset, data);
		return data;
	}

	uint64_t* _get_index_page(uint64_t offset) {
		auto cache_entry = cache.get(offset);
		uint64_t* data = nullptr;
		if (!get < 0 > (cache_entry)) {
			data = _get_index_page_from_disk(offset);
		} else {
			data = get < 1 > (cache_entry);
		}

		return data;
	}

	uint64_t _get_page_from_offset(uint64_t offset) {
		return (offset / page_size) * page_size;
	}

public:
	index(const std::string& file_name) :
			page_size(8192)  {
		index_file.open(file_name,
				std::ios::in | std::ios::out | std::ios::binary);

		index_file.seekg(0, ios_base::end);
		auto end_of_file = index_file.tellg();
		entry_count = end_of_file / sizeof(uint64_t);

		cache.set_on_evict([this](uint64_t page, uint64_t *data) {
			index_file.seekp(page, ios_base::beg);
			index_file.write((char *)data, page);
			delete [] data;
		});
	}

	uint64_t get_entry_offset(const entry_position& entry) {
		auto pos = entry.get_file_position();
		auto page = _get_page_from_offset(pos);

		uint64_t *data = _get_index_page(page);

		auto page_offset = pos - page;
		return data[page_offset];
	}

	void put_entry_offset(const entry_position& entry, uint64_t offset) {
		auto pos = entry.get_file_position();
		auto page = _get_page_from_offset(pos);

		uint64_t *data = _get_index_page(page);
		auto page_offset = pos - page;

		if (page_offset > (page_size / sizeof(uint64_t))) {
			return;
		}
		//data[page_offset] = offset;
	}

};

}

#endif /* INDEX_H_ */
