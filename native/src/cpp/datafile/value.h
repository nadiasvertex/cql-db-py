/*
 * value.h
 *
 *  Created on: Jul 7, 2013
 *      Author: Christopher Nelson
 */

#ifndef CQL_DATAFILE_VALUE_H_
#define CQL_DATAFILE_VALUE_H_

#include <fstream>

namespace cql {
namespace datafile {

template<typename T>
class translator {
public:
	virtual void put(std::fstream& stream, const T& value) {
		stream.write((const char*)&value, sizeof(value));
	}

	virtual void get(std::fstream& stream, T& value) {
		stream.read((char*)&value, sizeof(value));
	}
};

template<typename T>
class value {
	std::fstream value_file;
public:
	value(const std::string& file_name) {
		value_file.open(file_name,
				std::ios::in | std::ios::out | std::ios::binary);
		if (!value_file) {
			// The file doesn't exist. Open the file in out-only mode to create
			// it, then close the file and re-open in/out mode.
			value_file.clear();
			value_file.open(file_name, std::ios::out | std::ios::binary);
			value_file.close();
			value_file.clear();
			value_file.open(file_name,
					std::ios::in | std::ios::out | std::ios::binary);
		}
	}

	bool is_open() {
		return value_file.is_open();
	}

	uint64_t append(const T& value, translator<T> &xlator) {
		value_file.clear();
		value_file.seekp(0, std::ios_base::end);
		uint64_t offset = value_file.tellp();
		xlator.put(value_file, value);
		return offset;
	}

	uint64_t append(const T& value) {
		translator<T> t;
		return append(value, t);
	}

	void get(const uint64_t pos, T& value, translator<T> &xlator) {
		value_file.clear();
		value_file.seekg(pos, std::ios_base::beg);
		xlator.get(value_file, value);
	}

	void get(const uint64_t pos, T& value) {
		translator<T> xlator;
		get(pos, value, xlator);
	}

};

}
}

#endif /* VALUE_H_ */
