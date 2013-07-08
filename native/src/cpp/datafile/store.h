/*
 * store.h
 *
 *  Created on: Jul 8, 2013
 *      Author: christopher
 */

#ifndef CQL_DATAFILE_STORE_H_
#define CQL_DATAFILE_STORE_H_

#include <list>
#include <map>
#include <string>

#include "index.h"
#include "value.h"

namespace cql {
namespace datafile {

template<typename T>
class store {

	struct column_segment {
		uint64_t start, end;
	};

	typedef std::map<T, std::list<column_segment> > write_store_t;

	value<T> values;
	index value_index;

	value<uint64_t> columns;
	index column_index;

	write_store_t write_store;

public:
	store(const std::string& filename) :
			value_index(filename + ".idx"), values(filename + ".dat"), column_index(
					filename + ".column.idx"), columns(filename + ".column.dat") {

	}

	bool is_open() {
		return values.is_open() && value_index.is_open() && columns.is_open()
				&& column_index.is_open();
	}

	void put(const uint64_t column, const T& v) {
		auto it = write_store.find(v);
		if (it == write_store.end()) {
			auto result = write_store.insert(std::make_pair(v, std::list<column_segment>()));
			auto& l = result.first->second;
			l.emplace_back(column_segment { column, column });
			return;
		}

		// Traverse the list of segments and merge this column into the list
		for (auto it_s = it->second.begin(); it_s != it->second.end();
				it_s++) {
			auto& segment = *it_s;
			if (column == segment.start - 1) {
				segment.start--;
				return;
			}

			if (column == segment.end + 1) {
				segment.end++;
				return;
			}

			if (segment.start > column) {
				it->second.emplace(it_s, column_segment { column, column });
				return;
			}
		}

		it->second.emplace_back(column_segment { column, column });
	}

};

}
}

#endif /* CQL_DATAFILE_STORE_H_ */
