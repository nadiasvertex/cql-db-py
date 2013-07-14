/*
 * store.h
 *
 *  Created on: Jul 8, 2013
 *      Author: christopher
 */

#ifndef CQL_DATAFILE_STORE_H_
#define CQL_DATAFILE_STORE_H_

#include <algorithm>
#include <functional>
#include <list>
#include <map>
#include <string>

#include "index.h"
#include "value.h"

namespace cql {
namespace datafile {

template<typename T>
class store {
public:
	typedef std::function<bool(const T&)> predicate_t;
	typedef std::function<void(const T&, T&, uint64_t)> aggregate_t;

	struct column_segment {
		uint64_t start, end;
	};

private:
	typedef std::map<T, std::list<column_segment> > write_store_t;
	typedef std::map<uint64_t, T> fast_column_lookup_t;

	value<T> values;
	index value_index;

	value<uint64_t> columns;
	index column_index;

	write_store_t write_store;
	fast_column_lookup_t column_lookup;

	bool use_fast_column_lookup;

public:
	store(const std::string& filename) :
			value_index(filename + ".idx"), values(filename + ".dat"), column_index(
					filename + ".column.idx"), columns(
					filename + ".column.dat"), use_fast_column_lookup(true) {

	}

	bool is_open() {
		return values.is_open() && value_index.is_open() && columns.is_open()
				&& column_index.is_open();
	}

	uint64_t count() {
		return write_store.size();
	}

	void set_use_fast_column_lookup(bool _use_fast_column_lookup) {
		if (_use_fast_column_lookup == false) {
			column_lookup.clear();
		}

		use_fast_column_lookup = _use_fast_column_lookup;
	}

	void put(const uint64_t column, const T& v) {
		if (use_fast_column_lookup) {
			column_lookup[column] = v;
		}

		auto it = write_store.find(v);
		if (it == write_store.end()) {
			auto result = write_store.insert(
					std::make_pair(v, std::list<column_segment>()));
			auto& l = result.first->second;
			l.emplace_back(column_segment { column, column });
			return;
		}

		// Traverse the list of segments and merge this column into the list
		for (auto it_s = it->second.begin(); it_s != it->second.end(); it_s++) {
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

	std::tuple<bool, T> get(const uint64_t column) {
		// Use fast lookup for this column.
		if (use_fast_column_lookup) {
			auto it = column_lookup.find(column);
			if (it == column_lookup.end()) {
				return std::make_tuple(false, T { });
			}
			return std::make_tuple(true, it->second);
		}

		// Slow walk the whole map and find the column.
		for (auto it : write_store) {
			for (auto s_it : it.second) {
				// Early out of the segment search.
				if (s_it.start > column) {
					break;
				}

				if (column >= s_it.start && column <= s_it.end) {
					return std::make_tuple(true, it.first);
				}
			}
		}

		return std::make_tuple(false, T { });
	}

	std::vector<column_segment> get(predicate_t pred) {
		std::vector<column_segment> columns;

		for (auto it : write_store) {
			if (pred(it.first)) {
				for (auto& s_it : it.second) {
					columns.push_back(s_it);
				}
			}
		}

		return columns;
	}

	T aggregate(aggregate_t aggr) {
		T agg { };

		for (auto it : write_store) {
			uint64_t count = 0;
			for(auto s_it : it.second) {
				count += (s_it.end - s_it.start) + 1;
			}
			aggr(it.first, agg, count);
		}

		return agg;
	}
};

}
}

#endif /* CQL_DATAFILE_STORE_H_ */
