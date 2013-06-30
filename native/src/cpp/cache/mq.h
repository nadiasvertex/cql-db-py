/*
 * mq.h
 *
 *  Created on: Jun 28, 2013
 *      Author: Christopher Nelson
 */

#ifndef CQL_MQ_H_
#define CQL_MQ_H_

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <list>
#include <functional>
#include <map>
#include <tuple>
#include <utility>

namespace cql {

using namespace std;

/**
 *
 * Stores key/value references using a particular caching policy.
 *
 * The policy used is basically covered in the MQ algorithm description at
 * http://opera.ucsd.edu/paper/TPDS-final.pdf
 *
 * In short, there are a configurable number of queues. Each queue is an LRU. In
 * addition to tracking the LRU, we track how many accesses an item has had,
 * ever. Items are migrated from higher queues down to lower queues based on
 * capacity or expiration. When an item reaches the bottom element of queue 0,
 * the item is evicted from the cache.
 *
 * As items are accessed, they move up levels in the queue. The current level
 * function is a simple log2 of the number of accesses. So, for example, on the
 * 8th access, an item will move from the second level to the third level.
 */
template<typename K, typename V>
class mq {
private:
	typedef list<K> queue_list_t;

	struct history_item {
		uint64_t access_count;
		uint64_t expire_time;
	};

	struct cache_item {
		V value;
		uint8_t level;
		history_item info;
		typename queue_list_t::iterator q_el;
	};

	typedef map<K, uint64_t> history_map_t;
	typedef map<K, cache_item> cache_map_t;

	uint64_t current_time;
	uint64_t capacity;
	uint8_t life_time;
	uint8_t queue_count;

	history_map_t history;
	cache_map_t cache;
	queue_list_t *queues;

	function<void(K, V)> on_evict;

public:
	mq() :
			current_time(0), life_time(32), queue_count(8), capacity(1024) {
		queues = new queue_list_t[queue_count];
	}

	~mq() {
		delete[] queues;
	}

	/**
	 * Sets the eviction handler.
	 */
	void set_on_evict(decltype(on_evict) _on_evict) {
		on_evict = _on_evict;
	}

	/**
	 *
	 :synopsis: Tries to return the value associated with 'key'. If the
	 key is not found, a default value may be specified. If
	 that value is not None, a new key will be inserted into
	 the cache with that value. Otherwise 'None' will be returned.

	 :param key: The key for the value to fetch.
	 :returns: The value or None on a cache miss.
	 *
	 */
	tuple<bool, V> get(const K& key) {
		current_time += 1;
		auto it = cache.find(key);
		if (it == cache.end()) {
			return make_tuple(false, K());
		}

		auto entry = it->second;

		entry.info.expire_time = current_time + life_time;
		entry.info.access_count += 1;

		uint8_t requested_level = min(static_cast<int>(log2(entry.info.access_count)),
				queue_count - 1);
		if (requested_level > entry.level) {
			queues[entry.level].erase(entry.q_el);
			entry.level = requested_level;
		}

		return make_tuple(true, entry.value);
	}

	/*
	 *    Stores 'value' into the cache using 'key'. Uses the 'MQ'
	 * algorithm to maintain cache size.
	 *
	 * :param key: The key to associate with 'value'.
	 * :param value: The value to store.
	 *
	 * If the block is in our history (not our cache), then we will remember how
	 * many accesses it had. We use this to promote a frequently accessed block
	 * into a higher level than a brand new block.
	 *
	 */
	void put(const K &key, const V &value) {
		uint64_t access_count = 1;
		auto history_el = history.find(key);
		if (history_el != history.end()) {
			access_count = history_el->second;
		}

		uint8_t level = min(static_cast<int>(log2(access_count)),
				queue_count - 1);

		auto it = queues[level].insert(queues[level].end(), key);
	cache[key] = {
		.value = value,
		.level = level,
		.q_el = it,
		.info = {
			.access_count = access_count,
			.expire_time = current_time + life_time
		}
	};
	//_check_for_demotion();
}
};

} // end namespace

#endif /* CQL_MQ_H_ */
