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
#include <deque>
#include <map>
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
	struct history_item {
		uint64_t access_count;
		uint64_t expire_time;
	};

	struct cache_item {
		V value;
		uint8_t level;
		history_item info;
	};

	typedef map<K, uint64_t> history_map_t;
	typedef map<K, cache_item> cache_map_t;
	typedef deque<K> queue_list_t;

	uint64_t current_time;
	uint64_t capacity;
	uint8_t life_time;
	uint8_t queue_count;

	history_map_t history;
	cache_map_t cache;
	queue_list_t *queues;

	//self.on_evict = on_evict
	//self.cache = {}
	//self.history = OrderedDict()
	//self.queues = [OrderedDict() for _ in range(0, queue_count)]
public:
	mq() :
			current_time(0), life_time(32), queue_count(8), capacity(1024) {
		queues = new queue_list_t[queue_count];
	}

	~mq() {
		delete [] queues;
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
		auto access_count = 1;
		auto history_el = history.find(key);
		if (history_el != history.end()) {
			access_count = history_el->second;
		}

		auto level = min(log2(access_count), queue_count - 1);
		queues[level].push_back(key);
		cache[key] = {.value = value,
		              .level = level,
		              .info = {
		            		   .access_count=access_count,
		                       .expire_time = current_time + life_time
		                      }
					  };
		//_check_for_demotion();
	}
};

} // end namespace

#endif /* CQL_MQ_H_ */
