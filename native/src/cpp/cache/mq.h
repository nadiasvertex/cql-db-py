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
#include <functional>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

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
	// Types ///////////////////////////////////////////////////////////////////
	typedef deque<K> queue_list_t;

	struct history_item {
		uint64_t access_count;
		uint64_t expire_time;
	};

	struct cache_item {
		V value;
		uint8_t level;
		history_item info;
	};

	typedef map<K, history_item> history_map_t;
	typedef map<K, cache_item> cache_map_t;

	// Cache Variables /////////////////////////////////////////////////////////
	uint64_t current_time;
	uint64_t capacity;
	uint8_t life_time;
	uint8_t queue_count;

	history_map_t history;
	queue_list_t history_queue;

	cache_map_t cache;
	vector<queue_list_t> queues;

	function<void(K, V)> on_evict;

	// Statistics //////////////////////////////////////////////////////////////
	uint64_t eviction_count;
	uint64_t hit_count;
	uint64_t miss_count;
	uint64_t history_hit_count;
	uint64_t history_miss_count;

private:
	/*
	 *
	 *  Checks the various queue levels to see if we need to demote
	 *  a page from one level to another, or to evict a page from the
	 *  cache. This is always called from put().
	 *
	 *  If the user has specified an eviction handler, the handler will be called
	 *  right before the item is evicted from the queue.
	 *
	 */
	void _check_for_demotion() {
		for (auto i = queue_count - 1; i >= 0; i--) {
			auto& q = queues[i];
			if (q.empty()) {
				continue;
			}
			auto key = q.front();
			auto& el = cache[key];
			auto q_size = q.size();

			// If we are over capacity, or if a block has expired, move it to the
			// next level down.
			if (q_size > capacity || (el.info.expire_time < current_time)) {

				q.pop_front();
				auto level_down = i - 1;

				// If we are not at the very bottom, then just move it down a level
				if (level_down >= 0) {
					auto& nq = queues[level_down];
					nq.push_back(key);
				} else {
					// Otherwise we must evict the value. Inform the user.
					if (on_evict != nullptr) {
						on_evict(key, el.value);
						eviction_count++;
					}

					// Save the access count for this block. That way, if we
					// load it again before we run out of history space, we
					// can automatically promote it into the right level.
					history[key] = el.info;
					history_queue.push_back(key);
					cache.erase(key);
					// If we are over-capacity then remove the oldest entry.
					if (history.size() > capacity * 2) {
						history.erase(history_queue.front());
						history_queue.pop_front();
					}
				}
			}
		}
	}

public:
	mq(uint64_t _capacity, uint8_t _life_time, uint8_t _queue_count,
			decltype(on_evict) _on_evict) :
			current_time(0), life_time(_life_time), queue_count(_queue_count), capacity(
					_capacity / _queue_count), on_evict(_on_evict), eviction_count(
					0), hit_count(0), miss_count(0), history_hit_count(0), history_miss_count(
					0), queues(queue_count)

	{

	}

	mq(uint64_t _capacity) :
			mq(_capacity, 32, 8, nullptr) {
	}

	mq(uint64_t _capacity, decltype(on_evict) _on_evict) :
			mq(_capacity, 32, 8, _on_evict) {
	}

	mq() :
			mq(1024, 32, 8, nullptr) {
	}

	~mq() {
		//delete[] queues;
	}

	/**
	 * Sets the eviction handler.
	 *
	 *
	 */
	void set_on_evict(decltype(on_evict) _on_evict) {
		on_evict = _on_evict;
	}

	/**
	 *
	 * :synopsis: Tries to return the value associated with 'key'. If the
	 * key is not found, a default value may be specified. If
	 * that value is not None, a new key will be inserted into
	 * the cache with that value. Otherwise 'None' will be returned.
	 *
	 * :param key: The key for the value to fetch.
	 * :returns: The value or None on a cache miss.
	 *
	 */
	tuple<bool, V> get(const K& key) {
		current_time += 1;
		auto it = cache.find(key);
		if (it == cache.end()) {
			miss_count++;
			return make_tuple(false, V());
		}

		auto entry = it->second;

		entry.info.expire_time = current_time + life_time;
		entry.info.access_count += 1;

		uint8_t requested_level = min(
				static_cast<int>(log2(entry.info.access_count)),
				queue_count - 1);
		if (requested_level > entry.level) {
			auto& cq = queues[entry.level];
			remove(cq.begin(), cq.end(), key);

			entry.level = requested_level;
			auto& rq = queues[entry.level];
			rq.push_back(key);
		}

		hit_count++;
		return make_tuple(true, entry.value);
	}

	/*
	 * :synopsis: Stores 'value' into the cache using 'key'. Uses the 'MQ'
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
			access_count = history_el->second.access_count;
			history.erase(key);
			remove(history_queue.begin(), history_queue.end(), key);
			history_hit_count++;
		} else {
			history_miss_count++;
		}

		uint8_t level = min(static_cast<int>(log2(access_count)),
				queue_count - 1);

		auto& q = queues[level];
		q.push_back(key);
		cache[key] = {
			.value = value,
			.level = level,
			.info = {
				.access_count = access_count,
				.expire_time = current_time + life_time
			}
		};

		_check_for_demotion();
	}

	/**
	 * :returns: The number of cache hits.
	 */
	uint64_t get_hit_count() const {
		return hit_count;
	}
	/**
	 * :returns: The number of cache misses.
	 */
	uint64_t get_miss_count() const {
		return miss_count;
	}
	/**
	 * :returns: The number of cache evictions.
	 */
	uint64_t get_eviction_count() const {
		return eviction_count;
	}
	/**
	 * :returns: The number of history cache hits.
	 */
	uint64_t get_history_hit_count() const {
		return history_hit_count;
	}

	/**
	 * :returns: The number of history cache misses.
	 */
	uint64_t get_history_miss_count() const {
		return history_miss_count;
	}
};

} // end namespace

#endif /* CQL_MQ_H_ */
