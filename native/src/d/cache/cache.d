import std.algorithm;
import std.datetime;
import std.math;

template Cache(K,V) {
	struct CacheItem {
		uint level;
		uint accesses;
		ulong expires;
	}

	alias void function(K, V) on_eviction_type;
	alias K[] key_list;
	
	eviction_type on_evict;

	ulong current_time;

	uint capacity;
	ubyte queue_count;
	ubyte life_time;

	/** Maintains an ordered list of history items. */
	key_list history_order;
	K[ulong] history_accesses;
	
	/** The list of lists that maintains statistics. */
	K[][] queues;

	/** The cache, where we store the items. */
	K[CacheItem] cache;

	this(on_eviction_type _on_evict, 
		uint _capacity=1024, ubyte _queue_count=8, 
		ubyte _life_time=32) {

		on_evict = _on_evict;
		capacity = _capacity;
		queue_count = _queue_count;
		life_time = _life_time;

		foreach(i; 0 .. queue_count) {
			queues ~= new K[];
		}
	}

	V get(in K key, lazy V default_value) {
		current_time++;
		auto cache_item = cache.get(key, null);
		if (cache_item == null) {
			if (default_value != null) {
				put(key, default_value);
			}
			return default_value;
		}

		cache_item.expires = current_time + life_time;
		cache_item.accesses++;

		auto requested_level = min(log2(cache_item.accesses), queue_count-1);
	}

	/**
	 * Stores 'value' into the cache using 'key'. Uses the 'MQ'
     * algorithm to maintain cache size.
     * 
	 * Params:
     * 	key = The key to associate with 'value'.
     *  value = The value to store.
     *
     * If the block is in our history (not our cache), then we will remember how
     * many accesses it had. We use this to promote a frequently accessed block
     * into a higher level than a brand new block. */
	void put(in K key, in V value) {
		auto access_count = history_accesses.get(key, 1);
		auto level = min(log2(access_count), queue_count - 1);
		queues[level]~=key;
		cache[key] = new CacheItem(level, 
								   access_count, 
								   current_time + life_time);
	}
}

unittest {
	alias Cache!(int,int) int_cache;

	auto cache = int_cache(function (int k, int v) {
			// empty;
		});
}