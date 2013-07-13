/*
 * test_store.cpp
 *
 *  Created on: Jun 28, 2013
 *      Author: Christopher Nelson
 */

#include <datafile/store.h>
#include <memory>
#include <iostream>

#include <gtest/gtest.h>

using namespace std;

const int test_reps = 10000;

// Tests that we can create a cql::store<int> object.
TEST(StoreTest, CanCreate) {
	auto test =
			[] {std::unique_ptr<cql::datafile::store<int>>(new cql::datafile::store<int>("test"));};
	EXPECT_NO_THROW(test());
}

TEST(StoreTest, CanWrite) {
	cql::datafile::store<int> st("test");

	ASSERT_TRUE(st.is_open());

	for (int i = 0; i < test_reps; i++) {
		st.put(i, i * 1000);
	}
}

TEST(StoreTest, CanWriteDuplicates) {
	cql::datafile::store<int> st("test");

	ASSERT_TRUE(st.is_open());

	for (int j = 0; j < test_reps / 10; j++) {
		for (int i = 0; i < test_reps / 10; i++) {
			st.put(i, j);
		}
	}

	ASSERT_EQ(test_reps / 10, st.count());
}

TEST(StoreTest, CanReadMemoryByColumn) {
	cql::datafile::store<int> st("test");

	for (int i = 0; i < test_reps; i++) {
		st.put(i, i * 1000);
	}

	for (int i = 0; i < test_reps; i++) {
		auto r = st.get(i);

		ASSERT_TRUE(get < 0 > (r));
		ASSERT_EQ(i * 1000, get < 1 > (r));
	}
}

TEST(StoreTest, CanSlowReadMemoryByColumn) {
	cql::datafile::store<int> st("test");

	st.set_use_fast_column_lookup(false);

	for (int i = 0; i < test_reps; i++) {
		st.put(i, i * 1000);
	}

	for (int i = 0; i < test_reps; i++) {
		auto r = st.get(i);

		ASSERT_TRUE(get < 0 > (r));
		ASSERT_EQ(i * 1000, get < 1 > (r));
	}
}
