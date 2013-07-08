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
	auto test = [] {std::unique_ptr<cql::datafile::store<int>>(new cql::datafile::store<int>("test"));};
	EXPECT_NO_THROW(test());
}

TEST(StoreTest, CanWrite) {
	cql::datafile::store<int> st("test");

	ASSERT_TRUE(st.is_open());

	for (int i = 0; i < test_reps; i++) {
		st.put(i, i*1000);
	}
}

/*TEST(StoreTest, CanRead) {
	cql::datafile::store<int> st("test");

	for (int i = 0; i < test_reps; i++) {
		auto pos = cql::datafile::entry_position::from_uint64(i);
		st.put_entry_offset(pos, i * 100);
	}

	for (int i = 0; i < test_reps; i++) {
		auto pos = cql::datafile::entry_position::from_uint64(i);
		auto offset = st.get_entry_offset(pos);

		ASSERT_EQ(i*100, offset);
	}
}*/
