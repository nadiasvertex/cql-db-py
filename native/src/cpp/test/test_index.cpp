/*
 * test_mq.cpp
 *
 *  Created on: Jun 28, 2013
 *      Author: Christopher Nelson
 */

#include <datafile/index.h>
#include <memory>
#include <iostream>

#include <gtest/gtest.h>

using namespace std;

// Tests that we can create a cql::index object.
TEST(IndexFileTest, CanCreate) {
	auto test = [] {std::unique_ptr<cql::index>(new cql::index("test.idx"));};
	EXPECT_NO_THROW(test());
}

TEST(IndexFileTest, CanWrite) {
	cql::index idx("test.idx");

	ASSERT_TRUE(idx.is_open());

	for (int i = 0; i < 10000; i++) {
		auto pos = cql::entry_position::from_uint64(i);
		idx.put_entry_offset(pos, i * 100);
	}
}

TEST(IndexFileTest, CanRead) {
	cql::index idx("test.idx");

	for (int i = 0; i < 10000; i++) {
		auto pos = cql::entry_position::from_uint64(i);
		idx.put_entry_offset(pos, i * 100);
	}

	for (int i = 0; i < 10000; i++) {
		auto pos = cql::entry_position::from_uint64(i);
		auto offset = idx.get_entry_offset(pos);

		ASSERT_EQ(i*100, offset);
	}
}
