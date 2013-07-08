/*
 * test_value.cpp
 *
 *  Created on: Jul 8, 2013
 *      Author: Christopher Nelson
 */

#include <datafile/value.h>
#include <memory>
#include <iostream>

#include <gtest/gtest.h>

using namespace std;

const int test_reps = 10000;

// Tests that we can create a cql::index object.
TEST(ValueFileTest, CanCreate) {
	auto test = [] {std::unique_ptr<cql::datafile::value<int>>(new cql::datafile::value<int>("test.dat"));};
	EXPECT_NO_THROW(test());
}

TEST(ValueFileTest, CanWrite) {
	cql::datafile::value<int> val("test_w.dat");

	ASSERT_TRUE(val.is_open());

	for (int i = 0; i < test_reps; i++) {
		val.append(i);
	}
}

TEST(ValueFileTest, CanRead) {
	cql::datafile::value<int> val("test_r.dat");

	std::vector<uint64_t> offsets;

	for (int i = 0; i < test_reps; i++) {
		auto offset = val.append(i);
		offsets.push_back(offset);
	}

	for (int i = 0; i < test_reps; i++) {
		auto& offset = offsets[i];
		int value=0;
		val.get(offset, value);

		ASSERT_EQ(i, value);
	}
}
