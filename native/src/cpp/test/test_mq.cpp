/*
 * test_mq.cpp
 *
 *  Created on: Jun 28, 2013
 *      Author: Christopher Nelson
 */

#include <cache/mq.h>
#include <memory>

#include <gtest/gtest.h>

using namespace std;

// Tests that we can create a cql::mq object.
TEST(MqCachTest, CanCreate) {
  auto test = [] { std::unique_ptr<cql::mq<int,int>>(new cql::mq<int,int>()); };
  EXPECT_NO_THROW(test());
}

TEST(MqCachTest, CanPut) {
  cql::mq<int,int> q;
  q.put(1,10);
}

TEST(MqCachTest, CanGet) {
  cql::mq<int,int> q;
  q.put(1,10);

  EXPECT_TRUE(get<0>(q.get(1)));
}


