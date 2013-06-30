/*
 * test_mq.cpp
 *
 *  Created on: Jun 28, 2013
 *      Author: Christopher Nelson
 */

#include <cache/mq.h>
#include <memory>
#include <iostream>

#include <gtest/gtest.h>

using namespace std;

// Tests that we can create a cql::mq object.
TEST(MqCacheTest, CanCreate) {
  auto test = [] { std::unique_ptr<cql::mq<int,int>>(new cql::mq<int,int>()); };
  EXPECT_NO_THROW(test());
}

TEST(MqCacheTest, CanPut) {
  cql::mq<int,int> q;
  q.put(1,10);
}

TEST(MqCacheTest, CanGet) {
  cql::mq<int,int> q;
  q.put(1,10);

  EXPECT_TRUE(get<0>(q.get(1)));
}

TEST(MqCacheTest, CanGetRepeatedly) {
  cql::mq<int,int> q;
  q.put(1,10);

  auto repeats = 1<<10;

  for(int i=0; i<repeats; i++) {
	  EXPECT_TRUE(get<0>(q.get(1)));
  }

  EXPECT_EQ(repeats, q.get_hit_count());
}

TEST(MqCacheTest, CanPutMany) {
  cql::mq<int,int> q;

  auto limit = 100000;
  auto total_capacity = 128;

  for(int i=0; i<limit; i++) {
	  q.put(i,i*100);
  }

  for(int i=0; i<limit-total_capacity; i++) {
  	  auto r = q.get(i);
  	  EXPECT_FALSE(get<0>(r));
  	  EXPECT_EQ(int(), get<1>(r));
  	  if (get<0>(r)) {
  		  std::cout << "unexpected:" << i << "=" << get<1>(r) << std::endl;
  	  }
    }

  for(int i=limit-1; i>=limit-total_capacity; i--) {
	  auto r = q.get(i);
	  EXPECT_TRUE(get<0>(r));
	  EXPECT_EQ(get<1>(r), i*100);
  }

  EXPECT_EQ(total_capacity, q.get_hit_count());
}

