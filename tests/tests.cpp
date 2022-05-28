// Copyright 2022 Shevelyova Darya photodoshfy@gmail.com

#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include <example.hpp>

TEST(Example, EmptyTest) {
    EXPECT_THROW(example(), std::runtime_error);
}

TEST(DISABLED_Snapshot, Speen) {
  for (;;) std::this_thread::yield();
}
