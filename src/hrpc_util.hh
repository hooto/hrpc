// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//

#ifndef HRPC_UTIL_HH
#define HRPC_UTIL_HH

#include "hrpc.hh"

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sys/time.h>

#include "fmt/format.h"

namespace hrpc {
namespace util {

uint32_t bs_to_uint32(std::string bs) {

    if (bs.size() < 4) {
        return 0;
    }

    char *bsc = const_cast<char *>(bs.c_str());
    unsigned char *b = reinterpret_cast<unsigned char *>(bsc);

    return uint32_t(b[3]) | uint32_t(b[2]) << 8 | uint32_t(b[1]) << 16 |
           uint32_t(b[0]) << 24;
};

std::string uint32_to_bs(uint32_t v) {

    char b[4];

    b[0] = (v >> 24);
    b[1] = (v >> 16);
    b[2] = (v >> 8);
    b[3] = (v);

    return std::string(b, 4);
};

static inline int64_t timenow_us() {
    ::timeval tn;
    ::gettimeofday(&tn, NULL);
    return (tn.tv_sec * 1e6) + tn.tv_usec;
};

} // namespace util
} // namespace hrpc

#endif
