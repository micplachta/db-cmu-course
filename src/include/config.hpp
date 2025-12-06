#ifndef _CONFIG_HPP_
#define _CONFIG_HPP_

#include <cstddef>
#include <cstdint>

const size_t DB_PAGE_SIZE = 4096;
const size_t DEFAULT_DB_IO_SIZE = 16;

using FrameId_t = int32_t;
using PageId_t = int32_t;

#endif
