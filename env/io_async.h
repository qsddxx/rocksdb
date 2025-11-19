#ifndef IO_ASYNC_H
#define IO_ASYNC_H
#ifndef FOLLY_F14_INTRINSICS_MODE
#define FOLLY_F14_INTRINSICS_MODE 1  // 强制使用与你的 folly 匹配的 Mode=1
#endif
#include <sys/uio.h>
#include <unistd.h>
#include <atomic>
#include <functional>
#include <map>
#include <string>
#include <memory>
#include <liburing.h>
#include <algorithm>
#include <cerrno>
#if defined(OS_LINUX)
#include <linux/fs.h>
#ifndef FALLOC_FL_KEEP_SIZE
#include <linux/falloc.h>
#endif
#endif
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(OS_LINUX) || defined(OS_ANDROID)
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#endif
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/string_util.h"
#include <folly/AtomicHashMap.h>
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"
#include "util/string_util.h"
#include "../logging/logging.h"
#include "util/aligned_buffer.h"

namespace rocksdb {



}  // namespace rocksdb

#endif  // ROCKSDB_ASYNC_POSIX_WRITABLE_FILE_H