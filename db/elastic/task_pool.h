#pragma once
#include <folly/MPMCQueue.h>

#include "db/elastic/task.h"

namespace ROCKSDB_NAMESPACE {
template <typename T>
class task_pool {
 public:
  task_pool(int size) : pool_(size), mask_(size), size_(size) {}
  template <typename... Args>
  int AddTask(Args&&... args) {
    int idx = tail_.fetch_add(1, std::memory_order_relaxed) % size_;
    while (mask_[idx]) {
      idx = tail_.fetch_add(1, std::memory_order_relaxed) % size_;
    }
    mask_[idx].store(1);
    T* ptr = reinterpret_cast<T*>(&pool_[idx]);
    new (ptr) T(std::forward<Args>(args)...);
    return idx;
  }

  T& GetTask(int idx) { return *reinterpret_cast<T*>(&pool_[idx]); }
  void RemoveTask(int idx) {
    T* ptr = reinterpret_cast<T*>(&pool_[idx]);
    ptr->~T();
    mask_[idx].store(0);
  }

 private:
  using storage = std::aligned_storage_t<sizeof(T), alignof(T)>;
  std::vector<storage> pool_;
  std::vector<std::atomic<uint8_t>> mask_;
  uint32_t size_;
  std::atomic<int> tail_{0};
};
class TPTaskPool {
 public:
  TPTaskPool(int size)
      : put_pool(size),
        delete_pool(size),
        update_pool(size),
        get_pool(size),
        scan_pool(size) {}
  task_pool<put_task> put_pool;
  task_pool<delete_task> delete_pool;
  task_pool<update_task> update_pool;
  task_pool<get_task> get_pool;
  task_pool<scan_task> scan_pool;
};
}  // namespace ROCKSDB_NAMESPACE