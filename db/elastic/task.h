#pragma once
#include <functional>

namespace ROCKSDB_NAMESPACE {
struct task {
  enum task_type {
    TASK_TYPE_NONE,
    TASK_TYPE_TP,
    TASK_TYPE_AP,
  } type;
  std::function<void()>* callback;

 public:
  task(std::function<void()>* _callback = nullptr, task_type _type = TASK_TYPE_NONE)
      : type(_type), callback(_callback) {}
  virtual ~task() {
    delete callback;
  };
};

struct tp_task : public task {
  enum tp_task_type {
    TP_TASK_TYPE_NONE,
    TP_TASK_TYPE_PUT,
    TP_TASK_TYPE_DELETE,
    TP_TASK_TYPE_UPDATE,
    TP_TASK_TYPE_GET,
    TP_TASK_TYPE_SCAN
  } tp_type;

 public:
  tp_task(std::function<void()>* _callback = nullptr,
          tp_task_type _tp_type = TP_TASK_TYPE_NONE)
      : task(_callback, TASK_TYPE_TP), tp_type(_tp_type) {}
  virtual ~tp_task() = default;
};

struct put_task : public tp_task {
  const WriteOptions& write_options;
  ColumnFamilyHandle* column_family;
  const Slice& key;
  const Slice& value;

 public:
  put_task(const WriteOptions& _write_options,
           ColumnFamilyHandle* _column_family, const Slice& _key,
           const Slice& _value, std::function<void()>* _callback = nullptr)
      : tp_task(_callback, TP_TASK_TYPE_PUT),
        write_options(_write_options),
        column_family(_column_family),
        key(_key),
        value(_value) {}
};

struct delete_task : public tp_task {
  const WriteOptions& write_options;
  ColumnFamilyHandle* column_family;
  const Slice& key;

 public:
  delete_task(const WriteOptions& _write_options,
              ColumnFamilyHandle* _column_family, const Slice& _key,
              std::function<void()>* _callback = nullptr)
      : tp_task(_callback, TP_TASK_TYPE_DELETE),
        write_options(_write_options),
        column_family(_column_family),
        key(_key) {}
};

struct update_task : public tp_task {
  const WriteOptions& write_options;
  ColumnFamilyHandle* column_family;
  const Slice& key;
  const Slice& value;

 public:
  update_task(
      const WriteOptions& _write_options, ColumnFamilyHandle* _column_family,
      const Slice& _key, const Slice& _value,
      std::function<void()>* _callback = nullptr)
      : tp_task(_callback, TP_TASK_TYPE_UPDATE),
        write_options(_write_options),
        column_family(_column_family),
        key(_key),
        value(_value) {}
};

struct get_task : public tp_task {
  const ReadOptions& read_options;
  ColumnFamilyHandle* column_family;
  const Slice& key;
  std::string* value;

 public:
  get_task(
      const ReadOptions& _read_options, ColumnFamilyHandle* _column_family,
      const Slice& _key, std::string* _value,
      std::function<void()>* _callback = nullptr)
      : tp_task(_callback, TP_TASK_TYPE_GET),
        read_options(_read_options),
        column_family(_column_family),
        key(_key),
        value(_value) {}
};

struct scan_task : public tp_task {
  const ReadOptions& read_options;
  ColumnFamilyHandle* column_family;
  const Slice& key;
  int record_count;
  std::vector<std::string>* answer;

 public:
  scan_task(
      const ReadOptions& _read_options, ColumnFamilyHandle* _column_family,
      const Slice& _key, int _record_count, std::vector<std::string>* _answer,
      std::function<void()>* _callback = nullptr)
      : tp_task(_callback, TP_TASK_TYPE_SCAN),
        read_options(_read_options),
        column_family(_column_family),
        key(_key),
        record_count(_record_count),
        answer(_answer) {}
};

struct ap_task : public task {
  const ReadOptions& read_options;
  ColumnFamilyHandle* column_family;
  std::function<void(PinnableSlice*)> func;
  std::vector<std::string>* answer;

 public:
  ap_task(
      const ReadOptions& _read_options, ColumnFamilyHandle* _column_family,
      std::function<void(PinnableSlice*)>& _func,
      std::vector<std::string>* _answer,
      std::function<void()>* _callback = nullptr)
      : task(_callback, TASK_TYPE_AP),
        read_options(_read_options),
        column_family(_column_family),
        func(_func),
        answer(_answer) {}
};

struct pausable_task {
  struct promise_type {
    bool done = false;
    pausable_task get_return_object() {
      return pausable_task{
          std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept { done = true; }
    void unhandled_exception() { std::terminate(); }
    std::suspend_always yield_value(int /*x*/) { return {}; }
  };

  std::coroutine_handle<promise_type> handle;
  explicit pausable_task(std::coroutine_handle<promise_type> h) : handle(h) {}
  pausable_task(pausable_task&& t) noexcept : handle(t.handle) {
    t.handle = nullptr;
  }
  ~pausable_task() {
    if (handle) handle.destroy();
  }
  bool resume() {
    handle.resume();
    return done();
  }
  bool done() const { return handle.promise().done; }
};

typedef uint64_t slotmask;
static const size_t SLOT_NUM = sizeof(slotmask) * 8;

struct compaction_task {
  int priority = 0;
  int compaction_id = -1;
  int coro_works_count = 0;
  std::vector<pausable_task> coro_handles;
  std::atomic<slotmask> coro_works_mask{0};
  std::atomic<int> done_works_count{0};
  std::optional<pausable_task> done_work;
  bool done = true;
  bool flush = false;
};
struct AsyncWriteOp {
  AlignedBuffer buffer;
  size_t size;
  uint64_t offset;
  std::atomic<uint64_t>* compaction_write_num_count;
  int compaction_id;
  int fd;
};
}  // namespace ROCKSDB_NAMESPACE