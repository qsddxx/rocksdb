#pragma once

#include <folly/MPMCQueue.h>

#include <bit>
#include <semaphore>

#include "db/db_impl/db_impl.h"
#include "db/elastic/task.h"
#include "db/elastic/task_pool.h"
#include "liburing.h"
#include "rocksdb/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE {
class ElasticLSMImpl : public ElasticLSM {
 public:
  ElasticLSMImpl(const ElasticLSMOptions& elastic_options);
  ~ElasticLSMImpl();
  using ElasticLSM::Open;
  static Status Open(const Options& options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& dbname,
                     std::unique_ptr<ElasticLSM>* dbptr);
  static Status Open(const DBOptions& db_options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     std::unique_ptr<ElasticLSM>* dbptr);

  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value,
             std::function<void()>* callback) override;

  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key, std::function<void()>* callback) override;

  Status Update(const ReadOptions& _read_options, const WriteOptions& options,
                ColumnFamilyHandle* column_family, const Slice& key,
                std::string* value, std::function<bool()>* mid_callback,
                std::function<void()>* callback) override;

  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             std::string* value, std::function<void()>* callback) override;

  Status Scan(const ReadOptions& _read_options,
              ColumnFamilyHandle* column_family, 
              std::function<void(rocksdb::Iterator *)>* func,
              std::function<void()>* callback) override;

  ColumnFamilyHandle* DefaultColumnFamily() const override {
    return db_->DefaultColumnFamily();
  }

 private:
  bool closed_ = false;
  ElasticLSMOptions options_;
  DBImpl* db_;
  std::vector<std::thread> worker_thread_pool_;
  std::vector<std::thread> tp_thread_pool_;
  std::vector<std::thread> compaction_thread_pool_;
  std::vector<std::thread> schedular_thread_pool_;
  SystemClock* clock_;
  // tp related
  TPTaskPool tp_task_pool_;
  folly::MPMCQueue<std::pair<tp_task::tp_task_type, int>> tp_task_queue_;
  std::counting_semaphore<> tp_task_num{0};
  std::atomic<int> tp_working_threads_num{0};
  std::atomic<int> tp_throughput_{200};
  // ap
  folly::MPMCQueue<ap_task*> ap_task_queue_;
  // schedular
  std::counting_semaphore<> schedule_count_{0};
  std::atomic<int> maybe_schedule_count_{0};
  std::atomic<int> compaction_schedule_count_{0};
  folly::MPMCQueue<int> compaction_done_work_queue_;
  std::atomic<int> compaction_done_work_count_{0};
  std::mutex compaction_mutex_;
  std::condition_variable compaction_cv_;
  // io_uring related
  io_uring* ring_;
  struct cqe_task {
    uint64_t count = 0;
    std::shared_ptr<compaction_task> task;
    std::atomic<uint64_t>* compaction_write_num_count;
  };
  std::unordered_map<int, cqe_task> cqe_task_map_;
  void StartThreads();
  // functions for threads
  void TPTask(int idx);
  void ContinuousTPTask();
  void ContinuousCompactionTask(int idx, bool flush);
  pausable_task APTask();
  void BGWork(int idx);
  void UpdateCQEMap(int compaction_id, std::shared_ptr<compaction_task> task,
                    AsyncWriteOp* req = nullptr);
  void CheckCQE();
  void BGSchedule();

  bool HasAnyTPTask() const { return tp_task_queue_.size() > 0; };
  bool HasAnyAPTask() const { return ap_task_queue_.size() > 0; };
  bool HasCompactionTask() const { return compaction_tasks_mask_ != 0; };
  bool HasAnyTask() const {
    return HasAnyAPTask() || HasAnyTPTask() || HasCompactionTask();
  };

  // compaction and flush related
  std::array<std::shared_ptr<compaction_task>, SLOT_NUM> compaction_tasks_;
  slotmask compaction_tasks_mask_ = 0;

  class CompactionSchedular {
   public:
    CompactionSchedular(ElasticLSMImpl* elastic_lsm)
        : elastic_lsm_(elastic_lsm), del_task_list_(64) {}
    // add and remove task to unschedulable list
    void AddTask(std::shared_ptr<compaction_task> task) {
      // int max_c = elastic_lsm_->options_.max_compaction_num;
      tasks_list_.push(unscheduled_task{task, time_++});
      elastic_lsm_->NotifyBGThreadCompactionSchedule();
    }

    void RemoveTask(int idx) {
      del_task_list_.write(idx);
      elastic_lsm_->NotifyBGThreadCompactionSchedule();
    }

    bool IsTaskListEmpty() const { return tasks_list_.empty(); }

    // schedular put the highest priority task to compaction task slot
    // not thread safe, should be called by schedular thread only
    void schedule();

   private:
    struct unscheduled_task {
      std::shared_ptr<compaction_task> task;
      int arrive_time;
      bool operator<(const unscheduled_task& other) const {
        return arrive_time + task->priority >
               other.arrive_time + other.task->priority;
      }
    };
    ElasticLSMImpl* elastic_lsm_;
    std::priority_queue<unscheduled_task> tasks_list_;
    folly::MPMCQueue<int> del_task_list_;
    int time_ = 0;
  } schedular_;

  pausable_task Flush() {
    db_->BackgroundCallFlush(Env::Priority::HIGH);
    co_return;
  }

  void ScheduleFlush() {
    std::shared_ptr<compaction_task> task = std::make_shared<compaction_task>();
    task->priority = 0;
    task->coro_works_count = 1;
    task->coro_handles.emplace_back(Flush());
    task->flush = true;
    task->done = false;
    schedular_.AddTask(task);
  }

  void ScheduleCompaction() {
    std::shared_ptr<compaction_task> task = std::make_shared<compaction_task>();
    task->done_work.emplace(
        db_->BackgroundCallCompaction(nullptr, Env::Priority::LOW, task.get()));
    task->done_work->resume();
    task->coro_works_count = task->coro_handles.size();
    if (!task->done) schedular_.AddTask(task);
  }

  class StrideSchedular {
   public:
    StrideSchedular(ElasticLSMImpl* elastic_lsm, int idx_)
        : elastic_lsm_(elastic_lsm),
          local_idx_(idx_),
          task_queue_(priority_cmp(this), [] {
            std::vector<int> v;
            v.reserve(SLOT_NUM);
            return v;
          }()) {
      to_reinsert_task_queue_.reserve(SLOT_NUM);
    }
    // notice worker a new task is available
    void NewTask(int idx) { new_task_mask_.fetch_or(1 << idx); }
    // synchronize new tasks
    void sync();
    // get lowest pass task, return true if work was done
    bool work();
    bool compaction(bool flush = true);

   private:
    ElasticLSMImpl* elastic_lsm_;
    int local_idx_ = -1;
    double global_pass_ = 0;
    std::atomic<slotmask> new_task_mask_{0};
    slotmask active_task_mask_{0};
    std::array<std::shared_ptr<compaction_task>, SLOT_NUM> compaction_tasks_;
    std::array<double, SLOT_NUM> pass_;
    std::vector<int> skip_task_list_;

    struct priority_cmp {
      StrideSchedular* schedular_;
      priority_cmp(StrideSchedular* schedular) : schedular_(schedular) {}
      bool operator()(int a, int b) const {
        return schedular_->pass_[a] > schedular_->pass_[b];
      }
    };
    friend class priority_cmp;
    std::priority_queue<int, std::vector<int>, priority_cmp> task_queue_;
    std::vector<int> to_reinsert_task_queue_;

    void remove_done_work(int idx);
    bool docompaction(bool highpriority);
    // put back task to queue
    void put_back_task();
  };

  std::vector<std::unique_ptr<StrideSchedular>> stride_schedulars_;

  void NotifyBGThreadMaybeSchedule() {
    maybe_schedule_count_++;
    schedule_count_.release();
  }

  void NotifyBGThreadCompactionSchedule() {
    compaction_schedule_count_++;
    schedule_count_.release();
  }

  void NotifyBGThreadCompactionDoneWork() {
    compaction_done_work_count_++;
    schedule_count_.release();
  }

  bool CalcIfNeedTP() const {
    size_t num = tp_working_threads_num.load();
    if (num == 0) {
      return true;
    }
    return tp_task_queue_.size() / num > (size_t)tp_throughput_;
  }

  friend class DBImpl;
  friend class compaction_schedular;
};
}  // namespace ROCKSDB_NAMESPACE