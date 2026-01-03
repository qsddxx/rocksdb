#pragma once

#include <folly/MPMCQueue.h>
#include <folly/coro/Coroutine.h>
#include <folly/coro/Task.h>

#include <bit>
#include <semaphore>
#include "liburing.h"

#include "db/db_impl/db_impl.h"
#include "db/elastic/task.h"
#include "rocksdb/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE {
class ElasticLSMImpl : public ElasticLSM {
 public:
  ElasticLSMImpl(const ElasticLSMOptions& elastic_options, DBImpl* dbelastic, io_uring* ring);
  ~ElasticLSMImpl();
  using ElasticLSM::Open;
  static Status Open(const DBOptions& db_options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     std::unique_ptr<ElasticLSM>* dbptr);

  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override;

  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override;

  Status Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key, const Slice& value) override;

  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             std::string* value) override;

  Status Scan(const ReadOptions& _read_options,
              ColumnFamilyHandle* column_family, const Slice& key,
              int record_count, std::vector<std::string>* answer) override;

  ColumnFamilyHandle* DefaultColumnFamily() const override {
    return db_->DefaultColumnFamily();
  }

 private:
  bool closed_ = false;
  ElasticLSMOptions options_;
  DBImpl* db_;
  std::vector<std::thread> worker_thread_pool_;
  std::vector<std::thread> schedular_thread_pool_;
  SystemClock* clock_;
  // tp related
  folly::MPMCQueue<tp_task*> tp_task_queue_;
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
  // io_uring related
  io_uring* ring_;
  struct cqe_task {
    uint64_t count = 0;
    std::shared_ptr<compaction_task> task;
  };
  std::unordered_map<int, cqe_task> cqe_task_map_;

  // functions for threads
  void TPTask();
  pausable_task APTask();
  void BGWork(int idx);
  void CheckCQE();
  void BGSchedule();

  // compaction and flush related
  std::array<std::shared_ptr<compaction_task>, SLOT_NUM> compaction_tasks_;
  slotmask compaction_tasks_mask_;

  // folly::MPMCQueue<pausable_task*> flush_task_queue_;

  class CompactionSchedular {
   public:
    CompactionSchedular(ElasticLSMImpl* elastic_lsm)
        : elastic_lsm_(elastic_lsm) {}
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
    // schedular put the highest priority task to compaction task slot
    // not thread safe, should be called by schedular thread only
    void schedule();

   private:
    struct unscheduled_task {
      std::shared_ptr<compaction_task> task;
      int arrive_time;
      bool operator<(const unscheduled_task& other) const {
        return arrive_time + task->priority <
               other.arrive_time + other.task->priority;
      }
    };
    std::priority_queue<unscheduled_task> tasks_list_;
    folly::MPMCQueue<int> del_task_list_;
    ElasticLSMImpl* elastic_lsm_;
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
    schedular_.AddTask(task);
  }

  void ScheduleCompaction() {
    std::shared_ptr<compaction_task> task = std::make_shared<compaction_task>();
    task->done_work.emplace(
        db_->BackgroundCallCompaction(nullptr, Env::Priority::LOW, task));
    task->done_work->resume();
    task->coro_works_count = task->coro_handles.size();
    schedular_.AddTask(task);
  }

  class StrideSchedular {
   public:
    StrideSchedular(ElasticLSMImpl* elastic_lsm)
        : elastic_lsm_(elastic_lsm), task_queue_(priority_cmp(this)) {}
    // notice worker a new task is available
    void NewTask(int idx) { new_task_mask_.fetch_or(1 << idx); }
    // synchronize new tasks
    void sync();
    // get lowest pass task
    void work();

   private:
    ElasticLSMImpl* elastic_lsm_;
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
        return schedular_->pass_[a] < schedular_->pass_[b];
      }
    };
    friend class priority_cmp;
    std::priority_queue<int, std::vector<int>, priority_cmp> task_queue_;
    std::list<int> to_reinsert_task_queue_;

    bool docompaction(bool highpriority);
    // put back task to queue
    void put_back_task();
  };

  std::vector<StrideSchedular> stride_schedulars_;

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
    return tp_task_queue_.size() / tp_working_threads_num > tp_throughput_;
  }

  friend class DBImpl;
  friend class compaction_schedular;
};
}  // namespace ROCKSDB_NAMESPACE