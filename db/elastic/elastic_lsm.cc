#include "db/elastic/elastic_lsm.h"

#include <xmmintrin.h>

namespace ROCKSDB_NAMESPACE {
ElasticLSM::~ElasticLSM() = default;
Status ElasticLSM::Open(const Options& options,
                        const ElasticLSMOptions& elastic_options,
                        const std::string& name,
                        std::unique_ptr<ElasticLSM>* dbptr) {
  return ElasticLSMImpl::Open(options, elastic_options, name, dbptr);
}

Status ElasticLSM::Open(
    const DBOptions& db_options, const ElasticLSMOptions& elastic_options,
    const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<ElasticLSM>* dbptr) {
  return ElasticLSMImpl::Open(db_options, elastic_options, name, column_families,
                             handles, dbptr);
}

Status ElasticLSMImpl::Open(const Options& options,
                            const ElasticLSMOptions& elastic_options,
                            const std::string& dbname,
                            std::unique_ptr<ElasticLSM>* dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  if (db_options.persist_stats_to_disk) {
    column_families.emplace_back(kPersistentStatsColumnFamilyName,
                                  cf_options);
  }
  std::vector<ColumnFamilyHandle*> handles;
  Status s = Open(db_options, elastic_options, dbname, column_families,
                  &handles, dbptr);
  if (s.ok()) {
    if (db_options.persist_stats_to_disk) {
      assert(handles.size() == 2);
    } else {
      assert(handles.size() == 1);
    }
    if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
      delete handles[1];
    }
    delete handles[0];
  }
  return s;
}

Status ElasticLSMImpl::Open(
    const DBOptions& db_options, const ElasticLSMOptions& elastic_options,
    const std::string& name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<ElasticLSM>* dbptr) {

  ElasticLSMImpl* elastic = new ElasticLSMImpl(elastic_options);

  auto new_options = db_options;
  new_options.max_subcompactions = elastic_options.max_background_threads;
  new_options.use_direct_io_for_flush_and_compaction = false;
  new_options.compaction_morsel_size = elastic_options.compaction_morsel_size;
  auto new_cf_options = std::vector<ColumnFamilyDescriptor>(column_families);
  for (auto& cf_desc : new_cf_options) {
    cf_desc.options.level_compaction_dynamic_level_bytes = false;
    cf_desc.options.compaction_style = kCompactionStyleSegment;
  }
  std::unique_ptr<DB> db;
  Status s;

  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  ThreadStatusUtil::SetEnableTracking(db_options.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(
      ThreadStatus::OperationType::OP_DBOPEN);
  bool can_retry = false;
  do {
    s = DBImpl::Open(new_options, name, new_cf_options, handles, &db,
                      !kSeqPerBatch, kBatchPerTxn, can_retry, &can_retry,
                      elastic);
  } while (!s.ok() && can_retry);
  ThreadStatusUtil::ResetThreadStatus();

  if (!s.ok()) {
    delete elastic;
    return s;
  }

  DBImpl* dbelastic = dynamic_cast<DBImpl*>(db.get());
  db.release();

  elastic->db_ = dbelastic;
  elastic->clock_ = dbelastic->GetSystemClock();
  elastic->ring_ = dbelastic->GetFileSystem()->GetIoUring();

  elastic->StartThreads();

  dbptr->reset(elastic);

  return Status::OK();
}

ElasticLSMImpl::ElasticLSMImpl(const ElasticLSMOptions& elastic_options)
    : options_(elastic_options),
      tp_task_pool_(elastic_options.max_tp_task_queue),
      tp_task_queue_(elastic_options.max_tp_task_queue),
      ap_task_queue_(),
      compaction_done_work_queue_(1024),
      schedular_(this) {}

ElasticLSMImpl::~ElasticLSMImpl() {
  closed_ = true;
  schedule_count_.release();
  compaction_cv_.notify_all();

  // Wait for all threads to finish
  for (auto& t : schedular_thread_pool_) {
    if (t.joinable()) {
      t.join();
    }
  }
  for (auto& t : tp_thread_pool_) {
    if (t.joinable()) {
      t.join();
    }
  }
  for (auto& t : worker_thread_pool_) {
    if (t.joinable()) {
      t.join();
    }
  }
  for (auto& t : compaction_thread_pool_) {
    if (t.joinable()) {
      t.join();
    }
  }
}

void ElasticLSMImpl::StartThreads() {
  schedular_thread_pool_.emplace_back(&ElasticLSMImpl::BGSchedule, this);
  for (int i = 0; i < options_.min_tp_threads; ++i) {
    tp_thread_pool_.emplace_back(&ElasticLSMImpl::ContinuousTPTask, this);
  }
  int bg_work_threads =
      options_.max_background_threads - options_.min_tp_threads;
  stride_schedulars_.reserve(bg_work_threads);
  for (int i = 0; i < options_.min_compaction_threads; ++i) {
    stride_schedulars_.emplace_back(std::make_unique<StrideSchedular>(this, i));
    compaction_thread_pool_.emplace_back(
        &ElasticLSMImpl::ContinuousCompactionTask, this, i, false);
  }
  for (int i = options_.min_compaction_threads; i < bg_work_threads; ++i) {
    stride_schedulars_.emplace_back(std::make_unique<StrideSchedular>(this, i));
    worker_thread_pool_.emplace_back(&ElasticLSMImpl::BGWork, this, i);
  }
}

Status ElasticLSMImpl::Put(const WriteOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           const Slice& value,
                           std::function<void()>* callback) {
  int idx = tp_task_pool_.put_pool.AddTask(options, column_family, key, value, callback);
  tp_task_queue_.blockingWrite(tp_task::TP_TASK_TYPE_PUT, idx);
  return Status::OK();
}

Status ElasticLSMImpl::Delete(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key,
                              std::function<void()>* callback) {
  int idx = tp_task_pool_.delete_pool.AddTask(options, column_family, key, callback);
  tp_task_queue_.blockingWrite(tp_task::TP_TASK_TYPE_DELETE, idx);
  return Status::OK();
}

Status ElasticLSMImpl::Update(const ReadOptions& _read_options,
                              const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, std::string* value,
                              std::function<bool()>* mid_callback,
                              std::function<void()>* callback) {
  int idx = tp_task_pool_.update_pool.AddTask(_read_options, options, column_family, key, value,
                           mid_callback, callback);
  tp_task_queue_.blockingWrite(tp_task::TP_TASK_TYPE_UPDATE, idx);
  return Status::OK();
}

Status ElasticLSMImpl::Get(const ReadOptions& _read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           std::function<void()>* callback) {
  int idx = tp_task_pool_.get_pool.AddTask(_read_options, column_family, key, value, callback);
  tp_task_queue_.blockingWrite(tp_task::TP_TASK_TYPE_GET, idx);
  return Status::OK();
}

Status ElasticLSMImpl::Scan(const ReadOptions& _read_options,
                            ColumnFamilyHandle* column_family, 
                            std::function<void(rocksdb::Iterator *)>* func,
                            std::function<void()>* callback) {
  int idx = tp_task_pool_.scan_pool.AddTask(_read_options, column_family, func, callback);
  tp_task_queue_.blockingWrite(tp_task::TP_TASK_TYPE_SCAN, idx);
  return Status::OK();
}

void ElasticLSMImpl::TPTask(int idx) {
  // Pop up a task without locking
  // Note: tp_working_threads_num is managed by ContinuousTPTask threads
  std::pair<tp_task::tp_task_type, int> task;
  int task_cnt = 0;
  int target_cnt = tp_throughput_;
  int failed_cnt = 0;
  uint64_t start_time = clock_->NowMicros();
  while (true) {
    auto bo = tp_task_queue_.read(task);
    if (!bo) {
      if (failed_cnt++ > target_cnt - task_cnt) {
        break;
      }
      _mm_pause();
      continue;
    }
    failed_cnt = 0;
    Status s;
    switch (task.first) {
      case tp_task::TP_TASK_TYPE_PUT: {
        auto& t = tp_task_pool_.put_pool.GetTask(task.second);
        auto no_block_write_option = t.write_options;
        if (idx >= 0) {
          no_block_write_option.no_slowdown = true;
        }
        int cnt = 0;
      retry:
        s = db_->Put(no_block_write_option, t.column_family, t.key, t.value);
        if (s.IsIncomplete()) {
          stride_schedulars_[idx]->compaction(true);
          ++cnt;
          goto retry;
        }
        if (cnt > 10) {
          std::printf("tp put retry %d times\n", cnt);
        } 
        if (t.callback != nullptr) {
          (*t.callback)();
        }
        tp_task_pool_.put_pool.RemoveTask(task.second);
        break;
      }
      case tp_task::TP_TASK_TYPE_DELETE: {
        auto& t = tp_task_pool_.delete_pool.GetTask(task.second);
        s = db_->Delete(t.write_options, t.column_family, t.key);
        if (t.callback != nullptr) {
          (*t.callback)();
        }
        tp_task_pool_.delete_pool.RemoveTask(task.second);
        break;
      }
      case tp_task::TP_TASK_TYPE_UPDATE: {
        auto& t = tp_task_pool_.update_pool.GetTask(task.second);
        s = db_->Get(t.read_options, t.column_family, t.key, t.value);
        bool proceed = s.ok() && t.mid_callback != nullptr;
        if (proceed) {
          if ((*t.mid_callback)()) {
            s = db_->Put(t.write_options, t.column_family, t.key,
                         *(t.value));
          }
        }
        if (t.callback != nullptr) {
          (*t.callback)();
        }
        tp_task_pool_.update_pool.RemoveTask(task.second);
        break;
      }
      case tp_task::TP_TASK_TYPE_GET: {
        auto& t = tp_task_pool_.get_pool.GetTask(task.second);

        s = db_->Get(t.read_options, t.column_family, t.key, t.value);
        if (t.callback != nullptr) {
          (*t.callback)();
        }
        tp_task_pool_.get_pool.RemoveTask(task.second);
        break;
      }
      case tp_task::TP_TASK_TYPE_SCAN: {
        auto& t = tp_task_pool_.scan_pool.GetTask(task.second);
        // Perform range scanning using iterators and process each result
        // through callbacks
        auto* it = db_->NewIterator(t.read_options, t.column_family);
        (*t.process_func)(it);
        delete it;
        if (t.callback != nullptr) {
          (*t.callback)();
        }
        tp_task_pool_.scan_pool.RemoveTask(task.second);
        break;
      }
      default:
        break;
    }
    if (!s.ok()) {
      std::printf("Error \"%s\" in TP task\n", s.ToString().c_str());
    }
    task_cnt++;
    if (task_cnt >= target_cnt) {
      break;
    }
  }
  if (task_cnt * 10 > target_cnt) {
    uint64_t end_time = clock_->NowMicros();
    uint64_t elapsed = end_time - start_time;
    if (elapsed < options_.tp_morsel_size * 10 && elapsed > 0) {
      int new_throughput = task_cnt * 2000 / elapsed;
      tp_throughput_ = (tp_throughput_.load() * 8 + new_throughput * 2) / 10;
    }
  }
}

void ElasticLSMImpl::ContinuousTPTask() {
  tp_working_threads_num++;
  while (!closed_) {
    TPTask(-1);
  }
  tp_working_threads_num--;
}

void ElasticLSMImpl::ContinuousCompactionTask(int idx, bool flush) {
  while (!closed_) {
    int cnt = 0;
    while (!stride_schedulars_[idx]->compaction(flush) && cnt < 100) {
      ++cnt;
      _mm_pause();
    }
    if (cnt >= 100) {
      std::unique_lock<std::mutex> lock(compaction_mutex_);
      compaction_cv_.wait(lock);
    }
  }
}

pausable_task ElasticLSMImpl::APTask() {
  // if (task) {
  //   // Perform range scanning using iterators and process each result through
  //   callbacks auto* it = db_->NewIterator(task->read_options,
  //                               task->column_family);
  //   it->Seek(task->key);
  //   int cnt = 0;
  //   while (it->Valid() && cnt < task->record_count) {
  //     auto* ps = new PinnableSlice();
  //     ps->PinSlice(it->value());
  //     task->func(ps);
  //     delete ps;
  //     it->Next();
  //     ++cnt;
  //   }
  //   delete it;
  //   delete task;

  //   // if (ap_task_count_.load(std::memory_order_relaxed) > 0) {
  //   //   ap_thread_pool_->SubmitJob(
  //   //       std::bind(&ElasticLSM::BGAPWork, this));
  //   // }
  // }
  co_return;
}

void ElasticLSMImpl::BGWork(int idx) {
  while (!closed_) {
    stride_schedulars_[idx]->work();
  }
}
void ElasticLSMImpl::UpdateCQEMap(int compaction_id,
                                  std::shared_ptr<compaction_task> task,
                                  AsyncWriteOp* req) {
  auto it = cqe_task_map_.find(compaction_id);
  if (it == cqe_task_map_.end()) {
    if (task != nullptr) {
      it = cqe_task_map_.emplace(compaction_id, cqe_task{0, task, nullptr})
               .first;
    } else {
      it = cqe_task_map_
               .emplace(compaction_id,
                        cqe_task{1, nullptr, req->compaction_write_num_count})
               .first;
    }
  } else {
    if (task != nullptr) {
      it->second.task = task;
    } else {
      it->second.count++;
      if (it->second.compaction_write_num_count == nullptr) {
        it->second.compaction_write_num_count = req->compaction_write_num_count;
      }
    }
  }
  if (it->second.task != nullptr &&
      it->second.compaction_write_num_count != nullptr &&
      it->second.count == it->second.compaction_write_num_count->load()) {
    it->second.task->done_work->resume();
    delete it->second.compaction_write_num_count;
    cqe_task_map_.erase(it);
  }
}
void ElasticLSMImpl::CheckCQE() {
  struct io_uring_cqe* cqe;
  while (io_uring_peek_cqe(ring_, &cqe) == 0) {
    auto* req = static_cast<AsyncWriteOp*>(io_uring_cqe_get_data(cqe));
    if (cqe->res < 0) {
      fprintf(stderr, "write failed: %s\n", strerror(-cqe->res));
    }
    int compaction_id = req->compaction_id;
    UpdateCQEMap(compaction_id, nullptr, req);
    delete req;
    io_uring_cqe_seen(ring_, cqe);
  }
}
void ElasticLSMImpl::BGSchedule() {
  while (!closed_) {
    if (!cqe_task_map_.empty())
      CheckCQE();
    else
      schedule_count_.try_acquire_for(std::chrono::milliseconds(100));
    if (compaction_schedule_count_) {
      compaction_schedule_count_--;
      schedular_.schedule();
    } else if (compaction_done_work_count_) {
      compaction_done_work_count_--;
      int task_id;
      compaction_done_work_queue_.read(task_id);
      if (!compaction_tasks_[task_id]->flush) {
        UpdateCQEMap(compaction_tasks_[task_id]->compaction_id,
                     compaction_tasks_[task_id]);
      }
      schedular_.RemoveTask(task_id);
    } else if (maybe_schedule_count_) {
      maybe_schedule_count_--;
      db_->BackgroundMaybeScheduleFlushOrCompaction();
    }
  }
}

void ElasticLSMImpl::CompactionSchedular::schedule() {
  while (!del_task_list_.isEmpty()) {
    int idx;
    if (del_task_list_.read(idx)) {
      elastic_lsm_->compaction_tasks_mask_ &= ~(1 << idx);
      elastic_lsm_->compaction_tasks_[idx] = nullptr;
    }
  }
  while (!tasks_list_.empty()) {
    int idx = std::countr_one(elastic_lsm_->compaction_tasks_mask_);
    if (idx >= elastic_lsm_->options_.max_compaction_num) break;
    auto task = tasks_list_.top();
    tasks_list_.pop();
    elastic_lsm_->compaction_tasks_[idx] = task.task;
    elastic_lsm_->compaction_tasks_mask_ |= 1 << idx;
    for (auto& i : elastic_lsm_->stride_schedulars_) {
      i->NewTask(idx);
    }
    elastic_lsm_->compaction_cv_.notify_all();
  }
}
void ElasticLSMImpl::StrideSchedular::sync() {
  while (new_task_mask_ != 0) {
    int idx = std::countr_zero(new_task_mask_.load());
    new_task_mask_.fetch_and(~(1 << idx));
    active_task_mask_ |= 1 << idx;
    compaction_tasks_[idx] = elastic_lsm_->compaction_tasks_[idx];
    if (compaction_tasks_[idx] == nullptr) continue;
    pass_[idx] = global_pass_;
    task_queue_.push(idx);
    if (compaction_tasks_[idx]->priority == 0) {
      pass_[idx] = -1000.0;
    }
  }
}

bool ElasticLSMImpl::StrideSchedular::work() {
  sync();

  // do high proirity task first
  if (docompaction(true)) {
    return true;
  }

  // if tptask queue pressure is high
  if (elastic_lsm_->CalcIfNeedTP()) {
    elastic_lsm_->tp_working_threads_num++;
    elastic_lsm_->TPTask(local_idx_);
    elastic_lsm_->tp_working_threads_num--;
    return true;
  }

  // do compaction
  if (docompaction(false)) {
    return true;
  }

  // nothing to do, try tp work
  if (elastic_lsm_->HasAnyTPTask()) {
    elastic_lsm_->tp_working_threads_num++;
    elastic_lsm_->TPTask(local_idx_);
    elastic_lsm_->tp_working_threads_num--;
    return true;
  }

  // No work found
  return false;
}
bool ElasticLSMImpl::StrideSchedular::compaction(bool flush) {
  sync();
  return docompaction(flush);
}
void ElasticLSMImpl::StrideSchedular::remove_done_work(int idx) {
  compaction_tasks_[idx] = nullptr;
  to_reinsert_task_queue_.pop_back();
}
bool ElasticLSMImpl::StrideSchedular::docompaction(bool highpriority) {
  assert(to_reinsert_task_queue_.empty());
  while (!task_queue_.empty()) {
    int idx = task_queue_.top();
    auto& task = compaction_tasks_[idx];
    if (task == nullptr) {
      task_queue_.pop();
      continue;
    }
    if (highpriority && task->priority != 0) {
      break;
    }
    task_queue_.pop();
    if (task->done) {
      compaction_tasks_[idx] = nullptr;
      continue;
    }
    to_reinsert_task_queue_.push_back(idx);
    auto mask = task->coro_works_mask.load();
    int coro_idx = std::countr_one(mask);
    if (coro_idx >= task->coro_works_count) {
      if (task->priority == 0) {
        remove_done_work(idx);
      }
      continue;
    }
    if (task->coro_works_mask.compare_exchange_weak(mask,
                                                    mask | (1 << coro_idx))) {
      // work
      while (!task->coro_handles[coro_idx].resume() && task->priority == 0) {
        // if preempt, keep working
      }
      if (task->coro_handles[coro_idx].done()) {
        if (task->done_works_count.fetch_add(1) == task->coro_works_count - 1) {
          task->done = true;
          remove_done_work(idx);
          elastic_lsm_->compaction_done_work_queue_.write(idx);
          elastic_lsm_->NotifyBGThreadCompactionDoneWork();
        }
      } else {
        task->coro_works_mask.fetch_and(~(1 << coro_idx));
      }
      put_back_task();
      return true;
    }
  }
  put_back_task();
  return false;
}
void ElasticLSMImpl::StrideSchedular::put_back_task() {
  while (!task_queue_.empty()) {
    auto& task = compaction_tasks_[task_queue_.top()];
    if (task != nullptr) {
      if (task->priority != 0) {
        break;
      }
      to_reinsert_task_queue_.push_back(task_queue_.top());
    }
    task_queue_.pop();
  }
  if (!task_queue_.empty())
    global_pass_ = pass_[task_queue_.top()];
  else
    global_pass_ = std::numeric_limits<double>::max();
  for (auto idx : to_reinsert_task_queue_) {
    auto& task = compaction_tasks_[idx];
    if (task != nullptr) {
      if (task->priority > 0) {
        pass_[idx] += 1.0 / task->priority;
        global_pass_ = std::min(global_pass_, pass_[idx]);
      }
      task_queue_.push(idx);
    }
  }
  to_reinsert_task_queue_.clear();
  if (global_pass_ == std::numeric_limits<double>::max()) {
    global_pass_ = 0;
  }
}
}  // namespace ROCKSDB_NAMESPACE