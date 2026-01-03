#include "db/elastic/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE {
ElasticLSM::~ElasticLSM() = default;
Status ElasticLSM::Open(const Options& options,
                        const ElasticLSMOptions& elastic_options,
                        const std::string& name,
                        std::unique_ptr<ElasticLSM>* dbptr) {
  return ElasticLSMImpl::Open(
      options, elastic_options, name,
      std::vector<ColumnFamilyDescriptor>{ColumnFamilyDescriptor()}, nullptr,
      dbptr);
}

Status ElasticLSMImpl::Open(
    const DBOptions& db_options, const ElasticLSMOptions& elastic_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    std::unique_ptr<ElasticLSM>* dbptr) {
  std::unique_ptr<DB> db;
  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  bool can_retry = false;
  Status s;
  do {
    s = DBImpl::Open(db_options, dbname, column_families, handles, &db,
                     !kSeqPerBatch, kBatchPerTxn, can_retry, &can_retry);
  } while (!s.ok() && can_retry);

  if (!s.ok()) {
    return s;
  }

  DBImpl* dbelastic = dynamic_cast<DBImpl*>(db.get());
  if (!dbelastic) {
    return Status::InvalidArgument("Underlying DB is not DBImpl");
  }
  db.release();

  // Wrap in ElasticLSM
  ElasticLSMImpl* elastic = new ElasticLSMImpl(
      elastic_options, dbelastic, dbelastic->GetFileSystem()->GetIoUring());
  dbelastic->elastic_lsm_impl_ = elastic;

  // Return as DB interface
  dbptr->reset(elastic);

  return Status::OK();
}

ElasticLSMImpl::ElasticLSMImpl(const ElasticLSMOptions& elastic_options,
                               DBImpl* dbelastic, io_uring* ring)
    : options_(elastic_options),
      db_(dbelastic),
      // thread_pool_(),
      clock_(dbelastic->GetSystemClock()),
      tp_task_queue_(131072),
      ap_task_queue_(131072),
      ring_(ring),
      schedular_(this) {}

ElasticLSMImpl::~ElasticLSMImpl() {
  closed_ = true;
  schedule_count_.release();
  // for (auto& t : thread_pool_) {
  //   if (t.joinable()) {
  //     t.join();
  //   }
  // }
}

Status ElasticLSMImpl::Put(const WriteOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           const Slice& value) {
  auto t = new put_task(options, column_family, key, value);
  tp_task_queue_.blockingWrite(t);
  return Status::OK();
}

Status ElasticLSMImpl::Delete(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) {
  auto t = new delete_task(options, column_family, key);
  tp_task_queue_.blockingWrite(t);
  return Status::OK();
}

Status ElasticLSMImpl::Update(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) {
  auto t = new update_task(options, column_family, key, value);
  tp_task_queue_.blockingWrite(t);
  return Status::OK();
}

Status ElasticLSMImpl::Get(const ReadOptions& _read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value) {
  auto t = new get_task(_read_options, column_family, key, value);
  tp_task_queue_.blockingWrite(t);
  return Status::OK();
}

Status ElasticLSMImpl::Scan(const ReadOptions& _read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            int record_count,
                            std::vector<std::string>* answer) {
  auto t =
      new scan_task(_read_options, column_family, key, record_count, answer);
  tp_task_queue_.blockingWrite(t);
  return Status::OK();
}

void ElasticLSMImpl::TPTask() {
  // Pop up a task without locking
  tp_working_threads_num++;
  tp_task* task = nullptr;
  int task_cnt = 0;
  int target_cnt = tp_throughput_;
  uint64_t start_time = clock_->NowMicros();
  while (true) {
    auto bo = tp_task_queue_.read(task);
    if (!bo) break;
    switch (task->tp_type) {
      case tp_task::TP_TASK_TYPE_PUT: {
        auto* t = static_cast<put_task*>(task);
        db_->Put(t->write_options, t->column_family, t->key, t->value);
        break;
      }
      case tp_task::TP_TASK_TYPE_DELETE: {
        auto* t = static_cast<delete_task*>(task);
        db_->Delete(t->write_options, t->column_family, t->key);
        break;
      }
      case tp_task::TP_TASK_TYPE_UPDATE: {
        auto* t = static_cast<update_task*>(task);
        db_->Put(t->write_options, t->column_family, t->key, t->value);
        break;
      }
      case tp_task::TP_TASK_TYPE_GET: {
        auto* t = static_cast<get_task*>(task);
        db_->Get(t->read_options, t->column_family, t->key, t->value);
        break;
      }
      case tp_task::TP_TASK_TYPE_SCAN: {
        auto* t = static_cast<scan_task*>(task);
        // Perform range scanning using iterators and process each result
        // through callbacks
        auto* it = db_->NewIterator(t->read_options, t->column_family);
        it->Seek(t->key);
        int cnt = 0;
        while (it->Valid() && cnt < t->record_count) {
          t->answer->push_back(it->value().ToString());
          it->Next();
          ++cnt;
        }
        delete it;
        break;
      }
      default:
        break;
    }
    delete task;
    task_cnt++;
    if (task_cnt >= target_cnt) {
      task_cnt = 0;
      break;
    }
  }
  if (task_cnt * 10 > target_cnt) {
    uint64_t end_time = clock_->NowMicros();
    uint64_t elapsed = end_time - start_time;
    if (elapsed < 500000) {
      int new_throughput = task_cnt * 2000 / elapsed;
      tp_throughput_ = (tp_throughput_.load() * 8 + new_throughput * 2) / 10;
    }
  }
  tp_working_threads_num--;
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
    stride_schedulars_[idx].work();
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
    auto it = cqe_task_map_.find(compaction_id);
    it->second.count++;
    if (it->second.count == req->compaction_write_num_count->load()) {
      it->second.task->done_work->resume();
      cqe_task_map_.erase(it);
      delete req->compaction_write_num_count;
    }
    delete req;
    io_uring_cqe_seen(ring_, cqe);
  }
}
void ElasticLSMImpl::BGSchedule() {
  while (!closed_) {
    schedule_count_.acquire();
    if (compaction_schedule_count_) {
      compaction_schedule_count_--;
      schedular_.schedule();
    } else if (compaction_done_work_count_) {
      compaction_done_work_count_--;
      int task_id;
      compaction_done_work_queue_.read(task_id);
      int compaction_id = compaction_tasks_[task_id]->compaction_id;
      cqe_task_map_.emplace(compaction_id,
                            cqe_task{0, compaction_tasks_[task_id]});
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
      i.NewTask(idx);
    }
  }
}
void ElasticLSMImpl::StrideSchedular::sync() {
  while (new_task_mask_ != 0) {
    int idx = std::countr_zero(new_task_mask_.load());
    compaction_tasks_[idx] = elastic_lsm_->compaction_tasks_[idx];
    pass_[idx] = global_pass_;
    task_queue_.push(idx);
    active_task_mask_ |= 1 << idx;
    new_task_mask_.fetch_and(~(1 << idx));
    if (compaction_tasks_[idx]->priority == 0) {
      pass_[idx] -= 1000.0;
    }
  }
}

void ElasticLSMImpl::StrideSchedular::work() {
  sync();
  // do high proirity task first
  if (docompaction(true)) return;
  // if tptask queue is more than
  if (elastic_lsm_->CalcIfNeedTP()) {
    elastic_lsm_->TPTask();
    return;
  }
  // do compaction
  if (docompaction(false)) return;
  // nothing to do, do tp
  elastic_lsm_->TPTask();
  // or sleep
}
bool ElasticLSMImpl::StrideSchedular::docompaction(bool highpriority) {
  while (!task_queue_.empty()) {
    int idx = task_queue_.top();
    if (highpriority && compaction_tasks_[idx]->priority != 0) {
      break;
    }
    task_queue_.pop();
    to_reinsert_task_queue_.push_back(idx);
    auto task = compaction_tasks_[idx];
    if (task->done) {
      active_task_mask_ &= ~(1 << idx);
      compaction_tasks_[idx] = nullptr;
      to_reinsert_task_queue_.pop_back();
      continue;
    }
    auto mask = task->coro_works_mask.load();
    int coro_idx = std::countr_one(mask);
    if (coro_idx >= task->coro_works_count) continue;
    if (task->coro_works_mask.compare_exchange_weak(mask,
                                                    mask | (1 << coro_idx))) {
      // work
      task->coro_handles[coro_idx].resume();
      if (task->coro_handles[coro_idx].done()) {
        if (task->done_works_count.fetch_add(1) == task->coro_works_count - 1) {
          task->done = true;
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
  while (!task_queue_.empty() &&
         compaction_tasks_[task_queue_.top()]->priority == 0) {
    to_reinsert_task_queue_.push_back(task_queue_.top());
    task_queue_.pop();
  }
  if (!task_queue_.empty())
    global_pass_ = pass_[task_queue_.top()];
  else
    global_pass_ = std::numeric_limits<double>::max();
  while (!to_reinsert_task_queue_.empty()) {
    int idx = to_reinsert_task_queue_.front();
    if (compaction_tasks_[idx]->priority > 0) {
      pass_[idx] += 1.0 / compaction_tasks_[idx]->priority;
      global_pass_ = std::min(global_pass_, pass_[idx]);
    }
    to_reinsert_task_queue_.pop_front();
    task_queue_.push(idx);
  }
  if (global_pass_ == std::numeric_limits<double>::max()) {
    global_pass_ = 0;
  }
}
}  // namespace ROCKSDB_NAMESPACE