#pragma once

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {
struct ElasticLSMOptions {
  int max_background_threads = 16;
  int min_tp_threads = 2;
  int min_ap_threads = 2;
  int min_compaction_threads = 2;
  int max_tp_task_queue = 1024;
  int max_compaction_num = 32;
  uint64_t compaction_morsel_size = 2000;  // microseconds
  uint64_t tp_morsel_size = 2000;  // microseconds
};
class ElasticLSM {
 public:
  ElasticLSM() {};
  ElasticLSM(const ElasticLSM&) = delete;
  void operator=(const ElasticLSM&) = delete;

  virtual ~ElasticLSM();

  static Status Open(const Options& options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& name,
                     std::unique_ptr<ElasticLSM>* dbptr);
  static Status Open(const Options& options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& name,
                     ElasticLSM** dbptr) {
    std::unique_ptr<ElasticLSM> smart_ptr;
    Status s = Open(options, elastic_options, name, &smart_ptr);
    *dbptr = smart_ptr.release();
    return s;
  }
  static Status Open(const DBOptions& db_options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     std::unique_ptr<ElasticLSM>* dbptr);
  static Status Open(const DBOptions& db_options,
                     const ElasticLSMOptions& elastic_options,
                     const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     ElasticLSM** dbptr) {
    std::unique_ptr<ElasticLSM> smart_ptr;
    Status s = Open(db_options, elastic_options, name, column_families,
                    handles, &smart_ptr);
    *dbptr = smart_ptr.release();
    return s;
  }

  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value, std::function<void()>* callback) = 0;
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value,
             std::function<void()>* callback) {
    return Put(options, DefaultColumnFamily(), key, value, callback);
  }

  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        std::function<void()>* callback) = 0;
  Status Delete(const WriteOptions& options, const Slice& key,
                std::function<void()>* callback) {
    return Delete(options, DefaultColumnFamily(), key, callback);
  }

  virtual Status Update(const ReadOptions& _read_options,
                        const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        std::string* value, std::function<bool()>* mid_callback,
                        std::function<void()>* callback) = 0;
  Status Update(const ReadOptions& _read_options, const WriteOptions& options,
                const Slice& key, std::string* value,
                std::function<bool()>* mid_callback,
                std::function<void()>* callback) {
    return Update(_read_options, options, DefaultColumnFamily(), key, value,
                  mid_callback, callback);
  }

  virtual Status Get(const ReadOptions& _read_options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value, std::function<void()>* callback) = 0;
  Status Get(const ReadOptions& _read_options, const Slice& key,
             std::string* value, std::function<void()>* callback) {
    return Get(_read_options, DefaultColumnFamily(), key, value, callback);
  }

  virtual Status Scan(const ReadOptions& _read_options,
                      ColumnFamilyHandle* column_family, 
                      std::function<void(rocksdb::Iterator *)>* func,
                      std::function<void()>* callback) = 0;
  Status Scan(const ReadOptions& _read_options,
              std::function<void(rocksdb::Iterator *)>* func,
              std::function<void()>* callback) {
    return Scan(_read_options, DefaultColumnFamily(), func, callback);
  }

  Status DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
    if (DefaultColumnFamily() == column_family) {
      return Status::InvalidArgument(
          "Cannot destroy the handle returned by DefaultColumnFamily()");
    }
    delete column_family;
    return Status::OK();
  }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const = 0;
};
}  // namespace ROCKSDB_NAMESPACE