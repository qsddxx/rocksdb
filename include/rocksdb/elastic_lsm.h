#pragma once

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE 
{
  struct ElasticLSMOptions
  {
    int max_background_threads = 16;
    int min_tp_threads = 2;
    int min_ap_threads = 2;
    int min_compaction_threads = 2;
    int max_tp_task_queue = 1024;
    int max_compaction_num = 32;
  };
  class ElasticLSM
  {
  public:
    ElasticLSM() {};
    ElasticLSM(const ElasticLSM&) = delete;
    void operator=(const ElasticLSM&) = delete;

    virtual ~ElasticLSM();

    static Status Open(const Options& options, const ElasticLSMOptions& elastic_options,
      const std::string& name, std::unique_ptr<ElasticLSM>* dbptr);

    virtual Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value,
      std::function<void()>* callback) = 0;
    Status Put(const WriteOptions& options, const Slice& key, const Slice& value,
      std::function<void()>* callback) {
      return Put(options, DefaultColumnFamily(), key, value, callback);
    }

    virtual Status Delete(const WriteOptions& options,
      ColumnFamilyHandle* column_family,
      const Slice& key,
      std::function<void()>* callback) = 0;
    Status Delete(const WriteOptions& options, const Slice& key,
      std::function<void()>* callback) {
      return Delete(options, DefaultColumnFamily(), key, callback);
    }

    virtual Status Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value,
      std::function<void()>* callback) = 0;
    Status Update(const WriteOptions& options,
      const Slice& key, const Slice& value,
      std::function<void()>* callback) {
      return Update(options, DefaultColumnFamily(), key, value, callback);
    }

    virtual Status Get(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice& key,
      std::string* value,
      std::function<void()>* callback) = 0;
    Status Get(const ReadOptions& _read_options, const Slice& key,
      std::string* value,
      std::function<void()>* callback) {
      return Get(_read_options, DefaultColumnFamily(), key, value, callback);
    }

    virtual Status Scan(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice &key, int record_count,
      std::vector<std::string>* answer,
      std::function<void()>* callback) = 0;
    Status Scan(const ReadOptions& _read_options, const Slice &key, int record_count,
      std::vector<std::string>* answer,
      std::function<void()>* callback) {
      return Scan(_read_options, DefaultColumnFamily(), key, record_count, answer, callback);
    }

    virtual ColumnFamilyHandle* DefaultColumnFamily() const = 0;
  };
}