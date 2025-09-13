//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/blob/blob_index.h"
#include "db/version_set.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {}  // anonymous namespace

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

Status FileMetaData::UpdateBoundaries(const Slice& key, const Slice& value,
                                      SequenceNumber seqno,
                                      ValueType value_type) {
  if (value_type == kTypeBlobIndex) {
    BlobIndex blob_index;
    const Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }

    if (!blob_index.IsInlined() && !blob_index.HasTTL()) {
      if (blob_index.file_number() == kInvalidBlobFileNumber) {
        return Status::Corruption("Invalid blob file number");
      }

      if (oldest_blob_file_number == kInvalidBlobFileNumber ||
          oldest_blob_file_number > blob_index.file_number()) {
        oldest_blob_file_number = blob_index.file_number();
      }
    }
  }

  if (smallest.size() == 0) {
    smallest.DecodeFrom(key);
  }
  largest.DecodeFrom(key);
  fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
  fd.largest_seqno = std::max(fd.largest_seqno, seqno);

  return Status::OK();
}

void VersionEdit::Clear() { *this = VersionEdit(); }

bool VersionEdit::EncodeTo(std::string* dst,
                           std::optional<size_t> ts_sz) const {
  assert(!IsNoManifestWriteDummy());
  if (has_db_id_) {
    PutVarint32(dst, kDbId);
    PutLengthPrefixedSlice(dst, db_id_);
  }
  if (has_comparator_) {
    assert(has_persist_user_defined_timestamps_);
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32Varint64(dst, kLogNumber, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32Varint64(dst, kPrevLogNumber, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }
  if (has_max_column_family_) {
    PutVarint32Varint32(dst, kMaxColumnFamily, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    PutVarint32Varint64(dst, kMinLogNumberToKeep, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  for (size_t i = 0; i < compact_cursors_.size(); i++) {
    if (compact_cursors_[i].second.Valid()) {
      PutVarint32(dst, kCompactCursor);
      PutVarint32(dst, compact_cursors_[i].first);  // level
      PutLengthPrefixedSlice(dst, compact_cursors_[i].second.Encode());
    }
  }
  for (const auto& deleted : deleted_files_) {
    PutVarint32Varint32Varint64(dst, kDeletedFile, deleted.first /* level */,
                                deleted.second /* file number */);
  }

  bool min_log_num_written = false;

  assert(new_files_.empty() || ts_sz.has_value());
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    if (!f.smallest.Valid() || !f.largest.Valid() ||
        f.epoch_number == kUnknownEpochNumber) {
      return false;
    }
    PutVarint32(dst, kNewFile4);
    PutVarint32Varint64(dst, new_files_[i].first /* level */, f.fd.GetNumber());
    PutVarint64(dst, f.fd.GetFileSize());
    EncodeFileBoundaries(dst, f, ts_sz.value());
    PutVarint64Varint64(dst, f.fd.smallest_seqno, f.fd.largest_seqno);
    // Customized fields' format:
    // +-----------------------------+
    // | 1st field's tag (varint32)  |
    // +-----------------------------+
    // | 1st field's size (varint32) |
    // +-----------------------------+
    // |    bytes for 1st field      |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // |                             |
    // |          ......             |
    // |                             |
    // +-----------------------------+
    // | last field's size (varint32)|
    // +-----------------------------+
    // |    bytes for last field     |
    // |  (based on size decoded)    |
    // +-----------------------------+
    // | terminating tag (varint32)  |
    // +-----------------------------+
    //
    // Customized encoding for fields:
    //   tag kPathId: 1 byte as path_id
    //   tag kNeedCompaction:
    //        now only can take one char value 1 indicating need-compaction
    //
    PutVarint32(dst, NewFileCustomTag::kOldestAncesterTime);
    std::string varint_oldest_ancester_time;
    PutVarint64(&varint_oldest_ancester_time, f.oldest_ancester_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintOldestAncesterTime",
                             &varint_oldest_ancester_time);
    PutLengthPrefixedSlice(dst, Slice(varint_oldest_ancester_time));

    PutVarint32(dst, NewFileCustomTag::kFileCreationTime);
    std::string varint_file_creation_time;
    PutVarint64(&varint_file_creation_time, f.file_creation_time);
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:VarintFileCreationTime",
                             &varint_file_creation_time);
    PutLengthPrefixedSlice(dst, Slice(varint_file_creation_time));

    PutVarint32(dst, NewFileCustomTag::kEpochNumber);
    std::string varint_epoch_number;
    PutVarint64(&varint_epoch_number, f.epoch_number);
    PutLengthPrefixedSlice(dst, Slice(varint_epoch_number));

    if (f.file_checksum_func_name != kUnknownFileChecksumFuncName) {
      PutVarint32(dst, NewFileCustomTag::kFileChecksum);
      PutLengthPrefixedSlice(dst, Slice(f.file_checksum));

      PutVarint32(dst, NewFileCustomTag::kFileChecksumFuncName);
      PutLengthPrefixedSlice(dst, Slice(f.file_checksum_func_name));
    }

    if (f.fd.GetPathId() != 0) {
      PutVarint32(dst, NewFileCustomTag::kPathId);
      char p = static_cast<char>(f.fd.GetPathId());
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.temperature != Temperature::kUnknown) {
      PutVarint32(dst, NewFileCustomTag::kTemperature);
      char p = static_cast<char>(f.temperature);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (f.marked_for_compaction) {
      PutVarint32(dst, NewFileCustomTag::kNeedCompaction);
      char p = static_cast<char>(1);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    if (has_min_log_number_to_keep_ && !min_log_num_written) {
      PutVarint32(dst, NewFileCustomTag::kMinLogNumberToKeepHack);
      std::string varint_log_number;
      PutFixed64(&varint_log_number, min_log_number_to_keep_);
      PutLengthPrefixedSlice(dst, Slice(varint_log_number));
      min_log_num_written = true;
    }
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      PutVarint32(dst, NewFileCustomTag::kOldestBlobFileNumber);
      std::string oldest_blob_file_number;
      PutVarint64(&oldest_blob_file_number, f.oldest_blob_file_number);
      PutLengthPrefixedSlice(dst, Slice(oldest_blob_file_number));
    }
    UniqueId64x2 unique_id = f.unique_id;
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:UniqueId", &unique_id);
    if (unique_id != kNullUniqueId64x2) {
      PutVarint32(dst, NewFileCustomTag::kUniqueId);
      std::string unique_id_str = EncodeUniqueIdBytes(&unique_id);
      PutLengthPrefixedSlice(dst, Slice(unique_id_str));
    }
    if (f.compensated_range_deletion_size) {
      PutVarint32(dst, kCompensatedRangeDeletionSize);
      std::string compensated_range_deletion_size;
      PutVarint64(&compensated_range_deletion_size,
                  f.compensated_range_deletion_size);
      PutLengthPrefixedSlice(dst, Slice(compensated_range_deletion_size));
    }
    if (f.tail_size) {
      PutVarint32(dst, NewFileCustomTag::kTailSize);
      std::string varint_tail_size;
      PutVarint64(&varint_tail_size, f.tail_size);
      PutLengthPrefixedSlice(dst, Slice(varint_tail_size));
    }
    if (!f.user_defined_timestamps_persisted) {
      // The default value for the flag is true, it's only explicitly persisted
      // when it's false. We are putting 0 as the value here to signal false
      // (i.e. UDTS not persisted).
      PutVarint32(dst, NewFileCustomTag::kUserDefinedTimestampsPersisted);
      char p = static_cast<char>(0);
      PutLengthPrefixedSlice(dst, Slice(&p, 1));
    }
    TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                             dst);

    PutVarint32(dst, NewFileCustomTag::kTerminate);
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    PutVarint32(dst, kBlobFileAddition);
    blob_file_addition.EncodeTo(dst);
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    PutVarint32(dst, kBlobFileGarbage);
    blob_file_garbage.EncodeTo(dst);
  }

  for (const auto& wal_addition : wal_additions_) {
    PutVarint32(dst, kWalAddition2);
    std::string encoded;
    wal_addition.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  if (!wal_deletion_.IsEmpty()) {
    PutVarint32(dst, kWalDeletion2);
    std::string encoded;
    wal_deletion_.EncodeTo(&encoded);
    PutLengthPrefixedSlice(dst, encoded);
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32Varint32(dst, kColumnFamily, column_family_);
  }

  if (is_column_family_add_) {
    PutVarint32(dst, kColumnFamilyAdd);
    PutLengthPrefixedSlice(dst, Slice(column_family_name_));
  }

  if (is_column_family_drop_) {
    PutVarint32(dst, kColumnFamilyDrop);
  }

  if (is_in_atomic_group_) {
    PutVarint32(dst, kInAtomicGroup);
    PutVarint32(dst, remaining_entries_);
  }

  if (HasFullHistoryTsLow()) {
    PutVarint32(dst, kFullHistoryTsLow);
    PutLengthPrefixedSlice(dst, full_history_ts_low_);
  }

  if (HasPersistUserDefinedTimestamps()) {
    // persist_user_defined_timestamps flag should be logged in the same
    // VersionEdit as the user comparator name.
    assert(has_comparator_);
    PutVarint32(dst, kPersistUserDefinedTimestamps);
    char p = static_cast<char>(persist_user_defined_timestamps_);
    PutLengthPrefixedSlice(dst, Slice(&p, 1));
  }
  return true;
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return dst->Valid();
  } else {
    return false;
  }
}

bool VersionEdit::GetLevel(Slice* input, int* level, const char** /*msg*/) {
  uint32_t v = 0;
  if (GetVarint32(input, &v)) {
    *level = v;
    if (max_level_ < *level) {
      max_level_ = *level;
    }
    return true;
  } else {
    return false;
  }
}

const char* VersionEdit::DecodeNewFile4From(Slice* input) {
  const char* msg = nullptr;
  int level = 0;
  FileMetaData f;
  uint64_t number = 0;
  uint32_t path_id = 0;
  uint64_t file_size = 0;
  SequenceNumber smallest_seqno = 0;
  SequenceNumber largest_seqno = kMaxSequenceNumber;
  if (GetLevel(input, &level, &msg) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) && GetInternalKey(input, &f.smallest) &&
      GetInternalKey(input, &f.largest) &&
      GetVarint64(input, &smallest_seqno) &&
      GetVarint64(input, &largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag = 0;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
        break;
      }
      if (!GetLengthPrefixedSlice(input, &field)) {
        return "new-file4 custom field length prefixed slice error";
      }
      switch (custom_tag) {
        case kPathId:
          if (field.size() != 1) {
            return "path_id field wrong size";
          }
          path_id = field[0];
          if (path_id > 3) {
            return "path_id wrong vaue";
          }
          break;
        case kOldestAncesterTime:
          if (!GetVarint64(&field, &f.oldest_ancester_time)) {
            return "invalid oldest ancester time";
          }
          break;
        case kFileCreationTime:
          if (!GetVarint64(&field, &f.file_creation_time)) {
            return "invalid file creation time";
          }
          break;
        case kEpochNumber:
          if (!GetVarint64(&field, &f.epoch_number)) {
            return "invalid epoch number";
          }
          break;
        case kFileChecksum:
          f.file_checksum = field.ToString();
          break;
        case kFileChecksumFuncName:
          f.file_checksum_func_name = field.ToString();
          break;
        case kNeedCompaction:
          if (field.size() != 1) {
            return "need_compaction field wrong size";
          }
          f.marked_for_compaction = (field[0] == 1);
          break;
        case kMinLogNumberToKeepHack:
          // This is a hack to encode kMinLogNumberToKeep in a
          // forward-compatible fashion.
          if (!GetFixed64(&field, &min_log_number_to_keep_)) {
            return "deleted log number malformatted";
          }
          has_min_log_number_to_keep_ = true;
          break;
        case kOldestBlobFileNumber:
          if (!GetVarint64(&field, &f.oldest_blob_file_number)) {
            return "invalid oldest blob file number";
          }
          break;
        case kTemperature:
          if (field.size() != 1) {
            return "temperature field wrong size";
          } else {
            Temperature casted_field = static_cast<Temperature>(field[0]);
            if (casted_field <= Temperature::kCold) {
              f.temperature = casted_field;
            }
          }
          break;
        case kUniqueId:
          if (!DecodeUniqueIdBytes(field.ToString(), &f.unique_id).ok()) {
            f.unique_id = kNullUniqueId64x2;
            return "invalid unique id";
          }
          break;
        case kCompensatedRangeDeletionSize:
          if (!GetVarint64(&field, &f.compensated_range_deletion_size)) {
            return "Invalid compensated range deletion size";
          }
          break;
        case kTailSize:
          if (!GetVarint64(&field, &f.tail_size)) {
            return "invalid tail start offset";
          }
          break;
        case kUserDefinedTimestampsPersisted:
          if (field.size() != 1) {
            return "user-defined timestamps persisted field wrong size";
          }
          f.user_defined_timestamps_persisted = (field[0] == 1);
          break;
        default:
          if ((custom_tag & kCustomTagNonSafeIgnoreMask) != 0) {
            // Should not proceed if cannot understand it
            return "new-file4 custom field not supported";
          }
          break;
      }
    }
  } else {
    return "new-file4 entry";
  }
  f.fd =
      FileDescriptor(number, path_id, file_size, smallest_seqno, largest_seqno);
  new_files_.push_back(std::make_pair(level, f));
  return nullptr;
}

void VersionEdit::EncodeFileBoundaries(std::string* dst,
                                       const FileMetaData& meta,
                                       size_t ts_sz) const {
  if (ts_sz == 0 || meta.user_defined_timestamps_persisted) {
    PutLengthPrefixedSlice(dst, meta.smallest.Encode());
    PutLengthPrefixedSlice(dst, meta.largest.Encode());
    return;
  }
  std::string smallest_buf;
  std::string largest_buf;
  StripTimestampFromInternalKey(&smallest_buf, meta.smallest.Encode(), ts_sz);
  StripTimestampFromInternalKey(&largest_buf, meta.largest.Encode(), ts_sz);
  PutLengthPrefixedSlice(dst, smallest_buf);
  PutLengthPrefixedSlice(dst, largest_buf);
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
#ifndef NDEBUG
  bool ignore_ignorable_tags = false;
  TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:IgnoreIgnorableTags",
                           &ignore_ignorable_tags);
#endif
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag = 0;

  // Temporary storage for parsing
  int level = 0;
  FileMetaData f;
  Slice str;
  InternalKey key;
  while (msg == nullptr && GetVarint32(&input, &tag)) {
#ifndef NDEBUG
    if (ignore_ignorable_tags && tag > kTagSafeIgnoreMask) {
      tag = kTagSafeIgnoreMask;
    }
#endif
    switch (tag) {
      case kDbId:
        if (GetLengthPrefixedSlice(&input, &str)) {
          db_id_ = str.ToString();
          has_db_id_ = true;
        } else {
          msg = "db id";
        }
        break;
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kMaxColumnFamily:
        if (GetVarint32(&input, &max_column_family_)) {
          has_max_column_family_ = true;
        } else {
          msg = "max column family";
        }
        break;

      case kMinLogNumberToKeep:
        if (GetVarint64(&input, &min_log_number_to_keep_)) {
          has_min_log_number_to_keep_ = true;
        } else {
          msg = "min log number to kee";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactCursor:
        if (GetLevel(&input, &level, &msg) && GetInternalKey(&input, &key)) {
          // Here we re-use the output format of compact pointer in LevelDB
          // to persist compact_cursors_
          compact_cursors_.push_back(std::make_pair(level, key));
        } else {
          if (!msg) {
            msg = "compaction cursor";
          }
        }
        break;

      case kDeletedFile: {
        uint64_t number = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          if (!msg) {
            msg = "deleted file";
          }
        }
        break;
      }

      case kNewFile: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file entry";
          }
        }
        break;
      }
      case kNewFile2: {
        uint64_t number = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, 0, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file2 entry";
          }
        }
        break;
      }

      case kNewFile3: {
        uint64_t number = 0;
        uint32_t path_id = 0;
        uint64_t file_size = 0;
        SequenceNumber smallest_seqno = 0;
        SequenceNumber largest_seqno = kMaxSequenceNumber;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint32(&input, &path_id) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest) &&
            GetVarint64(&input, &smallest_seqno) &&
            GetVarint64(&input, &largest_seqno)) {
          f.fd = FileDescriptor(number, path_id, file_size, smallest_seqno,
                                largest_seqno);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file3 entry";
          }
        }
        break;
      }

      case kNewFile4: {
        msg = DecodeNewFile4From(&input);
        break;
      }

      case kBlobFileAddition:
      case kBlobFileAddition_DEPRECATED: {
        BlobFileAddition blob_file_addition;
        const Status s = blob_file_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFile(std::move(blob_file_addition));
        break;
      }

      case kBlobFileGarbage:
      case kBlobFileGarbage_DEPRECATED: {
        BlobFileGarbage blob_file_garbage;
        const Status s = blob_file_garbage.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        AddBlobFileGarbage(std::move(blob_file_garbage));
        break;
      }

      case kWalAddition: {
        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalAddition2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalAddition not prefixed by length";
          break;
        }

        WalAddition wal_addition;
        const Status s = wal_addition.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_additions_.emplace_back(std::move(wal_addition));
        break;
      }

      case kWalDeletion: {
        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&input);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kWalDeletion2: {
        Slice encoded;
        if (!GetLengthPrefixedSlice(&input, &encoded)) {
          msg = "WalDeletion not prefixed by length";
          break;
        }

        WalDeletion wal_deletion;
        const Status s = wal_deletion.DecodeFrom(&encoded);
        if (!s.ok()) {
          return s;
        }

        wal_deletion_ = std::move(wal_deletion);
        break;
      }

      case kColumnFamily:
        if (!GetVarint32(&input, &column_family_)) {
          if (!msg) {
            msg = "set column family id";
          }
        }
        break;

      case kColumnFamilyAdd:
        if (GetLengthPrefixedSlice(&input, &str)) {
          is_column_family_add_ = true;
          column_family_name_ = str.ToString();
        } else {
          if (!msg) {
            msg = "column family add";
          }
        }
        break;

      case kColumnFamilyDrop:
        is_column_family_drop_ = true;
        break;

      case kInAtomicGroup:
        is_in_atomic_group_ = true;
        if (!GetVarint32(&input, &remaining_entries_)) {
          if (!msg) {
            msg = "remaining entries";
          }
        }
        break;

      case kFullHistoryTsLow:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "full_history_ts_low";
        } else if (str.empty()) {
          msg = "full_history_ts_low: empty";
        } else {
          full_history_ts_low_.assign(str.data(), str.size());
        }
        break;

      case kPersistUserDefinedTimestamps:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          msg = "persist_user_defined_timestamps";
        } else if (str.size() != 1) {
          msg = "persist_user_defined_timestamps field wrong size";
        } else {
          persist_user_defined_timestamps_ = (str[0] == 1);
          has_persist_user_defined_timestamps_ = true;
        }
        break;

      default:
        if (tag & kTagSafeIgnoreMask) {
          // Tag from future which can be safely ignored.
          // The next field must be the length of the entry.
          uint32_t field_len;
          if (!GetVarint32(&input, &field_len) ||
              static_cast<size_t>(field_len) > input.size()) {
            if (!msg) {
              msg = "safely ignoreable tag length error";
            }
          } else {
            input.remove_prefix(static_cast<size_t>(field_len));
          }
        } else {
          msg = "unknown tag";
        }
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString(bool hex_key) const {
  std::string r;
  r.append("VersionEdit {");
  if (has_db_id_) {
    r.append("\n  DB ID: ");
    r.append(db_id_);
  }
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_persist_user_defined_timestamps_) {
    r.append("\n  PersistUserDefinedTimestamps: ");
    r.append(persist_user_defined_timestamps_ ? "true" : "false");
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFileNumber: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  if (has_min_log_number_to_keep_) {
    r.append("\n  MinLogNumberToKeep: ");
    AppendNumberTo(&r, min_log_number_to_keep_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (const auto& level_and_compact_cursor : compact_cursors_) {
    r.append("\n  CompactCursor: ");
    AppendNumberTo(&r, level_and_compact_cursor.first);
    r.append(" ");
    r.append(level_and_compact_cursor.second.DebugString(hex_key));
  }
  for (const auto& deleted_file : deleted_files_) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, deleted_file.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_file.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetNumber());
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetFileSize());
    r.append(" ");
    r.append(f.smallest.DebugString(hex_key));
    r.append(" .. ");
    r.append(f.largest.DebugString(hex_key));
    if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
      r.append(" blob_file:");
      AppendNumberTo(&r, f.oldest_blob_file_number);
    }
    r.append(" oldest_ancester_time:");
    AppendNumberTo(&r, f.oldest_ancester_time);
    r.append(" file_creation_time:");
    AppendNumberTo(&r, f.file_creation_time);
    r.append(" epoch_number:");
    AppendNumberTo(&r, f.epoch_number);
    r.append(" file_checksum:");
    r.append(Slice(f.file_checksum).ToString(true));
    r.append(" file_checksum_func_name: ");
    r.append(f.file_checksum_func_name);
    if (f.temperature != Temperature::kUnknown) {
      r.append(" temperature: ");
      // Maybe change to human readable format whenthe feature becomes
      // permanent
      r.append(std::to_string(static_cast<int>(f.temperature)));
    }
    if (f.unique_id != kNullUniqueId64x2) {
      r.append(" unique_id(internal): ");
      UniqueId64x2 id = f.unique_id;
      r.append(InternalUniqueIdToHumanString(&id));
      r.append(" public_unique_id: ");
      InternalUniqueIdToExternal(&id);
      r.append(UniqueIdToHumanString(EncodeUniqueIdBytes(&id)));
    }
    r.append(" tail size: ");
    AppendNumberTo(&r, f.tail_size);
    r.append(" User-defined timestamps persisted: ");
    r.append(f.user_defined_timestamps_persisted ? "true" : "false");
  }

  for (const auto& blob_file_addition : blob_file_additions_) {
    r.append("\n  BlobFileAddition: ");
    r.append(blob_file_addition.DebugString());
  }

  for (const auto& blob_file_garbage : blob_file_garbages_) {
    r.append("\n  BlobFileGarbage: ");
    r.append(blob_file_garbage.DebugString());
  }

  for (const auto& wal_addition : wal_additions_) {
    r.append("\n  WalAddition: ");
    r.append(wal_addition.DebugString());
  }

  if (!wal_deletion_.IsEmpty()) {
    r.append("\n  WalDeletion: ");
    r.append(wal_deletion_.DebugString());
  }

  r.append("\n  ColumnFamily: ");
  AppendNumberTo(&r, column_family_);
  if (is_column_family_add_) {
    r.append("\n  ColumnFamilyAdd: ");
    r.append(column_family_name_);
  }
  if (is_column_family_drop_) {
    r.append("\n  ColumnFamilyDrop");
  }
  if (is_in_atomic_group_) {
    r.append("\n  AtomicGroup: ");
    AppendNumberTo(&r, remaining_entries_);
    r.append(" entries remains");
  }
  if (HasFullHistoryTsLow()) {
    r.append("\n FullHistoryTsLow: ");
    r.append(Slice(full_history_ts_low_).ToString(hex_key));
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

  if (has_db_id_) {
    jw << "DB ID" << db_id_;
  }
  if (has_comparator_) {
    jw << "Comparator" << comparator_;
  }
  if (has_log_number_) {
    jw << "LogNumber" << log_number_;
  }
  if (has_prev_log_number_) {
    jw << "PrevLogNumber" << prev_log_number_;
  }
  if (has_next_file_number_) {
    jw << "NextFileNumber" << next_file_number_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }
  if (has_min_log_number_to_keep_) {
    jw << "MinLogNumberToKeep" << min_log_number_to_keep_;
  }
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (const auto& deleted_file : deleted_files_) {
      jw.StartArrayedObject();
      jw << "Level" << deleted_file.first;
      jw << "FileNumber" << deleted_file.second;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!new_files_.empty()) {
    jw << "AddedFiles";
    jw.StartArray();

    for (size_t i = 0; i < new_files_.size(); i++) {
      jw.StartArrayedObject();
      jw << "Level" << new_files_[i].first;
      const FileMetaData& f = new_files_[i].second;
      jw << "FileNumber" << f.fd.GetNumber();
      jw << "FileSize" << f.fd.GetFileSize();
      jw << "SmallestIKey" << f.smallest.DebugString(hex_key);
      jw << "LargestIKey" << f.largest.DebugString(hex_key);
      jw << "OldestAncesterTime" << f.oldest_ancester_time;
      jw << "FileCreationTime" << f.file_creation_time;
      jw << "EpochNumber" << f.epoch_number;
      jw << "FileChecksum" << Slice(f.file_checksum).ToString(true);
      jw << "FileChecksumFuncName" << f.file_checksum_func_name;
      if (f.temperature != Temperature::kUnknown) {
        jw << "temperature" << std::to_string(static_cast<int>(f.temperature));
      }
      if (f.oldest_blob_file_number != kInvalidBlobFileNumber) {
        jw << "OldestBlobFile" << f.oldest_blob_file_number;
      }
      if (f.temperature != Temperature::kUnknown) {
        // Maybe change to human readable format whenthe feature becomes
        // permanent
        jw << "Temperature" << static_cast<int>(f.temperature);
      }
      jw << "TailSize" << f.tail_size;
      jw << "UserDefinedTimestampsPersisted"
         << f.user_defined_timestamps_persisted;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_additions_.empty()) {
    jw << "BlobFileAdditions";

    jw.StartArray();

    for (const auto& blob_file_addition : blob_file_additions_) {
      jw.StartArrayedObject();
      jw << blob_file_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!blob_file_garbages_.empty()) {
    jw << "BlobFileGarbages";

    jw.StartArray();

    for (const auto& blob_file_garbage : blob_file_garbages_) {
      jw.StartArrayedObject();
      jw << blob_file_garbage;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_additions_.empty()) {
    jw << "WalAdditions";

    jw.StartArray();

    for (const auto& wal_addition : wal_additions_) {
      jw.StartArrayedObject();
      jw << wal_addition;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!wal_deletion_.IsEmpty()) {
    jw << "WalDeletion";
    jw.StartObject();
    jw << wal_deletion_;
    jw.EndObject();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (is_in_atomic_group_) {
    jw << "AtomicGroup" << remaining_entries_;
  }

  if (HasFullHistoryTsLow()) {
    jw << "FullHistoryTsLow" << Slice(full_history_ts_low_).ToString(hex_key);
  }

  jw.EndObject();

  return jw.Get();
}
bool Segment::NeedClearEmptyLevel(int num,int num2)
{
  if((level>num-num/4||num2+level>=num)&&has_empty_level==true)
  {
    return true;
  }
  return false;
}
void Segment::GetNotDeletedFiles(std::vector<FileMetaData*>& not_deleted_files)const
  {
    int position=0;
    not_deleted_files.clear();
    for(int i=0;i<int(files_.size());i++)
    {
      for(int j=0;j<int(files_[i].size());j++)
      {
        if(deleted_file_location[position].first==i&&deleted_file_location[position].second==j)
        {
          position++;
        }
        else
        {
          not_deleted_files.emplace_back(files_[i][j]);
        }
      }
    }
  }
  void Segment::AppendFileListAtLast(const std::vector<std::vector<FileMetaData*>> added_files,InternalKey largest_)
  {
    int new_level=added_files.size();
    int i=0;
    for(;i<std::min(new_level,level);i++)
    {
      for(auto added_file:added_files[i])
      {
        files_[i].emplace_back(added_file);
        file_number++;
      }
    }
    if(new_level>level)
    {
      for(;i<new_level;i++)
      {
        files_.emplace_back(std::vector<FileMetaData*>());/*对不对*/
        for(auto added_file:added_files[i])
        {
          files_[i].emplace_back(added_file);
          file_number++;
        }
        key_range.emplace_back(files_[i][0]->smallest,files_[i].back()->largest);
      }
    }
    for(int j=0;i<level;j++)
    {
      key_range[j].first=files_[j][0]->smallest;
      key_range[j].second=files_[j].back()->largest;
    }
    level=std::max(level,new_level);
    largest=largest_;
  }
  void Segment::RecordFileDeletion (std::unordered_set<uint64_t> deleted_files_for_judge)const
  {
    /*if(segment_is_changed==true)
    {
      return;
    }*/
    //deleted_file_num=file_number;
    deleted_file_location.clear();
    deleted_key_range.resize(level);
    deleted_file_location.resize(level);
    for(auto file: deleted_files_for_judge)
    {
      auto it = file_location.find(file);
      if(it==file_location.end())
      {

      }
      else
      {
        deleted_file_num--;
        const std::pair<int,int>& location=it->second;
        deleted_file_location.emplace_back(location);
        if(files_[location.first].size()==1)
          {
            deleted_key_range[location.first].is_empty=true;
            deleted_key_range[location.first].is_changed=true;
            continue;
          }
        if(location.second==int(files_[location.first].size())-1)
        {
          deleted_key_range[location.first].largest=files_[location.first][location.second-1]->largest;
          deleted_key_range[location.first].smallest=files_[location.first][0]->smallest;
          deleted_key_range[location.first].is_changed=true;
          continue;
        }
        if(location.second==0)
        {
          deleted_key_range[location.first].largest=files_[location.first][location.second]->largest;
          deleted_key_range[location.first].smallest=files_[location.first][1]->smallest;
          deleted_key_range[location.first].is_changed=true;
          continue;
        }
      }
    }
    std::sort(deleted_file_location.begin(), deleted_file_location.end());
  }
  int Segment::HasOverlapWithLevel(int lvl, const FileMetaData* file, const InternalKeyComparator* cmp)
  {
    auto& level_files = files_[lvl];
    if (level_files.empty())
    {
      InsertFileInLevel(level,file,files_[level].begin(),cmp);
      return level;
    }
    const auto& range = key_range[lvl];
    if (cmp->Compare(file->largest, range.first) < 0)
    {
      InsertFileInLevel(level,file,files_[level].begin(),cmp);
        return level;
    }
    if(cmp->Compare(file->smallest, range.second) > 0)
    {
      InsertFileInLevel(level,file,files_[level].end(),cmp);
        return level;
    }
    auto it = std::lower_bound(level_files.begin(), level_files.end(), file,
        [cmp](const FileMetaData* a, const FileMetaData* b) {
            return cmp->Compare(a->smallest, b->smallest) < 0;
        });
    if (it != level_files.end())
    {
        if (Overlaps(*it, file, cmp))
        {
          return -1;
        }
        if (it != level_files.begin())
        {
            FileMetaData* prev = *(it - 1);
            if (Overlaps(prev, file, cmp))
            {
              return -1;
            }
        }
    }
    if (it == level_files.end())
    {
        FileMetaData* last = level_files.back();
        if (Overlaps(last, file, cmp))
        {
          return -1;
        }
    }
    InsertFileInLevel(level,file,it,cmp);
    
    return level;
}
 void Segment::InsertFileInLevel(int lvl, const FileMetaData* file, std::vector<FileMetaData*>::iterator it,const InternalKeyComparator* cmp)
{
    if (lvl < 0 || lvl >= level)
    {
        return;
    }
    auto& level_files = files_[lvl];
    const int original_size = level_files.size();
    /*auto it = std::lower_bound(level_files.begin(), level_files.end(), file,
        [cmp](FileMetaData* a, FileMetaData* b) {
            return cmp->Compare(a->smallest, b->smallest) < 0;
        });*/
    int insert_pos = it - level_files.begin();
    level_files.insert(it, const_cast<FileMetaData*>(file));
    //file_location[file->fd.GetNumber()] = {lvl, insert_pos};
    /*for (int i = insert_pos + 1; i < level_files.size(); ++i) {
        FileMetaData* f = level_files[i];
        auto& loc = file_location[f->fd.GetNumber()];
        loc.second = i; // 只更新位置索引（层级不变）
    }*/
    key_range[lvl].first=files_[lvl][0]->smallest;
    key_range[lvl].second=files_[lvl].back()->largest;
    if (smallest.rep()->empty() || cmp->Compare(file->smallest, smallest) < 0)
    {
        smallest = file->smallest;
    }
    if (largest.rep()->empty() || cmp->Compare(file->largest, largest) > 0)
    {
        largest = file->largest;
    }
    file_number++;
}
std::vector<std::tuple<FileMetaData*,int,int>> Segment::MakeActualDeleteAndReturn(const InternalKeyComparator* cmp)
{
    std::vector<std::tuple<FileMetaData*,int,int>> return_vector;
    std::map<int, std::vector<int>> level_to_positions;
    for (const auto& loc : deleted_file_location)
    {
        level_to_positions[loc.first].push_back(loc.second);
    }
    for (auto& [lvl, positions] : level_to_positions)
    {
        std::sort(positions.rbegin(), positions.rend());
    }
    std::vector<FileMetaData*> files_to_delete;
    std::set<int> levels_to_remove;
    //int lowest_level=level_to_positions.begin()->first;
    for (auto& [lvl, positions] : level_to_positions)
    {
      auto& level_files = files_[lvl];
      for (int pos : positions)
      {
            if (pos < 0 || pos >= int(level_files.size()))
            {
                continue;
            }
            FileMetaData* file_to_delete = level_files[pos];
            files_to_delete.push_back(file_to_delete);
            level_files.erase(level_files.begin() + pos);
            //file_location.erase(file_to_delete->fd.GetNumber());
            if (level_files.empty())
            {
              levels_to_remove.insert(lvl);
              continue;
            }
             key_range[lvl] = {
                level_files.front()->smallest,
                level_files.back()->largest
            };
      }
    }
    //lowest_level=*std::min_element(levels_to_remove.begin(),levels_to_remove.end());
    int down_level=0;
    for(int i=0;i<int(files_.size());i++)
    {
      if(!files_[i].empty())
      {
        if(down_level==0)
        {
          continue;
        }
        for(int j=0;j<int(files_[i].size());j++)
        {
          FileMetaData* f= new FileMetaData (*files_[i][0]);
          files_[i].emplace_back(f);
          files_[i].erase(files_[0].begin());
          return_vector.emplace_back(f,i,i-down_level);
        }
      }
      else
      {
        down_level++;
        files_.erase(files_.begin() + i);
        key_range.erase(key_range.begin() + i);
        level--;
      }
    }
    /*if (!levels_to_remove.empty())
    {
        for (auto it = levels_to_remove.rbegin(); it != levels_to_remove.rend(); ++it)
        {
            int lvl = *it;
            files_.erase(files_.begin() + lvl);
            key_range.erase(key_range.begin() + lvl);
            level--;
        }
    }*/
    InternalKey new_smallest;
    InternalKey new_largest;
    for (int lvl = 0; lvl < level; lvl++)
    {
        if (files_[lvl].empty())
        {
          continue;
        }
        if(deleted_key_range[lvl].is_changed==true)
        {
          if (new_smallest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].smallest,new_smallest)<0)
          {
              new_smallest = key_range[lvl].first;
          }
          if (new_largest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].largest,new_largest)>0)
          {
              new_largest = key_range[lvl].second;
          }
          /*if(key_range[level].first.rep()->empty()||deleted_key_range[level].smallest.rep()->empty()||*(key_range[level].first.rep())>*deleted_key_range[level].smallest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }
          if(key_range[level].second.rep()->empty()||deleted_key_range[level].largest.rep()->empty()||*(key_range[level].second.rep())<*deleted_key_range[level].largest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }*/
        }
    }
    if((new_smallest.rep()->empty())||(smallest.rep()->empty())||(cmp->Compare(new_smallest,smallest)<0))
    {
      smallest = new_smallest;
    }
    if((new_largest.rep()->empty())||(largest.rep()->empty())||(cmp->Compare(new_largest,largest)>0))
    {
      largest = new_largest;
    }
    file_number-=files_to_delete.size();
    /*for (FileMetaData* file : files_to_delete)
    {
        delete file;
    }*/
   has_empty_level=false;
  return return_vector;
}
std::vector<std::pair<FileMetaData*,int>> Segment::MakeActualDeleteWithMiddleReturn(const InternalKeyComparator* cmp)
{
  std::vector<std::pair<FileMetaData*,int>> middle_return_vector;
  if (deleted_file_location.empty())
    {
        return middle_return_vector;
    }
    std::map<int, std::vector<int>> level_to_positions;
    for (const auto& loc : deleted_file_location)
    {
        level_to_positions[loc.first].push_back(loc.second);
    }
    for (auto& [lvl, positions] : level_to_positions)
    {
        std::sort(positions.rbegin(), positions.rend());
    }
    std::vector<FileMetaData*> files_to_delete;
    std::set<int> levels_to_remove;
    for (auto& [lvl, positions] : level_to_positions)
    {
      auto& level_files = files_[lvl];
      for (int pos : positions)
      {
            if (pos < 0 || pos >= int(level_files.size()))
            {
                continue;
            }
            FileMetaData* file_to_delete = level_files[pos];
            files_to_delete.push_back(file_to_delete);
            level_files.erase(level_files.begin() + pos);
            //file_location.erase(file_to_delete->fd.GetNumber());
            /*if (level_files.empty())
            {
              levels_to_remove.insert(lvl);
            }*/
             key_range[lvl] = {
                level_files.front()->smallest,
                level_files.back()->largest
            };
      }
    }
    for(int i=0;i<int(files_.size());i++)
    {
      if(!files_[i].empty())
      {
        for(int j=0;j<int(files_[i].size());j++)
        {
          middle_return_vector.emplace_back(files_[i][j],i);
        }
      }
      else
      {
        files_.erase(files_.begin() + i);
        key_range.erase(key_range.begin() + i);
        level--;
      }
    }
    /*if (!levels_to_remove.empty())
    {
        for (auto it = levels_to_remove.rbegin(); it != levels_to_remove.rend(); ++it)
        {
            int lvl = *it;
            files_.erase(files_.begin() + lvl);
            key_range.erase(key_range.begin() + lvl);
            level--;
        }
    }*/
    InternalKey new_smallest;
    InternalKey new_largest;
    for (int lvl = 0; lvl < level; lvl++)
    {
        if (files_[lvl].empty())
        {
          continue;
        }
        if(deleted_key_range[lvl].is_changed==true)
        {
          if (new_smallest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].smallest,new_smallest)<0)
          {
              new_smallest = key_range[lvl].first;
          }
          if (new_largest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].largest,new_largest)>0)
          {
              new_largest = key_range[lvl].second;
          }
          /*if(key_range[level].first.rep()->empty()||deleted_key_range[level].smallest.rep()->empty()||*(key_range[level].first.rep())>*deleted_key_range[level].smallest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }
          if(key_range[level].second.rep()->empty()||deleted_key_range[level].largest.rep()->empty()||*(key_range[level].second.rep())<*deleted_key_range[level].largest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }*/
        }
    }
    if((new_smallest.rep()->empty())||(smallest.rep()->empty())||(cmp->Compare(new_smallest,smallest)<0))
    {
      smallest = new_smallest;
    }
    if((new_largest.rep()->empty())||(largest.rep()->empty())||(cmp->Compare(new_largest,largest)>0))
    {
      largest = largest;
    }
    file_number-=files_to_delete.size();
    has_empty_level=false;
    return middle_return_vector;
}
void Segment::MakeActualDelete(const InternalKeyComparator* cmp)
  {
    if (deleted_file_location.empty())
    {
        return;
    }
    std::map<int, std::vector<int>> level_to_positions;
    for (const auto& loc : deleted_file_location)
    {
        level_to_positions[loc.first].push_back(loc.second);
    }
    for (auto& [lvl, positions] : level_to_positions)
    {
        std::sort(positions.rbegin(), positions.rend());
    }
    std::vector<FileMetaData*> files_to_delete;
    //std::set<int> levels_to_remove;
    for (auto& [lvl, positions] : level_to_positions)
    {
      auto& level_files = files_[lvl];
      for (int pos : positions)
      {
            if (pos < 0 || pos >= int(level_files.size()))
            {
                continue;
            }
            FileMetaData* file_to_delete = level_files[pos];
            files_to_delete.push_back(file_to_delete);
            level_files.erase(level_files.begin() + pos);
            //file_location.erase(file_to_delete->fd.GetNumber());
            if (level_files.empty())
            {
              has_empty_level=true;
            }
             key_range[lvl] = {
                level_files.front()->smallest,
                level_files.back()->largest
            };
      }
    }
    /*if (!levels_to_remove.empty())
    {
        for (auto it = levels_to_remove.rbegin(); it != levels_to_remove.rend(); ++it)
        {
            int lvl = *it;
            files_.erase(files_.begin() + lvl);
            key_range.erase(key_range.begin() + lvl);
            level--;
        }
    }*/
    InternalKey new_smallest;
    InternalKey new_largest;
    for (int lvl = 0; lvl < level; lvl++)
    {
        if (files_[lvl].empty())
        {
          continue;
        }
        if(deleted_key_range[lvl].is_changed==true)
        {
          if (new_smallest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].smallest,new_smallest)<0)
          {
              new_smallest = key_range[lvl].first;
          }
          if (new_largest.rep()->empty() ||
            cmp->Compare(deleted_key_range[lvl].largest,new_largest)>0)
          {
              new_largest = key_range[lvl].second;
          }
          /*if(key_range[level].first.rep()->empty()||deleted_key_range[level].smallest.rep()->empty()||*(key_range[level].first.rep())>*deleted_key_range[level].smallest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }
          if(key_range[level].second.rep()->empty()||deleted_key_range[level].largest.rep()->empty()||*(key_range[level].second.rep())<*deleted_key_range[level].largest.rep())
          {
            key_range[level].first=deleted_key_range[level].smallest;
          }*/
        }
    }
    if((new_smallest.rep()->empty())||(smallest.rep()->empty())||(cmp->Compare(new_smallest,smallest)<0))
    {
      smallest = new_smallest;
    }
    if((new_largest.rep()->empty())||(largest.rep()->empty())||(cmp->Compare(new_largest,largest)>0))
    {
      largest = new_largest;
    }
    file_number-=files_to_delete.size();
    /*for (FileMetaData* file : files_to_delete)
    {
        delete file;
    }*/
  }
  std::vector<std::pair<FileMetaData*,int>> Segment::AddFiles(const std::vector<std::vector<FileMetaData*>> add_files_list,const InternalKeyComparator* cmp)
  {
    std::vector<std::pair<FileMetaData*,int>> return_vector;
    for(const auto& added_files:add_files_list)
    {
      for(const auto added_file:added_files)
      {
        FileMetaData* actual_new_file=new FileMetaData(*added_file);
        actual_new_file->refs=1;
        return_vector.emplace_back(actual_new_file,AddFile(actual_new_file,cmp));
      }
    }
    return return_vector;
  }
  int Segment::AddFile(const FileMetaData* added_file,const InternalKeyComparator* cmp)
  {
    for (int lvl = 0; lvl < level; ++lvl)
    {
        int p=HasOverlapWithLevel(lvl, added_file, cmp);
        if (p!=-1)
        {
            return p;
        }
    }
    files_.emplace_back(std::vector<FileMetaData*>());
    files_.back().emplace_back(const_cast<FileMetaData*>(added_file));
    key_range.emplace_back(added_file->smallest, added_file->largest);
    //file_location[file_copy->fd.GetNumber()] = {level, 0};
    if (smallest.rep()->empty() || cmp->Compare(added_file->smallest, smallest) < 0)
    {
        smallest = added_file->smallest;
    }
    if (largest.rep()->empty() || cmp->Compare(added_file->largest, largest) > 0)
    {
        largest = added_file->largest;
    }
    
    level++;
    file_number++;
    return level;
  }
  void Segment::RebuildFileLocation()
    {
      file_location.clear();
      for (int lvl = 0; lvl < int(files_.size()); lvl++)
      {
        for (int pos = 0; pos < int(files_[lvl].size()); pos++)
        {
            if (files_[lvl][pos])
            {
                file_location[files_[lvl][pos]->fd.GetNumber()] = {lvl, pos};
            }
        }
      }
    }
     Segment::Segment(FileMetaData* file,const Comparator* ucmp)
         :smallest(file->smallest),
          largest(file->largest),
          file_indexer_(ucmp),
          level(1),
          file_number(1),
          deleted_file_num(1),
          //segment_is_changed(false),
          has_empty_level(false){
        files_.emplace_back(std::vector<FileMetaData*>());
        files_[0].push_back(file);
        key_range.emplace_back(file->smallest, file->largest);
        RebuildFileLocation();
    }
    Segment::Segment(const Segment& other)
        :smallest(other.smallest),
          largest(other.largest),
          level_files_brief_(other.level_files_brief_),
          file_indexer_(other.file_indexer_),
          key_range(other.key_range),
          deleted_file_location(other.deleted_file_location),
          deleted_key_range(other.deleted_key_range),
          level(other.level),
          segment_num_(other.segment_num_),
          file_number(other.file_number),
          //segment_is_changed(other.segment_is_changed),
          has_empty_level(other.has_empty_level) {
        files_.resize(other.files_.size());
        for (size_t i = 0; i < other.files_.size(); i++) {
            files_[i].resize(other.files_[i].size());
            for (size_t j = 0; j < other.files_[i].size(); j++) {
                files_[i][j] = other.files_[i][j];
            }
        }
        for (size_t i = 0; i < files_.size(); i++) {
            for (size_t j = 0; j < files_[i].size(); j++) {
                file_location[files_[i][j]->fd.GetNumber()] = {static_cast<int>(i), static_cast<int>(j)};
            }
        }
    }
    Segment::Segment(const Segment& other,const Comparator* cmp) 
        :smallest(other.smallest),
          largest(other.largest),
          level_files_brief_(other.level_files_brief_),
          file_indexer_(cmp),
          key_range(other.key_range),
          deleted_file_location(other.deleted_file_location),
          deleted_key_range(other.deleted_key_range),
          level(other.level),
          segment_num_(other.segment_num_),
          file_number(other.file_number),
          //(other.segment_is_changed),
          has_empty_level(other.has_empty_level) {
        files_.resize(other.files_.size());
        for (size_t i = 0; i < other.files_.size(); i++) {
            files_[i].resize(other.files_[i].size());
            for (size_t j = 0; j < other.files_[i].size(); j++) {
                files_[i][j] = other.files_[i][j];
            }
        }
        for (size_t i = 0; i < files_.size(); i++) {
            for (size_t j = 0; j < files_[i].size(); j++) {
                file_location[files_[i][j]->fd.GetNumber()] = {static_cast<int>(i), static_cast<int>(j)};
            }
        }
    }
void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                               const std::vector<FileMetaData*>& files,
                               Arena* arena) {
  assert(file_level);
  assert(arena);

  size_t num = files.size();
  file_level->num_files = num;
  char* mem = arena->AllocateAligned(num * sizeof(FdWithKeyRange));
  file_level->files = new (mem) FdWithKeyRange[num];

  for (size_t i = 0; i < num; i++) {
    Slice smallest_key = files[i]->smallest.Encode();
    Slice largest_key = files[i]->largest.Encode();

    // Copy key slice to sequential memory
    size_t smallest_size = smallest_key.size();
    size_t largest_size = largest_key.size();
    mem = arena->AllocateAligned(smallest_size + largest_size);
    memcpy(mem, smallest_key.data(), smallest_size);
    memcpy(mem + smallest_size, largest_key.data(), largest_size);

    FdWithKeyRange& f = file_level->files[i];
    f.fd = files[i]->fd;
    f.file_metadata = files[i];
    f.smallest_key = Slice(mem, smallest_size);
    f.largest_key = Slice(mem + smallest_size, largest_size);
  }
}
}  // namespace ROCKSDB_NAMESPACE
