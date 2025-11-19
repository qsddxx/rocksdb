//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_meta.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class VersionBuilder::Rep {
  class NewestFirstBySeqNo {
   public:
    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);

      if (lhs->fd.largest_seqno != rhs->fd.largest_seqno) {
        return lhs->fd.largest_seqno > rhs->fd.largest_seqno;
      }

      if (lhs->fd.smallest_seqno != rhs->fd.smallest_seqno) {
        return lhs->fd.smallest_seqno > rhs->fd.smallest_seqno;
      }

      // Break ties by file number
      return lhs->fd.GetNumber() > rhs->fd.GetNumber();
    }
  };

  class NewestFirstByEpochNumber {
   private:
    inline static const NewestFirstBySeqNo seqno_cmp;

   public:
    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);

      if (lhs->epoch_number != rhs->epoch_number) {
        return lhs->epoch_number > rhs->epoch_number;
      } else {
        return seqno_cmp(lhs, rhs);
      }
    }
  };
  class BySmallestKey {
   public:
    explicit BySmallestKey(const InternalKeyComparator* cmp) : cmp_(cmp) {}

    bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const {
      assert(lhs);
      assert(rhs);
      assert(cmp_);

      const int r = cmp_->Compare(lhs->smallest, rhs->smallest);
      if (r != 0) {
        return (r < 0);
      }

      // Break ties by file number
      return (lhs->fd.GetNumber() < rhs->fd.GetNumber());
    }

   private:
    const InternalKeyComparator* cmp_;
  };
  struct DeletedFileGroup
  {
    InternalKey smallest;
    InternalKey largest;
    std::vector<std::vector<uint64_t>> files_;
  };
  struct AddedFileGroup
  {
    InternalKey smallest;
    InternalKey largest;
    std::vector<std::vector<FileMetaData*>> files_;
  };
  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
    std::vector<FileMetaData*> middle_added_files;
    std::vector<FileMetaData*> final_added_files;
    std::unordered_map<uint64_t,FileMetaData*> added_files_for_judge;//无用
    std::unordered_set<uint64_t> deleted_files_for_judge;//无用
    std::unordered_map<uint64_t,Segment*> added_segments;//无用
    //std::unordered_map<uint64_t,Segment*> added_segments_by_changed;
    std::unordered_map<uint64_t,Segment*> deleted_segments;//无用
    std::unordered_map<uint64_t,Segment*> deleted_segments_caused_trush;//无用
    //std::unordered_map<uint64_t,Segment*> deleted_segments_not_exit;
    //std::vector<DeletedFileGroup> deleted_files_from_non_exit_segment;
    //std::vector<AddedFileGroup> added_files_from_non_exit_segment;
    //std::unordered_set<uint64_t> deleted_segments_history;
    //std::unordered_set<uint64_t> added_segments_history;
    //std::vector<Segment*> deleted_segments_not_available;
    //std::vector<Segment*> added_segments_not_available;
  };

  // A class that represents the accumulated changes (like additional garbage or
  // newly linked/unlinked SST files) for a given blob file after applying a
  // series of VersionEdits.
  class BlobFileMetaDataDelta {
   public:
    bool IsEmpty() const {
      return !additional_garbage_count_ && !additional_garbage_bytes_ &&
             newly_linked_ssts_.empty() && newly_unlinked_ssts_.empty();
    }

    uint64_t GetAdditionalGarbageCount() const {
      return additional_garbage_count_;
    }

    uint64_t GetAdditionalGarbageBytes() const {
      return additional_garbage_bytes_;
    }

    const std::unordered_set<uint64_t>& GetNewlyLinkedSsts() const {
      return newly_linked_ssts_;
    }

    const std::unordered_set<uint64_t>& GetNewlyUnlinkedSsts() const {
      return newly_unlinked_ssts_;
    }

    void AddGarbage(uint64_t count, uint64_t bytes) {
      additional_garbage_count_ += count;
      additional_garbage_bytes_ += bytes;
    }

    void LinkSst(uint64_t sst_file_number) {
      assert(newly_linked_ssts_.find(sst_file_number) ==
             newly_linked_ssts_.end());

      // Reconcile with newly unlinked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_unlinked_ssts_.find(sst_file_number);

      if (it != newly_unlinked_ssts_.end()) {
        newly_unlinked_ssts_.erase(it);
      } else {
        newly_linked_ssts_.emplace(sst_file_number);
      }
    }

    void UnlinkSst(uint64_t sst_file_number) {
      assert(newly_unlinked_ssts_.find(sst_file_number) ==
             newly_unlinked_ssts_.end());

      // Reconcile with newly linked SSTs on the fly. (Note: an SST can be
      // linked to and unlinked from the same blob file in the case of a trivial
      // move.)
      auto it = newly_linked_ssts_.find(sst_file_number);

      if (it != newly_linked_ssts_.end()) {
        newly_linked_ssts_.erase(it);
      } else {
        newly_unlinked_ssts_.emplace(sst_file_number);
      }
    }

   private:
    uint64_t additional_garbage_count_ = 0;
    uint64_t additional_garbage_bytes_ = 0;
    std::unordered_set<uint64_t> newly_linked_ssts_;
    std::unordered_set<uint64_t> newly_unlinked_ssts_;
  };

  // A class that represents the state of a blob file after applying a series of
  // VersionEdits. In addition to the resulting state, it also contains the
  // delta (see BlobFileMetaDataDelta above). The resulting state can be used to
  // identify obsolete blob files, while the delta makes it possible to
  // efficiently detect trivial moves.
  class MutableBlobFileMetaData {
   public:
    // To be used for brand new blob files
    explicit MutableBlobFileMetaData(
        std::shared_ptr<SharedBlobFileMetaData>&& shared_meta)
        : shared_meta_(std::move(shared_meta)) {}

    // To be used for pre-existing blob files
    explicit MutableBlobFileMetaData(
        const std::shared_ptr<BlobFileMetaData>& meta)
        : shared_meta_(meta->GetSharedMeta()),
          linked_ssts_(meta->GetLinkedSsts()),
          garbage_blob_count_(meta->GetGarbageBlobCount()),
          garbage_blob_bytes_(meta->GetGarbageBlobBytes()) {}

    const std::shared_ptr<SharedBlobFileMetaData>& GetSharedMeta() const {
      return shared_meta_;
    }

    uint64_t GetBlobFileNumber() const {
      assert(shared_meta_);
      return shared_meta_->GetBlobFileNumber();
    }

    bool HasDelta() const { return !delta_.IsEmpty(); }

    const std::unordered_set<uint64_t>& GetLinkedSsts() const {
      return linked_ssts_;
    }

    uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }

    uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

    bool AddGarbage(uint64_t count, uint64_t bytes) {
      assert(shared_meta_);

      if (garbage_blob_count_ + count > shared_meta_->GetTotalBlobCount() ||
          garbage_blob_bytes_ + bytes > shared_meta_->GetTotalBlobBytes()) {
        return false;
      }

      delta_.AddGarbage(count, bytes);

      garbage_blob_count_ += count;
      garbage_blob_bytes_ += bytes;

      return true;
    }

    void LinkSst(uint64_t sst_file_number) {
      delta_.LinkSst(sst_file_number);

      assert(linked_ssts_.find(sst_file_number) == linked_ssts_.end());
      linked_ssts_.emplace(sst_file_number);
    }

    void UnlinkSst(uint64_t sst_file_number) {
      delta_.UnlinkSst(sst_file_number);

      assert(linked_ssts_.find(sst_file_number) != linked_ssts_.end());
      linked_ssts_.erase(sst_file_number);
    }

   private:
    std::shared_ptr<SharedBlobFileMetaData> shared_meta_;
    // Accumulated changes
    BlobFileMetaDataDelta delta_;
    // Resulting state after applying the changes
    BlobFileMetaData::LinkedSsts linked_ssts_;
    uint64_t garbage_blob_count_ = 0;
    uint64_t garbage_blob_bytes_ = 0;
  };
  //std::unordered_map<uint64_t,int> deleted_files_by_segment_;
  std::unordered_map<uint64_t,int> added_files_single;//无用
  std::unordered_map<uint64_t,int> added_files_by_segment_;//无用
  std::unordered_map<uint64_t,int> files_may_caused_trush;//无用
  //std::unordered_set<uint64_t>added_segment_history;
  //std::unordered_set<uint64_t>deleted_segment_history;

  const FileOptions& file_options_;
  const ImmutableCFOptions* const ioptions_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  mutable std::vector<std::vector<Segment*>> base_segment_;
   /*class FileLocation {
   public:
    FileLocation() = default;
    FileLocation(int level, size_t position)
        : level_(level), position_(position) {}

    int GetLevel() const { return level_; }
    size_t GetPosition() const { return position_; }

    bool IsValid() const { return level_ >= 0; }

    bool operator==(const FileLocation& rhs) const {
      return level_ == rhs.level_ && position_ == rhs.position_;
    }

    bool operator!=(const FileLocation& rhs) const { return !(*this == rhs); }

    static FileLocation Invalid() { return FileLocation(); }

   private:
    int level_ = -1;
    size_t position_ = 0;
  };*/
  using SegmentLocations=UnorderedMap<uint64_t,rocksdb::VersionStorageInfo::FileLocation>;
  SegmentLocations segment_locations_;
  int level_per_segment_level=10;
  //std::atomic<uint64_t>& next_segment_number_;
  mutable bool has_new_versionedit;
  mutable bool has_base_segemnt_trush;
  VersionSet* version_set_;
  int num_levels_;
  LevelState* levels_;
  std::unordered_map<int,bool> deleted_segments_map{};
  std::unordered_set<uint64_t> compaction_added_files_set{};
  // Store sizes of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::unordered_map<int, size_t> invalid_level_sizes_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  // Current levels of table files affected by additions/deletions.
  std::unordered_map<uint64_t, int> table_file_levels_;
  // Current compact cursors that should be changed after the last compaction
  std::unordered_map<int, InternalKey> updated_compact_cursors_;
  const std::shared_ptr<const NewestFirstByEpochNumber>
      level_zero_cmp_by_epochno_;
  const std::shared_ptr<const NewestFirstBySeqNo> level_zero_cmp_by_seqno_;
  const std::shared_ptr<const BySmallestKey> level_nonzero_cmp_;

  // Mutable metadata objects for all blob files affected by the series of
  // version edits.
  std::map<uint64_t, MutableBlobFileMetaData> mutable_blob_file_metas_;

  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr_;

  ColumnFamilyData* cfd_;
  VersionEditHandler* version_edit_handler_;
  bool track_found_and_missing_files_;
  // If false, only a complete Version with all files consisting it found is
  // considered valid. If true, besides complete Version, if the Version is
  // never edited in an atomic group, an incomplete Version with only a suffix
  // of L0 files missing is also considered valid.
  bool allow_incomplete_valid_version_;

  // These are only tracked if `track_found_and_missing_files_` is enabled.

  // The SST files that are found (blob files not included yet).
  std::unordered_set<uint64_t> found_files_;
  // Missing SST files for L0
  std::unordered_set<uint64_t> l0_missing_files_;
  // Missing SST files for non L0 levels
  std::unordered_set<uint64_t> non_l0_missing_files_;
  // Intermediate SST files (blob files not included yet)
  std::vector<std::string> intermediate_files_;
  // The highest file number for all the missing blob files, useful to check
  // if a complete Version is available.
  uint64_t missing_blob_files_high_ = kInvalidBlobFileNumber;
  // Missing blob files, useful to check if only the missing L0 files'
  // associated blob files are missing.
  std::unordered_set<uint64_t> missing_blob_files_;
  // True if all files consisting the Version can be found. Or if
  // `allow_incomplete_valid_version_` is true and the version history is not
  // ever edited in an atomic group, this will be true if only a
  // suffix of L0 SST files and their associated blob files are missing.
  bool valid_version_available_;
  // True if version is ever edited in an atomic group.
  bool edited_in_atomic_group_;

  // Flag to indicate if the Version is updated since last validity check. If no
  // `Apply` call is made between a `Rep`'s construction and a
  // `ValidVersionAvailable` check or between two `ValidVersionAvailable` calls.
  // This flag will be true to indicate the cached validity value can be
  // directly used without a recheck.
  bool version_updated_since_last_check_;

  // End of fields that are only tracked when `track_found_and_missing_files_`
  // is enabled.

 public:
  Rep(const FileOptions& file_options, const ImmutableCFOptions* ioptions,
      TableCache* table_cache, VersionStorageInfo* base_vstorage,
      VersionSet* version_set,
      std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr,
      ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler,
      bool track_found_and_missing_files, bool allow_incomplete_valid_version)
      : added_files_single(),
        added_files_by_segment_(),
        files_may_caused_trush(),
        file_options_(file_options),
        ioptions_(ioptions),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        segment_locations_(base_vstorage->GetSegmentMap()),
        level_per_segment_level(base_vstorage->GetLevelPerSegmentLevel()),
        //next_segment_number_(version_set->GetSegmentNumber()),
        has_new_versionedit(false),
        has_base_segemnt_trush(false),
        version_set_(version_set),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false),
        level_zero_cmp_by_epochno_(
            std::make_shared<NewestFirstByEpochNumber>()),
        level_zero_cmp_by_seqno_(std::make_shared<NewestFirstBySeqNo>()),
        level_nonzero_cmp_(std::make_shared<BySmallestKey>(
            base_vstorage_->InternalComparator())),
        file_metadata_cache_res_mgr_(file_metadata_cache_res_mgr),
        cfd_(cfd),
        version_edit_handler_(version_edit_handler),
        track_found_and_missing_files_(track_found_and_missing_files),
        allow_incomplete_valid_version_(allow_incomplete_valid_version) {
    assert(ioptions_);

    levels_ = new LevelState[num_levels_];
    if (track_found_and_missing_files_) {
      assert(cfd_);
      assert(version_edit_handler_);
      // `track_found_and_missing_files_` mode used by VersionEditHandlerPIT
      // assumes the initial base version is valid. For best efforts recovery,
      // base will be empty. For manifest tailing usage like secondary instance,
      // they do not allow incomplete version, so the base version in subsequent
      // catch up attempts should be valid too.
      valid_version_available_ = true;
      edited_in_atomic_group_ = false;
      version_updated_since_last_check_ = false;
    }
  }

  Rep(const Rep& other)
      : added_files_single(other.added_files_single),
        added_files_by_segment_(other.added_files_by_segment_),
        files_may_caused_trush(other.files_may_caused_trush),
        file_options_(other.file_options_),
        ioptions_(other.ioptions_),
        table_cache_(other.table_cache_),
        base_vstorage_(other.base_vstorage_),
        segment_locations_(other.segment_locations_),
        level_per_segment_level(base_vstorage_->GetLevelPerSegmentLevel()),
        //next_segment_number_(other.version_set_->GetSegmentNumber()),
        has_new_versionedit(other.has_new_versionedit),
        has_base_segemnt_trush(other.has_base_segemnt_trush),
        version_set_(other.version_set_),
        num_levels_(other.num_levels_),
        invalid_level_sizes_(other.invalid_level_sizes_),
        has_invalid_levels_(other.has_invalid_levels_),
        table_file_levels_(other.table_file_levels_),
        updated_compact_cursors_(other.updated_compact_cursors_),
        level_zero_cmp_by_epochno_(other.level_zero_cmp_by_epochno_),
        level_zero_cmp_by_seqno_(other.level_zero_cmp_by_seqno_),
        level_nonzero_cmp_(other.level_nonzero_cmp_),
        mutable_blob_file_metas_(other.mutable_blob_file_metas_),
        file_metadata_cache_res_mgr_(other.file_metadata_cache_res_mgr_),
        cfd_(other.cfd_),
        version_edit_handler_(other.version_edit_handler_),
        track_found_and_missing_files_(other.track_found_and_missing_files_),
        allow_incomplete_valid_version_(other.allow_incomplete_valid_version_),
        found_files_(other.found_files_),
        l0_missing_files_(other.l0_missing_files_),
        non_l0_missing_files_(other.non_l0_missing_files_),
        intermediate_files_(other.intermediate_files_),
        missing_blob_files_high_(other.missing_blob_files_high_),
        missing_blob_files_(other.missing_blob_files_),
        valid_version_available_(other.valid_version_available_),
        edited_in_atomic_group_(other.edited_in_atomic_group_),
        version_updated_since_last_check_(
            other.version_updated_since_last_check_) {
    assert(ioptions_);
    levels_ = new LevelState[num_levels_];
    for (int level = 0; level < num_levels_; level++) {
      levels_[level] = other.levels_[level];
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        RefFile(pair.second);
      }
    }
    if (track_found_and_missing_files_) {
      assert(cfd_);
      assert(version_edit_handler_);
    }
  }

  ~Rep() {
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void RefFile(FileMetaData* f) {
    assert(f);
    assert(f->refs > 0);
    f->refs++;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        // NOTE: have to release in raw cache interface to avoid using a
        // TypedHandle for FileMetaData::table_reader_handle
        table_cache_->get_cache().get()->Release(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }

      if (file_metadata_cache_res_mgr_) {
        Status s = file_metadata_cache_res_mgr_->UpdateCacheReservation(
            f->ApproximateMemoryUsage(), false /* increase */);
        s.PermitUncheckedError();
      }
      delete f;
    }
  }

  // Mapping used for checking the consistency of links between SST files and
  // blob files. It is built using the forward links (table file -> blob file),
  // and is subsequently compared with the inverse mapping stored in the
  // BlobFileMetaData objects.
  using ExpectedLinkedSsts =
      std::unordered_map<uint64_t, BlobFileMetaData::LinkedSsts>;

  static void UpdateExpectedLinkedSsts(
      uint64_t table_file_number, uint64_t blob_file_number,
      ExpectedLinkedSsts* expected_linked_ssts) {
    assert(expected_linked_ssts);

    if (blob_file_number == kInvalidBlobFileNumber) {
      return;
    }

    (*expected_linked_ssts)[blob_file_number].emplace(table_file_number);
  }

  template <typename Checker>
  Status CheckConsistencyDetailsForLevel(
      const VersionStorageInfo* vstorage, int level, Checker checker,
      const std::string& sync_point,
      ExpectedLinkedSsts* expected_linked_ssts) const {
#ifdef NDEBUG
    (void)sync_point;
#endif

    assert(vstorage);
    assert(level >= 0 && level < num_levels_);
    assert(expected_linked_ssts);

    const auto& level_files = vstorage->LevelFiles(level);

    if (level_files.empty()) {
      return Status::OK();
    }

    assert(level_files[0]);
    UpdateExpectedLinkedSsts(level_files[0]->fd.GetNumber(),
                             level_files[0]->oldest_blob_file_number,
                             expected_linked_ssts);

    for (size_t i = 1; i < level_files.size(); ++i) {
      assert(level_files[i]);
      UpdateExpectedLinkedSsts(level_files[i]->fd.GetNumber(),
                               level_files[i]->oldest_blob_file_number,
                               expected_linked_ssts);

      auto lhs = level_files[i - 1];
      auto rhs = level_files[i];

#ifndef NDEBUG
      auto pair = std::make_pair(&lhs, &rhs);
      TEST_SYNC_POINT_CALLBACK(sync_point, &pair);
#endif

      const Status s = checker(lhs, rhs);
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  // Make sure table files are sorted correctly and that the links between
  // table files and blob files are consistent.
  Status CheckConsistencyDetails(const VersionStorageInfo* vstorage) const {
    assert(vstorage);

    ExpectedLinkedSsts expected_linked_ssts;

    if (num_levels_ > 0) {
      const InternalKeyComparator* const icmp = vstorage->InternalComparator();
      EpochNumberRequirement epoch_number_requirement =
          vstorage->GetEpochNumberRequirement();
      assert(icmp);
      // Check L0
      {
        auto l0_checker = [this, epoch_number_requirement, icmp](
                              const FileMetaData* lhs,
                              const FileMetaData* rhs) {
          assert(lhs);
          assert(rhs);

          if (epoch_number_requirement ==
              EpochNumberRequirement::kMightMissing) {
            if (!level_zero_cmp_by_seqno_->operator()(lhs, rhs)) {
              std::ostringstream oss;
              oss << "L0 files are not sorted properly: files #"
                  << lhs->fd.GetNumber() << " with seqnos (largest, smallest) "
                  << lhs->fd.largest_seqno << " , " << lhs->fd.smallest_seqno
                  << ", #" << rhs->fd.GetNumber()
                  << " with seqnos (largest, smallest) "
                  << rhs->fd.largest_seqno << " , " << rhs->fd.smallest_seqno;
              return Status::Corruption("VersionBuilder", oss.str());
            }
          } else if (epoch_number_requirement ==
                     EpochNumberRequirement::kMustPresent) {
            if (lhs->epoch_number == rhs->epoch_number) {
              bool range_overlapped =
                  icmp->Compare(lhs->smallest, rhs->largest) <= 0 &&
                  icmp->Compare(lhs->largest, rhs->smallest) >= 0;

              if (range_overlapped) {
                std::ostringstream oss;
                oss << "L0 files of same epoch number but overlapping range #"
                    << lhs->fd.GetNumber()
                    << " , smallest key: " << lhs->smallest.DebugString(true)
                    << " , largest key: " << lhs->largest.DebugString(true)
                    << " , epoch number: " << lhs->epoch_number << " vs. file #"
                    << rhs->fd.GetNumber()
                    << " , smallest key: " << rhs->smallest.DebugString(true)
                    << " , largest key: " << rhs->largest.DebugString(true)
                    << " , epoch number: " << rhs->epoch_number;
                return Status::Corruption("VersionBuilder", oss.str());
              }
            }

            if (!level_zero_cmp_by_epochno_->operator()(lhs, rhs)) {
              std::ostringstream oss;
              oss << "L0 files are not sorted properly: files #"
                  << lhs->fd.GetNumber() << " with epoch number "
                  << lhs->epoch_number << ", #" << rhs->fd.GetNumber()
                  << " with epoch number " << rhs->epoch_number;
              return Status::Corruption("VersionBuilder", oss.str());
            }
          }

          return Status::OK();
        };

        const Status s = CheckConsistencyDetailsForLevel(
            vstorage, /* level */ 0, l0_checker,
            "VersionBuilder::CheckConsistency0", &expected_linked_ssts);
        if (!s.ok()) {
          return s;
        }
      }

      // Check L1 and up

      for (int level = 1; level < num_levels_; ++level) {
        auto checker = [this, level, icmp](const FileMetaData* lhs,
                                           const FileMetaData* rhs) {
          assert(lhs);
          assert(rhs);

          if (!level_nonzero_cmp_->operator()(lhs, rhs)) {
            std::ostringstream oss;
            oss << 'L' << level << " files are not sorted properly: files #"
                << lhs->fd.GetNumber() << ", #" << rhs->fd.GetNumber();

            return Status::Corruption("VersionBuilder", oss.str());
          }

          // Make sure there is no overlap in level
          if (icmp->Compare(lhs->largest, rhs->smallest) >= 0) {
            std::ostringstream oss;
            oss << 'L' << level << " has overlapping ranges: file #"
                << lhs->fd.GetNumber()
                << " largest key: " << lhs->largest.DebugString(true)
                << " vs. file #" << rhs->fd.GetNumber()
                << " smallest key: " << rhs->smallest.DebugString(true);

            return Status::Corruption("VersionBuilder", oss.str());
          }

          return Status::OK();
        };

        const Status s = CheckConsistencyDetailsForLevel(
            vstorage, level, checker, "VersionBuilder::CheckConsistency1",
            &expected_linked_ssts);
        if (!s.ok()) {
          return s;
        }
      }
    }

    // Make sure that all blob files in the version have non-garbage data and
    // the links between them and the table files are consistent.
    const auto& blob_files = vstorage->GetBlobFiles();
    for (const auto& blob_file_meta : blob_files) {
      assert(blob_file_meta);

      const uint64_t blob_file_number = blob_file_meta->GetBlobFileNumber();

      if (blob_file_meta->GetGarbageBlobCount() >=
          blob_file_meta->GetTotalBlobCount()) {
        std::ostringstream oss;
        oss << "Blob file #" << blob_file_number
            << " consists entirely of garbage";

        return Status::Corruption("VersionBuilder", oss.str());
      }

      if (blob_file_meta->GetLinkedSsts() !=
          expected_linked_ssts[blob_file_number]) {
        std::ostringstream oss;
        oss << "Links are inconsistent between table files and blob file #"
            << blob_file_number;

        return Status::Corruption("VersionBuilder", oss.str());
      }
    }

    Status ret_s;
    TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistencyBeforeReturn",
                             &ret_s);
    return ret_s;
  }
  //无用
  std::pair<int,int> GetLevelForSegment(int position)const
  {
    /*int number=0;
    for(int i=0;i<position;i++)
    {
      number+=level_per_segment_level[i];
    }
    int number1=number+level_per_segment_level[position]-1;
    std::pair<int,int> result(number,number1);
    return result;*/
    std::pair<int,int> result(position*level_per_segment_level,(1+position)*level_per_segment_level-1);
    return result;
  }
  //无用
  int GetInsertLevelForSegment(int level)const
  {
    /*int i=-1;
    int j=0;
    for(;i<int(level_per_segment_level.size());i++)
    {
      j+=level_per_segment_level[i];
      if(j>=level)
      {
        break;
      }
    }
    if(i==int(level_per_segment_level.size())&&j<level)
    {
      return (i);
    }
    return i;*/
    for(int i=0;;i++)
    {
      if((i+1)*level_per_segment_level>level)
      {
        return i;
      }
    }
  }
  //无用
  void MakeDeleteSegmentClear()const
  {
    for(int i=0;i<num_levels_;i++)
    {
      for(auto&j:levels_[i].deleted_segments_caused_trush)
      {
        delete j.second;
      }
      levels_[i].deleted_segments.clear();
      levels_[i].deleted_segments_caused_trush.clear();
      levels_[i].added_segments.clear();
      //levels_[i].added_segments_by_changed.clear();
      levels_[i].deleted_files_for_judge.clear();
      levels_[i].added_files_for_judge.clear();
    }
  }
  Status CheckConsistency(const VersionStorageInfo* vstorage) const {
    assert(vstorage);

    // Always run consistency checks in debug build
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      return Status::OK();
    }
#endif
    Status s = CheckConsistencyDetails(vstorage);
    if (s.IsCorruption() && s.getState()) {
      // Make it clear the error is due to force_consistency_checks = 1 or
      // debug build
#ifdef NDEBUG
      auto prefix = "force_consistency_checks";
#else
      auto prefix = "force_consistency_checks(DEBUG)";
#endif
      s = Status::Corruption(prefix, s.getState());
    } else {
      // was only expecting corruption with message, or OK
      assert(s.ok());
    }
    return s;
  }

  bool CheckConsistencyForNumLevels() const {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }

    for (const auto& pair : invalid_level_sizes_) {
      const size_t level_size = pair.second;
      if (level_size != 0) {
        return false;
      }
    }

    return true;
  }

  bool IsBlobFileInVersion(uint64_t blob_file_number) const {
    auto mutable_it = mutable_blob_file_metas_.find(blob_file_number);
    if (mutable_it != mutable_blob_file_metas_.end()) {
      return true;
    }

    assert(base_vstorage_);
    const auto meta = base_vstorage_->GetBlobFileMetaData(blob_file_number);

    return !!meta;
  }

  MutableBlobFileMetaData* GetOrCreateMutableBlobFileMetaData(
      uint64_t blob_file_number) {
    auto mutable_it = mutable_blob_file_metas_.find(blob_file_number);
    if (mutable_it != mutable_blob_file_metas_.end()) {
      return &mutable_it->second;
    }

    assert(base_vstorage_);
    const auto meta = base_vstorage_->GetBlobFileMetaData(blob_file_number);

    if (meta) {
      mutable_it = mutable_blob_file_metas_
                       .emplace(blob_file_number, MutableBlobFileMetaData(meta))
                       .first;
      return &mutable_it->second;
    }

    return nullptr;
  }

  Status ApplyBlobFileAddition(const BlobFileAddition& blob_file_addition) {
    const uint64_t blob_file_number = blob_file_addition.GetBlobFileNumber();

    if (IsBlobFileInVersion(blob_file_number)) {
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " already added";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    auto deleter = [vs = version_set_, ioptions = ioptions_,
                    bc = cfd_ ? cfd_->blob_file_cache()
                              : nullptr](SharedBlobFileMetaData* shared_meta) {
      if (vs) {
        assert(ioptions);
        assert(!ioptions->cf_paths.empty());
        assert(shared_meta);

        vs->AddObsoleteBlobFile(shared_meta->GetBlobFileNumber(),
                                ioptions->cf_paths.front().path);
      }
      if (bc) {
        bc->Evict(shared_meta->GetBlobFileNumber());
      }

      delete shared_meta;
    };

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, blob_file_addition.GetTotalBlobCount(),
        blob_file_addition.GetTotalBlobBytes(),
        blob_file_addition.GetChecksumMethod(),
        blob_file_addition.GetChecksumValue(), std::move(deleter));

    mutable_blob_file_metas_.emplace(
        blob_file_number, MutableBlobFileMetaData(std::move(shared_meta)));

    Status s;
    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      s = version_edit_handler_->VerifyBlobFile(cfd_, blob_file_number,
                                                blob_file_addition);
      if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
        missing_blob_files_high_ =
            std::max(missing_blob_files_high_, blob_file_number);
        missing_blob_files_.insert(blob_file_number);
        s = Status::OK();
      } else if (!s.ok()) {
        return s;
      }
    }

    return s;
  }

  Status ApplyBlobFileGarbage(const BlobFileGarbage& blob_file_garbage) {
    const uint64_t blob_file_number = blob_file_garbage.GetBlobFileNumber();

    MutableBlobFileMetaData* const mutable_meta =
        GetOrCreateMutableBlobFileMetaData(blob_file_number);

    if (!mutable_meta) {
      std::ostringstream oss;
      oss << "Blob file #" << blob_file_number << " not found";

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (!mutable_meta->AddGarbage(blob_file_garbage.GetGarbageBlobCount(),
                                  blob_file_garbage.GetGarbageBlobBytes())) {
      std::ostringstream oss;
      oss << "Garbage overflow for blob file #" << blob_file_number;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    return Status::OK();
  }

  int GetCurrentLevelForTableFile(uint64_t file_number) const {
    auto it = table_file_levels_.find(file_number);
    if (it != table_file_levels_.end()) {
      return it->second;
    }

    assert(base_vstorage_);
    return base_vstorage_->GetFileLocation(file_number).GetLevel();
  }

  uint64_t GetOldestBlobFileNumberForTableFile(int level,
                                               uint64_t file_number) const {
    assert(level < num_levels_);

    const auto& added_files = levels_[level].added_files;

    auto it = added_files.find(file_number);
    if (it != added_files.end()) {
      const FileMetaData* const meta = it->second;
      assert(meta);

      return meta->oldest_blob_file_number;
    }

    assert(base_vstorage_);
    const FileMetaData* const meta =
        base_vstorage_->GetFileMetaDataByNumber(file_number);
    assert(meta);

    return meta->oldest_blob_file_number;
  }
  //无用
  Status ApplyFileDeletionIncompletely(int level,uint64_t file_number)
  {
    const uint64_t blob_file_number =
        GetOldestBlobFileNumberForTableFile(level, file_number);

    if (blob_file_number != kInvalidBlobFileNumber)
    {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta)
      {
        mutable_meta->UnlinkSst(file_number);
      }
    }
    auto& level_state = levels_[level];
    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end())
    {
      UnrefFile(add_it->second);
      add_files.erase(add_it);
    }
    auto& del_files = level_state.deleted_files;
    assert(del_files.find(file_number) == del_files.end());

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    if (track_found_and_missing_files_)
    {
      assert(version_edit_handler_);
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end())
      {
        l0_missing_files_.erase(file_number);
      }
      else if (non_l0_missing_files_.find(file_number) !=non_l0_missing_files_.end())
      {
        non_l0_missing_files_.erase(file_number);
      }
      else
      {
        auto fiter = found_files_.find(file_number);
        // Only mark new files added during this catchup attempt for deletion.
        // These files were never installed in VersionStorageInfo.
        // Already referenced files that are deleted by a VersionEdit will
        // be added to the VersionStorageInfo's obsolete files when the old
        // version is dereferenced.
        if (fiter != found_files_.end()) {
          assert(!ioptions_->cf_paths.empty());
          found_files_.erase(fiter);
        }
      }
    }

    return Status::OK();
  }
  //无用
  Status ApplyFileDeletionIncompletelyWithTrashRecycle(int level,uint64_t file_number)
  {
    const uint64_t blob_file_number =
        GetOldestBlobFileNumberForTableFile(level, file_number);

    if (blob_file_number != kInvalidBlobFileNumber)
    {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta)
      {
        mutable_meta->UnlinkSst(file_number);
      }
    }
    auto& level_state = levels_[level];
    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end())
    {
      UnrefFile(add_it->second);
      add_files.erase(add_it);
    }
    auto& del_files = level_state.deleted_files;
    assert(del_files.find(file_number) == del_files.end());

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
        l0_missing_files_.erase(file_number);
      } else if (non_l0_missing_files_.find(file_number) !=
                 non_l0_missing_files_.end()) {
        non_l0_missing_files_.erase(file_number);
      } else {
        auto fiter = found_files_.find(file_number);
        // Only mark new files added during this catchup attempt for deletion.
        // These files were never installed in VersionStorageInfo.
        // Already referenced files that are deleted by a VersionEdit will
        // be added to the VersionStorageInfo's obsolete files when the old
        // version is dereferenced.
        if (fiter != found_files_.end()) {
          assert(!ioptions_->cf_paths.empty());
          intermediate_files_.emplace_back(
              MakeTableFileName(ioptions_->cf_paths[0].path, file_number));
          found_files_.erase(fiter);
        }
      }
    }

    return Status::OK();
  }
  
  Status ApplyFileDeletion(int level, uint64_t file_number) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (level != current_level) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot delete table file #" << file_number << " from level "
          << level << " since it is ";
      if (current_level ==
          VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
        oss << "not in the LSM tree";
      } else {
        oss << "on level " << current_level;
      }

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      assert(invalid_level_sizes_[level] > 0);
      --invalid_level_sizes_[level];

      table_file_levels_[file_number] =
          VersionStorageInfo::FileLocation::Invalid().GetLevel();

      return Status::OK();
    }

    const uint64_t blob_file_number =
        GetOldestBlobFileNumberForTableFile(level, file_number);

    if (blob_file_number != kInvalidBlobFileNumber) {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta) {
        mutable_meta->UnlinkSst(file_number);
      }
    }

    auto& level_state = levels_[level];

    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end()) {
      UnrefFile(add_it->second);
      add_files.erase(add_it);
    }

    auto& del_files = level_state.deleted_files;
    assert(del_files.find(file_number) == del_files.end());
    del_files.emplace(file_number);

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
        l0_missing_files_.erase(file_number);
      } else if (non_l0_missing_files_.find(file_number) !=
                 non_l0_missing_files_.end()) {
        non_l0_missing_files_.erase(file_number);
      } else {
        auto fiter = found_files_.find(file_number);
        // Only mark new files added during this catchup attempt for deletion.
        // These files were never installed in VersionStorageInfo.
        // Already referenced files that are deleted by a VersionEdit will
        // be added to the VersionStorageInfo's obsolete files when the old
        // version is dereferenced.
        if (fiter != found_files_.end()) {
          assert(!ioptions_->cf_paths.empty());
          intermediate_files_.emplace_back(
              MakeTableFileName(ioptions_->cf_paths[0].path, file_number));
          found_files_.erase(fiter);
        }
      }
    }

    return Status::OK();
  }
  //无用
  Status ApplySegmentAddition(const Segment& sp,int new_level)
  {
      std::ostringstream oss;
      auto it=levels_[new_level].added_segments.find(sp.GetSegmentNum());
      if(it!=levels_[new_level].added_segments.end())
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      Segment* s=new Segment(sp);
      levels_[new_level].added_segments.emplace(s->GetSegmentNum(),s);
      return Status::OK();
  }
  //无用
  Status ApplySegmentChanged(const Segment& sp,int level,int new_level)
  {
    std::ostringstream oss;
      auto it=levels_[level].deleted_segments.find(sp.GetSegmentNum());
      if(it!=levels_[level].deleted_segments.end())
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      auto it1=levels_[new_level].added_segments.find(sp.GetSegmentNum());
      if(it1!=levels_[new_level].added_segments.end())
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      auto position=base_vstorage_->GetSegmentLocation(sp.GetSegmentNum());
      if(position.GetLevel()==VersionStorageInfo::FileLocation::Invalid().GetLevel()||position.GetLevel()!=level)
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      Segment* seg=base_segment_[position.GetLevel()][position.GetPosition()];
      Segment* s=new Segment(*seg);
      levels_[level].deleted_segments.emplace(s->GetSegmentNum(),s);
      levels_[new_level].added_segments.emplace(s->GetSegmentNum(),s);
      /*auto it=levels_[level].deleted_segments.find(sp.GetSegmentNum());
      if(it!=levels_[level].deleted_segments.end())
      {
        return;
      }
      auto it2=levels_[level].added_segments.find(sp.GetSegmentNum());
      if(it!=levels_[level].added_segments.end())
      {
        return;
      }
      int actual_level=base_vstorage_->GetSegmentLocation(sp.GetSegmentNum()).GetLevel();
      if(level!=actual_level)
      {
        if(actual_level==VersionStorageInfo::FileLocation::Invalid().GetLevel())
        {
          std::vector<std::vector<FileMetaData*>> filelist=sp.get_files();
          for(int i=0;i<int(filelist.size());i++)
          {
            bool is_ok=false;
            for(auto& file:filelist[i])
            {
              int based_level=GetFileLocation(file->fd.GetNumber()).GetLevel();
              if(based_level==VersionStorageInfo::FileLocation::Invalid().GetLevel())
              {

              }
              else
              {
                for(int i=1;;i++)
                {
                  if((based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1>level_per_segment_level*i-1)||(based_level>i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1))
                  {
                    is_ok=true;
                    return;
                  }
                  if((based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1)&&(based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1))
                  {
                    is_ok=false;
                    Segment* s=new Segment(sp);
                    std::vector<std::vector<FileMetaData*>> filelist=s->get_files();
                    for(int i=0;i<int(filelist.size());i++)
                    {
                      for(auto&f:filelist[i] )
                      {
                        FileMetaData* new_file=new FileMetaData(*f);
                        new_file->refs=1;
                        levels_[(level+1)*level_per_segment_level-i].deleted_files_from_non_exit_segment.emplace(f->fd.GetNumber());
                        levels_[(new_level+1)*level_per_segment_level-i].added_files_from_non_exit_segment.emplace(f->fd.GetNumber(),new_file);
                      }
                    }
                    return;
                  }
                }
                break;
              }
            }
            if(is_ok)
            {
              return;
            }
          }
          return;
        }
        else
        {
          return;
        }
      }
      Segment* s = new Segment(sp);
      //s->UpdateSegmentNum(version_set_->NewSegmentNumber());
      //s->UpdateSegmentIsChanged();
      levels_[level].deleted_segments.emplace(s->GetSegmentNum(),const_cast<Segment*>(s));
      levels_[new_level].added_segments_by_changed.emplace(s->GetSegmentNum(),const_cast<Segment*>(s));
      //added_segment_history.emplace(sp.GetSegmentNum());
      //deleted_segment_history.emplace(sp.GetSegmentNum());*/
      return Status::OK();
      //version_updated=true;
  }
  rocksdb::VersionStorageInfo::FileLocation GetSegmentLocation(uint64_t segment_number)
  {
    const auto it = segment_locations_.find(segment_number);

    if (it == segment_locations_.end()) {
      return rocksdb::VersionStorageInfo::FileLocation::Invalid();
    }

    /*assert(it->second.GetLevel() < num_levels_);
    assert(it->second.GetPosition() < files_[it->second.GetLevel()].size());
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]);
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]
               ->fd.GetNumber() == file_number);*/

    return it->second;
  }
  //无用
  Status ApplySegmentDeletion(const Segment&sp,int level)
  {
      std::ostringstream oss;
      auto it=levels_[level].deleted_segments.find(sp.GetSegmentNum());
      if(it!=levels_[level].deleted_segments.end())
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      auto position=base_vstorage_->GetSegmentLocation(sp.GetSegmentNum());
      if(position.GetLevel()==VersionStorageInfo::FileLocation::Invalid().GetLevel()||position.GetLevel()!=level)
      {
        return Status::Corruption("VersionBuilder", oss.str());
      }
      Segment* seg=base_segment_[position.GetLevel()][position.GetPosition()];
      Segment* s=new Segment(*seg);
      levels_[level].deleted_segments_caused_trush.emplace(s->GetSegmentNum(),s);
      /*int actual_level=GetSegmentLocation(sp.GetSegmentNum()).GetLevel();
      if(level!=actual_level)
      {
        if(actual_level==VersionStorageInfo::FileLocation::Invalid().GetLevel())
        {
          std::vector<std::vector<FileMetaData*>> filelist=sp.get_files();
          for(int i=0;i<int(filelist.size());i++)
          {
            bool is_ok=false;
            for(auto& file:filelist[i])
            {
              int based_level=GetFileLocation(file->fd.GetNumber()).GetLevel();
              if(based_level==VersionStorageInfo::FileLocation::Invalid().GetLevel())
              {

              }
              else
              {
                for(int i=1;;i++)
                {
                  if((based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1>level_per_segment_level*i-1)||(based_level>i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1))
                  {
                    is_ok=true;
                    return;
                  }
                  if((based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1)&&(based_level<=i*level_per_segment_level-1&&(level+1)*level_per_segment_level-1<=level_per_segment_level*i-1))
                  {
                    is_ok=false;
                    Segment* s=new Segment(sp);
                    std::vector<std::vector<FileMetaData*>> filelist=s->get_files();
                    for(int i=0;i<int(filelist.size());i++)
                    {
                      for(auto&f:filelist[i] )
                      {
                        levels_[(i+1)*level_per_segment_level-i].deleted_files_from_non_exit_segment.emplace(f->fd.GetNumber());
                      }
                    }
                    return;
                  }
                }
                break;
              }
            }
            if(is_ok)
            {
              return;
            }
          }
          return;
        }
        else
        {
          return;
        }
      }
      //Segment* s = new Segment(sp);
      //s->UpdateSegmentNum(version_set_->NewSegmentNumber());
      //s->UpdateSegmentIsChanged();
      levels_[level].deleted_segments_caused_trush.emplace(sp.GetSegmentNum(),&sp);
      //added_segment_history.emplace(sp.GetSegmentNum());
      //deleted_segment_history.emplace(sp.GetSegmentNum());*/
      return Status::OK();
    }
//无用
  Status ApplyFileAddition(int level, FileMetaData* meta) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const uint64_t file_number = meta->fd.GetNumber();

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (current_level !=
        VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot add table file #" << file_number << " to level " << level
          << " since it is already in the LSM tree on level " << current_level;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      ++invalid_level_sizes_[level];
      table_file_levels_[file_number] = level;

      return Status::OK();
    }

    auto& level_state = levels_[level];

    auto& del_files = level_state.deleted_files;
    auto del_it = del_files.find(file_number);
    if (del_it != del_files.end()) {
      del_files.erase(del_it);
    }

    FileMetaData*  f = meta;
    //f->refs = 1;

    if (file_metadata_cache_res_mgr_) {
      Status s = file_metadata_cache_res_mgr_->UpdateCacheReservation(
          f->ApproximateMemoryUsage(), true /* increase */);
      if (!s.ok()) {
        delete f;
        s = Status::MemoryLimit(
            "Can't allocate " +
            kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
                CacheEntryRole::kFileMetadata)] +
            " due to exceeding the memory limit "
            "based on "
            "cache capacity");
        return s;
      }
    }

    auto& add_files = level_state.added_files;
    assert(add_files.find(file_number) == add_files.end());
    add_files.emplace(file_number, f);
    const uint64_t blob_file_number = f->oldest_blob_file_number;

    if (blob_file_number != kInvalidBlobFileNumber) {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta) {
        mutable_meta->LinkSst(file_number);
      }
    }

    table_file_levels_[file_number] = level;

    Status s;
    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      assert(!ioptions_->cf_paths.empty());
      const std::string fpath =
          MakeTableFileName(ioptions_->cf_paths[0].path, file_number);
      s = version_edit_handler_->VerifyFile(cfd_, fpath, level, *meta);
      if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
        if (0 == level) {
          l0_missing_files_.insert(file_number);
        } else {
          non_l0_missing_files_.insert(file_number);
        }
        if (s.IsCorruption()) {
          found_files_.insert(file_number);
        }
        s = Status::OK();
      } else if (!s.ok()) {
        return s;
      } else {
        found_files_.insert(file_number);
      }
    }

    return s;
  }
Status ApplyFileAdditionWithReturn(int level, const FileMetaData& meta,FileMetaData* return_value) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const uint64_t file_number = meta.fd.GetNumber();

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (current_level !=
        VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot add table file #" << file_number << " to level " << level
          << " since it is already in the LSM tree on level " << current_level;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      ++invalid_level_sizes_[level];
      table_file_levels_[file_number] = level;

      return Status::OK();
    }

    auto& level_state = levels_[level];

    auto& del_files = level_state.deleted_files;
    auto del_it = del_files.find(file_number);
    if (del_it != del_files.end()) {
      del_files.erase(del_it);
    }

    FileMetaData* const f = return_value;
    f->refs = 1;

    if (file_metadata_cache_res_mgr_) {
      Status s = file_metadata_cache_res_mgr_->UpdateCacheReservation(
          f->ApproximateMemoryUsage(), true /* increase */);
      if (!s.ok()) {
        delete f;
        s = Status::MemoryLimit(
            "Can't allocate " +
            kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
                CacheEntryRole::kFileMetadata)] +
            " due to exceeding the memory limit "
            "based on "
            "cache capacity");
        return s;
      }
    }

    auto& add_files = level_state.added_files;
    assert(add_files.find(file_number) == add_files.end());
    add_files.emplace(file_number, f);

    const uint64_t blob_file_number = f->oldest_blob_file_number;

    if (blob_file_number != kInvalidBlobFileNumber) {
      MutableBlobFileMetaData* const mutable_meta =
          GetOrCreateMutableBlobFileMetaData(blob_file_number);
      if (mutable_meta) {
        mutable_meta->LinkSst(file_number);
      }
    }

    table_file_levels_[file_number] = level;

    Status s;
    if (track_found_and_missing_files_) {
      assert(version_edit_handler_);
      assert(!ioptions_->cf_paths.empty());
      const std::string fpath =
          MakeTableFileName(ioptions_->cf_paths[0].path, file_number);
      s = version_edit_handler_->VerifyFile(cfd_, fpath, level, meta);
      if (s.IsPathNotFound() || s.IsNotFound() || s.IsCorruption()) {
        if (0 == level) {
          l0_missing_files_.insert(file_number);
        } else {
          non_l0_missing_files_.insert(file_number);
        }
        if (s.IsCorruption()) {
          found_files_.insert(file_number);
        }
        s = Status::OK();
      } else if (!s.ok()) {
        return s;
      } else {
        found_files_.insert(file_number);
      }
    }

    return s;
  }
  Status ApplyCompactCursors(int level,
                             const InternalKey& smallest_uncompacted_key) {
    if (level < 0) {
      std::ostringstream oss;
      oss << "Cannot add compact cursor (" << level << ","
          << smallest_uncompacted_key.Encode().ToString()
          << " due to invalid level (level = " << level << ")";
      return Status::Corruption("VersionBuilder", oss.str());
    }
    if (level < num_levels_) {
      // Omit levels (>= num_levels_) when re-open with shrinking num_levels_
      updated_compact_cursors_[level] = smallest_uncompacted_key;
    }
    return Status::OK();
  }

  // Apply all of the edits in *edit to the current state.
  //在这里，我们进行这样的设计：我们需要
  Status Apply(const VersionEdit* edit) {
    bool version_updated = false;
    {
      const Status s = CheckConsistency(base_vstorage_);
      if (!s.ok()) {
        return s;
      }
    }

    // Note: we process the blob file related changes first because the
    // table file addition/deletion logic depends on the blob files
    // already being there.

    // Add new blob files
    for (const auto& blob_file_addition : edit->GetBlobFileAdditions()) {
      const Status s = ApplyBlobFileAddition(blob_file_addition);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    // Increase the amount of garbage for blob files affected by GC
    for (const auto& blob_file_garbage : edit->GetBlobFileGarbages()) {
      const Status s = ApplyBlobFileGarbage(blob_file_garbage);
      if (!s.ok()) {
        return s;
      }
      version_updated = true;
    }

    // Delete table files
    for (const auto& deleted_file : edit->GetDeletedFiles())
    {
      int file_number=deleted_file.second;
      int level=base_vstorage_->GetFileLocation(file_number).GetLevel();
      int segment_=base_vstorage_->GetFileInWhichSegment(file_number);
      if(static_cast<int>(level/level_per_segment_level)!=static_cast<int>(deleted_file.first/level_per_segment_level))
      {
        continue;
      }
      const Status s = ApplyFileDeletion(level, file_number);
      if (!s.ok())
      {
        return s;
      }
      deleted_segments_map.emplace(segment_,false);
      version_updated = true;
    }
    // Add new table files
    for (auto& new_file : edit->GetNewFiles())/*在这里，插入的单个文件要么从来没有在lsm中出现，要么是换层，且换层的的删除与插入在同一个edit中，则上方已经将删除的记录删掉了，故这里不需再检查*/
    {
      levels_[(static_cast<int>(new_file.first/level_per_segment_level)+1)*level_per_segment_level-1].middle_added_files.emplace_back(const_cast<FileMetaData*>(new FileMetaData(new_file.second)));
    }
    for(const auto& deleted_segments:edit->GetDeletedSegments())
    {
      if(base_vstorage_->JudgeSegment(deleted_segments.second.GetSegmentNum()))
      {
        deleted_segments_map[deleted_segments.second.GetSegmentNum()]=true;
      }
    }
    // Populate compact cursors for round-robin compaction, leave
    // the cursor to be empty to indicate it is invalid
    for(auto& compaction_added_files_:edit->GetCompactionAddedFiles())
    {
      if(compaction_added_files_==-1)
      {
        continue;
      }
      compaction_added_files_set.emplace(compaction_added_files_);
    }
    for (const auto& cursor : edit->GetCompactCursors()) {
      const int level = cursor.first;
      const InternalKey smallest_uncompacted_key = cursor.second;
      const Status s = ApplyCompactCursors(level, smallest_uncompacted_key);
      if (!s.ok()) {
        return s;
      }
    }

    if (track_found_and_missing_files_ && version_updated) {
      version_updated_since_last_check_ = true;
      if (!edited_in_atomic_group_ && edit->IsInAtomicGroup()) {
        edited_in_atomic_group_ = true;
      }
    }
    has_invalid_levels_=true;
    return Status::OK();
  }

  // Helper function template for merging the blob file metadata from the base
  // version with the mutable metadata representing the state after applying the
  // edits. The function objects process_base and process_mutable are
  // respectively called to handle a base version object when there is no
  // matching mutable object, and a mutable object when there is no matching
  // base version object. process_both is called to perform the merge when a
  // given blob file appears both in the base version and the mutable list. The
  // helper stops processing objects if a function object returns false. Blob
  // files with a file number below first_blob_file are not processed.
  template <typename ProcessBase, typename ProcessMutable, typename ProcessBoth>
  void MergeBlobFileMetas(uint64_t first_blob_file, ProcessBase process_base,
                          ProcessMutable process_mutable,
                          ProcessBoth process_both) const {
    assert(base_vstorage_);

    auto base_it = base_vstorage_->GetBlobFileMetaDataLB(first_blob_file);
    const auto base_it_end = base_vstorage_->GetBlobFiles().end();

    auto mutable_it = mutable_blob_file_metas_.lower_bound(first_blob_file);
    const auto mutable_it_end = mutable_blob_file_metas_.end();

    while (base_it != base_it_end && mutable_it != mutable_it_end) {
      const auto& base_meta = *base_it;
      assert(base_meta);

      const uint64_t base_blob_file_number = base_meta->GetBlobFileNumber();
      const uint64_t mutable_blob_file_number = mutable_it->first;

      if (base_blob_file_number < mutable_blob_file_number) {
        if (!process_base(base_meta)) {
          return;
        }

        ++base_it;
      } else if (mutable_blob_file_number < base_blob_file_number) {
        const auto& mutable_meta = mutable_it->second;

        if (!process_mutable(mutable_meta)) {
          return;
        }

        ++mutable_it;
      } else {
        assert(base_blob_file_number == mutable_blob_file_number);

        const auto& mutable_meta = mutable_it->second;

        if (!process_both(base_meta, mutable_meta)) {
          return;
        }

        ++base_it;
        ++mutable_it;
      }
    }

    while (base_it != base_it_end) {
      const auto& base_meta = *base_it;

      if (!process_base(base_meta)) {
        return;
      }

      ++base_it;
    }

    while (mutable_it != mutable_it_end) {
      const auto& mutable_meta = mutable_it->second;

      if (!process_mutable(mutable_meta)) {
        return;
      }

      ++mutable_it;
    }
  }

  // Helper function template for finding the first blob file that has linked
  // SSTs.
  template <typename Meta>
  static bool CheckLinkedSsts(const Meta& meta,
                              uint64_t* min_oldest_blob_file_num) {
    assert(min_oldest_blob_file_num);

    if (!meta.GetLinkedSsts().empty()) {
      assert(*min_oldest_blob_file_num == kInvalidBlobFileNumber);

      *min_oldest_blob_file_num = meta.GetBlobFileNumber();

      return false;
    }

    return true;
  }

  // Find the oldest blob file that has linked SSTs.
  uint64_t GetMinOldestBlobFileNumber() const {
    uint64_t min_oldest_blob_file_num = kInvalidBlobFileNumber;

    auto process_base =
        [&min_oldest_blob_file_num](
            const std::shared_ptr<BlobFileMetaData>& base_meta) {
          assert(base_meta);

          return CheckLinkedSsts(*base_meta, &min_oldest_blob_file_num);
        };

    auto process_mutable = [&min_oldest_blob_file_num](
                               const MutableBlobFileMetaData& mutable_meta) {
      return CheckLinkedSsts(mutable_meta, &min_oldest_blob_file_num);
    };

    auto process_both = [&min_oldest_blob_file_num](
                            const std::shared_ptr<BlobFileMetaData>& base_meta,
                            const MutableBlobFileMetaData& mutable_meta) {
#ifndef NDEBUG
      assert(base_meta);
      assert(base_meta->GetSharedMeta() == mutable_meta.GetSharedMeta());
#else
      (void)base_meta;
#endif

      // Look at mutable_meta since it supersedes *base_meta
      return CheckLinkedSsts(mutable_meta, &min_oldest_blob_file_num);
    };

    MergeBlobFileMetas(kInvalidBlobFileNumber, process_base, process_mutable,
                       process_both);

    return min_oldest_blob_file_num;
  }

  static std::shared_ptr<BlobFileMetaData> CreateBlobFileMetaData(
      const MutableBlobFileMetaData& mutable_meta) {
    return BlobFileMetaData::Create(
        mutable_meta.GetSharedMeta(), mutable_meta.GetLinkedSsts(),
        mutable_meta.GetGarbageBlobCount(), mutable_meta.GetGarbageBlobBytes());
  }

  bool OnlyLinkedToMissingL0Files(
      const std::unordered_set<uint64_t>& linked_ssts) const {
    return std::all_of(
        linked_ssts.begin(), linked_ssts.end(), [&](const uint64_t& element) {
          return l0_missing_files_.find(element) != l0_missing_files_.end();
        });
  }

  // Add the blob file specified by meta to *vstorage if it is determined to
  // contain valid data (blobs).
  template <typename Meta>
  void AddBlobFileIfNeeded(VersionStorageInfo* vstorage, Meta&& meta,
                           uint64_t blob_file_number) const {
    assert(vstorage);
    assert(meta);

    const auto& linked_ssts = meta->GetLinkedSsts();
    if (track_found_and_missing_files_) {
      if (missing_blob_files_.find(blob_file_number) !=
          missing_blob_files_.end()) {
        return;
      }
      // Leave the empty case for the below blob garbage collection logic.
      if (!linked_ssts.empty() && OnlyLinkedToMissingL0Files(linked_ssts)) {
        return;
      }
    }

    if (linked_ssts.empty() &&
        meta->GetGarbageBlobCount() >= meta->GetTotalBlobCount()) {
      return;
    }

    vstorage->AddBlobFile(std::forward<Meta>(meta));
  }

  // Merge the blob file metadata from the base version with the changes (edits)
  // applied, and save the result into *vstorage.
  void SaveBlobFilesTo(VersionStorageInfo* vstorage) const {
    assert(vstorage);
    assert(!track_found_and_missing_files_ || valid_version_available_);

    assert(base_vstorage_);
    vstorage->ReserveBlob(base_vstorage_->GetBlobFiles().size() +
                          mutable_blob_file_metas_.size());

    const uint64_t oldest_blob_file_with_linked_ssts =
        GetMinOldestBlobFileNumber();

    // If there are no blob files with linked SSTs, meaning that there are no
    // valid blob files
    if (oldest_blob_file_with_linked_ssts == kInvalidBlobFileNumber) {
      return;
    }

    auto process_base =
        [this, vstorage](const std::shared_ptr<BlobFileMetaData>& base_meta) {
          assert(base_meta);

          AddBlobFileIfNeeded(vstorage, base_meta,
                              base_meta->GetBlobFileNumber());

          return true;
        };

    auto process_mutable =
        [this, vstorage](const MutableBlobFileMetaData& mutable_meta) {
          AddBlobFileIfNeeded(vstorage, CreateBlobFileMetaData(mutable_meta),
                              mutable_meta.GetBlobFileNumber());

          return true;
        };

    auto process_both = [this, vstorage](
                            const std::shared_ptr<BlobFileMetaData>& base_meta,
                            const MutableBlobFileMetaData& mutable_meta) {
      assert(base_meta);
      assert(base_meta->GetSharedMeta() == mutable_meta.GetSharedMeta());

      if (!mutable_meta.HasDelta()) {
        assert(base_meta->GetGarbageBlobCount() ==
               mutable_meta.GetGarbageBlobCount());
        assert(base_meta->GetGarbageBlobBytes() ==
               mutable_meta.GetGarbageBlobBytes());
        assert(base_meta->GetLinkedSsts() == mutable_meta.GetLinkedSsts());

        AddBlobFileIfNeeded(vstorage, base_meta,
                            base_meta->GetBlobFileNumber());

        return true;
      }

      AddBlobFileIfNeeded(vstorage, CreateBlobFileMetaData(mutable_meta),
                          mutable_meta.GetBlobFileNumber());

      return true;
    };

    MergeBlobFileMetas(oldest_blob_file_with_linked_ssts, process_base,
                       process_mutable, process_both);
  }

  void MaybeAddFile(VersionStorageInfo* vstorage, int level,
                    FileMetaData* f) const {
    const uint64_t file_number = f->fd.GetNumber();
    if (track_found_and_missing_files_ && level == 0 &&
        l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
      return;
    }

    const auto& level_state = levels_[level];

    const auto& del_files = level_state.deleted_files;
    const auto del_it = del_files.find(file_number);

    if (del_it != del_files.end()) {
      // f is to-be-deleted table file
      vstorage->RemoveCurrentStats(f);
    } else {
      const auto& add_files = level_state.added_files;
      const auto add_it = add_files.find(file_number);

      // Note: if the file appears both in the base version and in the added
      // list, the added FileMetaData supersedes the one in the base version.
      if (add_it != add_files.end() && add_it->second != f) {
        vstorage->RemoveCurrentStats(f);
      } else {
        vstorage->AddFile(level, f);
      }
    }
  }

  bool ContainsCompleteVersion() const {
    assert(track_found_and_missing_files_);
    return l0_missing_files_.empty() && non_l0_missing_files_.empty() &&
           (missing_blob_files_high_ == kInvalidBlobFileNumber ||
            missing_blob_files_high_ < GetMinOldestBlobFileNumber());
  }

  bool HasMissingFiles() const {
    assert(track_found_and_missing_files_);
    return !l0_missing_files_.empty() || !non_l0_missing_files_.empty() ||
           missing_blob_files_high_ != kInvalidBlobFileNumber;
  }

  std::vector<std::string>& GetAndClearIntermediateFiles() {
    assert(track_found_and_missing_files_);
    return intermediate_files_;
  }

  void ClearFoundFiles() {
    assert(track_found_and_missing_files_);
    found_files_.clear();
  }

  template <typename Cmp>
  void SaveSSTFilesTo(VersionStorageInfo* vstorage, int level, Cmp cmp) const {
    // Merge the set of added files with the set of pre-existing files.
    // Drop any deleted files.  Store the result in *vstorage.
    const auto& base_files = base_vstorage_->LevelFiles(level);
    const auto& unordered_added_files = levels_[level].added_files;
    vstorage->Reserve(level, base_files.size() + unordered_added_files.size());

    MergeUnorderdAddedFilesWithBase(
        base_files, unordered_added_files, cmp,
        [&](FileMetaData* file) { MaybeAddFile(vstorage, level, file); });
  }

  template <typename Cmp, typename AddFileFunc>
  void MergeUnorderdAddedFilesWithBase(
      const std::vector<FileMetaData*>& base_files,
      const std::unordered_map<uint64_t, FileMetaData*>& unordered_added_files,
      Cmp cmp, AddFileFunc add_file_func) const {
    // Sort added files for the level.
    std::vector<FileMetaData*> added_files;
    added_files.reserve(unordered_added_files.size());
    for (const auto& pair : unordered_added_files) {
      added_files.push_back(pair.second);
    }
    std::sort(added_files.begin(), added_files.end(), cmp);

    auto base_iter = base_files.begin();
    auto base_end = base_files.end();
    auto added_iter = added_files.begin();
    auto added_end = added_files.end();
    while (added_iter != added_end || base_iter != base_end) {
      if (base_iter == base_end ||
          (added_iter != added_end && cmp(*added_iter, *base_iter))) {
        add_file_func(*added_iter++);
      } else {
        add_file_func(*base_iter++);
      }
    }
  }

  bool PromoteEpochNumberRequirementIfNeeded(
      VersionStorageInfo* vstorage) const {
    if (vstorage->HasMissingEpochNumber()) {
      return false;
    }

    for (int level = 0; level < num_levels_; ++level) {
      for (const auto& pair : levels_[level].added_files) {
        const FileMetaData* f = pair.second;
        if (f->epoch_number == kUnknownEpochNumber) {
          return false;
        }
      }
    }

    vstorage->SetEpochNumberRequirement(EpochNumberRequirement::kMustPresent);
    return true;
  }

  void SaveSSTFilesTo(VersionStorageInfo* vstorage) const {
    assert(vstorage);

    if (!num_levels_) {
      return;
    }

    EpochNumberRequirement epoch_number_requirement =
        vstorage->GetEpochNumberRequirement();

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      bool promoted = PromoteEpochNumberRequirementIfNeeded(vstorage);
      if (promoted) {
        epoch_number_requirement = vstorage->GetEpochNumberRequirement();
      }
    }

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      SaveSSTFilesTo(vstorage, /* level */ 0, *level_zero_cmp_by_seqno_);
    } else {
      SaveSSTFilesTo(vstorage, /* level */ 0, *level_zero_cmp_by_epochno_);
    }

    for (int level = 1; level < num_levels_; ++level) {
      SaveSSTFilesTo(vstorage, level, *level_nonzero_cmp_);
    }
  }
  //无用
  void ApplySegmentFileDeletion(FileMetaData* p,int level,int num)
  {
          //auto it1=deleted_files_by_segment_.find(p->fd.GetNumber());
          auto it2=files_may_caused_trush.find(p->fd.GetNumber());
          auto it3=added_files_by_segment_.find(p->fd.GetNumber());
          auto it4=added_files_single.find(p->fd.GetNumber());
          uint64_t file_number = p->fd.GetNumber();
            if(it3!=added_files_by_segment_.end())
            {
              if(it2==files_may_caused_trush.end())
              {
                int level1=it2->second;
                const Status s=ApplyFileDeletionIncompletely(level1,file_number);
                added_files_by_segment_.erase(it3);
              }
              else
              {
                  int level2=it2->second;
                  const Status s=ApplyFileDeletionIncompletelyWithTrashRecycle(level2,file_number);
                  added_files_by_segment_.erase(it3);
                  files_may_caused_trush.erase(it2);
              }
            }
            else if(it4!=added_files_single.end())
            {
              if(it2==files_may_caused_trush.end())
              {
                int level3=it2->second;
                const Status s=ApplyFileDeletionIncompletely(level3,file_number);
                added_files_single.erase(it3);
              }
              else
              {
                  int level4=it2->second;
                  const Status s=ApplyFileDeletionIncompletelyWithTrashRecycle(level4,file_number);
                  added_files_single.erase(it4);
                  files_may_caused_trush.erase(it2);
              }
            }
            else
            {
              ApplyFileDeletion((((level+1)*level_per_segment_level)-1)-num,p->fd.GetNumber());
              //levels_[(((level+1)*level_per_segment_level)-1)-std::get<1>(p)].deleted_files.emplace(std::get<0>(p)->fd.GetNumber());
            }
          //deleted_files_by_segment_.emplace(file_number,(((level+1)*level_per_segment_level)-1)-num);
  }
  /*无法处理以下情形：
  1.当该层级参与合并时，需确保该层级不可发生有重叠范围段的合并，否则会有重叠的文件，
    即我们在合并时，只使用段来换层或加入一个段，尽量不删除一个段
  2.不可发生有重叠范围段的删除（该段已经被合并消失，找不到这个段）
  3.不允许一个层级有多个有重叠范围的段向其发生合并，否则会有段内文件新旧的问题
  4.允许在一个segment进行换层时，对其中的一个单独的文件进行增删操作
  5.段中的压缩由versionbuilder直接进行，不需要手动触发。我们不需传入合并后产生的段*/
inline bool JudgeFileInRange(InternalKey largest,InternalKey smallest,const Comparator* ucmp,std::pair<InternalKey,InternalKey> key_range,std::pair<bool,bool> has_key)
{
  if(has_key.first==true&&has_key.second==true)
  {
    return ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(key_range.first.Encode()))>0&&ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(key_range.second.Encode()))<0;
  }
  else if(has_key.first==true&&has_key.second==false)
  {
    return ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(key_range.first.Encode()))>0;
  }
  else if(has_key.first==false&&has_key.second==true)
  {
    return ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(key_range.second.Encode()))<0;
  }
  else
  {
    return true;
  }
}
inline bool JudgeShouldStop(InternalKey largest,InternalKey target_key,bool has_target_key,const Comparator* ucmp)
{
  if(!has_target_key)
  {
    return false;
  }
  else
  {
    return ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(target_key.Encode()))>=0;
  }
}
bool ShouldCutTheSegment(std::vector<std::pair<InternalKey,InternalKey>> key_range,std::vector<std::pair<bool,bool>> has_key,const Comparator* ucmp)
{
  InternalKey smallest;
  InternalKey largest;
  bool has_smallest=false;
  bool has_largest=false;
  for(int i=0;i<static_cast<int>(key_range.size());i++)
  {
    if(has_key[i].first==true&&has_key[i].second==true)
    {
      if(!has_smallest)
      {
        smallest=key_range[i].first;
        has_smallest=true;
      }
      else
      {
        smallest=ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(key_range[i].first.Encode()))>=0?smallest:key_range[i].first;
      }
      if(!has_largest)
      {
        largest=key_range[i].second;
        has_largest=true;
      }
      else
      {
        largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(key_range[i].second.Encode()))<=0?largest:key_range[i].second;
      }
    }
    else if(has_key[i].first==true&&has_key[i].second==false)
    {
      if(!has_smallest)
      {
        smallest=key_range[i].first;
        has_smallest=true;
      }
      else
      {
        smallest=ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(key_range[i].first.Encode()))>=0?smallest:key_range[i].first;
      }
    }
    else if(has_key[i].first==false&&has_key[i].second==true)
    {
      if(!has_largest)
      {
        largest=key_range[i].second;
        has_largest=true;
      }
      else
      {
        largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(key_range[i].second.Encode()))<=0?largest:key_range[i].second;
      }
    }
    else
    {

    }
  }
  if(has_smallest==true&&has_largest==true)
  {
    return ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(largest.Encode()))<0;
  }
  else
  {
    return true;
  }
}
std::vector<std::vector<std::vector<FileMetaData*>>*> AddFileForSegment(VersionStorageInfo* vstorage_final,int segment_level,std::vector<std::vector<FileMetaData*>>* final_segment_filelist,std::vector<std::vector<FileMetaData*>>& segment_filelist,int max_level,const InternalKeyComparator* cmp)
{
  std::vector<std::vector<std::vector<FileMetaData*>>*> return_value_list{};
  int lower=0;
  int upper=max_level;
  std::vector<std::pair<std::vector<FileMetaData*>::iterator,std::vector<FileMetaData*>::iterator>> segment_iterator_list;
  for(int i=0;i<=max_level;i++)
  {
    segment_iterator_list.emplace_back(segment_filelist[i].begin(),segment_filelist[i].end());
  }
  std::vector<std::pair<std::vector<FileMetaData*>::iterator,std::vector<FileMetaData*>::iterator>> newfile_iterator_list;
  for(int i=0;i<=max_level;i++)
  {
    newfile_iterator_list.emplace_back(levels_[(segment_level+1)*level_per_segment_level-1-i].final_added_files.begin(),levels_[(segment_level+1)*level_per_segment_level-1-i].final_added_files.end());
  }
  std::vector<bool> not_empty_level(max_level+1,true);
  std::vector<std::pair<InternalKey,InternalKey>> key_range;
  bool should_remove=false;
  std::vector<std::pair<bool,bool>> has_key(max_level+1,std::make_pair(false,false));
  std::vector<FileMetaData*> middle_new_filelist;
  //FileMetaData* return_value;
  key_range.resize(max_level+1);
  InternalKey target_key;
  InternalKey next_target_key;
  bool has_next_target_key=false;
  //bool has_target_key_at_beginner;
  bool has_target_key=false;
  std::vector<bool> level_should_retry(max_level+1,false);
  int beginner=0;
  auto ucmp=cmp->user_comparator();
  bool has_emplaced;
  bool is_empty_segment;
  while(lower<=upper)
  {
    is_empty_segment=true;
    has_emplaced=false;
    has_target_key=false;
    has_next_target_key=false;
    //has_target_key_at_beginner=false;
    for(int i=0;i<=max_level;i++)
    {
      level_should_retry[i]=false;
    }
    if(beginner!=lower)
    {
      target_key=next_target_key;
      has_target_key=true;
    }
    for(int i=beginner;i<=upper;i++)
    {
      middle_new_filelist.clear();
      while(not_empty_level[i])
      {
        if(segment_iterator_list[i].first==segment_iterator_list[i].second)
        {
          if(newfile_iterator_list[i].first==newfile_iterator_list[i].second)
          {
            if(i==lower)
            {
              lower++;
            }
            if(i==upper)
            {
              upper--;
            }
            not_empty_level[i]=false;
            break;
          }
          else
          {
            if(levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.find((*newfile_iterator_list[i].first)->fd.GetNumber())!=levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.end())
            {
                should_remove=true;
                key_range[i].second=(*segment_iterator_list[i].first)->largest;
                newfile_iterator_list[i].first++;
            }
            else
            {
              if(i!=lower)
              {
                if(JudgeShouldStop((*newfile_iterator_list[i].first)->largest,target_key,has_target_key,ucmp))
                {
                  if(!should_remove)
                  {
                    //not_empty_level[i]=false;
                  }
                  break;
                }
                if(JudgeFileInRange((*newfile_iterator_list[i].first)->largest,(*newfile_iterator_list[i].first)->smallest,ucmp,key_range[i-1],has_key[i-1]))
                {
                  should_remove=true;
                  middle_new_filelist.emplace_back(*newfile_iterator_list[i].first);
                  (*newfile_iterator_list[i].first)->refs++;
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  newfile_iterator_list[i].first++;
                }
                else
                {
                  if(should_remove)
                  {
                    level_should_retry[i]=true;
                    break;
                  }
                  else
                  {
                    ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-i,**newfile_iterator_list[i].first,*newfile_iterator_list[i].first);
                    (*newfile_iterator_list[i].first)->refs++;
                    (*final_segment_filelist)[i].emplace_back(*newfile_iterator_list[i].first);
                    newfile_iterator_list[i].first++;
                  }
                }
              }
              else
              {
                if(should_remove)
                {
                  break;
                }
                else
                {
                  ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-i,**newfile_iterator_list[i].first,*newfile_iterator_list[i].first);
                  (*newfile_iterator_list[i].first)->refs++;
                  (*final_segment_filelist)[i].emplace_back(*newfile_iterator_list[i].first);
                  segment_iterator_list[i].first++;
                }
              }
            }
          }
        }
        else if(newfile_iterator_list[i].first==newfile_iterator_list[i].second)
        {
          if(segment_iterator_list[i].first==segment_iterator_list[i].second)
          {
            if(i==lower)
            {
              lower++;
            }
            if(i==upper)
            {
              upper--;
            }
            not_empty_level[i]=false;
            break;
          }
          else
          {
            if(levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.find((*segment_iterator_list[i].first)->fd.GetNumber())!=levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.end())
            {
              vstorage_final->RemoveCurrentStats(*segment_iterator_list[i].first);
              if(i==0)
              {
                auto it3=newfile_iterator_list[0].first;
                auto target=(*segment_iterator_list[0].first)->largest;
                auto target1=(*segment_iterator_list[0].first)->smallest;
                bool has_overlaped;
                int num=0;
                if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->smallest.Encode()),ExtractUserKey(target.Encode()))<=0)
                {
                  if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target1.Encode()))>=0)
                  {
                    has_overlaped=true;
                  }
                }
                if(has_overlaped)
                {
                  if(should_remove)
                  {
                    segment_iterator_list[i].first++;
                    break;
                  }
                  else
                  {
                    if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target.Encode()))<=0)
                    {
                      do
                      {
                        if(!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target.Encode()))<=0))
                        {
                          break;
                        }
                        ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1,**it3,*it3);
                        (*final_segment_filelist)[0].emplace_back(*it3);
                        (*newfile_iterator_list[i].first)->refs++;
                        it3++;
                        newfile_iterator_list[0].first++;
                      }while(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->smallest.Encode()),ExtractUserKey(target.Encode()))<=0&&ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target1.Encode()))>=0&&it3!=newfile_iterator_list[0].second);
                    }
                  }
                }
                else
                {
                  should_remove=true;
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  segment_iterator_list[i].first++;
                }
              }
              else
              {
                should_remove=true;
                key_range[i].second=(*segment_iterator_list[i].first)->largest;
                segment_iterator_list[i].first++;
              }
            }
            else
            {
              if(i!=lower)
              {
                if(JudgeShouldStop((*newfile_iterator_list[i].first)->largest,target_key,has_target_key,ucmp))
                {
                  if(!should_remove)
                  {
                    //not_empty_level[i]=false;
                  }
                  break;
                }
                if(JudgeFileInRange((*segment_iterator_list[i].first)->largest,(*segment_iterator_list[i].first)->smallest,ucmp,key_range[i-1],has_key[i-1]))
                {
                  should_remove=true;
                  middle_new_filelist.emplace_back(new FileMetaData(**segment_iterator_list[i].first));
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  ApplyFileDeletion(level_per_segment_level*(segment_level+1)-i-1,(*segment_iterator_list[i].first)->fd.GetNumber());
                  segment_iterator_list[i].first++;
                }
                else
                {
                  if(should_remove)
                  {
                    level_should_retry[i]=true;
                    break;
                  }
                  else
                  {
                    (*final_segment_filelist)[i].emplace_back(*segment_iterator_list[i].first);
                    segment_iterator_list[i].first++;
                  }
                }
              }
              else
              {
                if(should_remove)
                {
                  break;
                }
                else
                {
                  (*final_segment_filelist)[i].emplace_back(*segment_iterator_list[i].first);
                  segment_iterator_list[i].first++;
                }
              }
            }
          }
        }
        else
        {
          if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*segment_iterator_list[i].first)->smallest.Encode()),ExtractUserKey((*newfile_iterator_list[i].first)->smallest.Encode()))<=0)
          {
            if(levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.find((*segment_iterator_list[i].first)->fd.GetNumber())!=levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.end())
            {
              vstorage_final->RemoveCurrentStats(*segment_iterator_list[i].first);
              if(i==0)
              {
                auto it3=newfile_iterator_list[0].first;
                auto target=(*segment_iterator_list[0].first)->largest;
                auto target1=(*segment_iterator_list[0].first)->smallest;
                bool has_overlaped;
                int num=0;
                if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->smallest.Encode()),ExtractUserKey(target.Encode()))<=0)
                {
                  if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target1.Encode()))>=0)
                  {
                    has_overlaped=true;
                  }
                }
                if(has_overlaped)
                {
                  if(should_remove)
                  {
                    segment_iterator_list[i].first++;
                    break;
                  }
                  else
                  {
                    if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target.Encode()))<=0)
                    {
                      do
                      {
                        if(!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target.Encode()))<=0))
                        {
                          break;
                        }
                        ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1,**it3,*it3);
                        (*final_segment_filelist)[0].emplace_back(*it3);
                        (*newfile_iterator_list[i].first)->refs++;
                        it3++;
                        newfile_iterator_list[0].first++;
                      }while(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->smallest.Encode()),ExtractUserKey(target.Encode()))<=0&&ucmp->CompareWithoutTimestamp(ExtractUserKey((*it3)->largest.Encode()),ExtractUserKey(target1.Encode()))>=0&&it3!=newfile_iterator_list[0].second);
                    }
                  }
                }
                else
                {
                  should_remove=true;
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  segment_iterator_list[i].first++;
                }
              }
              else
              {
                should_remove=true;
                key_range[i].second=(*segment_iterator_list[i].first)->largest;
                segment_iterator_list[i].first++;
              }
            }
            else
            {
              if(i!=lower)
              {
                if(JudgeShouldStop((*newfile_iterator_list[i].first)->largest,target_key,has_target_key,ucmp))
                {
                  if(!should_remove)
                  {
                    //not_empty_level[i]=false;
                  }
                  break;
                }
                if(JudgeFileInRange((*segment_iterator_list[i].first)->largest,(*segment_iterator_list[i].first)->smallest,ucmp,key_range[i-1],has_key[i-1]))
                {
                  should_remove=true;
                  middle_new_filelist.emplace_back(new FileMetaData(**segment_iterator_list[i].first));
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  ApplyFileDeletion(level_per_segment_level*(segment_level+1)-i-1,(*segment_iterator_list[i].first)->fd.GetNumber());
                  segment_iterator_list[i].first++;
                }
                else
                {
                  if(should_remove)
                  {
                    level_should_retry[i]=true;
                    break;
                  }
                  else
                  {
                    (*final_segment_filelist)[i].emplace_back(*segment_iterator_list[i].first);
                    segment_iterator_list[i].first++;
                  }
                }
              }
              else
              {
                if(should_remove)
                {
                  break;
                }
                else
                {
                  (*final_segment_filelist)[i].emplace_back(*segment_iterator_list[i].first);
                  segment_iterator_list[i].first++;
                }
              }
            }
          }
          else
          {
            if(levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.find((*newfile_iterator_list[i].first)->fd.GetNumber())!=levels_[(segment_level+1)*level_per_segment_level-1-i].deleted_files.end())
            {
              should_remove=true;
              key_range[i].second=(*segment_iterator_list[i].first)->largest;
              newfile_iterator_list[i].first++;
            }
            else
            {
              if(i!=lower)
              {
                if(JudgeShouldStop((*newfile_iterator_list[i].first)->largest,target_key,has_target_key,ucmp))
                {
                  if(!should_remove)
                  {
                    //not_empty_level[i]=false;
                  }
                  break;
                }
                if(JudgeFileInRange((*newfile_iterator_list[i].first)->largest,(*newfile_iterator_list[i].first)->smallest,ucmp,key_range[i-1],has_key[i-1]))
                {
                  should_remove=true;
                  middle_new_filelist.emplace_back(*newfile_iterator_list[i].first);
                  (*newfile_iterator_list[i].first)->refs++;
                  key_range[i].second=(*segment_iterator_list[i].first)->largest;
                  newfile_iterator_list[i].first++;
                }
                else
                {
                  if(should_remove)
                  {
                    level_should_retry[i]=true;
                    break;
                  }
                  else
                  {
                    ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-i,**newfile_iterator_list[i].first,*newfile_iterator_list[i].first);
                    (*final_segment_filelist)[i].emplace_back(*newfile_iterator_list[i].first);
                    (*newfile_iterator_list[i].first)->refs++;
                    newfile_iterator_list[i].first++;
                  }
                }
              }
              else
              {
                if(should_remove)
                {
                  break;
                }
                else
                {
                  ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-i,**newfile_iterator_list[i].first,*newfile_iterator_list[i].first);
                  (*final_segment_filelist)[i].emplace_back(*newfile_iterator_list[i].first);
                  (*newfile_iterator_list[i].first)->refs++;
                  segment_iterator_list[i].first++;
                }
              }
            }
          }
        }
      }
      if(!final_segment_filelist[i].empty())
      {
        key_range[i].first=((*final_segment_filelist)[i].back())->largest;
        has_key[i].first=true;
      }
      else
      {
        has_key[i].first=false;
      }
      if(segment_iterator_list[i].first!=segment_iterator_list[i].second&&newfile_iterator_list[i].first!=newfile_iterator_list[i].second)
      {
        key_range[i].second=ucmp->CompareWithoutTimestamp(ExtractUserKey((*segment_iterator_list[i].first)->smallest.Encode()),ExtractUserKey((*newfile_iterator_list[i].first)->smallest.Encode()))<=0?(*segment_iterator_list[i].first)->smallest:(*newfile_iterator_list[i].first)->smallest;
        has_key[i].second=true;
        if(has_target_key)
        {
          target_key=ucmp->CompareWithoutTimestamp(ExtractUserKey((*segment_iterator_list[i].first)->smallest.Encode()),ExtractUserKey(target_key.Encode()))<=0?(*segment_iterator_list[i].first)->smallest:target_key;
          target_key=ucmp->CompareWithoutTimestamp(ExtractUserKey((*newfile_iterator_list[i].first)->smallest.Encode()),ExtractUserKey(target_key.Encode()))<=0?(*newfile_iterator_list[i].first)->smallest:target_key;
        }
        else
        {
          target_key=ucmp->CompareWithoutTimestamp(ExtractUserKey((*segment_iterator_list[i].first)->smallest.Encode()),ExtractUserKey((*newfile_iterator_list[i].first)->smallest.Encode()))<=0?(*segment_iterator_list[i].first)->smallest:(*newfile_iterator_list[i].first)->smallest;
        }
        has_target_key=true;
      }
      else if(segment_iterator_list[i].first!=segment_iterator_list[i].second)
      {
        key_range[i].second=(*segment_iterator_list[i].first)->smallest;
        has_key[i].second=true;
        if(has_target_key)
        {
          target_key=ucmp->CompareWithoutTimestamp(ExtractUserKey((*segment_iterator_list[i].first)->smallest.Encode()),ExtractUserKey(target_key.Encode()))<=0?(*segment_iterator_list[i].first)->smallest:target_key;
        }
        else
        {
          target_key=(*segment_iterator_list[i].first)->smallest;
        }
        has_target_key=true;
      }
      else if(newfile_iterator_list[i].first!=newfile_iterator_list[i].second)
      {
        key_range[i].second=(*newfile_iterator_list[i].first)->smallest;
        has_key[i].second=true;
        if(has_target_key)
        {
          target_key=ucmp->CompareWithoutTimestamp(ExtractUserKey((*newfile_iterator_list[i].first)->smallest.Encode()),ExtractUserKey(target_key.Encode()))<=0?(*segment_iterator_list[i].first)->smallest:target_key;
        }
        else
        {
          target_key=(*newfile_iterator_list[i].first)->smallest;
        }
        has_target_key=true;
      }
      else
      {
        has_key[i].second=false;
      }
      if((!has_next_target_key)&&has_target_key)
      {
        next_target_key=target_key;
        has_next_target_key=true;
      }
      if(should_remove)
      {
        auto it1=middle_new_filelist.begin();
        auto it2=middle_new_filelist.end();
        int middle_judge_level=i-1;
        while(it1!=it2&&middle_judge_level>=0)
        {
          if(has_key[middle_judge_level].first==false&&has_key[middle_judge_level].second==false)
          {
            middle_judge_level--;
            //continue;
          }
          else if(has_key[middle_judge_level].first==true&&has_key[middle_judge_level].second==false)
          {
            while(ucmp->CompareWithoutTimestamp(ExtractUserKey(key_range[i].first.Encode()),ExtractUserKey((*it1)->smallest.Encode()))>=0&&it1!=it2)
            {
              ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-middle_judge_level-1,**it1,*it1);
              (*final_segment_filelist)[middle_judge_level+1].emplace_back(*it1);
              key_range[middle_judge_level+1].first=(*it1)->largest;
              has_key[middle_judge_level+1].first=true;
              it1++;
            }
            middle_judge_level--;
          }
          else if(has_key[middle_judge_level].first==false&&has_key[middle_judge_level].second==true)
          {
            auto it2_begin=it2;
            while(ucmp->CompareWithoutTimestamp(ExtractUserKey(key_range[i].second.Encode()),ExtractUserKey((*(it2-1))->largest.Encode()))<=0&&it1!=it2)
            {
              it2--;
            }
            auto it2_end=it2;
            for(;it2_begin!=it2_begin;it2_end++)
            {
              ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-middle_judge_level-1,**it2_end,*it2_end);
              (*final_segment_filelist)[middle_judge_level+1].emplace_back(*it2_end);
              key_range[middle_judge_level+1].first=(*it2_end)->largest;
              has_key[middle_judge_level+1].first=true;
            }
            middle_judge_level--;
          }
          else
          {
            while(ucmp->CompareWithoutTimestamp(ExtractUserKey(key_range[i].first.Encode()),ExtractUserKey((*it1)->smallest.Encode()))>=0&&it1!=it2)
            {
              ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-middle_judge_level-1,**it1,*it1);
              (*final_segment_filelist)[middle_judge_level+1].emplace_back(*it1);
              key_range[middle_judge_level+1].first=(*it1)->largest;
              has_key[middle_judge_level+1].first=true;
              it1++;
            }
            auto it2_begin=it2;
            while(ucmp->CompareWithoutTimestamp(ExtractUserKey(key_range[i].second.Encode()),ExtractUserKey((*it2-1)->largest.Encode()))<=0&&it1!=it2)
            {
              it2--;
            }
            auto it2_end=it2;
            for(;it2_begin!=it2_begin;it2_end++)
            {
              ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1-middle_judge_level-1,**it2_end,*it2_end);
              (*final_segment_filelist)[middle_judge_level+1].emplace_back(*it2_end);
              key_range[middle_judge_level+1].first=(*it2_end)->largest;
              has_key[middle_judge_level+1].first=true;
            }
            middle_judge_level--;
          }
        }
        while(it1!=it2)
        {
          ApplyFileAdditionWithReturn((segment_level+1)*level_per_segment_level-1,**it1,*it1);
          key_range[0].first=(*it1)->largest;
          has_key[0].first=true;
          it1++;
        }
      }
    }
    bool level_lable=false;
    for(int i=0;i<static_cast<int>(level_should_retry.size());i++)
    {
      if(level_should_retry[i]==true)
      {
        level_lable=true;
        beginner=i;
        break;
      }
    }
    if(!level_lable)
    {
      beginner=lower;
    }
    //bool is_empty_segment=true;
    for(auto f:*final_segment_filelist)
    {
      if(!f.empty())
      {
        is_empty_segment=false;
        break;
      }
    }
    if(!is_empty_segment)
    {
      if(ShouldCutTheSegment(key_range,has_key,ucmp))
      {
        has_emplaced=true;
        return_value_list.emplace_back(std::move(final_segment_filelist));
        final_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
        final_segment_filelist->resize(level_per_segment_level);
      }
    }
  }
  if(upper==lower)
  {
    auto final_newfile_it1=newfile_iterator_list[lower].first;
    auto final_newfile_it2=newfile_iterator_list[lower].second;
    auto final_segment_it1=segment_iterator_list[lower].first;
    auto final_segment_it2=segment_iterator_list[lower].second;
    int insert_level=(segment_level+1)*level_per_segment_level-1-upper;
    while(final_newfile_it1!=final_newfile_it2||final_segment_it1!=final_segment_it2)
    {
      if(final_newfile_it1==final_newfile_it2)
      {
        while(final_segment_it1!=final_segment_it2)
        {
          (*final_segment_filelist)[upper].emplace_back(*final_segment_it1);
          final_segment_it1++;
        }
        break;
      }
      else if(final_segment_it1==final_segment_it2)
      {
        while(final_newfile_it1!=final_newfile_it2)
        {
          ApplyFileAdditionWithReturn(insert_level,**final_newfile_it1,*final_newfile_it1);
          (*final_segment_filelist)[upper].emplace_back(*final_newfile_it1);
          final_newfile_it1++;
        }
        break;
      }
      else
      {
        if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*final_newfile_it1)->smallest.Encode()),ExtractUserKey((*final_segment_it1)->smallest.Encode()))<=0)
        {
          ApplyFileAdditionWithReturn(insert_level,**final_newfile_it1,*final_newfile_it1);
          (*final_segment_filelist)[upper].emplace_back(*final_newfile_it1);
          final_newfile_it1++;
        }
        else
        {
          (*final_segment_filelist)[upper].emplace_back(*final_segment_it1);
          final_segment_it1++;
        }
      }
    }
  }
  else
  {

  }
  if(has_emplaced)
  {
    delete final_segment_filelist;
  }
  else
  {
    if(is_empty_segment)
    {
      delete final_segment_filelist;
    }
    else
    {
      return_value_list.emplace_back(final_segment_filelist);
    }
  }
  return return_value_list;
}
void AppendFileListAtLast(std::vector<std::vector<FileMetaData*>> files_,std::vector<std::vector<FileMetaData*>> added_files)
{
    int new_level=added_files.size();
    int i=0;
    for(;i<new_level;i++)
    {
      for(auto added_file:added_files[i])
      {
        files_[i].emplace_back(added_file);
      }
    }
}
int HasOverlapWithLevel(int segment_level,int lvl, const FileMetaData* file, const InternalKeyComparator* cmp,std::vector<std::vector<FileMetaData*>>& files_)
  {
    auto ucmp=cmp->user_comparator();
    auto& level_files = files_[segment_level*level_per_segment_level-1-lvl];
    
    if (level_files.empty())
    {
      return lvl;
    }
    if (ucmp->CompareWithoutTimestamp(ExtractUserKey(file->largest.Encode()), ExtractUserKey(level_files[0]->smallest.Encode())) < 0)
    {
        return lvl;
    }
    if(ucmp->CompareWithoutTimestamp(ExtractUserKey(file->smallest.Encode()), ExtractUserKey((level_files.back())->largest.Encode())) > 0)
    {
        return lvl;
    }
    auto it = std::lower_bound(level_files.begin(), level_files.end(), file,
        [ucmp](const FileMetaData* a, const FileMetaData* b) {
            return ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()), ExtractUserKey(b->largest.Encode())) <= 0;
        });
    if (it != level_files.begin())
    {
        if (!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*(it--))->smallest.Encode()),ExtractUserKey(file->smallest.Encode()))>=0))
        {
          return -1;
        }
    }
    else
    {
        return lvl;
    }
    return lvl;
}
int GetL0InputLevel(std::vector<std::vector<FileMetaData*>>& filelist,std::vector<FileMetaData*>new_files,const InternalKeyComparator* cmp,int segment_not_empty_level)
{
  auto ucmp=cmp->user_comparator();
  int level=segment_not_empty_level;
  int largest_level=0;
  int max_level=-1;
  InternalKey smallest;
  InternalKey largest;
  for(auto& file:new_files)
  {
    if(compaction_added_files_set.find(file->fd.GetNumber())!=compaction_added_files_set.end())
    {
      levels_[level_per_segment_level-1].final_added_files.emplace_back(file);
      continue;
    }
    int actual_level=-1;
    if(!(ucmp->CompareWithoutTimestamp(ExtractUserKey(file->smallest.Encode()),ExtractUserKey(largest.Encode()))>=0||ucmp->CompareWithoutTimestamp(ExtractUserKey(file->largest.Encode()),ExtractUserKey(smallest.Encode()))<=0))
    {
      actual_level=largest_level;
    }
    int lvl=level-1;
    if(lvl<=actual_level)
    {
      levels_[level_per_segment_level-actual_level-2].final_added_files.emplace_back(file);
      largest_level=std::max(largest_level,largest_level+1);
      largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(file->largest.Encode()))>=0?largest:file->largest;
      smallest=ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(file->smallest.Encode()))<=0?smallest:file->smallest;
      max_level=std::max(max_level,actual_level+1);
      continue;
    }
    for (; lvl >=actual_level+1; --lvl)
    {
        int p=HasOverlapWithLevel(0,lvl, file, cmp,filelist);
        if (p==-1)
        {
            //levels_[level_per_segment_level-lvl-2].final_added_files.emplace_back(file);
            //largest_level=std::max(largest_level,lvl+1);
            //largest=cmp->Compare(largest,file->largest)>=0?largest:file->largest;
            //max_level=std::max(max_level,lvl+1);
            //continue;
            break;
        }
    }
    levels_[level_per_segment_level-lvl-2].final_added_files.emplace_back(file);
    largest_level=std::max(largest_level,lvl+1);
    largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(largest.Encode()),ExtractUserKey(file->largest.Encode()))>=0?largest:file->largest;
    smallest=ucmp->CompareWithoutTimestamp(ExtractUserKey(smallest.Encode()),ExtractUserKey(file->smallest.Encode()))<=0?smallest:file->smallest;
    max_level=std::max(max_level,lvl+1);
  }
  return max_level;
}
int GetInputLevel(std::vector<std::vector<FileMetaData*>>& filelist,std::vector<FileMetaData*>new_files,const InternalKeyComparator* cmp,int segment_not_empty_level,int segment_level)
{
  int level=segment_not_empty_level;
  int largest_level=0;
  int max_level=-1;
  for(auto& file:new_files)
  {
    if(compaction_added_files_set.find(file->fd.GetNumber())!=compaction_added_files_set.end())
    {
      levels_[(segment_level+1)*level_per_segment_level-1].final_added_files.emplace_back(file);
      continue;
    }
    int lvl=level-1;
    for (; lvl >=0; --lvl)
    {
        int p=HasOverlapWithLevel(0,lvl, file, cmp,filelist);
        if (p==-1)
        {
            //levels_[(segment_level+1)*level_per_segment_level-lvl-2].final_added_files.emplace_back(file);
            //max_level=std::max(max_level,lvl+1);
            //continue;
            break;
        }
    }
    levels_[(segment_level+1)*level_per_segment_level-lvl-2].final_added_files.emplace_back(file);
    max_level=std::max(max_level,lvl+1);
  }
  return max_level;
}
void AddFileForEmptyL0Segment(std::vector<std::vector<FileMetaData*>>* final_new_filelist,std::vector<FileMetaData*> new_file_list,const InternalKeyComparator* cmp)
{
  int upper=0;
  auto it1=new_file_list.begin();
  auto it2=new_file_list.end();
  auto ucmp=cmp->user_comparator();
  FileMetaData* return_value;
  //std::vector<std::pair<InternalKey,InternalKey>> key_range;
  while(it1!=it2)
  {
    (*it1)->refs++;
    for(int i=upper;i>=0;i--)
    {
      if(i==0&&final_new_filelist[i].empty())
      {
        ApplyFileAdditionWithReturn(level_per_segment_level-1,**it1,*it1);
        (*final_new_filelist)[i].emplace_back(*it1);
        //key_range[i].first=(*it1)->smallest;
        //key_range[i].second=(*it1)->largest;
        it1++;
        upper++;
        break;
      }
      else
      {
        if(final_new_filelist[i].empty())
        {

        }
        else
        {
          if(ucmp->CompareWithoutTimestamp(ExtractUserKey((*it1)->smallest.Encode()),ExtractUserKey((*final_new_filelist)[i].back()->largest.Encode()))<=0)
          {
            ApplyFileAdditionWithReturn(level_per_segment_level-1-i-1,**it1,*it1);
            (*final_new_filelist)[i+1].emplace_back(*it1);
            //key_range[i+1].second=(*it1)->largest;
            it1++;
            if(i+1>upper)
            {
              upper++;
            }
            break;
          }
        }
      }
      if(i==0)
      {
        ApplyFileAdditionWithReturn(level_per_segment_level-1-i-1,**it1,*it1);
          (*final_new_filelist)[i+1].emplace_back(*it1);
          it1++;
          if(i+1>upper)
          {
            upper++;
          }
          break;
      }
    }
  }
}
  void SaveSegmentsTo(VersionStorageInfo* vstorage)
{
    //assert(vstorage);
    std::vector<std::vector<Segment*>> base_segment_trush(base_segment_);
    const InternalKeyComparator* cmp_=base_vstorage_->InternalComparator();
    auto ucmp=cmp_->user_comparator();
    auto pair_comp_for_files = [&](const auto& a, const auto& b)
    {
      int comp_result = ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()), ExtractUserKey(b->smallest.Encode()));
      return comp_result<=0;
    };
    auto pair_comp_for_l0_files_seg=[&](const auto& a, const auto& b)
    {
      int comp_result1=ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()),ExtractUserKey(b->largest.Encode()));
      int comp_result2=ucmp->CompareWithoutTimestamp(ExtractUserKey(b->smallest.Encode()),ExtractUserKey(a->largest.Encode()));
      if(comp_result1>0||comp_result2>0)
      {
        //return ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()),ExtractUserKey(b->smallest.Encode()))<=0;
        return comp_result1<=0;
      }
      else
      {
        if (a->fd.largest_seqno != b->fd.largest_seqno)
        {
          return a->fd.largest_seqno > b->fd.largest_seqno;
        }
        if (a->fd.smallest_seqno != b->fd.smallest_seqno)
        {
          return a->fd.smallest_seqno > b->fd.smallest_seqno;
        }
        return a->fd.GetNumber() > b->fd.GetNumber();
      }
    };
    auto pair_comp_for_l0_files_epoch=[&](const auto& a, const auto& b)
    {
      int comp_result1=ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()),ExtractUserKey(b->largest.Encode()));
      int comp_result2=ucmp->CompareWithoutTimestamp(ExtractUserKey(b->smallest.Encode()),ExtractUserKey(a->largest.Encode()));
      if(comp_result1>0||comp_result2>0)
      {
        return ucmp->CompareWithoutTimestamp(ExtractUserKey(a->smallest.Encode()),ExtractUserKey(b->smallest.Encode()))<=0;
      }
      else
      {
        if (a->epoch_number != b->epoch_number)
        {
          return a->epoch_number > b->epoch_number;
        } else
        {
          if (a->fd.largest_seqno != b->fd.largest_seqno)
          {
            return a->fd.largest_seqno > b->fd.largest_seqno;
          }
          if (a->fd.smallest_seqno != b->fd.smallest_seqno)
          {
            return a->fd.smallest_seqno > b->fd.smallest_seqno;
          }
          return a->fd.GetNumber() > b->fd.GetNumber();
        }
      }
    };
    for(int i=0;i<base_vstorage_->NumSegmentLevel();i++)
    {
      if(i==0)
      {
        EpochNumberRequirement epoch_number_requirement =
        vstorage->GetEpochNumberRequirement();
        if (epoch_number_requirement == EpochNumberRequirement::kMightMissing)
        {
          bool promoted = PromoteEpochNumberRequirementIfNeeded(vstorage);
          if (promoted)
          {
            epoch_number_requirement = vstorage->GetEpochNumberRequirement();
          }
        }
        if (epoch_number_requirement == EpochNumberRequirement::kMightMissing)
        {
          std::sort(levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin(),levels_[(i+1)*level_per_segment_level-1].middle_added_files.end(),pair_comp_for_l0_files_seg);
        }
        else
        {
          std::sort(levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin(),levels_[(i+1)*level_per_segment_level-1].middle_added_files.end(),pair_comp_for_l0_files_epoch);
        }
        //std::sort(levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin(),levels_[(i+1)*level_per_segment_level-1].middle_added_files.end(),pair_comp_for_files);
        std::vector<FileMetaData*>& filelist=levels_[(i+1)*level_per_segment_level-1].middle_added_files;
        std::vector<Segment*>& level_segments=*base_vstorage_->GetLevelSegments(i);
        //std::sort(levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin(),levels_[i].middle_added_files.end(),pair_comp_for_files);
        auto input_it=levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin();
        auto input_it_end=levels_[(i+1)*level_per_segment_level-1].middle_added_files.end();
        int middle=0;
        int position=-1;
        int total=0;
        auto segment_it=level_segments.begin();
        auto segment_it_end=level_segments.end();
        std::vector<Segment*> overlapping_segments;
        std::vector<FileMetaData*> new_file_list;
        InternalKey input_smallest;
        InternalKey input_largest;
        InternalKey segment_smallest;
        InternalKey segment_largest;
        bool has_updated=false;
        bool should_retry=true;
        int num_of_merge_level=0;
        int first_segment=0;
        int last_segment=0;
        new_file_list.clear();
        while(input_it!=input_it_end)
        {
          for(int j=i*level_per_segment_level;j<(i+1)*level_per_segment_level;j++)
          {
            levels_[j].final_added_files.clear();
          }
          position=-1;
          total=0;
          has_updated=false;
          overlapping_segments.clear();
          new_file_list.clear();
          new_file_list.emplace_back(*input_it);
          input_smallest=(*input_it)->smallest;
          input_largest=(*input_it)->largest;
          input_it++;
          while(should_retry)
          {
            should_retry=false;
            while(!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->smallest.Encode()),ExtractUserKey(input_largest.Encode()))>0||ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->largest.Encode()),ExtractUserKey(input_smallest.Encode()))<0)&&input_it!=input_it_end)
            {
              new_file_list.emplace_back(*input_it);
              input_largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(input_largest.Encode()),ExtractUserKey((*input_it)->largest.Encode()))>=0?input_largest:(*input_it)->largest;
              input_it++;
            }
            while (segment_it!=segment_it_end)
            {
              Segment* exiting_segments=*segment_it;
              auto it=deleted_segments_map.find(exiting_segments->GetSegmentNum());
              if(it!=deleted_segments_map.end())
              {
                if(it->second==true)
                {
                  for(auto fll:exiting_segments->get_files())
                  {
                    for(auto& f:fll)
                    {
                      vstorage->RemoveCurrentStats(f);
                    }
                  }
                }
                middle++;
                segment_it++;
                continue;
              }
              int first_overlapping_result= ucmp->CompareWithoutTimestamp(ExtractUserKey(input_largest.Encode()), ExtractUserKey(exiting_segments->smallest.Encode()));
              int second_overlapping_result=ucmp->CompareWithoutTimestamp(ExtractUserKey(exiting_segments->largest.Encode()),ExtractUserKey(input_smallest.Encode()));
              bool not_overlaps =first_overlapping_result < 0 ||second_overlapping_result < 0;
              if (!not_overlaps)
              {
                if(position==-1)
                {
                  position=middle;
                  last_segment=middle;
                }
                total++;
                overlapping_segments.emplace_back(exiting_segments);
                segment_it++;
                if(has_updated==false)
                {
                  segment_smallest=exiting_segments->smallest;
                  has_updated=true;
                }
                segment_largest=exiting_segments->largest;
                middle++;
                continue;
              }
              if(first_overlapping_result<0)
              {
                break;
              }
              segment_it++;
              middle++;
            }
            if(position==-1)
            {

            }
            else
            {
              while(!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->smallest.Encode()),ExtractUserKey(segment_largest.Encode()))>0||ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->largest.Encode()),ExtractUserKey(segment_smallest.Encode()))<0)&&input_it!=input_it_end)
              {
                should_retry=true;
                new_file_list.emplace_back(*input_it);
                input_largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(input_largest.Encode()),ExtractUserKey((*input_it)->largest.Encode()))>=0?input_largest:(*input_it)->largest;
                input_it++;
              }
            }
          }
          //std::sort(new_file_list.begin(),new_file_list.end(),pair_comp_for_l0_files);
          if(total==0)
          {
            if(first_segment<middle)
            {
              for(int j=first_segment;j<middle;j++)
              {
                auto it=deleted_segments_map.find(level_segments[j]->GetSegmentNum());
                if(it==deleted_segments_map.end())
                {
                  vstorage->AddSegments(i,level_segments[j],cmp_);
                  level_segments[j]->refs.fetch_add(1);
                }
                else
                {
                  if(it->second==true)
                  {

                  }
                  else
                  {
                    std::vector<std::vector<FileMetaData*>> new_segment_filelist((level_segments[j]->get_files()));
                    std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
                    new_segment_filelist.resize(level_per_segment_level);
                    final_new_segment_filelist->resize(level_per_segment_level);
                    int judge_level=level_per_segment_level-1;
                    while(judge_level!=-1)
                    {
                      if(!new_segment_filelist[judge_level].empty())
                      {
                        break;
                      }
                      judge_level--;
                    }
                    auto final_filelist=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
                    for(auto fl:final_filelist)
                    {
                      Segment* new_segment=new Segment(final_new_segment_filelist,cmp_);
                      new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                      new_segment->refs=1;
                      vstorage->AddSegments(i,new_segment,cmp_);
                    }
                  }
                }
              }
            }
            first_segment=middle;
            std::vector<std::vector<FileMetaData*>>* new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
            new_segment_filelist->resize(level_per_segment_level);
            AddFileForEmptyL0Segment(new_segment_filelist,new_file_list,cmp_);
            Segment* new_segment=new Segment(new_segment_filelist,cmp_);
            new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
            new_segment->refs=1;
            vstorage->AddSegments(i,new_segment,cmp_);
          }
          else
          {
            for(int j=first_segment;j<last_segment;j++)
            {
              auto it=deleted_segments_map.find(level_segments[j]->GetSegmentNum());
              if(it==deleted_segments_map.end())
              {
                vstorage->AddSegments(i,level_segments[j],cmp_);
                level_segments[j]->refs.fetch_add(1);
              }
              else
              {
                if(it->second==true)
                {

                }
                else
                {
                  std::vector<std::vector<FileMetaData*>> new_segment_filelist((level_segments[j]->get_files()));
                  std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
                  //new_segment_filelist.resize(level_per_segment_level);
                  final_new_segment_filelist->resize(level_per_segment_level);
                  int judge_level=level_per_segment_level-1;
                  while(judge_level!=-1)
                  {
                    if(!new_segment_filelist[judge_level].empty())
                    {
                      break;
                    }
                  judge_level--;
                  }
                  auto final_filelist=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
                  for(auto fl:final_filelist)
                  {
                    Segment* new_segment=new Segment(final_new_segment_filelist,cmp_);
                    new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                    new_segment->refs=1;
                    vstorage->AddSegments(i,new_segment,cmp_);
                  }
                }
              }
            }
            first_segment=last_segment+total;
            std::vector<std::vector<FileMetaData*>> new_segment_filelist(overlapping_segments[0]->get_files());
            int segment_not_empty_level=0;
            int judge_level=level_per_segment_level-1;
            while(judge_level!=-1)
            {
              if(!new_segment_filelist[judge_level].empty())
              {
                segment_not_empty_level=judge_level;
                break;
              }
              judge_level--;
            }
            //std::vector<int> file_input_level;
            //file_input_level.resize(static_cast<int>(new_file_list.size()),0);
            new_segment_filelist.resize(level_per_segment_level);
            std::unordered_map<int,bool>::iterator itm;
            for(int j=1;j<static_cast<int>(overlapping_segments.size());j++)
            {
              //itm=deleted_segments_map.find(overlapping_segments[j]->GetSegmentNum());
              //if(itm!=deleted_segments_map.end())
              //{
                //if(itm->second!=true)
                //{
                  //AppendFileListAtLast(new_segment_filelist,overlapping_segments[j]->get_files());
                //}
                //else
                //{
                  //for(auto fll:overlapping_segments[j]->get_files())
                  //{
                    //for(auto& f:fll)
                    //{
                      //vstorage->RemoveCurrentStats(f);
                    //}
                  //}
                //}
                //continue;
              //}
              AppendFileListAtLast(new_segment_filelist,overlapping_segments[j]->get_files());
            }
            int max_level=GetL0InputLevel(new_segment_filelist,new_file_list,cmp_,segment_not_empty_level);
            max_level=std::max(max_level,judge_level);
            std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
            final_new_segment_filelist->resize(level_per_segment_level);
            auto fs=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
            for(auto fl:fs)
            {
              Segment* new_segment=new Segment(fl,cmp_);
              new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
              new_segment->refs=1;
              vstorage->AddSegments(i,new_segment,cmp_);
            }
          }
        }
        for(int j=i*level_per_segment_level;j<(i+1)*level_per_segment_level;j++)
        {
          levels_[j].final_added_files.clear();
        }
        while(segment_it!=segment_it_end)
        {
          auto it=deleted_segments_map.find((*segment_it)->GetSegmentNum());
          if(it==deleted_segments_map.end())
          {
            vstorage->AddSegments(i,*segment_it,cmp_);
            (*segment_it)->refs.fetch_add(1);
          }
          else
          {
            if(it->second==true)
            {

            }
            else
            {
              std::vector<std::vector<FileMetaData*>> new_segment_filelist(((*segment_it)->get_files()));
              std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
              //new_segment_filelist.resize(level_per_segment_level);
              final_new_segment_filelist->resize(level_per_segment_level);
              int judge_level=level_per_segment_level-1;
              while(judge_level!=-1)
              {
                if(!new_segment_filelist[judge_level].empty())
                {
                  break;
                }
                judge_level--;
              }
              auto fs=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
              for(auto fl:fs)
              {
                Segment* new_segment=new Segment(fl,cmp_);
                new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                new_segment->refs=1;
                vstorage->AddSegments(i,new_segment,cmp_);
              }
            }
          }
        }
      }
      else
      {
        std::sort(levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin(),levels_[(i+1)*level_per_segment_level-1].middle_added_files.end(),pair_comp_for_files);
        std::vector<FileMetaData*>& filelist=levels_[(i+1)*level_per_segment_level-1].middle_added_files;
        std::vector<Segment*>& level_segments=*base_vstorage_->GetLevelSegments(i);
        //std::sort(levels_[i].middle_added_files.begin(),levels_[i].middle_added_files.end(),pair_comp_for_files);
        auto input_it=levels_[(i+1)*level_per_segment_level-1].middle_added_files.begin();
        auto input_it_end=levels_[(i+1)*level_per_segment_level-1].middle_added_files.end();
        int middle=-1;
        int position=-1;
        int total=0;
        auto segment_it=level_segments.begin();
        auto segment_it_end=level_segments.end();
        std::vector<Segment*> overlapping_segments;
        std::vector<FileMetaData*> new_file_list;
        InternalKey input_smallest;
        InternalKey input_largest;
        InternalKey segment_smallest;
        InternalKey segment_largest;
        bool has_updated=false;
        bool should_retry=true;
        int num_of_merge_level=0;
        int last_segment=0;
        int first_segment=0;
        new_file_list.clear();
        while(input_it!=input_it_end)
        {
          for(int j=i*level_per_segment_level;j<(i+1)*level_per_segment_level;j++)
          {
            levels_[j].final_added_files.clear();
          }
          position=-1;
          total=0;
          has_updated=false;
          overlapping_segments.clear();
          new_file_list.clear();
          new_file_list.emplace_back(*input_it);
          input_smallest=(*input_it)->smallest;
          input_largest=(*input_it)->largest;
          input_it++;
          while(should_retry)
          {
            should_retry=false;
            while (segment_it!=segment_it_end)
            {
              Segment* exiting_segments=*segment_it;
              auto it=deleted_segments_map.find(exiting_segments->GetSegmentNum());
              if(it!=deleted_segments_map.end())
              {
                if(it->second==true)
                {
                  for(auto fll:exiting_segments->get_files())
                  {
                    for(auto& f:fll)
                    {
                      vstorage->RemoveCurrentStats(f);
                    }
                  }
                }
                middle++;
                segment_it++;
                continue;
              }
              int first_overlapping_result= ucmp->CompareWithoutTimestamp(ExtractUserKey(input_largest.Encode()), ExtractUserKey(exiting_segments->smallest.Encode()));
              int second_overlapping_result=ucmp->CompareWithoutTimestamp(ExtractUserKey(exiting_segments->largest.Encode()),ExtractUserKey(input_smallest.Encode()));
              bool not_overlaps =first_overlapping_result < 0 ||second_overlapping_result < 0;
              if (!not_overlaps)
              {
                if(position==-1)
                {
                  position=middle;
                  last_segment=middle;
                }
                total++;
                overlapping_segments.emplace_back(exiting_segments);
                segment_it++;
                if(has_updated==false)
                {
                  segment_smallest=exiting_segments->smallest;
                  has_updated=true;
                }
                segment_largest=exiting_segments->largest;
                middle++;
                continue;
              }
              if(first_overlapping_result<0)
              {
                break;
              }
              segment_it++;
              middle++;
            }
            if(position==-1)
            {

            }
            else
            {
              while(!(ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->smallest.Encode()),ExtractUserKey(segment_largest.Encode()))>0||ucmp->CompareWithoutTimestamp(ExtractUserKey((*input_it)->largest.Encode()),ExtractUserKey(segment_smallest.Encode()))<0)&&input_it!=input_it_end)
              {
                should_retry=true;
                new_file_list.emplace_back(*input_it);
                input_largest=(*input_it)->largest;
                //input_largest=ucmp->CompareWithoutTimestamp(ExtractUserKey(input_largest.Encode()),ExtractUserKey((*input_it)->largest.Encode()))>=0?input_largest:(*input_it)->largest;
                input_it++;
              }
            }
          }
          if(total==0)
          {
            if(first_segment<middle)
            {
              for(int j=first_segment;j<middle;j++)
              {
                auto it=deleted_segments_map.find(level_segments[j]->GetSegmentNum());
                if(it==deleted_segments_map.end())
                {
                  vstorage->AddSegments(i,level_segments[j],cmp_);
                  level_segments[j]->refs.fetch_add(1);
                }
                else
                {
                  if(it->second==true)
                  {

                  }
                  else
                  {
                    std::vector<std::vector<FileMetaData*>> new_segment_filelist((level_segments[j]->get_files()));
                    std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
                    //new_segment_filelist.resize(level_per_segment_level);
                    final_new_segment_filelist->resize(level_per_segment_level);
                    int judge_level=level_per_segment_level-1;
                    while(judge_level!=-1)
                    {
                      if(!new_segment_filelist[judge_level].empty())
                      {
                        break;
                      }
                      judge_level--;
                    }
                    auto final_filelist=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
                    for(auto fl:final_filelist)
                    {
                      Segment* new_segment=new Segment(final_new_segment_filelist,cmp_);
                      new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                      new_segment->refs=1;
                      vstorage->AddSegments(i,new_segment,cmp_);
                    }
                  }
                }
              }
            }
            first_segment=middle;
            for(auto& file:new_file_list)
            {
              std::vector<std::vector<FileMetaData*>>* new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
              new_segment_filelist->resize(level_per_segment_level);
              (*new_segment_filelist)[0].emplace_back(file);
              file->refs++;
              Segment* new_segment=new Segment(new_segment_filelist,cmp_);
              new_segment->refs=1;
              new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
              vstorage->AddSegments(i,new_segment,cmp_);
            }
          }
          else
          {
            for(int j=first_segment;j<last_segment;j++)
            {
              auto it=deleted_segments_map.find(level_segments[j]->GetSegmentNum());
              if(it==deleted_segments_map.end())
              {
                vstorage->AddSegments(i,level_segments[j],cmp_);
                level_segments[j]->refs.fetch_add(1);
              }
              else
              {
                if(it->second==true)
                {

                }
                else
                {
                  std::vector<std::vector<FileMetaData*>> new_segment_filelist((level_segments[j]->get_files()));
                  std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
                  //new_segment_filelist.resize(level_per_segment_level);
                  final_new_segment_filelist->resize(level_per_segment_level);
                  int judge_level=level_per_segment_level-1;
                  while(judge_level!=-1)
                  {
                    if(!new_segment_filelist[judge_level].empty())
                    {
                      break;
                    }
                    judge_level--;
                  }
                  auto final_filelist=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
                  for(auto fl:final_filelist)
                  {
                    Segment* new_segment=new Segment(final_new_segment_filelist,cmp_);
                    new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                    new_segment->refs=1;
                    vstorage->AddSegments(i,new_segment,cmp_);
                  }
                }
              }
            }
            first_segment=last_segment+total;
            std::vector<std::vector<FileMetaData*>> new_segment_filelist(overlapping_segments[0]->get_files());
            int segment_not_empty_level=0;
            int judge_level=level_per_segment_level-1;
            while(judge_level!=-1)
            {
              if(!new_segment_filelist[judge_level].empty())
              {
                segment_not_empty_level=judge_level;
                break;
              }
              judge_level--;
            }
            if(judge_level==-1)
            {
              segment_not_empty_level=0;
            }
            //std::vector<int> file_input_level;
            //file_input_level.resize(static_cast<int>(new_file_list.size()),0);
            //new_segment_filelist.resize(level_per_segment_level);
            std::unordered_map<int,bool>::iterator itm;
            for(int j=1;j<static_cast<int>(overlapping_segments.size());j++)
            {/*
              itm=deleted_segments_map.find(overlapping_segments[j]->GetSegmentNum());
              if(itm!=deleted_segments_map.end())
              {
                if(itm->second!=true)
                {
                  AppendFileListAtLast(new_segment_filelist,overlapping_segments[j]->get_files());
                }
                else
                {
                  for(auto fll:overlapping_segments[j]->get_files())
                  {
                    for(auto& f:fll)
                    {
                      vstorage->RemoveCurrentStats(f);
                    }
                  }
                }
                continue;
              }
                */
              AppendFileListAtLast(new_segment_filelist,overlapping_segments[j]->get_files());
            }
            int max_level=GetInputLevel(new_segment_filelist,new_file_list,cmp_,segment_not_empty_level,i);
            max_level=std::max(max_level,judge_level);
            std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
            final_new_segment_filelist->resize(level_per_segment_level);
            auto fs=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
            for(auto fl:fs)
            {
              Segment* new_segment=new Segment(fl,cmp_);
              new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
              new_segment->refs=1;
              vstorage->AddSegments(i,new_segment,cmp_);
            }
          }
        }
        for(int j=i*level_per_segment_level;j<(i+1)*level_per_segment_level;j++)
        {
          levels_[j].final_added_files.clear();
        }
        while(segment_it!=segment_it_end)
        {
          auto it=deleted_segments_map.find((*segment_it)->GetSegmentNum());
          if(it==deleted_segments_map.end())
          {
            vstorage->AddSegments(i,*segment_it,cmp_);
            (*segment_it)->refs.fetch_add(1);
          }
          else
          {
            if(it->second==true)
            {

            }
            else
            {
              std::vector<std::vector<FileMetaData*>> new_segment_filelist(((*segment_it)->get_files()));
              std::vector<std::vector<FileMetaData*>>* final_new_segment_filelist=new std::vector<std::vector<FileMetaData*>>;
              new_segment_filelist.resize(level_per_segment_level);
              final_new_segment_filelist->resize(level_per_segment_level);
              int judge_level=level_per_segment_level-1;
              while(judge_level!=-1)
              {
                if(!new_segment_filelist[judge_level].empty())
                {
                  break;
                }
                judge_level--;
              }
              auto fs=AddFileForSegment(vstorage,i,final_new_segment_filelist,new_segment_filelist,judge_level,cmp_);
              for(auto fl:fs)
              {
                Segment* new_segment=new Segment(fl,cmp_);
                new_segment->UpdateSegmentNum(version_set_->NewSegmentNumber());
                new_segment->refs=1;
                vstorage->AddSegments(i,new_segment,cmp_);
              }
            }
          }
        }
      }
    }
    vstorage->AddFilesAfterSegment();



/*
    std::vector<std::vector<int>> deleted_segment_list;
    deleted_segment_list.reserve(base_vstorage_->NumSegmentLevel());
    std::vector<int> l0_file_input_level;
    for(int i=0;i<base_vstorage_->num_levels();i*=level_per_segment_level)
    {
      if(i==0)
      {
        std::sort(levels_[i].middle_added_files.begin(),levels_[i].middle_added_files.end(),pair_comp_for_l0_files);
        l0_file_input_level.reserve(levels_[i].middle_added_files.size());
        AddFileForL0Segments(i/level_per_segment_level,levels_[i].middle_added_files,cmp_,deleted_segment_list[i],l0_file_input_level);

      }
      else
      {
        std::sort(levels_[i].middle_added_files.begin(),levels_[i].middle_added_files.end(),pair_comp_for_files);
        AddFileForSegments(i/level_per_segment_level,levels_[i].middle_added_files,cmp_,deleted_segment_list[i]);
      }
    }
    for(int level=0;level<num_levels_;level++)进行基础插入
    {
      auto& base_segments = base_segment_[level];
      std::vector<Segment*> base_segments_copy(base_segments);
      //auto& added_files = levels_[level].added_files_for_judge;
      //auto& added_segments=levels_[level].added_segments_by_changed;
      auto& deleted_segments_map=levels_[level].deleted_segments;
      auto& deleted_segments_caused_trush=levels_[level].deleted_segments_caused_trush;
      std::vector<Segment*> deleted_segments;
      for(auto& s:deleted_segments_map)
      {
        deleted_segments.emplace_back(s.second);
      }
      for(auto& s:deleted_segments_caused_trush)
      {
        deleted_segments.emplace_back(s.second);
      }
      //std::sort(added_files.begin(),added_files.end(), pair_comp_for_files);
      std::sort(base_segments_copy.begin(),base_segments_copy.end(),pair_comp_for_basesegments);
      //std::sort(added_segments.begin(), added_segments.end(), pair_comp_for_segments);
      std::sort(deleted_segments.begin(), deleted_segments.end(), pair_comp_for_basesegments);
      //int added_it=0;
      int deleted_it=0;
      int base_it=0;
      //int added_final=added_segments.size();
      int base_final=base_segments_copy.size();
      //vstorage->ReserveForSegments(level, base_segments.size() + added_segments.size());
      while(base_it<base_final)
      {
        if(*base_segments_copy[base_it]!=*deleted_segments[deleted_it])
        {
          //base_segments_copy[base_it]->MakeActualDelete(cmp_);
          if(base_segments_copy[base_it]->IsEmpty())
          {
            continue;
          }
          std::pair<int,int> level_for_segment=GetLevelForSegment(level);
          for(int i=level_for_segment.first;i<=level_for_segment.second;i++)
          {
            base_segments_copy[base_it]->RecordFileDeletion(levels_[i].deleted_files_for_judge);
            if(base_segments_copy[base_it]->NeedClearEmptyLevel(level_per_segment_level,0))
            {
              std::vector<std::tuple<FileMetaData*,int,int>> filelist=base_segments_copy[base_it]->MakeActualDeleteAndReturn(cmp_);
              for(auto& t:filelist)
              {
                ApplySegmentFileDeletion(std::get<0>(t),i,std::get<1>(t));
                ApplyFileAddition((((i+1)*level_per_segment_level)-1)-std::get<2>(t),std::get<0>(t));
                files_may_caused_trush.emplace(std::get<0>(t)->fd.GetNumber(),(((i+1)*level_per_segment_level)-1)-std::get<2>(t));
                added_files_by_segment_.emplace(std::get<0>(t)->fd.GetNumber(),(((i+1)*level_per_segment_level)-1)-std::get<2>(t));
                //levels_[(((i+1)*level_per_segment_level)-1)-std::get<1>(t)].deleted_files.emplace(std::get<0>(t)->fd.GetNumber());
                //levels_[(((i+1)*level_per_segment_level)-1)-std::get<2>(t)].added_files.emplace(std::get<0>(t)->fd.GetNumber());
              }
            }
            else
            {
              base_segments_copy[base_it]->MakeActualDelete(cmp_);
            }
            //base_segments_copy[base_it]->UpdateSegmentIsChanged();
          }
          AddSegments(level, new Segment(*base_segments_copy[base_it]),cmp_);
          base_it++;
        }
        else
        {
          deleted_it++;
          base_it++;
        }
      }
      for(auto& deleted_segment:deleted_segments)
      {
        std::pair<int,int> level_for_segment=GetLevelForSegment(level);
        for(int i=level_for_segment.first;i<=level_for_segment.second;i++)
        {
          deleted_segment->RecordFileDeletion(levels_[level].deleted_files_for_judge);
          std::vector<std::pair<FileMetaData*,int>> filelist=deleted_segment->MakeActualDeleteWithMiddleReturn(cmp_);
          for(auto& p:filelist)
          {
            ApplySegmentFileDeletion(p.first,level,p.second);
          }
        }
      }
  }
  //for(int level=0;level<num_levels_;level++)
  {
    auto& deleted_segments_map=levels_[level].deleted_segments;
    auto& deleted_segments_caused_trush=levels_[level].deleted_segments_caused_trush;
    std::vector<Segment*> deleted_segments;
    for(auto& s:deleted_segments_map)
    {
      deleted_segments.emplace_back(s.second);
    }
    for(auto&s:deleted_segments_caused_trush)
    {
      deleted_segments.emplace_back(s.second);
    }
    for(auto& deleted_segment:deleted_segments)
    {
      std::pair<int,int> level_for_segment=GetLevelForSegment(level);
      for(int i=level_for_segment.first;i<=level_for_segment.second;i++)
      {
        deleted_segment->RecordFileDeletion(levels_[level].deleted_files_for_judge);
        std::vector<std::pair<FileMetaData*,int>> filelist=deleted_segment->MakeActualDeleteWithMiddleReturn(cmp_);
        for(auto& p:filelist)
        {
          ApplySegmentFileDeletion(p.first,level,p.second);
        }
      }
    }
  }
  for(int level=0;level<num_levels_;level++)
  //{
      //auto& base_segments = base_vstorage_->LevelSegments(level);
      //auto& added_files = levels_[level].added_files_for_judge;
      //auto& added_segment_map=levels_[level].added_segments;
      //std::vector<Segment*> added_segments;
      //for(auto& f:added_segment_map)
      //{
        //added_segments.emplace_back(f.second);
      //}
      //auto& deleted_segments=levels_[level].deleted_segments;
      //std::sort(added_files.begin(),added_files.end(), pair_comp_for_files);
      //std::sort(base_segments.begin(),base_segments.end(),pair_comp_for_segments);
      //std::sort(added_segments.begin(), added_segments.end(), pair_comp_for_basesegments);
      //std::sort(deleted_segments.begin(), deleted_segments.end(), pair_comp_for_segments);
      //for(auto& added_segment:added_segments)
      //{
        //int level_=added_segments.find(deleted_segment.second)->second;
        //std::pair<int,int> level_for_segment=GetLevelForSegment(level);
        for(int i=level_for_segment.first;i<=level_for_segment.second;i++)
        {
          added_segment->RecordFileDeletion(levels_[i].deleted_files_for_judge);
          added_segment->MakeActualDelete(cmp_);
          //added_segment->UpdateSegmentIsChanged();
        }
        auto after_deleted_files=added_segment->get_files();
        for(int i=0;i<int(after_deleted_files.size());i++)
        {
          for(auto file:after_deleted_files[i])
          {

          }
        }
        std::vector<std::pair<FileMetaData*,int>>filelist=AddSegmentsAndMerge(level,added_segment,cmp_);
        for(auto& sample_pair:filelist)
        {
          ApplyFileAddition((level+1)*level_per_segment_level-sample_pair.second,sample_pair.first);
          files_may_caused_trush.emplace(sample_pair.first->fd.GetNumber(),(level+1)*level_per_segment_level-sample_pair.second);
          added_files_by_segment_.emplace(sample_pair.first->fd.GetNumber(),(level+1)*level_per_segment_level-sample_pair.second);
          //levels_[(level+1)*level_per_segment_level-sample_pair.second].added_files.emplace(sample_pair.first);
        }
      }
  }
  for(int level=0;level<num_levels_;level++)
  {
     //int actual_level=vstorage->GetInsertLevelForSegment(level);
     int actual_level=GetInsertLevelForSegment(level);
     auto& added_files_map = levels_[actual_level].added_files_for_judge;
     std::vector<FileMetaData*> added_files;
     for(auto&f:added_files_map)
     {
      added_files.emplace_back(f.second);
     }
     std::sort(added_files.begin(),added_files.end(),pair_comp_for_files);
     //std::vector<std::pair<FileMetaData*,int>>result=AddFileForSegments(actual_level,added_files,cmp_);
     for(auto& sample_pair:result)
     {
        ApplyFileAddition((level+1)*level_per_segment_level-sample_pair.second,sample_pair.first);
        files_may_caused_trush.emplace(sample_pair.first->fd.GetNumber(),(level+1)*level_per_segment_level-sample_pair.second);
        added_files_single.emplace(sample_pair.first->fd.GetNumber(),(level+1)*level_per_segment_level-sample_pair.second);
        //levels_[(level+1)*level_per_segment_level-sample_pair.second].added_files.emplace(sample_pair.first);
     }
  }
  MakeDeleteSegmentClear();
  if(has_base_segemnt_trush)
  {
    for(auto& p:base_segment_trush)
    {
      for(auto&q:p)
      {
        delete q;
      }
    }
  }
    for (int level = 0; level < num_levels_; ++level)
    {
      for (size_t pos = 0; pos < base_segment_[level].size(); ++pos)
      {
        base_segment_[level][pos]->RebuildFileLocation();
      }
    }
  //vstorage->RebuildSegmentsMap();
  //vstorage->UpdateSegmentsLevel();
  has_new_versionedit=false;
  has_base_segemnt_trush=true;*/
}
/*void ActualSaveSegmentsTo(VersionStorageInfo* vstorage)
{
  if(has_new_versionedit)
  {
    SaveSegmentsTo();
  }
  vstorage->CopySegment(base_segment_);
  has_new_versionedit=false;
  has_base_segemnt_trush=false;
  files_may_caused_trush.clear();
}*/
//无用
void AddSegments(int level,Segment* segments,const InternalKeyComparator* cmp)
  {
    
    for(int i=int(base_segment_.size());i<(level+1);i++)
    {
      base_segment_.emplace_back(std::vector<Segment*>());
      //num_segments_level_++;
    }
    //segments->MakeActualDelete(cmp);
    if(!segments->IsEmpty())
    {
        base_segment_[level].emplace_back(segments);
    }
  }
  //无用
 void AddFileForSegments(int level,std::vector<FileMetaData*>& added_files,const InternalKeyComparator* cmp,std::vector<int>& deleted_segment_list)
  {
    std::vector<std::pair<FileMetaData*,int>> return_vector;
    if(level>=int(base_segment_.size()))
    {
      base_segment_.resize(level+1);
    }
    for(auto& new_file:added_files)
    {
      auto& level_segments = base_segment_[level];
      std::vector<Segment*> overlapping_segments;
      int deleted_it=0;
      int position=-1;
      int total=0;
      int middle=-1;
      if(level_segments.empty())
      {
          //const Comparator* ucmp = base_vstorage_->user_comparator();
          //base_segment_[level].emplace_back(new Segment(new_file,ucmp));
          //continue;
          ApplyFileAddition(level*level_per_segment_level+level_per_segment_level-1,new_file);
          continue;
      }
      for (auto& exiting_segments:level_segments)
      {
          middle++;
          bool overlaps = cmp->Compare(new_file->smallest, exiting_segments->largest) <= 0 &&
            cmp->Compare(new_file->largest, exiting_segments->smallest) >= 0;
          if (overlaps)
          {
            if(position==-1)
            {
              position=middle;
            }
            total++;
            overlapping_segments.emplace_back(exiting_segments);
            continue;
          }
          if(position!=-1)
          {
            break;
          }
      }
      if (overlapping_segments.empty())
      {
        ApplyFileAddition(level*level_per_segment_level+level_per_segment_level-1,new_file);
        continue;
      }
      Segment* sp=new Segment(*overlapping_segments[0]);
      
      overlapping_segments[0]=sp;
      overlapping_segments[0]->UpdateSegmentNum(version_set_->NewSegmentNumber());
      
      for(int i=1;i<int(overlapping_segments.size());i++)
      {
        const std::vector<std::vector<FileMetaData*>> should_added_files= overlapping_segments[i]->get_files();
        overlapping_segments[0]->AppendFileListAtLast(should_added_files,overlapping_segments[i]->largest);
      }
      /*FileMetaData* actual_new_file=new FileMetaData(*new_file);
      actual_new_file->refs=1;*/
      int new_level=overlapping_segments[0]->AddFile(new_file,cmp);
      /*
      for(int i=1;i<int(overlapping_segments.size());i++)
      {
        delete overlapping_segments[i];
      }*/
      base_segment_[level][position]=overlapping_segments[0];
      
      for(int i=1;i<total;i++)
      {
        deleted_segment_list.emplace_back(overlapping_segments[i]->GetSegmentNum());
      }
      //return_vector.emplace_back(new_file,new_level);
    }
    //return return_vector;
    
  }
  std::vector<std::pair<FileMetaData*,int>> AddSegmentsAndMerge(int level,Segment* new_segment,const InternalKeyComparator* cmp)const
  {
    if (level >= int(base_segment_.size()))
    {
      base_segment_.resize(level + 1);
      //num_segments_level_=(level+1);
    }
    //new_segment->MakeActualDelete(cmp);
    if(new_segment->IsEmpty())
    {
        std::vector<std::pair<FileMetaData*,int>> return_vector;
        return return_vector;
    }
    auto& level_segments = base_segment_[level];
    std::vector<Segment*> overlapping_segments;
    int position=-1;
    int total=0;
    int middle=-1;
    for (auto& exiting_segments:level_segments)
    {
        middle++;
        bool overlaps = cmp->Compare(new_segment->smallest, exiting_segments->largest) <= 0 &&
            cmp->Compare(new_segment->largest, exiting_segments->smallest) >= 0;
        if (overlaps)
        {
          if(position==-1)
          {
            position=middle;
          }
          total++;
          overlapping_segments.emplace_back(exiting_segments);
          continue;
        }
        if(position!=-1)
        {
          break;
        }
    }
    if (overlapping_segments.empty())
    {
        if(cmp->Compare(new_segment->smallest,level_segments.back()->largest)>0)
        {
          level_segments.push_back(new_segment);
          std::vector<std::vector<FileMetaData*>> filelist=new_segment->get_files();
          std::vector<std::pair<FileMetaData*,int>> return_vector;
          for(int i=0;i<int(filelist.size());i++)
          {
            for(auto&file:filelist[i])
            {
              return_vector.emplace_back(file,i);
            }
          }
          return return_vector;
        }
        else{
          level_segments.insert(level_segments.begin(),new_segment);
          std::vector<std::vector<FileMetaData*>> filelist=new_segment->get_files();
          std::vector<std::pair<FileMetaData*,int>> return_vector;
          for(int i=0;i<int(filelist.size());i++)
          {
            for(auto&file:filelist[i])
            {
              return_vector.emplace_back(file,i);
            }
          }
          return return_vector;
        }
    }
    Segment* s=new Segment(*overlapping_segments[0]);
    overlapping_segments[0]=s;
    overlapping_segments[0]->UpdateSegmentNum(version_set_->NewSegmentNumber());
    for(int i=1;i<int(overlapping_segments.size());i++)
    {
      const std::vector<std::vector<FileMetaData*>> added_files= overlapping_segments[i]->get_files();
      overlapping_segments[0]->AppendFileListAtLast(added_files,overlapping_segments[i]->largest);
    }
    std::vector<std::pair<FileMetaData*,int>>return_vector=overlapping_segments[0]->AddFiles(new_segment->get_files(),cmp);
    for(int i=1;i<int(overlapping_segments.size());i++)
    {
      delete overlapping_segments[i];
    }
    base_segment_[level][position]=overlapping_segments[0];
    for(int i=1;i<total;i++)
    {
      base_segment_[level].erase(base_segment_[level].begin()+position+i);
    }
    return return_vector;
  }
  void SaveCompactCursorsTo(VersionStorageInfo* vstorage) const {
    for (auto iter = updated_compact_cursors_.begin();
         iter != updated_compact_cursors_.end(); iter++) {
      vstorage->AddCursorForOneLevel(iter->first, iter->second);
    }
  }

  bool ValidVersionAvailable() {
    assert(track_found_and_missing_files_);
    if (version_updated_since_last_check_) {
      valid_version_available_ = ContainsCompleteVersion();
      if (!valid_version_available_ && !edited_in_atomic_group_ &&
          allow_incomplete_valid_version_) {
        valid_version_available_ = OnlyMissingL0Suffix();
      }
      version_updated_since_last_check_ = false;
    }
    return valid_version_available_;
  }

  bool OnlyMissingL0Suffix() const {
    if (!non_l0_missing_files_.empty()) {
      return false;
    }
    assert(!(l0_missing_files_.empty() && missing_blob_files_.empty()));

    if (!l0_missing_files_.empty() && !MissingL0FilesAreL0Suffix()) {
      return false;
    }
    if (!missing_blob_files_.empty() &&
        !RemainingSstFilesNotMissingBlobFiles()) {
      return false;
    }
    return true;
  }

  // Check missing L0 files are a suffix of expected sorted L0 files.
  bool MissingL0FilesAreL0Suffix() const {
    assert(non_l0_missing_files_.empty());
    assert(!l0_missing_files_.empty());
    std::vector<FileMetaData*> expected_sorted_l0_files;
    const auto& base_files = base_vstorage_->LevelFiles(0);
    const auto& unordered_added_files = levels_[0].added_files;
    expected_sorted_l0_files.reserve(base_files.size() +
                                     unordered_added_files.size());
    EpochNumberRequirement epoch_number_requirement =
        base_vstorage_->GetEpochNumberRequirement();

    if (epoch_number_requirement == EpochNumberRequirement::kMightMissing) {
      MergeUnorderdAddedFilesWithBase(
          base_files, unordered_added_files, *level_zero_cmp_by_seqno_,
          [&](FileMetaData* file) {
            expected_sorted_l0_files.push_back(file);
          });
    } else {
      MergeUnorderdAddedFilesWithBase(
          base_files, unordered_added_files, *level_zero_cmp_by_epochno_,
          [&](FileMetaData* file) {
            expected_sorted_l0_files.push_back(file);
          });
    }
    assert(expected_sorted_l0_files.size() >= l0_missing_files_.size());
    std::unordered_set<uint64_t> unaddressed_missing_files = l0_missing_files_;
    for (auto iter = expected_sorted_l0_files.begin();
         iter != expected_sorted_l0_files.end(); iter++) {
      uint64_t file_number = (*iter)->fd.GetNumber();
      if (l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
        assert(unaddressed_missing_files.find(file_number) !=
               unaddressed_missing_files.end());
        unaddressed_missing_files.erase(file_number);
      } else if (!unaddressed_missing_files.empty()) {
        return false;
      } else {
        break;
      }
    }
    return true;
  }

  // Check for each of the missing blob file missing, it either is older than
  // the minimum oldest blob file required by this Version or only linked to
  // the missing L0 files.
  bool RemainingSstFilesNotMissingBlobFiles() const {
    assert(non_l0_missing_files_.empty());
    assert(!missing_blob_files_.empty());
    bool no_l0_files_missing = l0_missing_files_.empty();
    uint64_t min_oldest_blob_file_num = GetMinOldestBlobFileNumber();
    for (const auto& missing_blob_file : missing_blob_files_) {
      if (missing_blob_file < min_oldest_blob_file_num) {
        continue;
      }
      auto iter = mutable_blob_file_metas_.find(missing_blob_file);
      assert(iter != mutable_blob_file_metas_.end());
      const std::unordered_set<uint64_t>& linked_ssts =
          iter->second.GetLinkedSsts();
      // TODO(yuzhangyu): In theory, if no L0 SST files ara missing, and only
      // blob files exclusively linked to a L0 suffix are missing, we can
      // recover to a valid point in time too. We don't recover that type of
      // incomplete Version yet.
      if (!linked_ssts.empty() && no_l0_files_missing) {
        return false;
      }
      if (!OnlyLinkedToMissingL0Files(linked_ssts)) {
        return false;
      }
    }
    return true;
  }

  // Save the current state in *vstorage.
  Status SaveTo(VersionStorageInfo* vstorage)  {
    assert(!track_found_and_missing_files_ || valid_version_available_);
    Status s;

#ifndef NDEBUG
    // The same check is done within Apply() so we skip it in release mode.
    s = CheckConsistency(base_vstorage_);
    if (!s.ok()) {
      return s;
    }
#endif  // NDEBUG

    s = CheckConsistency(vstorage);
    if (!s.ok()) {
      return s;
    }

    SaveSegmentsTo(vstorage);
    //vstorage->AddFilesAfterSegment();
    //SaveSSTFilesTo(vstorage);

    SaveBlobFilesTo(vstorage);

    SaveCompactCursorsTo(vstorage);

    

    s = CheckConsistency(vstorage);
    //加入等待IO完成的函数
    return s;
  }

  Status LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                           bool prefetch_index_and_filter_in_cache,
                           bool is_initial_load,
                           const MutableCFOptions& mutable_cf_options,
                           size_t max_file_size_for_l0_meta_pin,
                           const ReadOptions& read_options) {
    assert(table_cache_ != nullptr);
    assert(!track_found_and_missing_files_ || valid_version_available_);

    size_t table_cache_capacity =
        table_cache_->get_cache().get()->GetCapacity();
    bool always_load = (table_cache_capacity == TableCache::kInfiniteCapacity);
    size_t max_load = std::numeric_limits<size_t>::max();
    if (!always_load) {
      // If it is initial loading and not set to always loading all the
      // files, we only load up to kInitialLoadLimit files, to limit the
      // time reopening the DB.
      const size_t kInitialLoadLimit = 16;
      size_t load_limit;
      // If the table cache is not 1/4 full, we pin the table handle to
      // file metadata to avoid the cache read costs when reading the file.
      // The downside of pinning those files is that LRU won't be followed
      // for those files. This doesn't matter much because if number of files
      // of the DB excceeds table cache capacity, eventually no table reader
      // will be pinned and LRU will be followed.
      if (is_initial_load) {
        load_limit = std::min(kInitialLoadLimit, table_cache_capacity / 4);
      } else {
        load_limit = table_cache_capacity / 4;
      }

      size_t table_cache_usage = table_cache_->get_cache().get()->GetUsage();
      if (table_cache_usage >= load_limit) {
        // TODO (yanqin) find a suitable status code.
        return Status::OK();
      } else {
        max_load = load_limit - table_cache_usage;
      }
    }

    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    std::vector<Status> statuses;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        uint64_t file_number = file_meta->fd.GetNumber();
        if (track_found_and_missing_files_ && level == 0 &&
            l0_missing_files_.find(file_number) != l0_missing_files_.end()) {
          continue;
        }
        // If the file has been opened before, just skip it.
        if (!file_meta->table_reader_handle) {
          files_meta.emplace_back(file_meta, level);
          statuses.emplace_back(Status::OK());
        }
        if (files_meta.size() >= max_load) {
          break;
        }
      }
      if (files_meta.size() >= max_load) {
        break;
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        TableCache::TypedHandle* handle = nullptr;
        statuses[file_idx] = table_cache_->FindTable(
            read_options, file_options_,
            *(base_vstorage_->InternalComparator()), *file_meta, &handle,
            mutable_cf_options, false /*no_io */,
            internal_stats->GetFileReadHist(level), false, level,
            prefetch_index_and_filter_in_cache, max_file_size_for_l0_meta_pin,
            file_meta->temperature);
        if (handle != nullptr) {
          file_meta->table_reader_handle = handle;
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->get_cache().Value(handle);
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
    Status ret;
    for (const auto& s : statuses) {
      if (!s.ok()) {
        if (ret.ok()) {
          ret = s;
        }
      }
    }
    return ret;
  }
};

VersionBuilder::VersionBuilder(
    const FileOptions& file_options, const ImmutableCFOptions* ioptions,
    TableCache* table_cache, VersionStorageInfo* base_vstorage,
    VersionSet* version_set,
    std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr,
    ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler,
    bool track_found_and_missing_files, bool allow_incomplete_valid_version)
    : rep_(new Rep(file_options, ioptions, table_cache, base_vstorage,
                   version_set, file_metadata_cache_res_mgr, cfd,
                   version_edit_handler, track_found_and_missing_files,
                   allow_incomplete_valid_version)) {}

VersionBuilder::~VersionBuilder() =default;

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

Status VersionBuilder::Apply(const VersionEdit* edit) {
  return rep_->Apply(edit);
}

Status VersionBuilder::SaveTo(VersionStorageInfo* vstorage) const {
  return rep_->SaveTo(vstorage);
}

Status VersionBuilder::LoadTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache, bool is_initial_load,
    const MutableCFOptions& mutable_cf_options,
    size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options) {
  return rep_->LoadTableHandlers(internal_stats, max_threads,
                                 prefetch_index_and_filter_in_cache,
                                 is_initial_load, mutable_cf_options,
                                 max_file_size_for_l0_meta_pin, read_options);
}

void VersionBuilder::CreateOrReplaceSavePoint() {
  assert(rep_);
  savepoint_ = std::move(rep_);
  rep_ = std::make_unique<Rep>(*savepoint_);
}

bool VersionBuilder::ValidVersionAvailable() {
  return rep_->ValidVersionAvailable();
}

bool VersionBuilder::HasMissingFiles() const { return rep_->HasMissingFiles(); }

std::vector<std::string>& VersionBuilder::GetAndClearIntermediateFiles() {
  return rep_->GetAndClearIntermediateFiles();
}

void VersionBuilder::ClearFoundFiles() { return rep_->ClearFoundFiles(); }

Status VersionBuilder::SaveSavePointTo(VersionStorageInfo* vstorage) const {
  if (!savepoint_ || !savepoint_->ValidVersionAvailable()) {
    return Status::InvalidArgument();
  }
  return savepoint_->SaveTo(vstorage);
}

Status VersionBuilder::LoadSavePointTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache, bool is_initial_load,
    const MutableCFOptions& mutable_cf_options,
    size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options) {
  if (!savepoint_ || !savepoint_->ValidVersionAvailable()) {
    return Status::InvalidArgument();
  }
  return savepoint_->LoadTableHandlers(
      internal_stats, max_threads, prefetch_index_and_filter_in_cache,
      is_initial_load, mutable_cf_options, max_file_size_for_l0_meta_pin,
      read_options);
}

void VersionBuilder::ClearSavePoint() { savepoint_.reset(nullptr); }

BaseReferencedVersionBuilder::BaseReferencedVersionBuilder(
    ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler,
    bool track_found_and_missing_files, bool allow_incomplete_valid_version)
    : version_builder_(new VersionBuilder(
          cfd->current()->version_set()->file_options(), &cfd->ioptions(),
          cfd->table_cache(), cfd->current()->storage_info(),
          cfd->current()->version_set(),
          cfd->GetFileMetadataCacheReservationManager(), cfd,
          version_edit_handler, track_found_and_missing_files,
          allow_incomplete_valid_version)),
      version_(cfd->current()) {
  version_->Ref();
}

BaseReferencedVersionBuilder::BaseReferencedVersionBuilder(
    ColumnFamilyData* cfd, Version* v, VersionEditHandler* version_edit_handler,
    bool track_found_and_missing_files, bool allow_incomplete_valid_version)
    : version_builder_(new VersionBuilder(
          cfd->current()->version_set()->file_options(), &cfd->ioptions(),
          cfd->table_cache(), v->storage_info(), v->version_set(),
          cfd->GetFileMetadataCacheReservationManager(), cfd,
          version_edit_handler, track_found_and_missing_files,
          allow_incomplete_valid_version)),
      version_(v) {
  assert(version_ != cfd->current());
}

BaseReferencedVersionBuilder::~BaseReferencedVersionBuilder() {
  version_->Unref();
}

}  // namespace ROCKSDB_NAMESPACE
