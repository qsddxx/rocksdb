#pragma once

#include "db/compaction/compaction_picker.h"
#include "db/snapshot_checker.h"
namespace ROCKSDB_NAMESPACE {
// Picking compactions for leveled compaction. See wiki page
// https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
// for description of Leveled compaction.
class SegmentCompactionPicker : public CompactionPicker {
 public:
  SegmentCompactionPicker(const ImmutableOptions& ioptions,
                        const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& /* existing_snapshots */,
      const SnapshotChecker* /* snapshot_checker */,
      VersionStorageInfo* vstorage, LogBuffer* log_buffer,
      bool /*require_max_output_level*/ = false) override;

  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

  void RegisterCompaction(Compaction* c) override
{
  if (c == nullptr) {
    return;
  }
  assert(ioptions_.compaction_style != kCompactionStyleLevel ||
         c->output_level() == 0 ||
         !FilesRangeOverlapWithCompaction_Segment(*c->inputs(), c->output_level(),
                                          c->kInvalidLevel));
  // CompactionReason::kExternalSstIngestion's start level is just a placeholder
  // number without actual meaning as file ingestion technically does not have
  // an input level like other compactions
  /*if ((c->start_level() == 0 &&
       c->compaction_reason() != CompactionReason::kExternalSstIngestion) ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.insert(c);
  }*/
  compactions_in_progress_.insert(c);
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::RegisterCompaction:Registered",
                           c);
}
bool FilesRangeOverlapWithCompaction(const std::vector<CompactionInputFiles>& inputs, int level,
    int proximal_level) const override
{
      bool is_empty = true;
  for (auto& in : inputs) {
    if (!in.empty()) {
      is_empty = false;
      break;
    }
  }
   if (is_empty) {
    // No files in inputs
    return false;
  }
  InternalKey smallest, largest;
  GetRange(inputs, &smallest, &largest, Compaction::kInvalidLevel);
  return RangeOverlapWithCompaction_Segment(smallest.user_key(), largest.user_key(),
                                    level);
}
bool RangeOverlapWithCompaction(const Slice& smallest_user_key, const Slice& largest_user_key,int level) const override
{
  const Comparator* ucmp = icmp_->user_comparator();
  for (Compaction* c : compactions_in_progress_) {
    if (c->segment_level == level &&
        ucmp->CompareWithoutTimestamp(smallest_user_key,
                                      c->GetLargestUserKey()) <= 0 &&
        ucmp->CompareWithoutTimestamp(largest_user_key,
                                      c->GetSmallestUserKey()) >= 0) {
      // Overlap
      return true;
    }
    if (c->SupportsPerKeyPlacement()) {
      if (c->OverlapProximalLevelOutputRange(smallest_user_key,
                                             largest_user_key)) {
        return true;
      }
    }
  }
  // Did not overlap with any running compaction in level `level`
  return false;
}
};
}  // namespace ROCKSDB_NAMESPAC