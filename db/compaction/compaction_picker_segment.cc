#include "db/compaction/compaction_picker_segment.h"

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"
namespace ROCKSDB_NAMESPACE {
bool SegmentCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  for (int i = 0; i < vstorage->SegmentCompactionNum(); i++) {
    auto segment_compaction_score = vstorage->SegmentCompactionScore(i);
    if (segment_compaction_score.first >= 1.0 &&
        !vstorage->GetSegmentById(segment_compaction_score.second)
             ->being_compacted) {
      return true;
    }
  }
  if (vstorage->SegmentCompactionNum() == 0) {
    return false;
  }
  return vstorage->SegmentCompactionScore(0).first >= 1.0;
}
namespace {
class SegmentCompactionBuilder {
 public:
  SegmentCompactionBuilder(VersionStorageInfo* vstorage,
                           SegmentCompactionPicker* compaction_picker,
                           const InternalKeyComparator* const icmp,
                           const MutableCFOptions& mutable_cf_options,
                           const ImmutableOptions& ioptions,
                           const MutableDBOptions& mutable_db_options)
      : vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        icmp_(icmp),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  Compaction* PickCompaction();

 private:
  VersionStorageInfo* vstorage_;
  SegmentCompactionPicker* compaction_picker_;
  const InternalKeyComparator* icmp_;
  int output_level_ = -1;
  int segment_score_ = 1;
  uint64_t total_size = 0;
  uint64_t num_files = 0;
  std::vector<CompactionInputFiles> inputs_;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  Compaction* OutputCompaction(bool is_trivial_move = false);
  bool IsInputFilesNonOverlapping(Compaction* c);

  Compaction* PickFileToCompact(int start_segment_);
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};
Compaction* SegmentCompactionBuilder::PickCompaction() {
  std::vector<FileMetaData*> compaction_filelist;
  // find **first**(need random?) file to trivial move
  for (int i = 0; i < vstorage_->num_non_empty_segments_levels(); i++) {
    if (vstorage_->ShouldLevelTrivialMove(mutable_cf_options_, i)) {
      auto& level_segment = vstorage_->GetLevelSegments(i);
      for (auto& segment_ : level_segment) {
        if (segment_->being_compacted || segment_->IsEmpty()) {
          continue;
        }
        segment_->being_compacted = true;
        inputs_.resize(1);
        inputs_[0].files.emplace_back(segment_->files_.back()[0]);
        inputs_[0].level = i;
        output_level_ = i + 1;
        total_size = inputs_[0].files[0]->fd.GetFileSize();
        num_files = 1;
        return OutputCompaction(true);
      }
    }
  }
  // find first level with score >=1
  for (int i = 0; i < vstorage_->SegmentCompactionNum(); i++) {
    auto segment_compaction_score = vstorage_->SegmentCompactionScore(i);
    segment_score_ = segment_compaction_score.first;
    if (segment_score_ >= 1) {
      auto segment_id = segment_compaction_score.second;
      Segment* segment = vstorage_->GetSegmentById(segment_id);
      if (segment->being_compacted) {
        continue;
      }
      output_level_ = vstorage_->GetSegmentLocationById(segment_id).GetLevel();
      segment->being_compacted = true;
      for (auto& inner_level : segment->files_) {
        inputs_.emplace_back();
        inputs_.back().level = output_level_;
        for (auto& f : inner_level) {
          inputs_.back().files.emplace_back(f);
          total_size += f->fd.GetFileSize();
        }
        num_files += inner_level.size();
      }
      return OutputCompaction();
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  return nullptr;
}
Compaction* SegmentCompactionBuilder::OutputCompaction(bool is_trivial_move) {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          kCompactionStyleSegment),
      std::numeric_limits<uint64_t>::max(),
      GetPathId(ioptions_, mutable_cf_options_, total_size),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_, 1,
                         true /* enable_compression */),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_,
                            true /* enable_compression */),
      mutable_cf_options_.default_write_temperature,
      /* max_subcompactions */ 0, /* grandparents */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, CompactionReason::kUniversalSizeRatio,
      /* trim_ts */ "", segment_score_,
      /* l0_files_might_overlap */ true);
  c->set_is_trivial_move(is_trivial_move);
  RecordInHistogram(ioptions_.stats, NUM_FILES_IN_SINGLE_COMPACTION, num_files);
  // vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}
uint32_t SegmentCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint64_t file_size) {
  uint64_t accumulated_size = 0;
  uint64_t future_size =
      file_size *
      (100 - mutable_cf_options.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());
  for (; p < ioptions.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.cf_paths[p].target_size;
    if (target_size > file_size &&
        accumulated_size + (target_size - file_size) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
}
}  // namespace
Compaction* SegmentCompactionPicker::PickCompaction(
    const std::string& /* cf_name */, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots */,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* /* log_buffer */, bool /* require_max_output_level*/) {
  SegmentCompactionBuilder builder(vstorage, this, icmp_, mutable_cf_options,
                                   ioptions_, mutable_db_options);
  return builder.PickCompaction();
}

}  // namespace ROCKSDB_NAMESPACE