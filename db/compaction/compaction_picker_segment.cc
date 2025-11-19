#include "db/compaction/compaction_picker_segment.h"

#include <string>
#include <utility>
#include <vector>
#include <queue>
#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"
namespace ROCKSDB_NAMESPACE
{
    bool SegmentCompactionPicker::NeedsCompaction(const VersionStorageInfo* vstorage) const 
    {
      for (int i = 0; i <= vstorage->MaxInputLevel(); i++)
      {
        if (vstorage->CompactionScore(i) >= 1)
        {
          return true;
        }
      }
      return false;
    }
    namespace
    {
      struct InputFileInfo
      {
        InputFileInfo() : InputFileInfo(nullptr, 0, 0) {}
        InputFileInfo(FileMetaData* file_meta, size_t l, size_t i)
      : f(file_meta), level(l), index(i) {}

        FileMetaData* f;
        size_t level;
        size_t index;
      };

// Used in universal compaction when trivial move is enabled.
// This comparator is used for the construction of min heap
// based on the smallest key of the file.
      struct SmallestKeyHeapComparator
      {
        explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

        bool operator()(InputFileInfo i1, InputFileInfo i2) const
        {
          return (ucmp_->CompareWithoutTimestamp(i1.f->smallest.user_key(),
                                           i2.f->smallest.user_key()) > 0);
        }

        private:
        const Comparator* ucmp_;
      };

using SmallestKeyHeap =
    std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                        SmallestKeyHeapComparator>;

// This function creates the heap that is used to find if the files are
// overlapping during universal compaction when the allow_trivial_move
// is set.
SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          smallest_key_priority_q.emplace(c->input(0, i), 0, i);
        }
      } else {
        smallest_key_priority_q.emplace(c->input(l, 0), l, 0);
      }
    }
  }
  return smallest_key_priority_q;
}


  class SegmentCompactionBuilder
  {
    public:
    SegmentCompactionBuilder(const std::string& cf_name,
                          VersionStorageInfo* vstorage,
                          CompactionPicker* compaction_picker,
                          const InternalKeyComparator* const icmp,
                          LogBuffer* log_buffer,
                          const MutableCFOptions& mutable_cf_options,
                          const ImmutableOptions& ioptions,
                          const MutableDBOptions& mutable_db_options)
    : cf_name_(cf_name),
      vstorage_(vstorage),
      compaction_picker_(compaction_picker),
      icmp_(icmp),
      log_buffer_(log_buffer),
      mutable_cf_options_(mutable_cf_options),
      ioptions_(ioptions),
      mutable_db_options_(mutable_db_options) {}

    // Pick and return a compaction.
    Compaction* PickCompaction();
    bool IsInputFilesNonOverlapping(Compaction* c);

    // Pick the initial files to compact to the next level. (or together
    // in Intra-L0 compactions)
    uint64_t GetMaxOverlappingBytes()const;

    // From `start_level_`, pick files to compact to `output_level_`.
    // Returns false if there is no file to compact.
    // If it returns true, inputs->files.size() will be exactly one for
    // all compaction priorities except round-robin. For round-robin,
    // multiple consecutive files may be put into inputs->files.
    // If level is 0 and there is already a compaction on that level, this
    // function will return false.
    Compaction* PickFileToCompact(int start_segment_);
    const std::string& cf_name_;
    VersionStorageInfo* vstorage_;
    CompactionPicker* compaction_picker_;
    const InternalKeyComparator* icmp_;
    LogBuffer* log_buffer_;
    int start_level_ = -1;
    int output_level_ = -1;
    int parent_index_ = -1;
    int base_index_ = -1;
    int start_segment_=-1;
    int level_per_segment_level=0;
    int start_segment_score_=-1;
    int segment_num=0;
    double start_level_score_ = 0;
    //bool is_l0_trivial_move_ = false;
    CompactionInputFiles start_level_inputs_;
    std::vector<CompactionInputFiles> compaction_inputs_;
    CompactionInputFiles output_level_inputs_;
    std::vector<FileMetaData*> grandparents_;
    CompactionReason compaction_reason_ = CompactionReason::kUnknown;

    const MutableCFOptions& mutable_cf_options_;
    const ImmutableOptions& ioptions_;
    const MutableDBOptions& mutable_db_options_;
    // Pick a path ID to place a newly generated file, with its level
    static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                              const MutableCFOptions& mutable_cf_options,
                              uint64_t file_size);

    //static const int kMinFilesForIntraL0Compaction = 4;
  };
Compaction* SegmentCompactionBuilder::PickCompaction() {
  // Find the compactions by size on all levels.
  //bool skipped_l0_to_base = false;
  level_per_segment_level=vstorage_->GetLevelPerSegmentLevel();
  std::vector<FileMetaData*> compaction_filelist;
  int total_size;
  bool has_level=false;
  for(int i=0;i<vstorage_->GetSegmentLevel();i++)
  {
    if(vstorage_->ShouldCompactLevel(ioptions_,mutable_cf_options_,i))
    {
      output_level_=(i+2)*level_per_segment_level-1;
      std::vector<Segment*>* level_segment=vstorage_->GetLevelSegments(i);
      for(auto & segment_:*level_segment)
      {
        if(segment_->being_compacted||compaction_picker_->RangeOverlapWithCompaction_Segment(segment_->smallest.user_key(),segment_->largest.user_key(),i))
        {
          continue;
        }
        auto fl=segment_->GetLevelFiles(0);
        if(fl.empty())
        {
          continue;
        }
        compaction_filelist.emplace_back(fl[0]);
        has_level=true;
        start_segment_=segment_->GetSegmentNum();
        segment_->being_compacted=true;
        break;
      }
      if(has_level)
      {
        std::vector<CompactionInputFiles> inputs(2);
        for(auto& f:compaction_filelist)
        {
          inputs[0].files.emplace_back(f);
          total_size+=f->fd.GetFileSize();
        }
        inputs[0].level=(i+1)*level_per_segment_level-1;
        inputs[1].level=(i+2)*level_per_segment_level-1;
        uint32_t path_id =GetPathId(ioptions_, mutable_cf_options_, total_size);
        Compaction* c=new Compaction(
        vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
        std::move(inputs), output_level_,
        MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                            kCompactionStyleSegment),
        GetMaxOverlappingBytes(), path_id,
        GetCompressionType(vstorage_, mutable_cf_options_, output_level_, 1,
                           true /* enable_compression */),
        GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_,
                            true /* enable_compression */),
        mutable_cf_options_.default_write_temperature,
        /* max_subcompactions */ 0, /* grandparents */ {},
        /* earliest_snapshot */ std::nullopt,
        /* snapshot_checker */ nullptr, CompactionReason::kUniversalSortedRunNum,
        /* trim_ts */ "", start_segment_score_,
        /* l0_files_might_overlap */ true);
        if (mutable_cf_options_.compaction_options_universal.allow_trivial_move ==true)
        {
          c->set_is_trivial_move(true);
        }
        size_t num_files = 0;
        for (auto& each_level : *c->inputs())
        {
          num_files += each_level.files.size();
        }
        c->segment_level=start_level_;
        c->segment_number.emplace_back(start_segment_);
        c->next_level=false;
        //c->compaction_id=compaction_picker_->NewCompactionNumber();
        RecordInHistogram(ioptions_.stats, NUM_FILES_IN_SINGLE_COMPACTION, num_files);
        compaction_picker_->RegisterCompaction_Segment(c);
        vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
        return c;
      }
    }
  }
  for (int i = 0; i < vstorage_->CompactionSegmentNum(); i++) {
    start_segment_score_ = vstorage_->CompactionScore(i);
    start_segment_ = vstorage_->CompactionScoreLevel(i);
   
    if (start_segment_score_ >= 1) {
      start_level_=vstorage_->GetSegmentLocation(start_segment_).GetLevel();
      output_level_=(start_level_+1)*level_per_segment_level+level_per_segment_level-1;
      Compaction* c = PickFileToCompact(start_segment_);
      if(c==nullptr)
      {
        continue;
      }
      if (mutable_cf_options_.compaction_options_universal.allow_trivial_move ==true)
      {
        c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
      }
      size_t num_files = 0;
      for (auto& each_level : *c->inputs())
      {
        num_files += each_level.files.size();
      }
      //Compaction_Filelist_for_Delete* cffd=new Compaction_Filelist_for_Delete(c);
      //compaction_picker_->Record_Segment_Delete_Filelist(cffd,start_segment_);//不对，难以确定删除时机，并且可能一个segment对应多个filelist
      c->segment_number.emplace_back(start_segment_);
      c->segment_level=start_level_;
      c->next_level=true;
      //c->compaction_id=compaction_picker_->NewCompactionNumber();
      RecordInHistogram(ioptions_.stats, NUM_FILES_IN_SINGLE_COMPACTION, num_files);
      compaction_picker_->RegisterCompaction_Segment(c);
      vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
      return c;
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  return nullptr;
}
Compaction* SegmentCompactionBuilder::PickFileToCompact(int segment_number_)
{
    Segment* segment_=vstorage_->GetSegment(segment_number_);
    if(segment_->being_compacted||compaction_picker_->RangeOverlapWithCompaction_Segment(segment_->smallest.user_key(),segment_->largest.user_key(),start_level_))
    {
      return nullptr;
    }
    std::vector<std::vector<FileMetaData*>> filelist=segment_->get_files();
    std::vector<std::vector<FileMetaData*>> compaction_filelist;
    compaction_filelist.reserve(segment_->GetNotEmptyLevel());
    int max_input_level=segment_->GetNotEmptyLevel()-1;
    std::vector<int> files_level;
    files_level.reserve(segment_->GetNotEmptyLevel());
    uint64_t total_size=0;
    int file_num;
    for(int i=max_input_level;i>=0;i--)
    {
        if(filelist[i].empty())
        {
          continue;
        }
        compaction_filelist.emplace_back(std::vector<FileMetaData*>());
        for(auto& f:(filelist)[i])
        {
            if(!(f->being_compacted))
            {
              file_num++;
              compaction_filelist[i].emplace_back(f);
              total_size+=f->fd.GetFileSize();
            }
        }
        files_level.emplace_back(i);
    }
    int num_levels_=static_cast<int>(files_level.size());
   uint32_t path_id =GetPathId(ioptions_, mutable_cf_options_, total_size);
   std::vector<CompactionInputFiles> inputs(num_levels_+1);
   for (size_t i = 0; i <static_cast<size_t>(num_levels_); ++i)
   {
      inputs[i].level = (start_level_+1)*level_per_segment_level -1-(files_level[i]) ;
      for(auto&f: compaction_filelist[i])
      {
        inputs[i].files.emplace_back(f);
      }
   }
   //int output_level= ((int(start_level_/level_per_segment_level))+1)*level_per_segment_level - 1+level_per_segment_level;
   inputs.back().level=output_level_;
   inputs.back().files.resize(0);
   
   if (compaction_picker_->FilesRangeOverlapWithCompaction_Segment(
                               inputs, start_level_,
                               Compaction::kInvalidLevel)){
    return nullptr;
  }
  segment_->being_compacted=true;
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(inputs), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          kCompactionStyleUniversal),
      GetMaxOverlappingBytes(), path_id,
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_, 1,
                         true /* enable_compression */),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_,
                            true /* enable_compression */),
      mutable_cf_options_.default_write_temperature,
      /* max_subcompactions */ 0, /* grandparents */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, CompactionReason::kUniversalSortedRunNum,
      /* trim_ts */ "", start_segment_score_,
      /* l0_files_might_overlap */ true);
}
bool SegmentCompactionBuilder::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev, curr;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->CompareWithoutTimestamp(
              prev.f->largest.user_key(), curr.f->smallest.user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->CompareWithoutTimestamp(
                 curr.f->largest.user_key(), prev.f->largest.user_key()) > 0);
      prev = curr;
    }

    if (c->level(curr.level) != 0 &&
        curr.index < c->num_input_files(curr.level) - 1) {
      smallest_key_priority_q.emplace(c->input(curr.level, curr.index + 1),
                                      curr.level, curr.index + 1);
    }
  }
  return true;
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
uint64_t SegmentCompactionBuilder::GetMaxOverlappingBytes() const {
  if (!mutable_cf_options_.compaction_options_universal.incremental) {
    return std::numeric_limits<uint64_t>::max();
  } else {
    // Try to align cutting boundary with files at the next level if the
    // file isn't end up with 1/2 of target size, or it would overlap
    // with two full size files at the next level.
    return mutable_cf_options_.target_file_size_base / 2 * 3;
  }
}
}
    Compaction* SegmentCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots */,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool /* require_max_output_level*/)
    {
      SegmentCompactionBuilder builder(cf_name, vstorage, this, icmp_,log_buffer,
                                     mutable_cf_options, ioptions_,
                                     mutable_db_options);
      return builder.PickCompaction();
    }

}