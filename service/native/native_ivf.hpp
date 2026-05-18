#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

class NativeIVF {
 public:
  bool load(const char* path);
  void warmup();
  int classify(const float* q);
  void set_profile(bool enabled) { profile_enabled_ = enabled; }
  void profile_report() const;
  const std::string& error() const { return error_; }

 private:
  static constexpr int kDim = 14;
  static constexpr int kMaxCentroids = 4096;
  static constexpr int kMaxProbe = 32;
  static constexpr int kQuickProbe = 8;
  static constexpr int kExpandedProbe = 20;
  static constexpr int kVectorsPerBlock = 16;
  static constexpr int kBlockStride = kDim * kVectorsPerBlock;
  static constexpr float kVectorScale = 0.0001f;
  static constexpr float kInitialTopDist = 1.0e30f;

  uint32_t k_ = 0;
  uint32_t total_blocks_ = 0;
  std::vector<float> centroids_;
  std::vector<uint32_t> offsets_;
  std::vector<uint8_t> labels_;
  std::vector<int16_t> blocks_;
  std::vector<float> block_min_radii_;
  std::vector<float> block_max_radii_;
  std::string error_;
  bool use_block_bounds_ = true;
  bool profile_enabled_ = false;

  struct ProfileCounters {
    uint64_t calls = 0;
    uint64_t quick_only = 0;
    uint64_t rescore = 0;
    uint64_t centroid_ns = 0;
    uint64_t select8_ns = 0;
    uint64_t quick_scan_ns = 0;
    uint64_t select20_ns = 0;
    uint64_t rescore_ns = 0;
  };
  ProfileCounters profile_{};

  alignas(32) std::array<float, kMaxCentroids> centroid_dists_{};
  std::array<int, kMaxProbe> probes_{};
  std::array<float, 5> top_dist_{};
  std::array<uint8_t, 5> top_label_{};
  std::array<float, kDim> quantized_{};

  void centroid_dists(const float* q);
  void centroid_dists_select8(const float* q);
  template <int N>
  void select_top();
  template <int N>
  void select_top_masked();
  void reset_top();
  void scan_probes(const float* q, int from, int to);
  void scan_probes_bounded(const float* q, int from, int to);
  void build_block_radii();
  void bound_probe_range(uint32_t& start, uint32_t& end, float center_dist_sq, float worst_dist_sq) const;
  int count_frauds() const;
  int rescore_quantized(const float* q);
};
