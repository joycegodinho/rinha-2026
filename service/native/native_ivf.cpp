#include "native_ivf.hpp"
#include "decision_tree.hpp"

#include <immintrin.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <fstream>
#include <string>
#include <time.h>

#ifndef ENABLE_RESIDUAL8_INDEX
#define ENABLE_RESIDUAL8_INDEX 0
#endif

namespace {

volatile uint64_t g_native_ivf_warmup_sink = 0;
constexpr int kNativeDim = 14;
constexpr int kNativeVectorsPerBlock = 16;
constexpr bool kEnableResidual8Index = ENABLE_RESIDUAL8_INDEX != 0;
constexpr int kResidual8PaddingValue = 127;
constexpr std::array<int, kNativeDim> kHot6DimOrder = {0, 1, 2, 7, 8, 12, 3,
                                                       4, 5, 6, 9, 10, 11, 13};
constexpr std::array<uint64_t, 7> kProfileBucketLimitsNs = {
    50000, 100000, 200000, 500000, 1000000, 2000000, static_cast<uint64_t>(-1)};
constexpr std::array<const char*, 7> kProfileBucketLabels = {
    "<50us", "<100us", "<200us", "<500us", "<1ms", "<2ms", ">=2ms"};
constexpr std::array<float, 16> kMarginProfileThresholds = {
    0.0f, 0.000001f, 0.000002f, 0.000005f, 0.00001f, 0.00002f,
    0.00005f, 0.0001f, 0.0002f, 0.0005f, 0.001f, 0.002f,
    0.005f, 0.01f, 0.02f, 0.05f};
constexpr int kJLCandidateMax = 2048;
constexpr int kJLProjectionMaxDims = 8;
constexpr float kJLProjectionScale = 0.2672612419f;  // 1 / sqrt(14)
constexpr uint8_t kFraudLabelMask = 1u;
constexpr uint8_t kReferenceBorderlineMask = 2u;
constexpr std::array<std::array<int8_t, kNativeDim>, kJLProjectionMaxDims> kJLProjection = {{
    {{1, -1, 1, 1, -1, 1, -1, 1, -1, -1, 1, -1, 1, -1}},
    {{1, 1, -1, 1, 1, -1, -1, -1, 1, -1, -1, 1, 1, -1}},
    {{-1, 1, 1, -1, 1, 1, -1, 1, -1, 1, -1, -1, 1, -1}},
    {{1, -1, -1, -1, 1, -1, 1, 1, 1, -1, 1, -1, -1, 1}},
    {{-1, -1, 1, 1, 1, -1, 1, -1, 1, 1, -1, 1, -1, -1}},
    {{1, 1, 1, -1, -1, 1, 1, -1, -1, 1, -1, -1, -1, 1}},
    {{-1, 1, -1, 1, -1, -1, 1, 1, -1, 1, 1, -1, 1, -1}},
    {{1, -1, 1, -1, 1, 1, 1, -1, -1, -1, 1, 1, -1, -1}},
}};

struct JLCandidate {
  float dist;
  int idx;
};

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

template <size_t N>
void record_profile_ns(uint64_t ns, std::array<uint64_t, N>& bins, uint64_t& max_ns) {
  if (ns > max_ns) max_ns = ns;
  for (size_t i = 0; i < N; ++i) {
    if (ns < kProfileBucketLimitsNs[i]) {
      ++bins[i];
      return;
    }
  }
}

template <size_t N>
void print_profile_bins(const char* prefix, const std::array<uint64_t, N>& bins) {
  std::fprintf(stderr, "%s", prefix);
  for (size_t i = 0; i < N; ++i) {
    std::fprintf(stderr, " %s=%llu", kProfileBucketLabels[i],
                 static_cast<unsigned long long>(bins[i]));
  }
  std::fprintf(stderr, "\n");
}

template <typename T>
bool read_exact(std::ifstream& in, T* dst, size_t count) {
  in.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(sizeof(T) * count));
  return static_cast<size_t>(in.gcount()) == sizeof(T) * count;
}

uint32_t read_u32(std::ifstream& in, bool& ok) {
  uint8_t b[4];
  ok = read_exact(in, b, 4);
  if (!ok) return 0;
  return static_cast<uint32_t>(b[0]) |
         (static_cast<uint32_t>(b[1]) << 8) |
         (static_cast<uint32_t>(b[2]) << 16) |
         (static_cast<uint32_t>(b[3]) << 24);
}

int env_int_clamped(const char* name, int fallback, int min_value, int max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value) return fallback;
  if (parsed < min_value) return min_value;
  if (parsed > max_value) return max_value;
  return static_cast<int>(parsed);
}

float env_float_clamped(const char* name, float fallback, float min_value, float max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  char* end = nullptr;
  float parsed = std::strtof(value, &end);
  if (end == value || !std::isfinite(parsed)) return fallback;
  if (parsed < min_value) return min_value;
  if (parsed > max_value) return max_value;
  return parsed;
}

bool env_bool_or(const char* name, bool fallback) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  if (std::strcmp(value, "0") == 0 || std::strcmp(value, "false") == 0 ||
      std::strcmp(value, "FALSE") == 0 || std::strcmp(value, "no") == 0 ||
      std::strcmp(value, "NO") == 0) {
    return false;
  }
  return true;
}

struct IndexResidencyConfig {
  bool madvise_index = false;
  bool madvise_hugepage = true;
  bool mlock_index = false;
  bool touch_index = false;
};

struct IndexResidencyStats {
  uint64_t bytes = 0;
  uint32_t regions = 0;
  uint32_t madvise_failures = 0;
  uint32_t mlock_failures = 0;
};

void apply_index_residency_region(const char* name, const void* data, size_t len,
                                  const IndexResidencyConfig& config,
                                  IndexResidencyStats& stats) {
  if (len == 0 || data == nullptr) return;
  const long page_size_raw = sysconf(_SC_PAGESIZE);
  const uintptr_t page_size = page_size_raw > 0 ? static_cast<uintptr_t>(page_size_raw) : 4096u;
  const uintptr_t begin = reinterpret_cast<uintptr_t>(data);
  const uintptr_t page_begin = begin & ~(page_size - 1u);
  const uintptr_t page_end = (begin + len + page_size - 1u) & ~(page_size - 1u);
  if (page_end <= page_begin) return;

  void* aligned = reinterpret_cast<void*>(page_begin);
  const size_t aligned_len = static_cast<size_t>(page_end - page_begin);
  ++stats.regions;
  stats.bytes += static_cast<uint64_t>(aligned_len);

  if (config.madvise_index) {
    if (config.madvise_hugepage && madvise(aligned, aligned_len, MADV_HUGEPAGE) != 0) {
      ++stats.madvise_failures;
    }
    if (madvise(aligned, aligned_len, MADV_WILLNEED) != 0) {
      ++stats.madvise_failures;
    }
  }

  if (config.touch_index) {
    const volatile uint8_t* p = reinterpret_cast<const volatile uint8_t*>(page_begin);
    uint64_t sink = 0;
    for (uintptr_t addr = page_begin; addr < page_end; addr += page_size) {
      sink += *p;
      p = reinterpret_cast<const volatile uint8_t*>(addr + page_size);
    }
    g_native_ivf_warmup_sink += sink;
  }

  if (config.mlock_index && mlock(aligned, aligned_len) != 0) {
    ++stats.mlock_failures;
  }

  (void)name;
}

template <typename T>
void apply_index_residency_vector(const char* name, const std::vector<T>& values,
                                  const IndexResidencyConfig& config,
                                  IndexResidencyStats& stats) {
  if (values.empty()) return;
  apply_index_residency_region(name, values.data(), values.size() * sizeof(T), config, stats);
}

const char* skip_policy_separators(const char* p) {
  while (*p == ' ' || *p == '\t' || *p == ',' || *p == ':' || *p == ';') ++p;
  return p;
}

bool approved_decision(int fraud_score) {
  return fraud_score < 3;
}

inline uint8_t fraud_label(uint8_t label) {
  return static_cast<uint8_t>(label & kFraudLabelMask);
}

inline bool is_reference_borderline_label(uint8_t label) {
  return (label & kReferenceBorderlineMask) != 0;
}

constexpr uint64_t kRH26Magic = 0x524832364956460Aull;

struct RH26Header {
  uint64_t magic;
  uint32_t version;
  uint32_t n;
  uint32_t k;
  uint32_t total_blocks;
  uint32_t block_size;
  uint32_t dims;
  uint32_t train_sample;
  uint32_t train_iters;
  uint32_t train_max_iters;
  uint32_t reserved[5];
};
static_assert(sizeof(RH26Header) == 64);

size_t align_up_size(size_t value, size_t align) {
  return (value + align - 1) & ~(align - 1);
}

size_t rh26_pair_offset(int dim, int lane) {
  return static_cast<size_t>(dim / 2) * 8u * 2u +
         static_cast<size_t>(lane) * 2u + static_cast<size_t>(dim & 1);
}

uint16_t as_u16(int16_t value) {
  return static_cast<uint16_t>(value);
}

__attribute__((always_inline)) inline __m256i rh26_make_qpair(const int16_t* q, int pair) {
  const uint32_t lo = static_cast<uint32_t>(as_u16(q[pair * 2]));
  const uint32_t hi = static_cast<uint32_t>(as_u16(q[pair * 2 + 1]));
  return _mm256_set1_epi32(static_cast<int32_t>(lo | (hi << 16)));
}

__attribute__((always_inline)) inline int16_t rh26_quantize_query_value(float value) {
  const double scaled = static_cast<double>(value) * 10000.0;
  if (scaled <= static_cast<double>(INT16_MIN)) return INT16_MIN;
  if (scaled >= static_cast<double>(INT16_MAX)) return INT16_MAX;
  int v = static_cast<int>(scaled < 0.0 ? scaled - 0.5 : scaled + 0.5);
  return static_cast<int16_t>(v);
}

int predict_rescore_tree_from_query(const float* raw_q) {
  float features[TREE_FEATURE_COUNT]{};
  for (int d = 0; d < kNativeDim; ++d) {
    features[d] = raw_q[d];
  }
  return rescore_tree_predict_score(features);
}

bool is_padding_slot(const std::vector<int16_t>& blocks, size_t base, int slot) {
  constexpr int16_t kPadding = 32767;
  for (int d = 0; d < kNativeDim; ++d) {
    if (blocks[base + static_cast<size_t>(d) * kNativeVectorsPerBlock +
               static_cast<size_t>(slot)] != kPadding) {
      return false;
    }
  }
  return true;
}

template <size_t LabelCount>
inline bool update_candidate(float dist, uint8_t label, std::array<float, 5>& top_dist,
                             std::array<uint8_t, LabelCount>& top_label, int& worst) {
  if (dist >= top_dist[static_cast<size_t>(worst)]) return false;
  top_dist[static_cast<size_t>(worst)] = dist;
  top_label[static_cast<size_t>(worst)] = label;

  worst = 0;
  float max_dist = top_dist[0];
  for (int i = 1; i < 5; ++i) {
    if (top_dist[static_cast<size_t>(i)] > max_dist) {
      max_dist = top_dist[static_cast<size_t>(i)];
      worst = i;
    }
  }
  return true;
}

inline bool update_candidate_slot(float dist, uint8_t label, int slot,
                                  std::array<float, 5>& top_dist,
                                  std::array<uint8_t, 5>& top_label,
                                  std::array<int, 5>& top_slot, int& worst) {
  const float worst_dist = top_dist[static_cast<size_t>(worst)];
  const int worst_slot = top_slot[static_cast<size_t>(worst)];
  if (dist > worst_dist || (dist == worst_dist && worst_slot >= 0 && slot >= worst_slot)) {
    return false;
  }
  top_dist[static_cast<size_t>(worst)] = dist;
  top_label[static_cast<size_t>(worst)] = label;
  top_slot[static_cast<size_t>(worst)] = slot;

  worst = 0;
  float max_dist = top_dist[0];
  int max_slot = top_slot[0];
  for (int i = 1; i < 5; ++i) {
    const float candidate_dist = top_dist[static_cast<size_t>(i)];
    const int candidate_slot = top_slot[static_cast<size_t>(i)];
    if (candidate_dist > max_dist ||
        (candidate_dist == max_dist && candidate_slot > max_slot)) {
      max_dist = candidate_dist;
      max_slot = candidate_slot;
      worst = i;
    }
  }
  return true;
}

template <int N>
inline void insert_probe_candidate(float dist, int idx, std::array<float, N>& top_d,
                                   std::array<int, N>& top_i) {
  if (dist >= top_d[static_cast<size_t>(N - 1)]) return;
  int pos = N - 1;
  while (pos > 0 && dist < top_d[static_cast<size_t>(pos - 1)]) {
    top_d[static_cast<size_t>(pos)] = top_d[static_cast<size_t>(pos - 1)];
    top_i[static_cast<size_t>(pos)] = top_i[static_cast<size_t>(pos - 1)];
    --pos;
  }
  top_d[static_cast<size_t>(pos)] = dist;
  top_i[static_cast<size_t>(pos)] = idx;
}

inline void update_top8_from_vec(__m256 dist, int base, std::array<float, 8>& top_d,
                                 std::array<int, 8>& top_i) {
  int mask = _mm256_movemask_ps(_mm256_cmp_ps(dist, _mm256_set1_ps(top_d[7]), _CMP_LT_OQ));
  if (mask == 0) return;

  alignas(32) float tmp[8];
  _mm256_store_ps(tmp, dist);
  while (mask) {
    const int bit = __builtin_ctz(static_cast<unsigned>(mask));
    mask &= mask - 1;
    insert_probe_candidate<8>(tmp[bit], base + bit, top_d, top_i);
  }
}

template <int N>
inline void update_probe_top_from_vec(__m256 dist, int base, std::array<float, N>& top_d,
                                      std::array<int, N>& top_i) {
  int mask = _mm256_movemask_ps(
      _mm256_cmp_ps(dist, _mm256_set1_ps(top_d[static_cast<size_t>(N - 1)]), _CMP_LT_OQ));
  if (mask == 0) return;

  alignas(32) float tmp[8];
  _mm256_store_ps(tmp, dist);
  while (mask) {
    const int bit = __builtin_ctz(static_cast<unsigned>(mask));
    mask &= mask - 1;
    insert_probe_candidate<N>(tmp[bit], base + bit, top_d, top_i);
  }
}

template <size_t N>
inline void insert_probe_candidate_dynamic(float dist, int idx, int n,
                                           std::array<float, N>& top_d,
                                           std::array<int, N>& top_i) {
  if (n <= 0 || dist >= top_d[static_cast<size_t>(n - 1)]) return;
  int pos = n - 1;
  while (pos > 0 && dist < top_d[static_cast<size_t>(pos - 1)]) {
    top_d[static_cast<size_t>(pos)] = top_d[static_cast<size_t>(pos - 1)];
    top_i[static_cast<size_t>(pos)] = top_i[static_cast<size_t>(pos - 1)];
    --pos;
  }
  top_d[static_cast<size_t>(pos)] = dist;
  top_i[static_cast<size_t>(pos)] = idx;
}

inline void insert_rh26_centroid_candidate(uint32_t dist, uint32_t idx, int n,
                                           uint32_t* top_d, uint32_t* top_i) {
  if (n <= 0 || dist >= top_d[n - 1]) return;
  int pos = n - 1;
  while (pos > 0 && dist < top_d[pos - 1]) {
    top_d[pos] = top_d[pos - 1];
    top_i[pos] = top_i[pos - 1];
    --pos;
  }
  top_d[pos] = dist;
  top_i[pos] = idx;
}

constexpr int kRH26LocalPairs = kNativeDim / 2;
constexpr int kRH26LocalTopK = 5;
constexpr int kRH26LocalGroupLanes = 8;
constexpr int kRH26LocalMaxCentroids = 4096;
constexpr int kRH26LocalRepairCandidates = 1024;

struct RH26TopSummary {
  bool has_borderline = false;
  int frauds = 0;

  bool in_repair_band(int min_count, int max_count) const {
    return frauds >= min_count && frauds <= max_count;
  }
};

struct RH26Top5View {
  uint32_t* dist = nullptr;
  uint8_t* label = nullptr;
  uint32_t& ceiling;

  void offer(uint32_t candidate_dist, uint8_t candidate_label) const {
    if (candidate_dist >= ceiling) return;
    int slot = kRH26LocalTopK - 1;
    while (slot > 0 && candidate_dist < dist[slot - 1]) {
      dist[slot] = dist[slot - 1];
      label[slot] = label[slot - 1];
      --slot;
    }
    dist[slot] = candidate_dist;
    label[slot] = candidate_label;
    ceiling = dist[kRH26LocalTopK - 1];
  }

  RH26TopSummary summarize() const {
    RH26TopSummary out{};
    for (int i = 0; i < kRH26LocalTopK; ++i) {
      out.frauds += static_cast<int>(label[i] & kFraudLabelMask);
      out.has_borderline =
          out.has_borderline || ((label[i] & kReferenceBorderlineMask) != 0);
    }
    return out;
  }

  int fraud_count() const {
    int count = 0;
    for (int i = 0; i < kRH26LocalTopK; ++i) {
      count += static_cast<int>(label[i] & kFraudLabelMask);
    }
    return count;
  }
};

inline uint32_t rh26_i32_ceiling(uint32_t value) {
  return std::min(value, static_cast<uint32_t>(INT32_MAX));
}

__attribute__((always_inline)) inline __m256i rh26_pair_term(const __m256i query_pair,
                                                            const int16_t* packed_pair) {
  const __m256i values =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(packed_pair));
  const __m256i delta = _mm256_sub_epi16(query_pair, values);
  return _mm256_madd_epi16(delta, delta);
}

__attribute__((always_inline)) inline __m256i rh26_add_pair_term(__m256i acc,
                                                                 const __m256i query_pair,
                                                                 const int16_t* packed_pair) {
  return _mm256_add_epi32(acc, rh26_pair_term(query_pair, packed_pair));
}

inline bool rh26_any_lane_under(__m256i even_acc, __m256i odd_acc, uint32_t ceiling) {
  const __m256i bound = _mm256_set1_epi32(static_cast<int32_t>(rh26_i32_ceiling(ceiling)));
  const __m256i partial = _mm256_add_epi32(even_acc, odd_acc);
  return _mm256_movemask_epi8(_mm256_cmpgt_epi32(bound, partial)) != 0;
}

inline void rh26_prepare_query_pairs(const float* q, int16_t* q16, __m256i* vq) {
  for (int d = 0; d < kNativeDim; ++d) {
    q16[d] = rh26_quantize_query_value(q[d]);
  }
  for (int pair = 0; pair < kRH26LocalPairs; ++pair) {
    vq[pair] = rh26_make_qpair(q16, pair);
  }
}

inline bool rh26_centroid_pair_enabled(uint32_t mask, int requested_pairs, int pair) {
  if (mask != 0) return (mask & (1u << static_cast<uint32_t>(pair))) != 0;
  return pair < requested_pairs;
}

inline __m256i rh26_centroid_group_distances(const __m256i* vq, const int16_t* group,
                                             uint32_t pair_mask, int pair_limit) {
  __m256i acc = _mm256_setzero_si256();
  for (int pair = 0; pair < kRH26LocalPairs; ++pair) {
    if (!rh26_centroid_pair_enabled(pair_mask, pair_limit, pair)) continue;
    acc = rh26_add_pair_term(acc, vq[pair],
                             group + static_cast<size_t>(pair) * 16u);
  }
  return acc;
}

inline __m256i rh26_bbox_group_lower_bounds(const __m256i* vq, const int16_t* mins,
                                            const int16_t* maxs) {
  __m256i acc = _mm256_setzero_si256();
  const __m256i zero = _mm256_setzero_si256();
  for (int pair = 0; pair < kRH26LocalPairs; ++pair) {
    const int16_t* min_pair = mins + static_cast<size_t>(pair) * 16u;
    const int16_t* max_pair = maxs + static_cast<size_t>(pair) * 16u;
    const __m256i mn =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(min_pair));
    const __m256i mx =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(max_pair));
    const __m256i below = _mm256_max_epi16(zero, _mm256_sub_epi16(mn, vq[pair]));
    const __m256i above = _mm256_max_epi16(zero, _mm256_sub_epi16(vq[pair], mx));
    const __m256i gap = _mm256_max_epi16(below, above);
    acc = _mm256_add_epi32(acc, _mm256_madd_epi16(gap, gap));
  }
  return acc;
}

inline void rh26_build_skip_bitmap(const uint32_t* ids, int count,
                                   std::array<uint64_t, 64>& bitmap) {
  bitmap.fill(0);
  for (int i = 0; i < count; ++i) {
    bitmap[ids[i] >> 6] |= 1ull << (ids[i] & 63);
  }
}

inline bool rh26_bitmap_contains(const std::array<uint64_t, 64>& bitmap, uint32_t id) {
  return (bitmap[id >> 6] & (1ull << (id & 63))) != 0;
}

struct RH26RepairCandidate {
  uint32_t lb = UINT32_MAX;
  uint32_t centroid = UINT32_MAX;
};

template <size_t Capacity>
struct RH26RepairQueue {
  std::array<RH26RepairCandidate, Capacity>& items;
  int count = 0;
  int limit = 0;
  int worst = 0;
  uint32_t worst_lb = 0;

  RH26RepairQueue(std::array<RH26RepairCandidate, Capacity>& storage, int requested)
      : items(storage),
        limit(std::max(1, std::min(requested, static_cast<int>(Capacity)))) {}

  void refresh_worst() {
    worst = 0;
    worst_lb = items[0].lb;
    for (int i = 1; i < count; ++i) {
      if (items[static_cast<size_t>(i)].lb > worst_lb) {
        worst = i;
        worst_lb = items[static_cast<size_t>(i)].lb;
      }
    }
  }

  void offer(uint32_t lb, uint32_t centroid) {
    const RH26RepairCandidate candidate{lb, centroid};
    if (count < limit) {
      items[static_cast<size_t>(count++)] = candidate;
      if (count == 1 || lb > worst_lb) {
        worst = count - 1;
        worst_lb = lb;
      }
      return;
    }
    if (lb >= worst_lb) return;
    items[static_cast<size_t>(worst)] = candidate;
    refresh_worst();
  }

  void sort_by_bound() {
    std::sort(items.begin(), items.begin() + static_cast<std::ptrdiff_t>(count),
              [](const RH26RepairCandidate& a, const RH26RepairCandidate& b) {
                return a.lb < b.lb;
              });
  }

  bool take_lowest(uint32_t ceiling, RH26RepairCandidate& out) {
    int best = -1;
    uint32_t best_lb = ceiling;
    for (int i = 0; i < count; ++i) {
      const RH26RepairCandidate& candidate = items[static_cast<size_t>(i)];
      if (candidate.centroid != UINT32_MAX && candidate.lb < best_lb) {
        best = i;
        best_lb = candidate.lb;
      }
    }
    if (best < 0) return false;
    out = items[static_cast<size_t>(best)];
    items[static_cast<size_t>(best)].centroid = UINT32_MAX;
    return true;
  }
};

inline void accumulate_residual8_dim(const int8_t* row, float scale_low, float scale_high,
                                     float q_center,
                                     bool capture_padding, __m256& low, __m256& high,
                                     __m256& pad_low, __m256& pad_high) {
  const __m128i raw_low8 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(row));
  const __m128i raw_high8 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(row + 8));
  const __m256i raw_low = _mm256_cvtepi8_epi32(raw_low8);
  const __m256i raw_high = _mm256_cvtepi8_epi32(raw_high8);
  if (capture_padding) {
    const __m256i padding = _mm256_set1_epi32(kResidual8PaddingValue);
    pad_low = _mm256_castsi256_ps(_mm256_cmpeq_epi32(raw_low, padding));
    pad_high = _mm256_castsi256_ps(_mm256_cmpeq_epi32(raw_high, padding));
  }
  const __m256 scale_low_v = _mm256_set1_ps(scale_low);
  const __m256 scale_high_v = _mm256_set1_ps(scale_high);
  const __m256 qv = _mm256_set1_ps(q_center);
  const __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(raw_low), scale_low_v);
  const __m256 vf_high = _mm256_mul_ps(_mm256_cvtepi32_ps(raw_high), scale_high_v);
  const __m256 dl = _mm256_sub_ps(vf_low, qv);
  const __m256 dh = _mm256_sub_ps(vf_high, qv);
  low = _mm256_fmadd_ps(dl, dl, low);
  high = _mm256_fmadd_ps(dh, dh, high);
}

inline int8_t quantize_residual8(float value, float scale) {
  if (!(scale > 0.0f)) return 0;
  int q = static_cast<int>(std::lrint(value / scale));
  if (q < -126) q = -126;
  if (q > 126) q = 126;
  return static_cast<int8_t>(q);
}

inline bool is_seed_probe(int idx, const std::array<int, 8>& seed_i) {
  return idx == seed_i[0] || idx == seed_i[1] || idx == seed_i[2] || idx == seed_i[3] ||
         idx == seed_i[4] || idx == seed_i[5] || idx == seed_i[6] || idx == seed_i[7];
}

template <typename T>
uint64_t touch_pages(const std::vector<T>& data) {
  if (data.empty()) return 0;
  constexpr size_t kPageBytes = 4096;
  size_t step = kPageBytes / sizeof(T);
  if (step == 0) step = 1;
  uint64_t acc = 0;
  for (size_t i = 0; i < data.size(); i += step) {
    if constexpr (sizeof(T) == 4) {
      uint32_t bits = 0;
      std::memcpy(&bits, &data[i], sizeof(bits));
      acc += bits;
    } else if constexpr (sizeof(T) == 2) {
      acc += static_cast<uint16_t>(data[i]);
    } else {
      acc += static_cast<uint8_t>(data[i]);
    }
  }
  if constexpr (sizeof(T) == 4) {
    uint32_t bits = 0;
    std::memcpy(&bits, &data.back(), sizeof(bits));
    acc += bits;
  } else if constexpr (sizeof(T) == 2) {
    acc += static_cast<uint16_t>(data.back());
  } else {
    acc += static_cast<uint8_t>(data.back());
  }
  return acc;
}

}  // namespace

void NativeIVF::profile_report() const {
  if (!profile_enabled_ || profile_.calls == 0) return;
  const uint64_t calls = profile_.calls;
  const uint64_t rescore = profile_.rescore == 0 ? 1 : profile_.rescore;
  std::fprintf(stderr,
               "[native-cpp-ivf-profile] calls=%llu quick_only=%llu rescore=%llu "
               "rescore_tree=%llu "
               "avg_total_ns=%llu max_total_ns=%llu "
               "avg_centroid_ns=%llu avg_select8_ns=%llu avg_quick_scan_ns=%llu "
               "max_quick_scan_ns=%llu avg_select20_ns=%llu avg_rescore_ns=%llu "
               "max_rescore_ns=%llu quick_frauds=[%llu,%llu,%llu,%llu,%llu,%llu]\n",
               static_cast<unsigned long long>(calls),
               static_cast<unsigned long long>(profile_.quick_only),
               static_cast<unsigned long long>(profile_.rescore),
               static_cast<unsigned long long>(profile_.rescore_tree),
               static_cast<unsigned long long>(profile_.total_ns / calls),
               static_cast<unsigned long long>(profile_.max_total_ns),
               static_cast<unsigned long long>(profile_.centroid_ns / calls),
               static_cast<unsigned long long>(profile_.select8_ns / calls),
               static_cast<unsigned long long>(profile_.quick_scan_ns / calls),
               static_cast<unsigned long long>(profile_.max_quick_scan_ns),
               static_cast<unsigned long long>(profile_.select20_ns / rescore),
               static_cast<unsigned long long>(profile_.rescore_ns / rescore),
               static_cast<unsigned long long>(profile_.max_rescore_ns),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[0]),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[1]),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[2]),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[3]),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[4]),
               static_cast<unsigned long long>(profile_.quick_fraud_counts[5]));
  std::fprintf(stderr,
               "[native-cpp-ivf-profile] rescore_by_fast=[%llu,%llu,%llu,%llu,%llu,%llu] "
               "avg_rescore_by_fast_ns=[%llu,%llu,%llu,%llu,%llu,%llu] "
               "max_rescore_by_fast_ns=[%llu,%llu,%llu,%llu,%llu,%llu]\n",
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[0]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[1]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[2]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[3]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[4]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[5]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[0] == 0 ? 0 : profile_.rescore_ns_by_fast[0] / profile_.rescore_fraud_counts[0]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[1] == 0 ? 0 : profile_.rescore_ns_by_fast[1] / profile_.rescore_fraud_counts[1]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[2] == 0 ? 0 : profile_.rescore_ns_by_fast[2] / profile_.rescore_fraud_counts[2]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[3] == 0 ? 0 : profile_.rescore_ns_by_fast[3] / profile_.rescore_fraud_counts[3]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[4] == 0 ? 0 : profile_.rescore_ns_by_fast[4] / profile_.rescore_fraud_counts[4]),
               static_cast<unsigned long long>(profile_.rescore_fraud_counts[5] == 0 ? 0 : profile_.rescore_ns_by_fast[5] / profile_.rescore_fraud_counts[5]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[0]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[1]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[2]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[3]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[4]),
               static_cast<unsigned long long>(profile_.max_rescore_ns_by_fast[5]));
  print_profile_bins("[native-cpp-ivf-profile] total_bins", profile_.total_bins);
  print_profile_bins("[native-cpp-ivf-profile] quick_scan_bins", profile_.quick_scan_bins);
  print_profile_bins("[native-cpp-ivf-profile] rescore_bins", profile_.rescore_bins);
  for (size_t fast = 0; fast < profile_.rescore_result_by_fast.size(); ++fast) {
    const auto& row = profile_.rescore_result_by_fast[fast];
    std::fprintf(stderr,
                 "[native-cpp-ivf-profile] rescore_result_fast%zu=[%llu,%llu,%llu,%llu,%llu,%llu]\n",
                 fast, static_cast<unsigned long long>(row[0]),
                 static_cast<unsigned long long>(row[1]),
                 static_cast<unsigned long long>(row[2]),
                 static_cast<unsigned long long>(row[3]),
                 static_cast<unsigned long long>(row[4]),
                 static_cast<unsigned long long>(row[5]));
  }
  if (margin_profile_enabled_) {
    for (size_t fast = 0; fast < profile_.margin_rescore_observed.size(); ++fast) {
      const uint64_t observed = profile_.margin_rescore_observed[fast];
      if (observed == 0) continue;
      std::fprintf(stderr,
                   "[native-cpp-ivf-margin-profile] fast=%zu observed=%llu changed=%llu",
                   fast, static_cast<unsigned long long>(observed),
                   static_cast<unsigned long long>(profile_.margin_rescore_changed[fast]));
      for (size_t i = 0; i < kMarginProfileThresholds.size(); ++i) {
        const uint64_t candidates = profile_.margin_candidate_skips[fast][i];
        if (candidates == 0) continue;
        std::fprintf(stderr, " t>=%.9g skip=%llu safe=%llu bad=%llu",
                     kMarginProfileThresholds[i],
                     static_cast<unsigned long long>(candidates),
                     static_cast<unsigned long long>(profile_.margin_safe_skips[fast][i]),
                     static_cast<unsigned long long>(profile_.margin_bad_skips[fast][i]));
      }
      std::fprintf(stderr, "\n");
    }
  }
  if (centroid_flip_profile_enabled_) {
    constexpr std::array<uint64_t, 5> kObservedBucketLimits = {1, 4, 9, 24,
                                                               static_cast<uint64_t>(-1)};
    constexpr std::array<const char*, 5> kObservedBucketLabels = {"1", "2-4", "5-9", "10-24",
                                                                 ">=25"};
    std::array<uint64_t, 5> active_by_bucket{};
    std::array<uint64_t, 5> rescore_by_bucket{};
    std::array<uint64_t, 5> stable_active_by_bucket{};
    std::array<uint64_t, 5> stable_rescore_by_bucket{};
    uint64_t active = 0;
    uint64_t stable_active = 0;
    uint64_t rescores_total = 0;
    uint64_t stable_rescores = 0;
    uint64_t approval_changed_total = 0;
    for (size_t ci = 0; ci < profile_.centroid_rescore_observed.size(); ++ci) {
      const uint64_t observed = profile_.centroid_rescore_observed[ci];
      if (observed == 0) continue;
      const uint64_t changed = profile_.centroid_rescore_approval_changed[ci];
      ++active;
      rescores_total += observed;
      approval_changed_total += changed;
      if (changed == 0) {
        ++stable_active;
        stable_rescores += observed;
      }
      for (size_t b = 0; b < kObservedBucketLimits.size(); ++b) {
        if (observed <= kObservedBucketLimits[b]) {
          ++active_by_bucket[b];
          rescore_by_bucket[b] += observed;
          if (changed == 0) {
            ++stable_active_by_bucket[b];
            stable_rescore_by_bucket[b] += observed;
          }
          break;
        }
      }
    }
    std::fprintf(stderr,
                 "[native-cpp-ivf-centroid-flip-profile] active_centroids=%llu "
                 "stable_centroids=%llu rescores=%llu stable_rescores=%llu "
                 "approval_changed=%llu\n",
                 static_cast<unsigned long long>(active),
                 static_cast<unsigned long long>(stable_active),
                 static_cast<unsigned long long>(rescores_total),
                 static_cast<unsigned long long>(stable_rescores),
                 static_cast<unsigned long long>(approval_changed_total));
    std::fprintf(stderr, "[native-cpp-ivf-centroid-flip-profile] buckets");
    for (size_t b = 0; b < kObservedBucketLabels.size(); ++b) {
      std::fprintf(stderr, " obs=%s active=%llu stable=%llu rescores=%llu stable_rescores=%llu",
                   kObservedBucketLabels[b],
                   static_cast<unsigned long long>(active_by_bucket[b]),
                   static_cast<unsigned long long>(stable_active_by_bucket[b]),
                   static_cast<unsigned long long>(rescore_by_bucket[b]),
                   static_cast<unsigned long long>(stable_rescore_by_bucket[b]));
    }
    std::fprintf(stderr, "\n");
    if (centroid_flip_dump_) {
      for (size_t fast = 0; fast < 6; ++fast) {
        const size_t base = fast * static_cast<size_t>(kMaxCentroids);
        for (size_t ci = 0; ci < k_; ++ci) {
          const uint32_t observed = profile_.centroid_fast_rescore_observed[base + ci];
          if (observed == 0) continue;
          const uint32_t changed = profile_.centroid_fast_rescore_approval_changed[base + ci];
          std::fprintf(stderr,
                       "[native-cpp-ivf-centroid-fast-profile] fast=%zu centroid=%zu "
                       "observed=%u changed=%u\n",
                       fast, ci, observed, changed);
        }
      }
    }
  }
  if (residual8_prefilter_profile_) {
    uint64_t active = 0;
    uint64_t checks = 0;
    uint64_t skipped = 0;
    for (size_t ci = 0; ci < k_; ++ci) {
      const uint32_t probe_checks = profile_.residual8_probe_checks[ci];
      if (probe_checks == 0) continue;
      const uint32_t probe_skipped = profile_.residual8_probe_skipped[ci];
      ++active;
      checks += probe_checks;
      skipped += probe_skipped;
      std::fprintf(stderr,
                   "[native-cpp-ivf-residual8-probe-profile] centroid=%zu checks=%u skipped=%u\n",
                   ci, probe_checks, probe_skipped);
    }
    std::fprintf(stderr,
                 "[native-cpp-ivf-residual8-probe-profile] active=%llu checks=%llu skipped=%llu\n",
                 static_cast<unsigned long long>(active),
                 static_cast<unsigned long long>(checks),
                 static_cast<unsigned long long>(skipped));
  }
  if (binary_prefilter_profile_) {
    uint64_t active = 0;
    uint64_t checks = 0;
    uint64_t skipped = 0;
    for (size_t ci = 0; ci < k_; ++ci) {
      const uint32_t probe_checks = profile_.binary_probe_checks[ci];
      if (probe_checks == 0) continue;
      const uint32_t probe_skipped = profile_.binary_probe_skipped[ci];
      ++active;
      checks += probe_checks;
      skipped += probe_skipped;
      std::fprintf(stderr,
                   "[native-cpp-ivf-binary-probe-profile] centroid=%zu checks=%u skipped=%u\n",
                   ci, probe_checks, probe_skipped);
    }
    std::fprintf(stderr,
                 "[native-cpp-ivf-binary-probe-profile] active=%llu checks=%llu skipped=%llu\n",
                 static_cast<unsigned long long>(active),
                 static_cast<unsigned long long>(checks),
                 static_cast<unsigned long long>(skipped));
  }
  std::fprintf(stderr,
               "[native-cpp-ivf-scan-profile] "
               "avg_probe_blocks=%llu avg_bound_skipped=%llu avg_blocks_scanned=%llu "
               "avg_dynamic_low_skipped=%llu avg_dynamic_high_skipped=%llu "
               "quick_bbox_checks=%llu quick_bbox_skipped=%llu "
               "rescore_bbox_checks=%llu rescore_bbox_skipped=%llu "
               "quick_hot_bbox_checks=%llu quick_hot_bbox_skipped=%llu "
               "rescore_hot_bbox_checks=%llu rescore_hot_bbox_skipped=%llu "
               "quick_center_checks=%llu quick_center_skipped=%llu "
               "rescore_center_checks=%llu rescore_center_skipped=%llu "
               "quick_label_skipped=%llu rescore_label_skipped=%llu "
               "quick_residual8_checks=%llu quick_residual8_skipped=%llu "
               "rescore_residual8_checks=%llu rescore_residual8_skipped=%llu "
               "quick_binary_checks=%llu quick_binary_skipped=%llu "
               "rescore_binary_checks=%llu rescore_binary_skipped=%llu "
               "avg_stage1_dead=%llu avg_stage1_alive=%llu avg_final_dead=%llu "
               "avg_stage1_low_dead_halves=%llu avg_stage1_high_dead_halves=%llu "
               "avg_stage1_both_alive_halves=%llu avg_final_lanes=%llu "
               "avg_top5_accepts=%llu empty_probes=%llu "
               "dynamic_low_events=%llu dynamic_high_breaks=%llu "
               "stage1_low_lanes=%llu stage1_high_lanes=%llu totals_probe_blocks=%llu "
               "totals_scanned=%llu totals_bound_skipped=%llu totals_dynamic_skipped=%llu\n",
               static_cast<unsigned long long>(profile_.quick_probe_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_bound_skipped_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_blocks_scanned / calls),
               static_cast<unsigned long long>(profile_.quick_dynamic_low_skipped_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_dynamic_high_skipped_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_bbox_checks),
               static_cast<unsigned long long>(profile_.quick_bbox_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_bbox_checks),
               static_cast<unsigned long long>(profile_.rescore_bbox_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_hot_bbox_checks),
               static_cast<unsigned long long>(profile_.quick_hot_bbox_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_hot_bbox_checks),
               static_cast<unsigned long long>(profile_.rescore_hot_bbox_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_center_checks),
               static_cast<unsigned long long>(profile_.quick_center_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_center_checks),
               static_cast<unsigned long long>(profile_.rescore_center_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_label_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_label_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_residual8_checks),
               static_cast<unsigned long long>(profile_.quick_residual8_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_residual8_checks),
               static_cast<unsigned long long>(profile_.rescore_residual8_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_binary_checks),
               static_cast<unsigned long long>(profile_.quick_binary_skipped_blocks),
               static_cast<unsigned long long>(profile_.rescore_binary_checks),
               static_cast<unsigned long long>(profile_.rescore_binary_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_stage1_dead_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_stage1_alive_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_final_dead_blocks / calls),
               static_cast<unsigned long long>(profile_.quick_stage1_low_dead_halves / calls),
               static_cast<unsigned long long>(profile_.quick_stage1_high_dead_halves / calls),
               static_cast<unsigned long long>(profile_.quick_stage1_both_alive_halves / calls),
               static_cast<unsigned long long>(profile_.quick_final_lanes / calls),
               static_cast<unsigned long long>(profile_.quick_top5_accepts / calls),
               static_cast<unsigned long long>(profile_.quick_bound_empty_probes),
               static_cast<unsigned long long>(profile_.quick_dynamic_low_events),
               static_cast<unsigned long long>(profile_.quick_dynamic_high_breaks),
               static_cast<unsigned long long>(profile_.quick_stage1_low_lanes),
               static_cast<unsigned long long>(profile_.quick_stage1_high_lanes),
               static_cast<unsigned long long>(profile_.quick_probe_blocks),
               static_cast<unsigned long long>(profile_.quick_blocks_scanned),
               static_cast<unsigned long long>(profile_.quick_bound_skipped_blocks),
               static_cast<unsigned long long>(profile_.quick_dynamic_low_skipped_blocks +
                                               profile_.quick_dynamic_high_skipped_blocks));
  if (profile_.centroid_prefilter_chunks > 0 || profile_.centroid_prefilter_scalar_full > 0 ||
      profile_.centroid_prefilter_scalar_skipped > 0) {
    std::fprintf(stderr,
                 "[native-cpp-ivf-centroid-prefilter] chunks=%llu full_chunks=%llu "
                 "skipped_chunks=%llu scalar_full=%llu scalar_skipped=%llu\n",
                 static_cast<unsigned long long>(profile_.centroid_prefilter_chunks),
                 static_cast<unsigned long long>(profile_.centroid_prefilter_full_chunks),
                 static_cast<unsigned long long>(profile_.centroid_prefilter_skipped_chunks),
                 static_cast<unsigned long long>(profile_.centroid_prefilter_scalar_full),
                 static_cast<unsigned long long>(profile_.centroid_prefilter_scalar_skipped));
  }
  if (profile_.local_repair_calls > 0) {
    std::fprintf(stderr,
                 "[native-cpp-ivf-local-repair-profile] calls=%llu candidates=%llu "
                 "accepts=%llu fast_changed=%llu\n",
                 static_cast<unsigned long long>(profile_.local_repair_calls),
                 static_cast<unsigned long long>(profile_.local_repair_candidates),
                 static_cast<unsigned long long>(profile_.local_repair_accepts),
                 static_cast<unsigned long long>(profile_.local_repair_fast_changed));
  }
	  if (profile_.local_fraud_graph_calls > 0) {
	    std::fprintf(stderr,
	                 "[native-cpp-ivf-local-fraud-graph-profile] calls=%llu candidates=%llu "
	                 "accepts=%llu fast_changed=%llu\n",
	                 static_cast<unsigned long long>(profile_.local_fraud_graph_calls),
	                 static_cast<unsigned long long>(profile_.local_fraud_graph_candidates),
	                 static_cast<unsigned long long>(profile_.local_fraud_graph_accepts),
	                 static_cast<unsigned long long>(profile_.local_fraud_graph_fast_changed));
	  }
  if (profile_.exact_kd_calls > 0) {
    const uint64_t calls = profile_.exact_kd_calls;
    std::fprintf(stderr,
                 "[native-exact-kd-profile] calls=%llu avg_ns=%llu max_ns=%llu "
                 "avg_nodes=%llu avg_leaves=%llu avg_slots=%llu\n",
                 static_cast<unsigned long long>(calls),
                 static_cast<unsigned long long>(profile_.exact_kd_ns / calls),
                 static_cast<unsigned long long>(profile_.exact_kd_max_ns),
                 static_cast<unsigned long long>(profile_.exact_kd_nodes / calls),
                 static_cast<unsigned long long>(profile_.exact_kd_leaves / calls),
                 static_cast<unsigned long long>(profile_.exact_kd_slots / calls));
  }
	}

bool NativeIVF::load(const char* path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    error_ = "could not open index";
    return false;
  }

  char magic[4];
  if (!read_exact(in, magic, 4)) {
    error_ = "invalid index magic";
    return false;
  }
  vector_scales_.fill(kVectorScale);
  vector_inv_scales_.fill(kVectorInvScale);
  query_scales_.fill(1.0f);
  query_matrix_.fill(0.0f);
  for (int d = 0; d < kDim; ++d) {
    query_matrix_[static_cast<size_t>(d) * kDim + static_cast<size_t>(d)] = 1.0f;
  }
  rh26_index_ = false;
  if (static_cast<uint8_t>(magic[0]) == 0x0au && magic[1] == 'F' &&
      magic[2] == 'V' && magic[3] == 'I') {
    in.clear();
    in.seekg(0, std::ios::beg);
    return load_rh26_index(in);
  }
  per_dim_scale_index_ = std::memcmp(magic, "IVS1", 4) == 0;
  query_transform_index_ = std::memcmp(magic, "IVD1", 4) == 0;
  query_matrix_index_ = std::memcmp(magic, "IVM1", 4) == 0;
  residual8_half_scales_ = kEnableResidual8Index && std::memcmp(magic, "IVH1", 4) == 0;
  residual8_block_scales_ = residual8_half_scales_ ||
                            (kEnableResidual8Index && std::memcmp(magic, "IVB1", 4) == 0);
  residual8_index_ = residual8_block_scales_ ||
                     (kEnableResidual8Index && std::memcmp(magic, "IVR1", 4) == 0);
  if (!per_dim_scale_index_ && !query_transform_index_ && !query_matrix_index_ &&
      !residual8_index_ &&
      std::memcmp(magic, "IVF1", 4) != 0) {
    error_ = "invalid index magic";
    return false;
  }

  bool ok = false;
  const uint32_t n = read_u32(in, ok);
  (void)n;
  if (!ok) {
    error_ = "could not read n";
    return false;
  }
  k_ = read_u32(in, ok);
  if (!ok || k_ == 0 || k_ > kMaxCentroids) {
    error_ = "invalid k";
    return false;
  }
  const uint32_t dim = read_u32(in, ok);
  if (!ok || dim != kDim) {
    error_ = "invalid dimension";
    return false;
  }

  centroids_.resize(static_cast<size_t>(k_) * kDim);
  if (!read_exact(in, centroids_.data(), centroids_.size())) {
    error_ = "could not read centroids";
    return false;
  }

  if (query_transform_index_) {
    if (!read_exact(in, query_scales_.data(), query_scales_.size())) {
      error_ = "could not read query scales";
      return false;
    }
    for (int d = 0; d < kDim; ++d) {
      const float s = query_scales_[static_cast<size_t>(d)];
      if (!(s > 0.0f) || !std::isfinite(s)) {
        error_ = "invalid query scale";
        return false;
      }
    }
  }

  if (query_matrix_index_) {
    if (!read_exact(in, query_matrix_.data(), query_matrix_.size())) {
      error_ = "could not read query matrix";
      return false;
    }
    for (float v : query_matrix_) {
      if (!std::isfinite(v)) {
        error_ = "invalid query matrix";
        return false;
      }
    }
  }

  if (per_dim_scale_index_) {
    if (!read_exact(in, vector_scales_.data(), vector_scales_.size())) {
      error_ = "could not read vector scales";
      return false;
    }
    for (int d = 0; d < kDim; ++d) {
      const float scale = vector_scales_[static_cast<size_t>(d)];
      if (!(scale > 0.0f) || !std::isfinite(scale)) {
        error_ = "invalid vector scale";
        return false;
      }
      vector_inv_scales_[static_cast<size_t>(d)] = 1.0f / scale;
    }
  }

  if (kEnableResidual8Index && residual8_index_ && !residual8_block_scales_) {
    residual8_scales_.resize(static_cast<size_t>(k_) * kDim);
    if (!read_exact(in, residual8_scales_.data(), residual8_scales_.size())) {
      error_ = "could not read residual8 scales";
      return false;
    }
  } else {
    residual8_scales_.clear();
  }

  offsets_.resize(static_cast<size_t>(k_) + 1);
  if (!read_exact(in, offsets_.data(), offsets_.size())) {
    error_ = "could not read offsets";
    return false;
  }
  total_blocks_ = offsets_[k_];
  const size_t padded_n = static_cast<size_t>(total_blocks_) * kVectorsPerBlock;

  if (residual8_block_scales_) {
    const size_t scale_count = static_cast<size_t>(total_blocks_) * kDim *
                               (residual8_half_scales_ ? 2u : 1u);
    residual8_scales_.resize(scale_count);
    if (!read_exact(in, residual8_scales_.data(), residual8_scales_.size())) {
      error_ = "could not read residual8 block scales";
      return false;
    }
  }

  labels_.resize(padded_n);
  if (!read_exact(in, labels_.data(), labels_.size())) {
    error_ = "could not read labels";
    return false;
  }
  exact_kd_primary_ = env_bool_or("NATIVE_EXACT_KD", false);
  exact_kd_repair_ = env_bool_or("NATIVE_EXACT_KD_REPAIR", false);
  exact_kd_enabled_ = exact_kd_primary_ || exact_kd_repair_;
  exact_kd_leaf_size_ = env_int_clamped("NATIVE_EXACT_KD_LEAF_SIZE", 256, 8, 512);
  exact_kd_leaf_block_simd_ = env_bool_or("NATIVE_EXACT_KD_LEAF_BLOCK_SIMD", false);
  exact_kd_leaf_block_simd_min_lanes_ =
      env_int_clamped("NATIVE_EXACT_KD_LEAF_BLOCK_SIMD_MIN_LANES", 4, 1, kVectorsPerBlock);
  exact_kd_partitioned_ = env_bool_or("NATIVE_EXACT_KD_PARTITIONED", true);
  exact_kd_repair_min_ = env_int_clamped("NATIVE_EXACT_KD_REPAIR_MIN", 1, 0, 5);
  exact_kd_repair_max_ = env_int_clamped("NATIVE_EXACT_KD_REPAIR_MAX", 4, 0, 5);
  if (exact_kd_repair_min_ > exact_kd_repair_max_) {
    exact_kd_repair_min_ = exact_kd_repair_max_;
  }
  exact_kd_repair_policy_.clear();
  exact_kd_repair_policy_enabled_ = false;
  const char* exact_kd_repair_policy_path = std::getenv("NATIVE_EXACT_KD_REPAIR_POLICY");
  if (exact_kd_repair_ && exact_kd_repair_policy_path &&
      exact_kd_repair_policy_path[0] != '\0') {
    if (!load_exact_kd_repair_policy(exact_kd_repair_policy_path)) {
      std::fprintf(stderr,
                   "[native-cpp-ivf-policy] exact KD repair policy empty; "
                   "falling back to count range repair\n");
    }
  }
  const char* exact_kd_partition_mode = std::getenv("NATIVE_EXACT_KD_PARTITION_MODE");
  if (!exact_kd_partition_mode || std::strcmp(exact_kd_partition_mode, "cell") == 0) {
    exact_kd_partition_scheme_ = 2;
    exact_kd_partition_count_ = 8192;
  } else if (std::strcmp(exact_kd_partition_mode, "amt16_dow7") == 0 ||
             std::strcmp(exact_kd_partition_mode, "amount_dow") == 0) {
    exact_kd_partition_scheme_ = 3;
    exact_kd_partition_count_ = 16 * 7;
  } else {
    exact_kd_partition_scheme_ = 1;
    exact_kd_partition_count_ = 256;
  }
  label_skip_ = env_bool_or("NATIVE_IVF_LABEL_SKIP", false);
  label_skip_cached_ = env_bool_or("NATIVE_IVF_LABEL_SKIP_CACHED", false);
  label_skip_cluster_ = env_bool_or("NATIVE_IVF_LABEL_SKIP_CLUSTER", false);
  label_skip_sparse_ = env_bool_or("NATIVE_IVF_LABEL_SKIP_SPARSE", false);
  quick8_label_skip_specialized_ =
      env_bool_or("NATIVE_IVF_QUICK8_LABEL_SKIP_SPECIALIZED", false);
  rescore_label_skip_ = env_bool_or("NATIVE_IVF_RESCORE_LABEL_SKIP", false);
  local_repair_ = env_bool_or("NATIVE_IVF_LOCAL_REPAIR", false);
  local_repair_window_ = env_int_clamped("NATIVE_IVF_LOCAL_REPAIR_WINDOW", 16, 1, 128);
  local_repair_min_fast_ = env_int_clamped("NATIVE_IVF_LOCAL_REPAIR_MIN_FAST", 2, 0, 5);
  local_repair_max_fast_ = env_int_clamped("NATIVE_IVF_LOCAL_REPAIR_MAX_FAST", 5, 0, 5);
  if (local_repair_min_fast_ > local_repair_max_fast_) {
    local_repair_min_fast_ = local_repair_max_fast_;
  }
  local_fraud_graph_enabled_ = env_bool_or("NATIVE_IVF_LOCAL_FRAUD_GRAPH", false);
  local_fraud_graph_min_fast_ =
      env_int_clamped("NATIVE_IVF_LOCAL_FRAUD_GRAPH_MIN_FAST", 0, 0, 5);
  local_fraud_graph_max_fast_ =
      env_int_clamped("NATIVE_IVF_LOCAL_FRAUD_GRAPH_MAX_FAST", 5, 0, 5);
  if (local_fraud_graph_min_fast_ > local_fraud_graph_max_fast_) {
    local_fraud_graph_min_fast_ = local_fraud_graph_max_fast_;
  }
  quick_periodic_upper_bound_ = env_bool_or("NATIVE_IVF_QUICK_PERIODIC_UPPER_BOUND", false);
  quick_periodic_upper_step_ =
      env_int_clamped("NATIVE_IVF_QUICK_PERIODIC_UPPER_STEP", 8, 1, 64);
  label_skip_max_worst_ =
      env_float_clamped("NATIVE_IVF_LABEL_SKIP_MAX_WORST", 0.0f, 0.0f, 100.0f);
  prefetch_blocks_ = env_bool_or("NATIVE_IVF_PREFETCH_BLOCKS", false);
  if (label_skip_) {
    block_label_masks_.resize(static_cast<size_t>(total_blocks_));
    for (uint32_t block = 0; block < total_blocks_; ++block) {
      const size_t label_base = static_cast<size_t>(block) * kVectorsPerBlock;
      uint16_t mask = 0;
      for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
        if (fraud_label(labels_[label_base + static_cast<size_t>(lane)]) != 0) {
          mask = static_cast<uint16_t>(mask | (uint16_t{1} << lane));
        }
      }
      block_label_masks_[static_cast<size_t>(block)] = mask;
    }
    cluster_label_pure_.resize(k_);
    for (uint32_t ci = 0; ci < k_; ++ci) {
      const uint32_t start = offsets_[static_cast<size_t>(ci)];
      const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
      int8_t pure = -1;
      for (uint32_t block = start; block < end; ++block) {
        const uint16_t mask = block_label_masks_[static_cast<size_t>(block)];
        const int8_t block_pure = mask == 0 ? 0 : (mask == 0xffffu ? 1 : -1);
        if (block_pure < 0 || (pure >= 0 && pure != block_pure)) {
          pure = -1;
          break;
        }
        pure = block_pure;
      }
      cluster_label_pure_[static_cast<size_t>(ci)] = pure;
    }
    if (label_skip_sparse_) {
      label_skip_nonzero_offsets_.assign(static_cast<size_t>(k_) + 1, 0);
      label_skip_notfull_offsets_.assign(static_cast<size_t>(k_) + 1, 0);
      label_skip_nonzero_blocks_.clear();
      label_skip_notfull_blocks_.clear();
      label_skip_nonzero_blocks_.reserve(static_cast<size_t>(total_blocks_));
      label_skip_notfull_blocks_.reserve(static_cast<size_t>(total_blocks_));
      for (uint32_t ci = 0; ci < k_; ++ci) {
        label_skip_nonzero_offsets_[static_cast<size_t>(ci)] =
            static_cast<uint32_t>(label_skip_nonzero_blocks_.size());
        label_skip_notfull_offsets_[static_cast<size_t>(ci)] =
            static_cast<uint32_t>(label_skip_notfull_blocks_.size());
        const uint32_t start = offsets_[static_cast<size_t>(ci)];
        const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
        for (uint32_t block = start; block < end; ++block) {
          const uint16_t mask = block_label_masks_[static_cast<size_t>(block)];
          if (mask != 0) {
            label_skip_nonzero_blocks_.push_back(block);
          }
          if (mask != 0xffffu) {
            label_skip_notfull_blocks_.push_back(block);
          }
        }
      }
      label_skip_nonzero_offsets_[static_cast<size_t>(k_)] =
          static_cast<uint32_t>(label_skip_nonzero_blocks_.size());
      label_skip_notfull_offsets_[static_cast<size_t>(k_)] =
          static_cast<uint32_t>(label_skip_notfull_blocks_.size());
    } else {
      label_skip_nonzero_offsets_.clear();
      label_skip_nonzero_blocks_.clear();
      label_skip_notfull_offsets_.clear();
      label_skip_notfull_blocks_.clear();
    }
  } else {
    block_label_masks_.clear();
    cluster_label_pure_.clear();
    label_skip_nonzero_offsets_.clear();
    label_skip_nonzero_blocks_.clear();
    label_skip_notfull_offsets_.clear();
    label_skip_notfull_blocks_.clear();
  }

  if (kEnableResidual8Index && residual8_index_) {
    residual8_blocks_.resize(static_cast<size_t>(total_blocks_) * kBlockStride);
    if (!read_exact(in, residual8_blocks_.data(), residual8_blocks_.size())) {
      error_ = "could not read residual8 blocks";
      return false;
    }
    blocks_.clear();
	  } else {
	    blocks_.resize(static_cast<size_t>(total_blocks_) * kBlockStride);
	    if (!read_exact(in, blocks_.data(), blocks_.size())) {
	      error_ = "could not read blocks";
	      return false;
	    }
	    residual8_blocks_.clear();
	  }

  if (exact_kd_enabled_) {
    if (blocks_.empty()) {
      std::fprintf(stderr, "[native-exact-kd] disabled: quantized blocks are not available\n");
      exact_kd_enabled_ = false;
      exact_kd_primary_ = false;
      exact_kd_repair_ = false;
    } else {
      const uint64_t build_start = now_ns();
      if (!build_exact_kd()) {
        std::fprintf(stderr, "[native-exact-kd] disabled: failed to build tree\n");
        exact_kd_enabled_ = false;
        exact_kd_primary_ = false;
        exact_kd_repair_ = false;
      } else {
        std::fprintf(stderr,
                     "[native-exact-kd] built usage=%s%s mode=%s slots=%zu nodes=%zu "
                     "partitions=%zu scheme=%s leaf_size=%d repair=[%d,%d] elapsed=%.3fms\n",
                     exact_kd_primary_ ? "primary" : "",
                     exact_kd_repair_ ? "+repair" : "",
                     exact_kd_partitioned_ ? "partitioned" : "global",
                     exact_kd_slots_.size(), exact_kd_nodes_.size(),
                     exact_kd_populated_partitions_.size(),
                     exact_kd_partition_scheme_ == 2
                         ? "cell"
                         : (exact_kd_partition_scheme_ == 3 ? "amt16_dow7" : "pair"),
                     exact_kd_leaf_size_, exact_kd_repair_min_, exact_kd_repair_max_,
                     static_cast<double>(now_ns() - build_start) / 1000000.0);
      }
    }
  } else {
    exact_kd_slots_.clear();
    exact_kd_nodes_.clear();
    exact_kd_partition_roots_.fill(-1);
    exact_kd_partition_counts_.fill(0);
    exact_kd_populated_partitions_.clear();
  }

	  if (local_fraud_graph_enabled_) {
    const char* graph_path = std::getenv("NATIVE_IVF_LOCAL_FRAUD_GRAPH_PATH");
    if (!load_local_fraud_graph(graph_path && graph_path[0] != '\0'
                                    ? graph_path
                                    : "service/local_fraud_graph.bin")) {
      local_fraud_graph_enabled_ = false;
    }
  } else {
    local_fraud_graph_.clear();
  }

  const bool residual8_prefilter_requested =
      env_bool_or("NATIVE_IVF_RESIDUAL8_PREFILTER", false);
  residual8_hybrid_repair_ =
      env_bool_or("NATIVE_IVF_RESIDUAL8_HYBRID_REPAIR", false) && !residual8_index_;
  residual8_hybrid_candidates_ =
      env_int_clamped("NATIVE_IVF_RESIDUAL8_HYBRID_CANDIDATES", 128, 5, 512);
  residual8_hybrid_min_probe_ =
      env_int_clamped("NATIVE_IVF_RESIDUAL8_HYBRID_MIN_PROBE", 0, 0, 512);
  residual8_prefilter_ =
      (residual8_prefilter_requested || residual8_hybrid_repair_) && !residual8_index_;
  residual8_prefilter_dims_ =
      env_int_clamped("NATIVE_IVF_RESIDUAL8_PREFILTER_DIMS", 6, 1, 6);
  residual8_filter_dims_ = residual8_hybrid_repair_ ? kDim : residual8_prefilter_dims_;
  residual8_prefilter_slack_ =
      env_float_clamped("NATIVE_IVF_RESIDUAL8_PREFILTER_SLACK", 2.0f, 1.0f, 100.0f);
  residual8_prefilter_quick_label_ =
      residual8_prefilter_requested && residual8_prefilter_ &&
      env_bool_or("NATIVE_IVF_RESIDUAL8_PREFILTER_QUICK_LABEL", false);
  residual8_prefilter_rescore_ =
      residual8_prefilter_requested && residual8_prefilter_ &&
      env_bool_or("NATIVE_IVF_RESIDUAL8_PREFILTER_RESCORE", false);
  residual8_prefilter_profile_ =
      residual8_prefilter_ && env_bool_or("NATIVE_IVF_RESIDUAL8_PREFILTER_PROFILE", false);
  residual8_prefilter_policy_.fill(0);
  residual8_prefilter_policy_enabled_ = false;
  if (residual8_prefilter_) {
    build_residual8_prefilter();
    if (residual8_filter_blocks_.empty() || residual8_filter_dims_ < kDim) {
      residual8_hybrid_repair_ = false;
    }
    const char* residual_policy_path = std::getenv("NATIVE_IVF_RESIDUAL8_PREFILTER_POLICY");
    if (residual8_prefilter_requested && residual_policy_path && residual_policy_path[0] != '\0') {
      residual8_prefilter_policy_enabled_ = true;
      if (!load_residual8_prefilter_policy(residual_policy_path)) {
        std::fprintf(stderr,
                     "[native-cpp-ivf-policy] residual8 prefilter policy empty; "
                     "residual8 prefilter will be disabled by policy\n");
      }
    }
  } else {
    residual8_filter_blocks_.clear();
    residual8_filter_scales_.clear();
    residual8_prefilter_quick_label_ = false;
    residual8_prefilter_rescore_ = false;
    residual8_prefilter_profile_ = false;
  }

  binary_prefilter_ =
      env_bool_or("NATIVE_IVF_BINARY_PREFILTER", false) && !residual8_index_;
  binary_prefilter_threshold_ =
      env_int_clamped("NATIVE_IVF_BINARY_PREFILTER_THRESHOLD", 4, 0, kDim);
  binary_prefilter_profile_ =
      binary_prefilter_ && env_bool_or("NATIVE_IVF_BINARY_PREFILTER_PROFILE", false);
  binary_prefilter_policy_.fill(0);
  binary_prefilter_policy_enabled_ = false;
  if (binary_prefilter_) {
    build_binary_signatures();
    const char* binary_policy_path = std::getenv("NATIVE_IVF_BINARY_PREFILTER_POLICY");
    if (binary_policy_path && binary_policy_path[0] != '\0') {
      binary_prefilter_policy_enabled_ = true;
      if (!load_binary_prefilter_policy(binary_policy_path)) {
        std::fprintf(stderr,
                     "[native-cpp-ivf-policy] binary prefilter policy empty; "
                     "binary prefilter will be disabled by policy\n");
      }
    }
  } else {
    binary_signatures_.clear();
    binary_prefilter_profile_ = false;
  }

  const char* bounds_env = std::getenv("NATIVE_IVF_BLOCK_BOUNDS");
  use_block_bounds_ = !(bounds_env && std::strcmp(bounds_env, "0") == 0);
  const bool bbox_default = env_bool_or("NATIVE_IVF_BLOCK_BBOX", false);
  quick_block_bbox_ = env_bool_or("NATIVE_IVF_QUICK_BBOX", bbox_default);
  rescore_block_bbox_ = env_bool_or("NATIVE_IVF_RESCORE_BBOX", bbox_default);
  use_block_bbox_ = quick_block_bbox_ || rescore_block_bbox_;
  bbox_dims_ = env_int_clamped("NATIVE_IVF_BBOX_DIMS", kBBoxStride, 8, kBBoxStride);
  bbox_dims_ = bbox_dims_ <= 8 ? 8 : kBBoxStride;
  const bool hot_bbox_default = env_bool_or("NATIVE_IVF_HOT_BBOX", false);
  quick_hot_bbox_ = env_bool_or("NATIVE_IVF_QUICK_HOT_BBOX", hot_bbox_default);
  rescore_hot_bbox_ = env_bool_or("NATIVE_IVF_RESCORE_HOT_BBOX", hot_bbox_default);
  use_hot_bbox_ = quick_hot_bbox_ || rescore_hot_bbox_;
  if (kEnableResidual8Index && residual8_index_ && use_hot_bbox_) {
    quick_hot_bbox_ = false;
    rescore_hot_bbox_ = false;
    use_hot_bbox_ = false;
  }
  hot_bbox_dims_ = env_int_clamped("NATIVE_IVF_HOT_BBOX_DIMS", 6, 1, 6);
  const bool center_bound_default = env_bool_or("NATIVE_IVF_BLOCK_CENTER_BOUND", false);
  quick_center_bound_ = env_bool_or("NATIVE_IVF_QUICK_CENTER_BOUND", center_bound_default);
  rescore_center_bound_ = env_bool_or("NATIVE_IVF_RESCORE_CENTER_BOUND", center_bound_default);
  use_center_bound_ = quick_center_bound_ || rescore_center_bound_;
  center_bound_dims_ = env_int_clamped("NATIVE_IVF_BLOCK_CENTER_DIMS", 6, 1, kHotBBoxStride);
  center_bound_min_from_ = env_int_clamped("NATIVE_IVF_CENTER_BOUND_MIN_FROM", 28, 0, kMaxProbe);
  const char* radius_env = std::getenv("NATIVE_IVF_RADIUS_SCAN");
  quick_radius_order_ = radius_env && std::strcmp(radius_env, "1") == 0;
  const char* dynamic_env = std::getenv("NATIVE_IVF_DYNAMIC_BOUNDS");
  quick_dynamic_bounds_ = !(dynamic_env && std::strcmp(dynamic_env, "0") == 0);
  scan_stage8_ = env_bool_or("NATIVE_IVF_SCAN_STAGE8", false);
  quick8_specialized_ = env_bool_or("NATIVE_IVF_QUICK8_SPECIALIZED", false);
  const char* dim_order_env = std::getenv("NATIVE_IVF_DIM_ORDER");
  dim_hot6_order_ = dim_order_env && std::strcmp(dim_order_env, "hot6") == 0;
  active_dims_ = env_int_clamped("NATIVE_IVF_ACTIVE_DIMS", kDim, 1, kDim);
  const char* trace_env = std::getenv("NATIVE_IVF_TRACE");
	  trace_enabled_ = trace_env && std::strcmp(trace_env, "1") == 0;
	  profile_actual_scan_ = env_bool_or("NATIVE_IVF_PROFILE_ACTUAL_SCAN", false);
  margin_profile_enabled_ = env_bool_or("NATIVE_IVF_MARGIN_PROFILE", false);
  centroid_flip_profile_enabled_ = env_bool_or("NATIVE_IVF_CENTROID_FLIP_PROFILE", false);
  centroid_flip_dump_ = env_bool_or("NATIVE_IVF_CENTROID_FLIP_DUMP", false);
  centroid_rescore_skip_ = env_bool_or("NATIVE_IVF_CENTROID_RESCORE_SKIP", false);
  rescore_tree_enabled_ = env_bool_or("NATIVE_IVF_RESCUE_TREE", false);
  rescore_disabled_ = env_bool_or("NATIVE_IVF_RESCUE_DISABLE", false);
  fast3_approve_rule_ = env_bool_or("NATIVE_IVF_FAST3_APPROVE_RULE", false);
  fast3_approve_max_worst_ =
      env_float_clamped("NATIVE_IVF_FAST3_APPROVE_MAX_WORST", 0.03405f, 0.0f, 1.0f);
  fast3_approve_max_margin_ =
      env_float_clamped("NATIVE_IVF_FAST3_APPROVE_MAX_MARGIN", -0.03081f, -1.0f, 1.0f);
  centroid_rescore_skip_policy_.fill(CentroidRescoreSkipRule{});
  if (centroid_rescore_skip_) {
    const char* policy_path = std::getenv("NATIVE_IVF_CENTROID_RESCORE_SKIP_POLICY");
    if (!load_centroid_rescore_skip_policy(policy_path)) {
      centroid_rescore_skip_ = false;
    }
  }
  trace_calls_ = 0;
  const char* rescore_quantized_env = std::getenv("NATIVE_IVF_RESCORE_QUANTIZED");
  rescore_quantized_ =
      !(rescore_quantized_env && std::strcmp(rescore_quantized_env, "0") == 0);
  const char* rescore_bounds_env = std::getenv("NATIVE_IVF_RESCORE_BOUNDS");
  rescore_bounds_ = rescore_bounds_env && std::strcmp(rescore_bounds_env, "1") == 0;
  rescore_specialized_ = env_bool_or("NATIVE_IVF_RESCORE_SPECIALIZED", false);
  scan_scaled_ = env_bool_or("NATIVE_IVF_SCAN_SCALED", false);
  rescore_scaled_ = env_bool_or("NATIVE_IVF_RESCORE_SCALED", false);
  if (per_dim_scale_index_) {
    scan_scaled_ = false;
    rescore_scaled_ = false;
  }
  rescore_radius_probes_ = env_bool_or("NATIVE_IVF_RESCORE_RADIUS_PROBES", false);
  rescore_tail_select_ = env_bool_or("NATIVE_IVF_RESCORE_TAIL_SELECT", false);
  rescore_prefilter_select_ = env_bool_or("NATIVE_IVF_RESCORE_PREFILTER_SELECT", false);
  rescore_early_unambig_ = env_bool_or("NATIVE_IVF_RESCORE_EARLY_UNAMBIG", false);
  rescore_early_primary_ = env_bool_or("NATIVE_IVF_RESCORE_EARLY_PRIMARY", true);
  rescore_order_lower_bound_ =
      env_bool_or("NATIVE_IVF_RESCORE_ORDER_LOWER_BOUND", false);
  rescore_block_order_ = env_bool_or("NATIVE_IVF_RESCORE_BLOCK_ORDER", false);
  rescore_radius_order_ = env_bool_or("NATIVE_IVF_RESCORE_RADIUS_ORDER", false);
  fused_top_select_ = env_bool_or("NATIVE_IVF_FUSED_TOP_SELECT", false);
  centroid_graph_rescore_ = env_bool_or("NATIVE_IVF_CENTROID_GRAPH", false);
  centroid_graph_neighbors_ =
      env_int_clamped("NATIVE_IVF_CENTROID_GRAPH_NEIGHBORS", 16, 4, 64);
  if (centroid_graph_neighbors_ >= static_cast<int>(k_)) {
    centroid_graph_neighbors_ = static_cast<int>(k_) - 1;
  }
  if (centroid_graph_neighbors_ < 1) centroid_graph_neighbors_ = 1;
  centroid_graph_seed_count_ =
      env_int_clamped("NATIVE_IVF_CENTROID_GRAPH_SEEDS", kQuickProbe, 1, kQuickProbe);
  centroid_graph_max_probe_ =
      env_int_clamped("NATIVE_IVF_CENTROID_GRAPH_MAX_PROBE", 28, kQuickProbe, kMaxProbe);
  quick_probe_ = env_int_clamped("NATIVE_IVF_QUICK_PROBE", kQuickProbe, 1, kMaxProbe);
  query_borderline_quick_select_ =
      env_bool_or("NATIVE_IVF_QUERY_BORDERLINE_QUICK_SELECT", false);
  query_obvious_quick_probe_ =
      env_int_clamped("NATIVE_IVF_QUERY_OBVIOUS_QUICK_PROBE", 1, 1, kMaxProbe);
  query_borderline_quick_probe_ =
      env_int_clamped("NATIVE_IVF_QUERY_BORDERLINE_QUICK_PROBE", quick_probe_, 1, kMaxProbe);
  expanded_probe_ =
      env_int_clamped("NATIVE_IVF_EXPANDED_PROBE", kExpandedProbe, quick_probe_, kMaxProbe);
  if (expanded_probe_ < quick_probe_) expanded_probe_ = quick_probe_;
  if (query_obvious_quick_probe_ > expanded_probe_) {
    query_obvious_quick_probe_ = expanded_probe_;
  }
  if (query_borderline_quick_probe_ > expanded_probe_) {
    query_borderline_quick_probe_ = expanded_probe_;
  }
  classsplit_select_ = env_bool_or("NATIVE_IVF_CLASSSPLIT_SELECT", false);
  classsplit_split_ = env_int_clamped("NATIVE_IVF_CLASSSPLIT_SPLIT",
                                      static_cast<int>(k_ / 2), 1,
                                      std::max(1, static_cast<int>(k_) - 1));
  classsplit_quick_fraud_probes_ =
      env_int_clamped("NATIVE_IVF_CLASSSPLIT_QUICK_FRAUD_PROBES",
                      std::max(1, quick_probe_ / 2), 0, quick_probe_);
  classsplit_rescore_fraud_pct_ =
      env_int_clamped("NATIVE_IVF_CLASSSPLIT_RESCORE_FRAUD_PCT", 50, 0, 100);
  rescore5_probe_ = env_int_clamped("NATIVE_IVF_RESCUE5_PROBE", 0, 0, kMaxProbe);
  if (rescore5_probe_ > 0) {
    if (rescore5_probe_ < quick_probe_) rescore5_probe_ = quick_probe_;
    if (rescore5_probe_ > expanded_probe_) rescore5_probe_ = expanded_probe_;
  }
  rescore234_two_phase_ = env_bool_or("NATIVE_IVF_RESCUE234_TWO_PHASE", false);
  rescore234_probe_ = env_int_clamped("NATIVE_IVF_RESCUE234_PROBE", 0, 0, kMaxProbe);
  if (rescore234_probe_ > 0) {
    if (rescore234_probe_ < quick_probe_) rescore234_probe_ = quick_probe_;
    if (rescore234_probe_ > expanded_probe_) rescore234_probe_ = expanded_probe_;
  }
  rescore234_second_min_ = env_int_clamped("NATIVE_IVF_RESCUE234_SECOND_MIN", 2, 0, 5);
  rescore234_second_max_ = env_int_clamped("NATIVE_IVF_RESCUE234_SECOND_MAX", 3, 0, 5);
  if (rescore234_second_min_ > rescore234_second_max_) {
    std::swap(rescore234_second_min_, rescore234_second_max_);
  }
  remaining_repair_ = env_bool_or("NATIVE_IVF_REMAINING_REPAIR", false);
  remaining_repair_bbox_ =
      remaining_repair_ && env_bool_or("NATIVE_IVF_REMAINING_REPAIR_BBOX", false);
  remaining_repair_min_ = env_int_clamped("NATIVE_IVF_REMAINING_REPAIR_MIN", 1, 0, 5);
  remaining_repair_max_ = env_int_clamped("NATIVE_IVF_REMAINING_REPAIR_MAX", 4, 0, 5);
  if (remaining_repair_min_ > remaining_repair_max_) {
    std::swap(remaining_repair_min_, remaining_repair_max_);
  }
  remaining_repair_candidates_ =
      env_int_clamped("NATIVE_IVF_REMAINING_REPAIR_CANDIDATES", 128, 1,
                      kMaxRepairCandidates);
  reference_borderline_repair_ =
      remaining_repair_ && env_bool_or("NATIVE_IVF_REFERENCE_BORDERLINE_REPAIR", false);
  second_chance_probe_ = env_int_clamped("NATIVE_IVF_SECOND_CHANCE_PROBE", 0, 0, kMaxProbe);
  if (second_chance_probe_ <= quick_probe_) second_chance_probe_ = 0;
  rescore_min_ = env_int_clamped("NATIVE_IVF_RESCUE_MIN", 2, 0, 5);
  rescore_max_ = env_int_clamped("NATIVE_IVF_RESCUE_MAX", 3, 0, 5);
  if (rescore_min_ > rescore_max_) std::swap(rescore_min_, rescore_max_);
  rescore_early_min_ =
      env_int_clamped("NATIVE_IVF_RESCORE_EARLY_MIN", rescore_min_, 0, 5);
  rescore_early_max_ =
      env_int_clamped("NATIVE_IVF_RESCORE_EARLY_MAX", rescore_max_, 0, 5);
  if (rescore_early_min_ > rescore_early_max_) {
    std::swap(rescore_early_min_, rescore_early_max_);
  }
  rescore_early_min_probe_ =
      env_int_clamped("NATIVE_IVF_RESCORE_EARLY_MIN_PROBE", 1, 1, kMaxProbe);
  second_chance_min_ = env_int_clamped("NATIVE_IVF_SECOND_CHANCE_MIN", 3, 0, 5);
  second_chance_max_ = env_int_clamped("NATIVE_IVF_SECOND_CHANCE_MAX", 3, 0, 5);
  if (second_chance_min_ > second_chance_max_) std::swap(second_chance_min_, second_chance_max_);
  second_chance_min_worst_.fill(0.0f);
  second_chance_max_worst_.fill(0.0f);
  second_chance_min_worst_[3] =
      env_float_clamped("NATIVE_IVF_SECOND_CHANCE3_MIN_WORST", 0.0f, 0.0f, 100.0f);
  second_chance_min_worst_[4] =
      env_float_clamped("NATIVE_IVF_SECOND_CHANCE4_MIN_WORST", 0.0f, 0.0f, 100.0f);
  second_chance_max_worst_[3] =
      env_float_clamped("NATIVE_IVF_SECOND_CHANCE3_MAX_WORST", 0.0f, 0.0f, 100.0f);
  second_chance_max_worst_[4] =
      env_float_clamped("NATIVE_IVF_SECOND_CHANCE4_MAX_WORST", 0.0f, 0.0f, 100.0f);
  rescore_skip_worst_.fill(0.0f);
  rescore_skip_margin_.fill(0.0f);
  rescore_skip_worst_[2] =
      env_float_clamped("NATIVE_IVF_RESCUE2_MIN_WORST", 0.0f, 0.0f, 100.0f);
  rescore_skip_worst_[3] =
      env_float_clamped("NATIVE_IVF_RESCUE3_MIN_WORST", 0.0f, 0.0f, 100.0f);
  rescore_skip_worst_[4] =
      env_float_clamped("NATIVE_IVF_RESCUE4_MIN_WORST", 0.0f, 0.0f, 100.0f);
  rescore_skip_worst_[5] =
      env_float_clamped("NATIVE_IVF_RESCUE5_MIN_WORST", 0.0f, 0.0f, 100.0f);
  rescore_skip_margin_[2] =
      env_float_clamped("NATIVE_IVF_RESCUE2_MIN_MARGIN", 0.0f, 0.0f, 100.0f);
  rescore_skip_margin_[3] =
      env_float_clamped("NATIVE_IVF_RESCUE3_MIN_MARGIN", 0.0f, 0.0f, 100.0f);
  rescore_skip_margin_[4] =
      env_float_clamped("NATIVE_IVF_RESCUE4_MIN_MARGIN", 0.0f, 0.0f, 100.0f);
  rescore5_min_centroid_gap_ =
      env_float_clamped("NATIVE_IVF_RESCUE5_MIN_CENTROID_GAP", 0.0f, 0.0f, 100.0f);
  rescore5_min_last_centroid_gap_ =
      env_float_clamped("NATIVE_IVF_RESCUE5_MIN_LAST_CENTROID_GAP", 0.0f, 0.0f, 100.0f);
  rescore5_max_centroid_gap_ =
      env_float_clamped("NATIVE_IVF_RESCUE5_MAX_CENTROID_GAP", 0.0f, 0.0f, 100.0f);
  rescore5_max_last_centroid_gap_ =
      env_float_clamped("NATIVE_IVF_RESCUE5_MAX_LAST_CENTROID_GAP", 0.0f, 0.0f, 100.0f);
  rescore5_max_best_ =
      env_float_clamped("NATIVE_IVF_RESCUE5_MAX_BEST", 0.0f, 0.0f, 100.0f);
  centroid_dot_distance_ = env_bool_or("NATIVE_IVF_CENTROID_DOT", false);
  centroid_prefilter_ = env_bool_or("NATIVE_IVF_CENTROID_PREFILTER", false);
  centroid_prefilter_sparse_lanes_ =
      env_bool_or("NATIVE_IVF_CENTROID_PREFILTER_SPARSE_LANES", false);
  centroid_prefilter_top_store_only_ =
      env_bool_or("NATIVE_IVF_CENTROID_PREFILTER_TOP_STORE_ONLY", false);
  centroid_prefilter_two_pass_ =
      env_bool_or("NATIVE_IVF_CENTROID_PREFILTER_TWO_PASS", false);
  centroid_jl_prefilter_ = env_bool_or("NATIVE_IVF_CENTROID_JL_PREFILTER", false);
  centroid_jl_dims_ =
      env_int_clamped("NATIVE_IVF_CENTROID_JL_DIMS", 6, 2, kJLProjectionMaxDims);
  centroid_prefilter_dims_ = env_int_clamped("NATIVE_IVF_CENTROID_PREFILTER_DIMS", 6, 1, kDim);
  int centroid_prefilter_min = kQuickProbe;
  if (quick_probe_ > centroid_prefilter_min) centroid_prefilter_min = quick_probe_;
  centroid_prefilter_probe_ = env_int_clamped("NATIVE_IVF_CENTROID_PREFILTER_TARGET",
                                             centroid_prefilter_min, centroid_prefilter_min,
                                             kMaxProbe);
  if (centroid_prefilter_probe_ > static_cast<int>(k_)) {
    centroid_prefilter_probe_ = static_cast<int>(k_);
  }
  centroid_jl_candidates_ =
      env_int_clamped("NATIVE_IVF_CENTROID_JL_CANDIDATES", 512, centroid_prefilter_probe_,
                      kJLCandidateMax);
  if (centroid_jl_candidates_ > static_cast<int>(k_)) {
    centroid_jl_candidates_ = static_cast<int>(k_);
  }
  if (rescore_radius_probes_) {
    centroid_prefilter_ = false;
    centroid_jl_prefilter_ = false;
  }
  if (centroid_dot_distance_) {
    build_centroid_norms();
  }
  if (centroid_jl_prefilter_) {
    build_centroid_projection();
  }
  if (centroid_graph_rescore_) {
    build_centroid_neighbors();
  }
  if (use_block_bounds_ || use_block_bbox_ || use_hot_bbox_ || use_center_bound_ ||
      remaining_repair_bbox_) {
    build_block_radii();
  }

  IndexResidencyConfig residency_config{};
  residency_config.madvise_index = env_bool_or("NATIVE_IVF_MADVISE_INDEX", false);
  residency_config.madvise_hugepage =
      env_bool_or("NATIVE_IVF_MADVISE_HUGEPAGE", residency_config.madvise_index);
  residency_config.mlock_index = env_bool_or("NATIVE_IVF_MLOCK_INDEX", false);
  residency_config.touch_index = env_bool_or("NATIVE_IVF_TOUCH_INDEX", false);
  if (residency_config.madvise_index || residency_config.mlock_index ||
      residency_config.touch_index) {
    IndexResidencyStats residency_stats{};
    apply_index_residency_vector("centroids", centroids_, residency_config, residency_stats);
    apply_index_residency_vector("centroid_norms", centroid_norms_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("offsets", offsets_, residency_config, residency_stats);
    apply_index_residency_vector("labels", labels_, residency_config, residency_stats);
    apply_index_residency_vector("blocks", blocks_, residency_config, residency_stats);
    apply_index_residency_vector("block_label_masks", block_label_masks_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("cluster_label_pure", cluster_label_pure_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("label_skip_nonzero_offsets", label_skip_nonzero_offsets_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("label_skip_nonzero_blocks", label_skip_nonzero_blocks_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("label_skip_notfull_offsets", label_skip_notfull_offsets_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("label_skip_notfull_blocks", label_skip_notfull_blocks_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("residual8_blocks", residual8_blocks_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("residual8_scales", residual8_scales_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("residual8_filter_blocks", residual8_filter_blocks_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("residual8_filter_scales", residual8_filter_scales_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("binary_signatures", binary_signatures_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_min_radii", block_min_radii_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_max_radii", block_max_radii_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("cluster_max_radii", cluster_max_radii_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("cluster_bbox_min", cluster_bbox_min_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("cluster_bbox_max", cluster_bbox_max_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_bbox_min", block_bbox_min_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_bbox_max", block_bbox_max_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_hot_min", block_hot_min_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_hot_max", block_hot_max_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_center", block_center_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("block_center_radius", block_center_radius_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("centroid_neighbors", centroid_neighbors_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("local_fraud_graph", local_fraud_graph_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("centroid_jl", centroid_jl_, residency_config, residency_stats);
    std::fprintf(stderr,
                 "[native-cpp-ivf-residency] regions=%u bytes=%llu madvise=%d "
                 "hugepage=%d touch=%d mlock=%d madvise_failures=%u mlock_failures=%u\n",
                 residency_stats.regions,
                 static_cast<unsigned long long>(residency_stats.bytes),
                 residency_config.madvise_index ? 1 : 0,
                 residency_config.madvise_hugepage ? 1 : 0,
                 residency_config.touch_index ? 1 : 0,
                 residency_config.mlock_index ? 1 : 0,
                 residency_stats.madvise_failures,
                 residency_stats.mlock_failures);
  }

  return true;
}

bool NativeIVF::load_rh26_index(std::ifstream& in) {
  RH26Header hdr{};
  if (!read_exact(in, &hdr, 1)) {
    error_ = "could not read RH26 header";
    return false;
  }
  if (hdr.magic != kRH26Magic || hdr.version != 1 ||
      hdr.block_size != kRH26VectorsPerBlock || hdr.dims != kDim ||
      hdr.k == 0 || hdr.k > kMaxCentroids || hdr.total_blocks == 0) {
    error_ = "invalid RH26 header";
    return false;
  }

  rh26_index_ = true;
  k_ = hdr.k;
  total_blocks_ = hdr.total_blocks;
  rh26_groups_ = (k_ + 7u) / 8u;
  rh26_nprobe_ = env_int_clamped("NATIVE_RH26_NPROBE", 1, 1, 256);
  rh26_initial_nprobe_ =
      env_int_clamped("NATIVE_RH26_INITIAL_NPROBE", 0, 0, 256);
  rh26_border_nprobe_ = 0;
  rh26_repair_min_ = env_int_clamped("NATIVE_RH26_REPAIR_MIN", 1, 0, 5);
  rh26_repair_max_ = env_int_clamped("NATIVE_RH26_REPAIR_MAX", 4, 0, 5);
  if (rh26_repair_min_ > rh26_repair_max_) {
    std::swap(rh26_repair_min_, rh26_repair_max_);
  }
  rh26_repair_candidates_ =
      env_int_clamped("NATIVE_RH26_REPAIR_CANDIDATES", 1024, 1, kMaxRepairCandidates);
  rh26_prefetch_distance_ =
      env_int_clamped("NATIVE_RH26_PREFETCH_DISTANCE", 2, 0, 8);
  rh26_expand_nprobe_ =
      env_int_clamped("NATIVE_RH26_EXPAND_NPROBE", 0, 0, 256);
  rh26_repair_select_min_ =
      env_bool_or("NATIVE_RH26_REPAIR_SELECT_MIN", false);
  rh26_defer_full_top_ =
      env_bool_or("NATIVE_RH26_DEFER_FULL_TOP", false);
  rh26_top_pairs_ =
      env_int_clamped("NATIVE_RH26_TOP_PAIRS", kRH26Pairs, 1, kRH26Pairs);
  rh26_top_candidates_ =
      env_int_clamped("NATIVE_RH26_TOP_CANDIDATES", 0, 0, 256);
  rh26_top_pair_mask_ =
      static_cast<uint32_t>(env_int_clamped("NATIVE_RH26_TOP_PAIR_MASK", 0, 0,
                                            (1 << kRH26Pairs) - 1));
  rh26_expand_policy_.fill(0);
  uint64_t rh26_expand_policy_loaded = 0;
  uint64_t rh26_expand_policy_ignored = 0;
  if (rh26_expand_nprobe_ > 0) {
    const char* expand_keys = std::getenv("NATIVE_RH26_EXPAND_KEYS");
    const char* p = expand_keys ? expand_keys : "";
    while (*p != '\0') {
      p = skip_policy_separators(p);
      if (*p == '\0') break;
      char* end = nullptr;
      const long fast = std::strtol(p, &end, 10);
      if (end == p) {
        ++rh26_expand_policy_ignored;
        break;
      }
      p = skip_policy_separators(end);
      const long centroid = std::strtol(p, &end, 10);
      if (end == p || fast < 0 || fast >= kFraudCount || centroid < 0 ||
          centroid >= kMaxCentroids) {
        ++rh26_expand_policy_ignored;
        p = end == p ? p + 1 : end;
        continue;
      }
      const size_t idx = static_cast<size_t>(fast) * kMaxCentroids +
                         static_cast<size_t>(centroid);
      if (rh26_expand_policy_[idx] == 0) {
        rh26_expand_policy_[idx] = 1;
        ++rh26_expand_policy_loaded;
      }
      p = end;
    }
    if (rh26_expand_policy_loaded == 0) {
      rh26_expand_nprobe_ = 0;
    }
  }

  rh26_initial_full_policy_.fill(0);
  rh26_initial_full_policy_enabled_ = false;
  uint64_t rh26_initial_full_policy_loaded = 0;
  uint64_t rh26_initial_full_policy_ignored = 0;
  if (const char* initial_full_keys = std::getenv("NATIVE_RH26_INITIAL_FULL_KEYS")) {
    const char* p = initial_full_keys;
    while (*p != '\0') {
      p = skip_policy_separators(p);
      if (*p == '\0') break;
      char* end = nullptr;
      long centroid = std::strtol(p, &end, 10);
      if (end == p) {
        ++rh26_initial_full_policy_ignored;
        break;
      }
      p = skip_policy_separators(end);
      if (*p == ':') {
        p = skip_policy_separators(p + 1);
        centroid = std::strtol(p, &end, 10);
        if (end == p) {
          ++rh26_initial_full_policy_ignored;
          break;
        }
        p = end;
      }
      if (centroid < 0 || centroid >= kMaxCentroids) {
        ++rh26_initial_full_policy_ignored;
        continue;
      }
      const size_t idx = static_cast<size_t>(centroid);
      if (rh26_initial_full_policy_[idx] == 0) {
        rh26_initial_full_policy_[idx] = 1;
        ++rh26_initial_full_policy_loaded;
      }
    }
    rh26_initial_full_policy_enabled_ = rh26_initial_full_policy_loaded > 0;
  }

  rh26_repair5_policy_.fill(0);
  rh26_repair5_policy_enabled_ = false;
  uint64_t rh26_repair5_policy_loaded = 0;
  uint64_t rh26_repair5_policy_ignored = 0;
  if (const char* repair5_keys = std::getenv("NATIVE_RH26_REPAIR5_KEYS")) {
    const char* p = repair5_keys;
    while (*p != '\0') {
      p = skip_policy_separators(p);
      if (*p == '\0') break;
      char* end = nullptr;
      long centroid = std::strtol(p, &end, 10);
      if (end == p) {
        ++rh26_repair5_policy_ignored;
        break;
      }
      p = skip_policy_separators(end);
      if (*p == ':') {
        p = skip_policy_separators(p + 1);
        centroid = std::strtol(p, &end, 10);
        if (end == p) {
          ++rh26_repair5_policy_ignored;
          break;
        }
        p = end;
      }
      if (centroid < 0 || centroid >= kMaxCentroids) {
        ++rh26_repair5_policy_ignored;
        continue;
      }
      const size_t idx = static_cast<size_t>(centroid);
      if (rh26_repair5_policy_[idx] == 0) {
        rh26_repair5_policy_[idx] = 1;
        ++rh26_repair5_policy_loaded;
      }
    }
    rh26_repair5_policy_enabled_ = rh26_repair5_policy_loaded > 0;
  }
  rh26_repair5_skip_q0_min_ =
      env_float_clamped("NATIVE_RH26_REPAIR5_SKIP_Q0_MIN", 0.0f, 0.0f, 1.0f);
  rh26_repair5_skip_requires_no_expand_ =
      env_bool_or("NATIVE_RH26_REPAIR5_SKIP_REQUIRE_NO_EXPAND", true);

  rh26_centroids_.resize(static_cast<size_t>(k_) * kDim);
  rh26_bbox_min_.resize(static_cast<size_t>(k_) * kDim);
  rh26_bbox_max_.resize(static_cast<size_t>(k_) * kDim);
  rh26_offsets_.resize(static_cast<size_t>(k_) + 1);
  rh26_counts_.resize(static_cast<size_t>(k_));
  rh26_labels_.resize(static_cast<size_t>(total_blocks_) * kRH26VectorsPerBlock);
  rh26_blocks_.resize(static_cast<size_t>(total_blocks_) * kRH26BlockStride);

  if (!read_exact(in, rh26_centroids_.data(), rh26_centroids_.size()) ||
      !read_exact(in, rh26_bbox_min_.data(), rh26_bbox_min_.size()) ||
      !read_exact(in, rh26_bbox_max_.data(), rh26_bbox_max_.size())) {
    error_ = "could not read RH26 centroids/bbox";
    return false;
  }

  const std::streamoff after_bbox = in.tellg();
  if (after_bbox < 0) {
    error_ = "could not locate RH26 offsets";
    return false;
  }
  in.seekg(static_cast<std::streamoff>(
               align_up_size(static_cast<size_t>(after_bbox), alignof(uint32_t))),
           std::ios::beg);
  if (!read_exact(in, rh26_offsets_.data(), rh26_offsets_.size()) ||
      !read_exact(in, rh26_counts_.data(), rh26_counts_.size())) {
    error_ = "could not read RH26 offsets/counts";
    return false;
  }
  if (rh26_offsets_[k_] != total_blocks_) {
    error_ = "invalid RH26 block offsets";
    return false;
  }
  if (!read_exact(in, rh26_labels_.data(), rh26_labels_.size())) {
    error_ = "could not read RH26 labels";
    return false;
  }
  const std::streamoff after_labels = in.tellg();
  if (after_labels < 0) {
    error_ = "could not locate RH26 blocks";
    return false;
  }
  in.seekg(static_cast<std::streamoff>(
               align_up_size(static_cast<size_t>(after_labels), alignof(int16_t))),
           std::ios::beg);
  if (!read_exact(in, rh26_blocks_.data(), rh26_blocks_.size())) {
    error_ = "could not read RH26 blocks";
    return false;
  }

  build_rh26_psoa();

  IndexResidencyConfig residency_config{};
  residency_config.madvise_index = env_bool_or("NATIVE_IVF_MADVISE_INDEX", false);
  residency_config.madvise_hugepage =
      env_bool_or("NATIVE_IVF_MADVISE_HUGEPAGE", residency_config.madvise_index);
  residency_config.mlock_index = env_bool_or("NATIVE_IVF_MLOCK_INDEX", false);
  residency_config.touch_index = env_bool_or("NATIVE_IVF_TOUCH_INDEX", false);
  if (residency_config.madvise_index || residency_config.mlock_index ||
      residency_config.touch_index) {
    IndexResidencyStats residency_stats{};
    apply_index_residency_vector("rh26_centroids", rh26_centroids_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_centroids_psoa", rh26_centroids_psoa_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("rh26_bbox_min", rh26_bbox_min_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_bbox_max", rh26_bbox_max_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_bbox_min_psoa", rh26_bbox_min_psoa_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("rh26_bbox_max_psoa", rh26_bbox_max_psoa_,
                                 residency_config, residency_stats);
    apply_index_residency_vector("rh26_offsets", rh26_offsets_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_counts", rh26_counts_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_labels", rh26_labels_, residency_config,
                                 residency_stats);
    apply_index_residency_vector("rh26_blocks", rh26_blocks_, residency_config,
                                 residency_stats);
    std::fprintf(stderr,
                 "[native-rh26-ivf-residency] regions=%u bytes=%llu madvise=%d "
                 "hugepage=%d touch=%d mlock=%d madvise_failures=%u mlock_failures=%u\n",
                 residency_stats.regions,
                 static_cast<unsigned long long>(residency_stats.bytes),
                 residency_config.madvise_index ? 1 : 0,
                 residency_config.madvise_hugepage ? 1 : 0,
                 residency_config.touch_index ? 1 : 0,
                 residency_config.mlock_index ? 1 : 0,
                 residency_stats.madvise_failures,
                 residency_stats.mlock_failures);
  }

  std::fprintf(stderr,
		               "[native-rh26-ivf] loaded n=%u k=%u blocks=%u nprobe=%d "
		               "initial=%d border=%d repair=[%d,%d] candidates=%d prefetch=%d "
                 "repair_select_min=%d defer_top=%d top_pairs=%d top_candidates=%d "
                   "top_mask=%u expand=%d "
                   "expand_keys=%llu ignored=%llu initial_full_keys=%llu ignored=%llu "
                   "repair5_keys=%llu ignored=%llu "
                   "repair5_skip_q0=%.6f no_expand=%d "
                   "train=%u/%u\n",
		               hdr.n, hdr.k, hdr.total_blocks, rh26_nprobe_, rh26_initial_nprobe_,
		               rh26_border_nprobe_, rh26_repair_min_, rh26_repair_max_,
		               rh26_repair_candidates_, rh26_prefetch_distance_,
                   rh26_repair_select_min_ ? 1 : 0, rh26_defer_full_top_ ? 1 : 0,
                   rh26_top_pairs_, rh26_top_candidates_, rh26_top_pair_mask_,
                   rh26_expand_nprobe_,
			               static_cast<unsigned long long>(rh26_expand_policy_loaded),
                   static_cast<unsigned long long>(rh26_expand_policy_ignored),
                   static_cast<unsigned long long>(rh26_initial_full_policy_loaded),
                   static_cast<unsigned long long>(rh26_initial_full_policy_ignored),
                   static_cast<unsigned long long>(rh26_repair5_policy_loaded),
                   static_cast<unsigned long long>(rh26_repair5_policy_ignored),
                   rh26_repair5_skip_q0_min_,
                   rh26_repair5_skip_requires_no_expand_ ? 1 : 0,
			               hdr.train_iters, hdr.train_max_iters);
  return true;
}

void NativeIVF::build_rh26_psoa() {
  const size_t psoa_size = static_cast<size_t>(rh26_groups_) * kRH26Pairs * 16u;
  rh26_centroids_psoa_.assign(psoa_size, 0);
  rh26_bbox_min_psoa_.assign(psoa_size, 0);
  rh26_bbox_max_psoa_.assign(psoa_size, 0);
  for (uint32_t g = 0; g < rh26_groups_; ++g) {
    for (int pair = 0; pair < kRH26Pairs; ++pair) {
      const size_t base = (static_cast<size_t>(g) * kRH26Pairs +
                           static_cast<size_t>(pair)) * 16u;
      for (int lane = 0; lane < kRH26VectorsPerBlock; ++lane) {
        const uint32_t ci = g * 8u + static_cast<uint32_t>(lane);
        if (ci >= k_) continue;
        for (int half = 0; half < 2; ++half) {
          const int dim = pair * 2 + half;
          const size_t dst = base + static_cast<size_t>(lane) * 2u + half;
          const size_t src = static_cast<size_t>(ci) * kDim + dim;
          rh26_centroids_psoa_[dst] = rh26_centroids_[src];
          rh26_bbox_min_psoa_[dst] = rh26_bbox_min_[src];
          rh26_bbox_max_psoa_[dst] = rh26_bbox_max_[src];
        }
      }
    }
  }
}

void NativeIVF::rh26_find_top_centroids(const __m256i* vq, uint32_t* out,
                                        int n) const {
  n = std::max(1, std::min(n, static_cast<int>(k_)));
  const int pool_n =
      std::min(rh26_top_candidates_ > 0 ? rh26_top_candidates_ : n,
               static_cast<int>(k_));
  const bool partial_top =
      pool_n > n && (rh26_top_pair_mask_ != 0 || rh26_top_pairs_ < kRH26Pairs);

  auto rank_groups = [&](int want, uint32_t pair_mask, int pair_limit,
                         uint32_t* top_d, uint32_t* top_c) {
    for (int i = 0; i < want; ++i) {
      top_d[i] = UINT32_MAX;
      top_c[i] = 0;
    }
    for (uint32_t g = 0; g < rh26_groups_; ++g) {
      const int16_t* src =
          rh26_centroids_psoa_.data() + static_cast<size_t>(g) * kRH26Pairs * 16u;
      const __m256i acc = rh26_centroid_group_distances(vq, src, pair_mask, pair_limit);
      uint32_t vals[kRH26LocalGroupLanes];
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(vals), acc);
      const uint32_t base = g * static_cast<uint32_t>(kRH26LocalGroupLanes);
      const uint32_t lim = std::min(static_cast<uint32_t>(kRH26LocalGroupLanes),
                                    k_ - base);
      for (uint32_t lane = 0; lane < lim; ++lane) {
        insert_rh26_centroid_candidate(vals[lane], base + lane, want, top_d, top_c);
      }
    }
  };

  if (partial_top) {
    uint32_t cand_d[256];
    uint32_t cand_c[256];
    rank_groups(pool_n, rh26_top_pair_mask_, rh26_top_pairs_, cand_d, cand_c);

    uint32_t top_d[256];
    uint32_t top_c[256];
    for (int i = 0; i < n; ++i) {
      top_d[i] = UINT32_MAX;
      top_c[i] = 0;
    }
    for (int i = 0; i < pool_n; ++i) {
      const uint32_t c = cand_c[i];
      if (c >= k_) continue;
      const uint32_t dist = rh26_centroid_distance(vq, c);
      insert_rh26_centroid_candidate(dist, c, n, top_d, top_c);
    }
    for (int i = 0; i < n; ++i) out[i] = top_c[i];
    return;
  }

  uint32_t top_d[256];
  uint32_t top_c[256];
  rank_groups(n, 0, kRH26Pairs, top_d, top_c);
  for (int i = 0; i < n; ++i) out[i] = top_c[i];
}

uint32_t NativeIVF::rh26_centroid_distance(const __m256i* vq, uint32_t c) const {
  if (c >= k_) return UINT32_MAX;
  const uint32_t g = c >> 3;
  const uint32_t lane = c & 7u;
  const int16_t* src =
      rh26_centroids_psoa_.data() + static_cast<size_t>(g) * kRH26Pairs * 16u;
  const __m256i acc = rh26_centroid_group_distances(vq, src, 0, kRH26Pairs);
  uint32_t vals[kRH26LocalGroupLanes];
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(vals), acc);
  return vals[lane];
}

void NativeIVF::rh26_scan_cluster(const __m256i* vq, uint32_t c,
                                  uint32_t* top_dists, uint8_t* top_labels,
                                  uint32_t& max_top) const {
  if (c >= k_ || rh26_counts_[c] == 0) return;

  const RH26Top5View top{top_dists, top_labels, max_top};
  const uint32_t block_start = rh26_offsets_[c];
  const uint32_t block_end = rh26_offsets_[static_cast<size_t>(c) + 1u];
  for (uint32_t bi = block_start; bi < block_end; ++bi) {
    if (rh26_prefetch_distance_ > 0) {
      const uint32_t prefetch_bi = bi + static_cast<uint32_t>(rh26_prefetch_distance_);
      if (prefetch_bi < block_end) {
        __builtin_prefetch(rh26_blocks_.data() +
                               static_cast<size_t>(prefetch_bi) * kRH26BlockStride,
                           0, 1);
      }
    }
    const int16_t* blk = rh26_blocks_.data() +
                         static_cast<size_t>(bi) * kRH26BlockStride;
    __m256i even_pairs = _mm256_setzero_si256();
    __m256i odd_pairs = _mm256_setzero_si256();

    even_pairs = rh26_add_pair_term(even_pairs, vq[0], blk + 0u * 16u);
    odd_pairs = rh26_add_pair_term(odd_pairs, vq[1], blk + 1u * 16u);
    even_pairs = rh26_add_pair_term(even_pairs, vq[2], blk + 2u * 16u);
    if (!rh26_any_lane_under(even_pairs, odd_pairs, max_top)) {
      continue;
    }

    odd_pairs = rh26_add_pair_term(odd_pairs, vq[3], blk + 3u * 16u);
    even_pairs = rh26_add_pair_term(even_pairs, vq[4], blk + 4u * 16u);
    if (!rh26_any_lane_under(even_pairs, odd_pairs, max_top)) {
      continue;
    }

    odd_pairs = rh26_add_pair_term(odd_pairs, vq[5], blk + 5u * 16u);
    even_pairs = rh26_add_pair_term(even_pairs, vq[6], blk + 6u * 16u);

    uint32_t dists[kRH26LocalGroupLanes];
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dists),
                        _mm256_add_epi32(even_pairs, odd_pairs));
    const uint32_t pos = (bi - block_start) * kRH26VectorsPerBlock;
    const int n_valid =
        static_cast<int>(std::min(static_cast<uint32_t>(kRH26VectorsPerBlock),
                                  rh26_counts_[c] - pos));
    const uint8_t* labels =
        rh26_labels_.data() + static_cast<size_t>(bi) * kRH26VectorsPerBlock;
    for (int lane = 0; lane < n_valid; ++lane) {
      top.offer(dists[lane], labels[lane]);
    }
  }
}

void NativeIVF::rh26_repair(const __m256i* vq, const uint32_t* skip, int nskip,
                            uint32_t* top_dists, uint8_t* top_labels,
                            uint32_t& max_top) const {
  const RH26Top5View top{top_dists, top_labels, max_top};
  static thread_local std::array<RH26RepairCandidate, kRH26LocalRepairCandidates>
      candidates;
  RH26RepairQueue<kRH26LocalRepairCandidates> repair_queue(candidates,
                                                           rh26_repair_candidates_);
  std::array<uint64_t, 64> skip_set{};
  rh26_build_skip_bitmap(skip, nskip, skip_set);

  const __m256i vmt =
      _mm256_set1_epi32(static_cast<int32_t>(rh26_i32_ceiling(max_top)));
  uint32_t lbs[kRH26LocalGroupLanes];
  for (uint32_t g = 0; g < rh26_groups_; ++g) {
    const int16_t* smin =
        rh26_bbox_min_psoa_.data() + static_cast<size_t>(g) * kRH26Pairs * 16u;
    const int16_t* smax =
        rh26_bbox_max_psoa_.data() + static_cast<size_t>(g) * kRH26Pairs * 16u;
    const __m256i acc = rh26_bbox_group_lower_bounds(vq, smin, smax);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lbs), acc);
    const __m256i vlbs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lbs));
    int pass_mask =
        _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(vmt, vlbs)));
    if (!pass_mask) continue;

    const uint32_t base = g * 8u;
    int mask = pass_mask;
    while (mask) {
      const int lane = __builtin_ctz(mask);
      mask &= mask - 1;
      const uint32_t c = base + static_cast<uint32_t>(lane);
      if (c >= k_) continue;
      if (rh26_bitmap_contains(skip_set, c)) continue;
      if (rh26_counts_[c] == 0) continue;
      repair_queue.offer(lbs[lane], c);
    }
  }

  auto scan_candidate = [&](const RH26RepairCandidate& candidate) {
    if (candidate.lb >= max_top) return false;
    rh26_scan_cluster(vq, candidate.centroid, top_dists, top_labels, max_top);
    const RH26TopSummary after_scan = top.summarize();
    return after_scan.has_borderline &&
           after_scan.in_repair_band(rh26_repair_min_, rh26_repair_max_);
  };

  if (rh26_repair_select_min_) {
    RH26RepairCandidate candidate{};
    while (repair_queue.take_lowest(max_top, candidate)) {
      if (!scan_candidate(candidate)) break;
    }
    return;
  }

  repair_queue.sort_by_bound();
  for (int i = 0; i < repair_queue.count; ++i) {
    const RH26RepairCandidate& candidate = candidates[static_cast<size_t>(i)];
    if (!scan_candidate(candidate)) break;
  }
}

bool NativeIVF::rh26_expand_policy_matches(int fast, uint32_t primary_centroid) const {
  if (rh26_expand_nprobe_ <= 0 || fast < 0 || fast >= kFraudCount ||
      primary_centroid >= kMaxCentroids) {
    return false;
  }
  const size_t idx = static_cast<size_t>(fast) * kMaxCentroids +
                     static_cast<size_t>(primary_centroid);
  return rh26_expand_policy_[idx] != 0;
}

int NativeIVF::classify_rh26(const float* q) const {
  int16_t q16[kDim];
  __m256i vq[kRH26Pairs];
  rh26_prepare_query_pairs(q, q16, vq);

  int nprobe = rh26_nprobe_;
  nprobe = std::max(1, std::min(nprobe, static_cast<int>(k_)));

  uint32_t top_dists[5] = {UINT32_MAX, UINT32_MAX, UINT32_MAX, UINT32_MAX,
                           UINT32_MAX};
  uint8_t top_labels[5] = {};
  uint32_t max_top = UINT32_MAX;
  const RH26Top5View top{top_dists, top_labels, max_top};

  const int initial_probe =
      rh26_initial_nprobe_ > 0 ? std::min(rh26_initial_nprobe_, nprobe) : nprobe;
  uint32_t probes[256];
  const int top_probe =
      (rh26_defer_full_top_ && initial_probe < nprobe) ? initial_probe : nprobe;
  rh26_find_top_centroids(vq, probes, top_probe);
  int ranked_probe_count = top_probe;
  int scanned_probe_count = 0;

  auto ensure_ranked = [&](int desired) {
    desired = std::max(1, std::min(desired, static_cast<int>(k_)));
    if (ranked_probe_count >= desired) return;
    rh26_find_top_centroids(vq, probes, desired);
    ranked_probe_count = desired;
  };

  auto scan_ranked_range = [&](int from, int to) {
    to = std::min(to, ranked_probe_count);
    for (int i = from; i < to; ++i) {
      rh26_scan_cluster(vq, probes[i], top_dists, top_labels, max_top);
    }
    if (to > scanned_probe_count) scanned_probe_count = to;
  };

  scan_ranked_range(0, initial_probe);

  RH26TopSummary summary = top.summarize();
  const bool force_initial_full =
      rh26_initial_full_policy_enabled_ && initial_probe < nprobe &&
      probes[0] < kMaxCentroids &&
      rh26_initial_full_policy_[static_cast<size_t>(probes[0])] != 0;
  if (initial_probe < nprobe &&
      (summary.has_borderline ||
       summary.in_repair_band(rh26_repair_min_, rh26_repair_max_) ||
       force_initial_full)) {
    ensure_ranked(nprobe);
    scan_ranked_range(initial_probe, nprobe);
    summary = top.summarize();
  }

  bool expanded = false;
  if (rh26_expand_nprobe_ > nprobe &&
      rh26_expand_policy_matches(summary.frauds, probes[0])) {
    const int scanned_before_expand = scanned_probe_count;
    ensure_ranked(nprobe);
    const int expanded_nprobe =
        std::min(rh26_expand_nprobe_, static_cast<int>(k_));
    uint32_t expanded_probes[256];
    rh26_find_top_centroids(vq, expanded_probes, expanded_nprobe);
    for (int i = 0; i < expanded_nprobe; ++i) {
      bool already_scanned = false;
      for (int j = 0; j < scanned_before_expand; ++j) {
        if (expanded_probes[i] == probes[j]) {
          already_scanned = true;
          break;
        }
      }
      if (!already_scanned) {
        rh26_scan_cluster(vq, expanded_probes[i], top_dists, top_labels, max_top);
      }
    }
    for (int i = 0; i < expanded_nprobe; ++i) {
      probes[i] = expanded_probes[i];
    }
    nprobe = expanded_nprobe;
    ranked_probe_count = expanded_nprobe;
    scanned_probe_count = expanded_nprobe;
    expanded = true;
    summary = top.summarize();
  }
  bool should_repair = summary.has_borderline;
  if (should_repair && summary.frauds == 5 && rh26_repair5_skip_q0_min_ > 0.0f &&
      q[0] >= rh26_repair5_skip_q0_min_ &&
      (!rh26_repair5_skip_requires_no_expand_ || !expanded)) {
    should_repair = false;
  }
  if (should_repair && summary.frauds == 5 && rh26_repair5_policy_enabled_) {
    const uint32_t primary = probes[0];
    should_repair = primary < kMaxCentroids && rh26_repair5_policy_[primary] != 0;
  }
  if (should_repair) {
    rh26_repair(vq, probes, scanned_probe_count, top_dists, top_labels, max_top);
  }

  return top.fraud_count();
}

bool NativeIVF::load_exact_kd_repair_policy(const char* path) {
  exact_kd_repair_policy_.assign(
      static_cast<size_t>(kFraudCount) * kFraudCount * 2u * kMaxCentroids,
      ExactKDRepairRule{});
  exact_kd_repair_policy_enabled_ = false;
  exact_kd_repair_feature_policy_enabled_ = false;
  if (!path || path[0] == '\0') {
    std::fprintf(stderr, "[native-cpp-ivf-policy] missing exact KD repair policy path\n");
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    std::fprintf(stderr, "[native-cpp-ivf-policy] could not open %s\n", path);
    return false;
  }

  uint64_t loaded = 0;
  uint64_t ignored = 0;
  std::string line;
  while (std::getline(in, line)) {
    const char* p = skip_policy_separators(line.c_str());
    if (*p == '\0' || *p == '#') continue;
    char* end = nullptr;
    const long fast = std::strtol(p, &end, 10);
    if (end == p) {
      ++ignored;
      continue;
    }
    p = skip_policy_separators(end);
    const long count = std::strtol(p, &end, 10);
    if (end == p) {
      ++ignored;
      continue;
    }
    p = skip_policy_separators(end);
    const long would = std::strtol(p, &end, 10);
    if (end == p) {
      ++ignored;
      continue;
    }
    p = skip_policy_separators(end);
    const long centroid = std::strtol(p, &end, 10);
    if (end == p || fast < 0 || fast >= kFraudCount || count < 0 ||
        count >= kFraudCount || would < 0 || would > 1 || centroid < 0 ||
        centroid >= kMaxCentroids) {
      ++ignored;
      continue;
    }
    const size_t idx =
        (((static_cast<size_t>(fast) * kFraudCount + static_cast<size_t>(count)) * 2u +
          static_cast<size_t>(would)) *
         static_cast<size_t>(kMaxCentroids)) +
        static_cast<size_t>(centroid);
    ExactKDRepairRule rule{};
    rule.enabled = 1;

    p = skip_policy_separators(end);
    if (*p != '\0' && *p != '#') {
      auto parse_feature = [&](float& out) -> bool {
        char* feature_end = nullptr;
        const float parsed = std::strtof(p, &feature_end);
        if (feature_end == p || !std::isfinite(parsed)) return false;
        out = parsed;
        p = skip_policy_separators(feature_end);
        return true;
      };
      if (!parse_feature(rule.min_margin) || !parse_feature(rule.max_worst) ||
          !parse_feature(rule.min_gap) || !parse_feature(rule.max_gap) ||
          !parse_feature(rule.min_last_gap) || !parse_feature(rule.max_last_gap)) {
        ++ignored;
        continue;
      }
      if (rule.min_margin > -0.5f || rule.max_worst > 0.0f ||
          rule.min_gap > 0.0f || rule.max_gap > 0.0f ||
          rule.min_last_gap > 0.0f || rule.max_last_gap > 0.0f) {
        exact_kd_repair_feature_policy_enabled_ = true;
      }
    }

    if (exact_kd_repair_policy_[idx].enabled == 0) {
      ++loaded;
    }
    exact_kd_repair_policy_[idx] = rule;
  }
  exact_kd_repair_policy_enabled_ = loaded > 0;
  std::fprintf(stderr,
               "[native-cpp-ivf-policy] exact_kd_repair loaded=%llu ignored=%llu path=%s\n",
               static_cast<unsigned long long>(loaded),
               static_cast<unsigned long long>(ignored), path);
  return loaded > 0;
}

bool NativeIVF::load_centroid_rescore_skip_policy(const char* path) {
  centroid_rescore_skip_policy_.fill(CentroidRescoreSkipRule{});
  if (!path || path[0] == '\0') {
    std::fprintf(stderr, "[native-cpp-ivf-policy] missing centroid rescore policy path\n");
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    std::fprintf(stderr, "[native-cpp-ivf-policy] could not open %s\n", path);
    return false;
  }
  std::string line;
  uint64_t loaded = 0;
  uint64_t conditional = 0;
  uint64_t ignored = 0;
  while (std::getline(in, line)) {
    const char* p = line.c_str();
    p = skip_policy_separators(p);
    if (*p == '\0' || *p == '#') continue;
    char* end = nullptr;
    long fast = std::strtol(p, &end, 10);
    if (end == p) {
      ++ignored;
      continue;
    }
    p = skip_policy_separators(end);
    long centroid = std::strtol(p, &end, 10);
    if (end == p || fast < 0 || fast > 5 || centroid < 0 ||
        centroid >= static_cast<long>(k_)) {
      ++ignored;
      continue;
    }
    const size_t idx = static_cast<size_t>(fast) * static_cast<size_t>(kMaxCentroids) +
                       static_cast<size_t>(centroid);
    CentroidRescoreSkipRule rule{};
    rule.enabled = 1;
    p = skip_policy_separators(end);
    if (*p != '\0' && *p != '#') {
      float* fields[] = {&rule.min_margin, &rule.max_worst, &rule.min_gap,
                         &rule.max_gap, &rule.min_last_gap, &rule.max_last_gap};
      int parsed = 0;
      for (float* field : fields) {
        char* f_end = nullptr;
        const float value = std::strtof(p, &f_end);
        if (f_end == p) break;
        *field = value;
        ++parsed;
        p = skip_policy_separators(f_end);
        if (*p == '\0' || *p == '#') break;
      }
      if (parsed > 0) ++conditional;
    }
    if (centroid_rescore_skip_policy_[idx].enabled == 0) {
      centroid_rescore_skip_policy_[idx] = rule;
      ++loaded;
    }
  }
  std::fprintf(stderr,
               "[native-cpp-ivf-policy] centroid_rescore_skip loaded=%llu conditional=%llu "
               "ignored=%llu path=%s\n",
               static_cast<unsigned long long>(loaded),
               static_cast<unsigned long long>(conditional),
               static_cast<unsigned long long>(ignored), path);
  return loaded > 0;
}

bool NativeIVF::load_local_fraud_graph(const char* path) {
  local_fraud_graph_.clear();
  if (!path || path[0] == '\0') {
    std::fprintf(stderr, "[native-cpp-ivf-local-graph] missing graph path\n");
    return false;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    std::fprintf(stderr, "[native-cpp-ivf-local-graph] could not open %s\n", path);
    return false;
  }
  char magic[4];
  if (!read_exact(in, magic, 4) || std::memcmp(magic, "LFG1", 4) != 0) {
    std::fprintf(stderr, "[native-cpp-ivf-local-graph] invalid graph magic in %s\n", path);
    return false;
  }
  bool ok = false;
  const uint32_t graph_k = read_u32(in, ok);
  const uint32_t graph_blocks = read_u32(in, ok);
  const uint32_t graph_window = read_u32(in, ok);
  (void)graph_window;
  if (!ok || graph_k != k_ || graph_blocks != total_blocks_) {
    std::fprintf(stderr,
                 "[native-cpp-ivf-local-graph] incompatible graph %s "
                 "k=%u blocks=%u expected k=%u blocks=%u\n",
                 path, graph_k, graph_blocks, k_, total_blocks_);
    return false;
  }
  const size_t count = static_cast<size_t>(total_blocks_) * kVectorsPerBlock;
  local_fraud_graph_.resize(count);
  if (!read_exact(in, local_fraud_graph_.data(), local_fraud_graph_.size())) {
    std::fprintf(stderr, "[native-cpp-ivf-local-graph] truncated graph %s\n", path);
    local_fraud_graph_.clear();
    return false;
  }
  uint64_t active = 0;
  for (uint16_t v : local_fraud_graph_) {
    if (v != UINT16_MAX) ++active;
  }
  std::fprintf(stderr,
               "[native-cpp-ivf-local-graph] loaded %s active=%llu total=%zu\n",
               path, static_cast<unsigned long long>(active), count);
  return active > 0;
}

bool NativeIVF::load_residual8_prefilter_policy(const char* path) {
  residual8_prefilter_policy_.fill(0);
  if (!path || path[0] == '\0') {
    std::fprintf(stderr, "[native-cpp-ivf-policy] missing residual8 prefilter policy path\n");
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    std::fprintf(stderr, "[native-cpp-ivf-policy] could not open %s\n", path);
    return false;
  }
  std::string line;
  uint64_t loaded = 0;
  uint64_t ignored = 0;
  while (std::getline(in, line)) {
    const char* p = line.c_str();
    p = skip_policy_separators(p);
    if (*p == '\0' || *p == '#') continue;
    char* end = nullptr;
    long centroid = std::strtol(p, &end, 10);
    if (end == p || centroid < 0 || centroid >= static_cast<long>(k_)) {
      ++ignored;
      continue;
    }
    const size_t idx = static_cast<size_t>(centroid);
    if (residual8_prefilter_policy_[idx] == 0) {
      residual8_prefilter_policy_[idx] = 1;
      ++loaded;
    }
  }
  std::fprintf(stderr,
               "[native-cpp-ivf-policy] residual8_prefilter loaded=%llu ignored=%llu path=%s\n",
               static_cast<unsigned long long>(loaded),
               static_cast<unsigned long long>(ignored), path);
  return loaded > 0;
}

bool NativeIVF::load_binary_prefilter_policy(const char* path) {
  binary_prefilter_policy_.fill(0);
  if (!path || path[0] == '\0') {
    std::fprintf(stderr, "[native-cpp-ivf-policy] missing binary prefilter policy path\n");
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    std::fprintf(stderr, "[native-cpp-ivf-policy] could not open %s\n", path);
    return false;
  }
  std::string line;
  uint64_t loaded = 0;
  uint64_t ignored = 0;
  while (std::getline(in, line)) {
    const char* p = line.c_str();
    p = skip_policy_separators(p);
    if (*p == '\0' || *p == '#') continue;
    char* end = nullptr;
    long centroid = std::strtol(p, &end, 10);
    if (end == p || centroid < 0 || centroid >= static_cast<long>(k_)) {
      ++ignored;
      continue;
    }
    p = skip_policy_separators(end);
    long threshold = binary_prefilter_threshold_;
    if (*p != '\0' && *p != '#') {
      threshold = std::strtol(p, &end, 10);
      if (end == p) {
        ++ignored;
        continue;
      }
    }
    if (threshold <= 0 || threshold > kDim) {
      ++ignored;
      continue;
    }
    const size_t idx = static_cast<size_t>(centroid);
    if (binary_prefilter_policy_[idx] == 0) {
      binary_prefilter_policy_[idx] = static_cast<uint8_t>(threshold);
      ++loaded;
    }
  }
  std::fprintf(stderr,
               "[native-cpp-ivf-policy] binary_prefilter loaded=%llu ignored=%llu path=%s\n",
               static_cast<unsigned long long>(loaded),
               static_cast<unsigned long long>(ignored), path);
  return loaded > 0;
}

bool NativeIVF::residual8_prefilter_enabled_for(int probe) const {
  if (!residual8_prefilter_policy_enabled_) return true;
  if (probe < 0 || probe >= static_cast<int>(k_)) return false;
  return residual8_prefilter_policy_[static_cast<size_t>(probe)] != 0;
}

bool NativeIVF::binary_prefilter_enabled_for(int probe) const {
  if (!binary_prefilter_ || binary_signatures_.empty()) return false;
  if (probe < 0 || probe >= static_cast<int>(k_)) return false;
  if (!binary_prefilter_policy_enabled_) return binary_prefilter_threshold_ > 0;
  return binary_prefilter_policy_[static_cast<size_t>(probe)] != 0;
}

int NativeIVF::binary_prefilter_threshold_for(int probe) const {
  if (binary_prefilter_policy_enabled_ && probe >= 0 && probe < static_cast<int>(k_)) {
    const uint8_t threshold = binary_prefilter_policy_[static_cast<size_t>(probe)];
    if (threshold != 0) return static_cast<int>(threshold);
  }
  return binary_prefilter_threshold_;
}

void NativeIVF::record_quick_residual8_prefilter(int probe, bool skipped) {
  if (!profile_enabled_) return;
  ++profile_.quick_residual8_checks;
  if (skipped) ++profile_.quick_residual8_skipped_blocks;
  if (!residual8_prefilter_profile_ || probe < 0 || probe >= static_cast<int>(k_)) return;
  const size_t idx = static_cast<size_t>(probe);
  ++profile_.residual8_probe_checks[idx];
  if (skipped) ++profile_.residual8_probe_skipped[idx];
}

void NativeIVF::record_rescore_residual8_prefilter(int probe, bool skipped) {
  if (!profile_enabled_) return;
  ++profile_.rescore_residual8_checks;
  if (skipped) ++profile_.rescore_residual8_skipped_blocks;
  if (!residual8_prefilter_profile_ || probe < 0 || probe >= static_cast<int>(k_)) return;
  const size_t idx = static_cast<size_t>(probe);
  ++profile_.residual8_probe_checks[idx];
  if (skipped) ++profile_.residual8_probe_skipped[idx];
}

const float* NativeIVF::order_query(const float* q) {
  if (query_matrix_index_) {
    for (int d = 0; d < kDim; ++d) {
      float sum = 0.0f;
      const float* row = query_matrix_.data() + static_cast<size_t>(d) * kDim;
      for (int s = 0; s < kDim; ++s) {
        sum += row[static_cast<size_t>(s)] * q[static_cast<size_t>(s)];
      }
      ordered_query_[static_cast<size_t>(d)] = sum;
    }
    return ordered_query_.data();
  }
  if (!dim_hot6_order_ && !query_transform_index_) return q;
  for (int d = 0; d < kDim; ++d) {
    const size_t src = dim_hot6_order_ ? static_cast<size_t>(kHot6DimOrder[d])
                                       : static_cast<size_t>(d);
    float v = q[src];
    if (query_transform_index_) {
      v *= query_scales_[static_cast<size_t>(d)];
    }
    ordered_query_[static_cast<size_t>(d)] = v;
  }
  return ordered_query_.data();
}

bool NativeIVF::query_is_borderline(const float* q) const {
  (void)q;
  return true;
}

void NativeIVF::warmup() {
  uint64_t acc = 0;
  if (rh26_index_) {
    acc += touch_pages(rh26_centroids_);
    acc += touch_pages(rh26_centroids_psoa_);
    acc += touch_pages(rh26_bbox_min_);
    acc += touch_pages(rh26_bbox_max_);
    acc += touch_pages(rh26_bbox_min_psoa_);
    acc += touch_pages(rh26_bbox_max_psoa_);
    acc += touch_pages(rh26_offsets_);
    acc += touch_pages(rh26_counts_);
    acc += touch_pages(rh26_labels_);
    acc += touch_pages(rh26_blocks_);
    if (k_ > 0 && rh26_centroids_.size() >= static_cast<size_t>(k_) * kDim) {
      std::array<float, kDim> q{};
      int samples = static_cast<int>(std::min<uint32_t>(k_, 32));
      int step = samples > 0 ? std::max(1, static_cast<int>(k_) / samples) : 1;
      int warmed = 0;
      for (int ci = 0; ci < static_cast<int>(k_) && warmed < samples; ci += step) {
        for (int d = 0; d < kDim; ++d) {
          q[static_cast<size_t>(d)] =
              static_cast<float>(rh26_centroids_[static_cast<size_t>(ci) * kDim + d]) *
              kVectorScale;
        }
        acc += static_cast<uint64_t>(classify_rh26(q.data()));
        ++warmed;
      }
    }
    g_native_ivf_warmup_sink = acc;
    return;
  }
  acc += touch_pages(centroids_);
  acc += touch_pages(centroid_norms_);
  acc += touch_pages(offsets_);
  acc += touch_pages(labels_);
  acc += touch_pages(block_label_masks_);
  acc += touch_pages(cluster_label_pure_);
  acc += touch_pages(label_skip_nonzero_offsets_);
  acc += touch_pages(label_skip_nonzero_blocks_);
  acc += touch_pages(label_skip_notfull_offsets_);
  acc += touch_pages(label_skip_notfull_blocks_);
  acc += touch_pages(residual8_filter_blocks_);
  acc += touch_pages(residual8_filter_scales_);
  acc += touch_pages(blocks_);
  acc += touch_pages(residual8_blocks_);
  acc += touch_pages(residual8_scales_);

  if (k_ > 0 && centroids_.size() >= static_cast<size_t>(kDim) * k_) {
    std::array<float, kDim> q{};
    int samples = static_cast<int>(k_);
    if (samples > 32) samples = 32;
    int step = 1;
    if (samples > 0 && static_cast<int>(k_) > samples) {
      step = static_cast<int>(k_) / samples;
      if (step < 1) step = 1;
    }
    int warmed = 0;
    for (int ci = 0; ci < static_cast<int>(k_) && warmed < samples; ci += step) {
      for (int d = 0; d < kDim; ++d) {
        q[static_cast<size_t>(d)] = centroids_[static_cast<size_t>(d) * k_ + ci];
      }
      acc += static_cast<uint64_t>(classify(q.data()));
      ++warmed;
    }
  }

  g_native_ivf_warmup_sink = acc;
}

void NativeIVF::centroid_dists(const float* q) {
  if (centroid_dot_distance_ && centroid_norms_.size() == k_) {
    centroid_dists_dot(q);
    return;
  }
  centroid_dists_diff(q);
}

void NativeIVF::centroid_dists_diff(const float* q) {
  const int k = static_cast<int>(k_);
  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 c2 = _mm256_loadu_ps(row + 16);
      const __m256 c3 = _mm256_loadu_ps(row + 24);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      const __m256 diff2 = _mm256_sub_ps(c2, qv);
      const __m256 diff3 = _mm256_sub_ps(c3, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
      acc2 = _mm256_fmadd_ps(diff2, diff2, acc2);
      acc3 = _mm256_fmadd_ps(diff3, diff3, acc3);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
    _mm256_store_ps(centroid_dists_.data() + ci + 16, acc2);
    _mm256_store_ps(centroid_dists_.data() + ci + 24, acc3);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc);
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    centroid_dists_[static_cast<size_t>(ci)] = sum;
  }
}

void NativeIVF::centroid_dists_dot(const float* q) {
  float q_norm = 0.0f;
  for (int d = 0; d < kDim; ++d) {
    q_norm += q[d] * q[d];
  }

  const int k = static_cast<int>(k_);
  const __m256 q_norm_v = _mm256_set1_ps(q_norm);
  const __m256 minus_two = _mm256_set1_ps(-2.0f);
  const __m256 zero = _mm256_setzero_ps();
  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 dot0 = _mm256_setzero_ps();
    __m256 dot1 = _mm256_setzero_ps();
    __m256 dot2 = _mm256_setzero_ps();
    __m256 dot3 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 qv = _mm256_set1_ps(q[d]);
      dot0 = _mm256_fmadd_ps(_mm256_loadu_ps(row), qv, dot0);
      dot1 = _mm256_fmadd_ps(_mm256_loadu_ps(row + 8), qv, dot1);
      dot2 = _mm256_fmadd_ps(_mm256_loadu_ps(row + 16), qv, dot2);
      dot3 = _mm256_fmadd_ps(_mm256_loadu_ps(row + 24), qv, dot3);
    }
    const float* norms = centroid_norms_.data() + ci;
    __m256 dist0 = _mm256_add_ps(_mm256_loadu_ps(norms), q_norm_v);
    __m256 dist1 = _mm256_add_ps(_mm256_loadu_ps(norms + 8), q_norm_v);
    __m256 dist2 = _mm256_add_ps(_mm256_loadu_ps(norms + 16), q_norm_v);
    __m256 dist3 = _mm256_add_ps(_mm256_loadu_ps(norms + 24), q_norm_v);
    dist0 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot0, dist0));
    dist1 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot1, dist1));
    dist2 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot2, dist2));
    dist3 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot3, dist3));
    _mm256_store_ps(centroid_dists_.data() + ci, dist0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, dist1);
    _mm256_store_ps(centroid_dists_.data() + ci + 16, dist2);
    _mm256_store_ps(centroid_dists_.data() + ci + 24, dist3);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 dot0 = _mm256_setzero_ps();
    __m256 dot1 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 qv = _mm256_set1_ps(q[d]);
      dot0 = _mm256_fmadd_ps(_mm256_loadu_ps(row), qv, dot0);
      dot1 = _mm256_fmadd_ps(_mm256_loadu_ps(row + 8), qv, dot1);
    }
    const float* norms = centroid_norms_.data() + ci;
    __m256 dist0 = _mm256_add_ps(_mm256_loadu_ps(norms), q_norm_v);
    __m256 dist1 = _mm256_add_ps(_mm256_loadu_ps(norms + 8), q_norm_v);
    dist0 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot0, dist0));
    dist1 = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot1, dist1));
    _mm256_store_ps(centroid_dists_.data() + ci, dist0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, dist1);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 dot = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      dot = _mm256_fmadd_ps(c, _mm256_set1_ps(q[d]), dot);
    }
    __m256 dist = _mm256_add_ps(_mm256_loadu_ps(centroid_norms_.data() + ci), q_norm_v);
    dist = _mm256_max_ps(zero, _mm256_fmadd_ps(minus_two, dot, dist));
    _mm256_store_ps(centroid_dists_.data() + ci, dist);
  }
  for (; ci < k; ++ci) {
    centroid_dists_[static_cast<size_t>(ci)] = centroid_distance_sq_dot(q, ci);
  }
}

void NativeIVF::centroid_dists_prefilter_twopass8(const float* q) {
  std::array<float, 8> partial_d;
  std::array<int, 8> partial_i;
  partial_d.fill(kInitialTopDist);
  partial_i.fill(0);

  const int k = static_cast<int>(k_);
  const int split = centroid_prefilter_dims_;
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d]);
  }

  auto finish_partial = [&](int base, __m256 acc) {
    _mm256_storeu_ps(centroid_dists_.data() + base, acc);
    update_top8_from_vec(acc, base, partial_d, partial_i);
    if (profile_enabled_) ++profile_.centroid_prefilter_chunks;
  };

  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 qv = qv_by_dim[d];
      const __m256 diff0 = _mm256_sub_ps(_mm256_loadu_ps(row), qv);
      const __m256 diff1 = _mm256_sub_ps(_mm256_loadu_ps(row + 8), qv);
      const __m256 diff2 = _mm256_sub_ps(_mm256_loadu_ps(row + 16), qv);
      const __m256 diff3 = _mm256_sub_ps(_mm256_loadu_ps(row + 24), qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
      acc2 = _mm256_fmadd_ps(diff2, diff2, acc2);
      acc3 = _mm256_fmadd_ps(diff3, diff3, acc3);
    }
    finish_partial(ci, acc0);
    finish_partial(ci + 8, acc1);
    finish_partial(ci + 16, acc2);
    finish_partial(ci + 24, acc3);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 qv = qv_by_dim[d];
      const __m256 diff0 = _mm256_sub_ps(_mm256_loadu_ps(row), qv);
      const __m256 diff1 = _mm256_sub_ps(_mm256_loadu_ps(row + 8), qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
    }
    finish_partial(ci, acc0);
    finish_partial(ci + 8, acc1);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 diff = _mm256_sub_ps(c, qv_by_dim[d]);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    finish_partial(ci, acc);
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < split; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    centroid_dists_[static_cast<size_t>(ci)] = sum;
    insert_probe_candidate<8>(sum, ci, partial_d, partial_i);
    if (profile_enabled_) ++profile_.centroid_prefilter_scalar_full;
  }

  std::array<float, 8> top_d;
  std::array<int, 8> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);
  const std::array<int, 8> seed_i = partial_i;
  for (int i = 0; i < 8; ++i) {
    const int idx = seed_i[static_cast<size_t>(i)];
    const float dist = centroid_distance_sq(q, idx);
    centroid_dists_[static_cast<size_t>(idx)] = dist;
    insert_probe_candidate<8>(dist, idx, top_d, top_i);
  }

  alignas(32) float full_tmp[8];
  for (ci = 0; ci + 8 <= k; ci += 8) {
    const __m256 partial = _mm256_loadu_ps(centroid_dists_.data() + ci);
    int mask = _mm256_movemask_ps(
        _mm256_cmp_ps(partial, _mm256_set1_ps(top_d[7]), _CMP_LT_OQ));
    if (mask == 0) {
      if (profile_enabled_) ++profile_.centroid_prefilter_skipped_chunks;
      continue;
    }

    __m256 acc = partial;
    for (int d = split; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 diff = _mm256_sub_ps(c, qv_by_dim[d]);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    _mm256_store_ps(full_tmp, acc);
    while (mask) {
      const int bit = __builtin_ctz(static_cast<unsigned>(mask));
      mask &= mask - 1;
      const int idx = ci + bit;
      if (is_seed_probe(idx, seed_i)) continue;
      insert_probe_candidate<8>(full_tmp[bit], idx, top_d, top_i);
    }
    if (profile_enabled_) ++profile_.centroid_prefilter_full_chunks;
  }
  for (; ci < k; ++ci) {
    const float partial = centroid_dists_[static_cast<size_t>(ci)];
    if (partial >= top_d[7]) {
      if (profile_enabled_) ++profile_.centroid_prefilter_scalar_skipped;
      continue;
    }
    if (is_seed_probe(ci, seed_i)) continue;
    float sum = partial;
    for (int d = split; d < kDim; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    insert_probe_candidate<8>(sum, ci, top_d, top_i);
    if (profile_enabled_) ++profile_.centroid_prefilter_scalar_full;
  }

  for (int i = 0; i < 8; ++i) {
    const size_t pos = static_cast<size_t>(i);
    probes_[pos] = top_i[pos];
    centroid_dists_[static_cast<size_t>(top_i[pos])] = top_d[pos];
  }
}

template <int N>
void NativeIVF::centroid_dists_prefilter_target(const float* q) {
  static_assert(N >= kQuickProbe);
  static_assert(N <= kMaxProbe);
  if constexpr (N == kQuickProbe) {
    if (centroid_prefilter_two_pass_ && quick_probe_ == kQuickProbe &&
        centroid_prefilter_probe_ == kQuickProbe && !centroid_dot_distance_) {
      centroid_dists_prefilter_twopass8(q);
      return;
    }
  }

  std::array<float, N> top_d;
  std::array<int, N> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  const int split = centroid_prefilter_dims_;
  const __m256 sentinel = _mm256_set1_ps(kInitialTopDist);
  const bool can_skip_sentinel_store =
      N == kQuickProbe && quick_probe_ == kQuickProbe &&
      centroid_prefilter_probe_ == kQuickProbe;
  const bool sparse_lane_mode = can_skip_sentinel_store && centroid_prefilter_sparse_lanes_;
  const bool top_store_only = can_skip_sentinel_store && centroid_prefilter_top_store_only_;

  auto finish_vec = [&](int base, __m256 acc) {
    if (profile_enabled_) ++profile_.centroid_prefilter_chunks;
    const __m256 cutoff = _mm256_set1_ps(top_d[static_cast<size_t>(N - 1)]);
    const int mask = _mm256_movemask_ps(_mm256_cmp_ps(acc, cutoff, _CMP_LT_OQ));
    if (mask == 0) {
      if (!can_skip_sentinel_store) {
        _mm256_store_ps(centroid_dists_.data() + base, sentinel);
      }
      if (profile_enabled_) ++profile_.centroid_prefilter_skipped_chunks;
      return;
    }

    if (sparse_lane_mode && __builtin_popcount(static_cast<unsigned>(mask)) <= 2) {
      alignas(32) float partial[8];
      _mm256_store_ps(partial, acc);
      int live = mask;
      while (live) {
        const int bit = __builtin_ctz(static_cast<unsigned>(live));
        live &= live - 1;
        float sum = partial[bit];
        const int ci = base + bit;
        for (int d = split; d < kDim; ++d) {
          const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
          sum += diff * diff;
        }
        insert_probe_candidate<N>(sum, ci, top_d, top_i);
      }
      if (profile_enabled_) ++profile_.centroid_prefilter_full_chunks;
      return;
    }

    for (int d = split; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + base);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    if (!sparse_lane_mode && !top_store_only) {
      _mm256_store_ps(centroid_dists_.data() + base, acc);
    }
    update_probe_top_from_vec<N>(acc, base, top_d, top_i);
    if (profile_enabled_) ++profile_.centroid_prefilter_full_chunks;
  };

  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 c2 = _mm256_loadu_ps(row + 16);
      const __m256 c3 = _mm256_loadu_ps(row + 24);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      const __m256 diff2 = _mm256_sub_ps(c2, qv);
      const __m256 diff3 = _mm256_sub_ps(c3, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
      acc2 = _mm256_fmadd_ps(diff2, diff2, acc2);
      acc3 = _mm256_fmadd_ps(diff3, diff3, acc3);
    }
    finish_vec(ci, acc0);
    finish_vec(ci + 8, acc1);
    finish_vec(ci + 16, acc2);
    finish_vec(ci + 24, acc3);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
    }
    finish_vec(ci, acc0);
    finish_vec(ci + 8, acc1);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int d = 0; d < split; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    finish_vec(ci, acc);
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < split; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    if (sum < top_d[static_cast<size_t>(N - 1)]) {
      for (int d = split; d < kDim; ++d) {
        const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
        sum += diff * diff;
      }
      centroid_dists_[static_cast<size_t>(ci)] = sum;
      insert_probe_candidate<N>(sum, ci, top_d, top_i);
      if (profile_enabled_) ++profile_.centroid_prefilter_scalar_full;
    } else {
      if (!can_skip_sentinel_store) {
        centroid_dists_[static_cast<size_t>(ci)] = kInitialTopDist;
      }
      if (profile_enabled_) ++profile_.centroid_prefilter_scalar_skipped;
    }
  }

  for (int i = 0; i < N; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
  if (sparse_lane_mode || top_store_only) {
    for (int i = 0; i < N; ++i) {
      centroid_dists_[static_cast<size_t>(top_i[static_cast<size_t>(i)])] =
          top_d[static_cast<size_t>(i)];
    }
  }
}

void NativeIVF::centroid_dists_prefilter_select8(const float* q) {
  switch (centroid_prefilter_probe_) {
    case 8:
      centroid_dists_prefilter_target<8>(q);
      return;
    case 10:
      centroid_dists_prefilter_target<10>(q);
      return;
    case 12:
      centroid_dists_prefilter_target<12>(q);
      return;
    case 16:
      centroid_dists_prefilter_target<16>(q);
      return;
    case 20:
      centroid_dists_prefilter_target<20>(q);
      return;
    case 24:
      centroid_dists_prefilter_target<24>(q);
      return;
    case 28:
      centroid_dists_prefilter_target<28>(q);
      return;
    case 32:
      centroid_dists_prefilter_target<32>(q);
      return;
    case 48:
      centroid_dists_prefilter_target<48>(q);
      return;
    case 64:
      centroid_dists_prefilter_target<64>(q);
      return;
    case 80:
      centroid_dists_prefilter_target<80>(q);
      return;
    case 88:
      centroid_dists_prefilter_target<88>(q);
      return;
    case 96:
      centroid_dists_prefilter_target<96>(q);
      return;
    case 112:
      centroid_dists_prefilter_target<112>(q);
      return;
    case 128:
      centroid_dists_prefilter_target<128>(q);
      return;
    case 129:
      centroid_dists_prefilter_target<129>(q);
      return;
    case 132:
      centroid_dists_prefilter_target<132>(q);
      return;
    case 134:
      centroid_dists_prefilter_target<134>(q);
      return;
    case 136:
      centroid_dists_prefilter_target<136>(q);
      return;
    case 144:
      centroid_dists_prefilter_target<144>(q);
      return;
    case 152:
      centroid_dists_prefilter_target<152>(q);
      return;
    case 160:
      centroid_dists_prefilter_target<160>(q);
      return;
    case 168:
      centroid_dists_prefilter_target<168>(q);
      return;
    case 192:
      centroid_dists_prefilter_target<192>(q);
      return;
    case 224:
      centroid_dists_prefilter_target<224>(q);
      return;
    case 256:
      centroid_dists_prefilter_target<256>(q);
      return;
    default:
      centroid_dists_prefilter_target<512>(q);
      return;
  }
}

void NativeIVF::centroid_dists_jl_prefilter_select8(const float* q) {
  std::array<float, kJLProjectionMaxDims> q_proj{};
  for (int pd = 0; pd < centroid_jl_dims_; ++pd) {
    float sum = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      sum += static_cast<float>(kJLProjection[static_cast<size_t>(pd)][static_cast<size_t>(d)]) *
             q[d];
    }
    q_proj[static_cast<size_t>(pd)] = sum * kJLProjectionScale;
  }

  std::array<JLCandidate, kMaxCentroids> candidates_by_projection;

  const int k = static_cast<int>(k_);
  const int candidates = std::min(centroid_jl_candidates_, std::min(k, kJLCandidateMax));
  int ci = 0;
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int pd = 0; pd < centroid_jl_dims_; ++pd) {
      const __m256 c =
          _mm256_loadu_ps(centroid_jl_.data() + static_cast<size_t>(pd) * k_ + ci);
      const __m256 qv = _mm256_set1_ps(q_proj[static_cast<size_t>(pd)]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    alignas(32) float tmp[8];
    _mm256_store_ps(tmp, acc);
    for (int lane = 0; lane < 8; ++lane) {
      candidates_by_projection[static_cast<size_t>(ci + lane)] = {tmp[lane], ci + lane};
    }
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int pd = 0; pd < centroid_jl_dims_; ++pd) {
      const float diff = centroid_jl_[static_cast<size_t>(pd) * k_ + ci] -
                         q_proj[static_cast<size_t>(pd)];
      sum += diff * diff;
    }
    candidates_by_projection[static_cast<size_t>(ci)] = {sum, ci};
  }

  if (candidates < k) {
    std::nth_element(candidates_by_projection.begin(),
                     candidates_by_projection.begin() + candidates,
                     candidates_by_projection.begin() + k,
                     [](const JLCandidate& a, const JLCandidate& b) {
                       return a.dist < b.dist;
                     });
  }

  centroid_dists_.fill(kInitialTopDist);
  std::array<float, 8> top_d;
  std::array<int, 8> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  for (int i = 0; i < candidates; ++i) {
    const int idx = candidates_by_projection[static_cast<size_t>(i)].idx;
    const float dist = centroid_distance_sq(q, idx);
    centroid_dists_[static_cast<size_t>(idx)] = dist;
    insert_probe_candidate<8>(dist, idx, top_d, top_i);
    if (profile_enabled_) ++profile_.centroid_prefilter_scalar_full;
  }

  for (int i = 0; i < 8; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
  if (profile_enabled_) {
    profile_.centroid_prefilter_chunks += static_cast<uint64_t>((k + 7) / 8);
    profile_.centroid_prefilter_full_chunks += static_cast<uint64_t>((candidates + 7) / 8);
    if (k > candidates) {
      profile_.centroid_prefilter_skipped_chunks += static_cast<uint64_t>((k - candidates + 7) / 8);
    }
  }
}

void NativeIVF::centroid_dists_select8(const float* q) {
  if (classsplit_select_) {
    centroid_dists(q);
    select_classsplit_probes(quick_probe_);
    return;
  }
  if (centroid_jl_prefilter_) {
    centroid_dists_jl_prefilter_select8(q);
    return;
  }
  if (centroid_prefilter_) {
    centroid_dists_prefilter_select8(q);
    return;
  }

  std::array<float, 8> top_d;
  std::array<int, 8> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 c2 = _mm256_loadu_ps(row + 16);
      const __m256 c3 = _mm256_loadu_ps(row + 24);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      const __m256 diff2 = _mm256_sub_ps(c2, qv);
      const __m256 diff3 = _mm256_sub_ps(c3, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
      acc2 = _mm256_fmadd_ps(diff2, diff2, acc2);
      acc3 = _mm256_fmadd_ps(diff3, diff3, acc3);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
    _mm256_store_ps(centroid_dists_.data() + ci + 16, acc2);
    _mm256_store_ps(centroid_dists_.data() + ci + 24, acc3);
    update_top8_from_vec(acc0, ci, top_d, top_i);
    update_top8_from_vec(acc1, ci + 8, top_d, top_i);
    update_top8_from_vec(acc2, ci + 16, top_d, top_i);
    update_top8_from_vec(acc3, ci + 24, top_d, top_i);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
    update_top8_from_vec(acc0, ci, top_d, top_i);
    update_top8_from_vec(acc1, ci + 8, top_d, top_i);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc);
    update_top8_from_vec(acc, ci, top_d, top_i);
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    centroid_dists_[static_cast<size_t>(ci)] = sum;
    insert_probe_candidate<8>(sum, ci, top_d, top_i);
  }

  for (int i = 0; i < 8; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

template <int N>
void NativeIVF::centroid_dists_select_top(const float* q) {
  static_assert(N > 0);
  static_assert(N <= kMaxProbe);

  std::array<float, N> top_d;
  std::array<int, N> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  int ci = 0;
  for (; ci + 32 <= k; ci += 32) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    __m256 acc2 = _mm256_setzero_ps();
    __m256 acc3 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 c2 = _mm256_loadu_ps(row + 16);
      const __m256 c3 = _mm256_loadu_ps(row + 24);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      const __m256 diff2 = _mm256_sub_ps(c2, qv);
      const __m256 diff3 = _mm256_sub_ps(c3, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
      acc2 = _mm256_fmadd_ps(diff2, diff2, acc2);
      acc3 = _mm256_fmadd_ps(diff3, diff3, acc3);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
    _mm256_store_ps(centroid_dists_.data() + ci + 16, acc2);
    _mm256_store_ps(centroid_dists_.data() + ci + 24, acc3);
    update_probe_top_from_vec<N>(acc0, ci, top_d, top_i);
    update_probe_top_from_vec<N>(acc1, ci + 8, top_d, top_i);
    update_probe_top_from_vec<N>(acc2, ci + 16, top_d, top_i);
    update_probe_top_from_vec<N>(acc3, ci + 24, top_d, top_i);
  }
  for (; ci + 16 <= k; ci += 16) {
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const float* row = centroids_.data() + static_cast<size_t>(d) * k_ + ci;
      const __m256 c0 = _mm256_loadu_ps(row);
      const __m256 c1 = _mm256_loadu_ps(row + 8);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff0 = _mm256_sub_ps(c0, qv);
      const __m256 diff1 = _mm256_sub_ps(c1, qv);
      acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
      acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc0);
    _mm256_store_ps(centroid_dists_.data() + ci + 8, acc1);
    update_probe_top_from_vec<N>(acc0, ci, top_d, top_i);
    update_probe_top_from_vec<N>(acc1, ci + 8, top_d, top_i);
  }
  for (; ci + 8 <= k; ci += 8) {
    __m256 acc = _mm256_setzero_ps();
    for (int d = 0; d < kDim; ++d) {
      const __m256 c = _mm256_loadu_ps(centroids_.data() + static_cast<size_t>(d) * k_ + ci);
      const __m256 qv = _mm256_set1_ps(q[d]);
      const __m256 diff = _mm256_sub_ps(c, qv);
      acc = _mm256_fmadd_ps(diff, diff, acc);
    }
    _mm256_store_ps(centroid_dists_.data() + ci, acc);
    update_probe_top_from_vec<N>(acc, ci, top_d, top_i);
  }
  for (; ci < k; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      const float diff = centroids_[static_cast<size_t>(d) * k_ + ci] - q[d];
      sum += diff * diff;
    }
    centroid_dists_[static_cast<size_t>(ci)] = sum;
    insert_probe_candidate<N>(sum, ci, top_d, top_i);
  }

  for (int i = 0; i < N; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

void NativeIVF::centroid_dists_select_top_dispatch(const float* q, int n) {
  switch (n) {
    case 20:
      centroid_dists_select_top<20>(q);
      return;
    case 24:
      centroid_dists_select_top<24>(q);
      return;
    case 28:
      centroid_dists_select_top<28>(q);
      return;
    case 32:
      centroid_dists_select_top<32>(q);
      return;
    case 48:
      centroid_dists_select_top<48>(q);
      return;
    case 64:
      centroid_dists_select_top<64>(q);
      return;
    case 80:
      centroid_dists_select_top<80>(q);
      return;
    case 88:
      centroid_dists_select_top<88>(q);
      return;
    case 96:
      centroid_dists_select_top<96>(q);
      return;
    case 128:
      centroid_dists_select_top<128>(q);
      return;
    case 129:
      centroid_dists_select_top<129>(q);
      return;
    case 132:
      centroid_dists_select_top<132>(q);
      return;
    case 134:
      centroid_dists_select_top<134>(q);
      return;
    case 136:
      centroid_dists_select_top<136>(q);
      return;
    case 144:
      centroid_dists_select_top<144>(q);
      return;
    case 152:
      centroid_dists_select_top<152>(q);
      return;
    case 160:
      centroid_dists_select_top<160>(q);
      return;
    case 168:
      centroid_dists_select_top<168>(q);
      return;
    case 192:
      centroid_dists_select_top<192>(q);
      return;
    case 224:
      centroid_dists_select_top<224>(q);
      return;
    case 256:
      centroid_dists_select_top<256>(q);
      return;
    case 512:
      centroid_dists_select_top<512>(q);
      return;
    default:
      centroid_dists_select_top<512>(q);
      return;
  }
}

template <int N>
void NativeIVF::select_top() {
  std::array<float, N> top_d;
  std::array<int, N> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  for (int ci = 0; ci < k; ++ci) {
    const float dist = centroid_dists_[static_cast<size_t>(ci)];
    if (dist >= top_d[static_cast<size_t>(N - 1)]) continue;
    int pos = N - 1;
    while (pos > 0 && dist < top_d[static_cast<size_t>(pos - 1)]) {
      top_d[static_cast<size_t>(pos)] = top_d[static_cast<size_t>(pos - 1)];
      top_i[static_cast<size_t>(pos)] = top_i[static_cast<size_t>(pos - 1)];
      --pos;
    }
    top_d[static_cast<size_t>(pos)] = dist;
    top_i[static_cast<size_t>(pos)] = ci;
  }

  for (int i = 0; i < N; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

template <int N>
void NativeIVF::select_top_masked() {
  std::array<float, N> top_d;
  std::array<int, N> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  int ci = 0;
  alignas(32) float tmp[8];
  for (; ci + 8 <= k; ci += 8) {
    const __m256 dist = _mm256_load_ps(centroid_dists_.data() + ci);
    int mask = _mm256_movemask_ps(_mm256_cmp_ps(dist, _mm256_set1_ps(top_d[N - 1]), _CMP_LT_OQ));
    if (mask == 0) continue;

    _mm256_store_ps(tmp, dist);
    while (mask) {
      const int bit = __builtin_ctz(static_cast<unsigned>(mask));
      mask &= mask - 1;
      insert_probe_candidate<N>(tmp[bit], ci + bit, top_d, top_i);
    }
  }
  for (; ci < k; ++ci) {
    insert_probe_candidate<N>(centroid_dists_[static_cast<size_t>(ci)], ci, top_d, top_i);
  }

  for (int i = 0; i < N; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

int NativeIVF::classsplit_fraud_probe_count(int n) const {
  if (!classsplit_select_ || n <= 0) return 0;
  if (n == quick_probe_) {
    return std::clamp(classsplit_quick_fraud_probes_, 0, n);
  }
  int fraud = (n * classsplit_rescore_fraud_pct_ + 50) / 100;
  if (fraud <= 0 && classsplit_rescore_fraud_pct_ > 0) fraud = 1;
  if (fraud >= n && classsplit_rescore_fraud_pct_ < 100) fraud = n - 1;
  return std::clamp(fraud, 0, n);
}

void NativeIVF::select_classsplit_probes(int n) {
  if (n <= 0) return;
  const int total = std::min(n, kMaxProbe);
  const int split = std::clamp(classsplit_split_, 1, std::max(1, static_cast<int>(k_) - 1));
  const int fraud_count = classsplit_fraud_probe_count(total);
  const int legit_count = total - fraud_count;

  std::array<float, 512> legit_d;
  std::array<int, 512> legit_i;
  std::array<float, 512> fraud_d;
  std::array<int, 512> fraud_i;
  legit_d.fill(kInitialTopDist);
  legit_i.fill(0);
  fraud_d.fill(kInitialTopDist);
  fraud_i.fill(split);

  for (int ci = 0; ci < split; ++ci) {
    insert_probe_candidate_dynamic(centroid_dists_[static_cast<size_t>(ci)], ci, legit_count,
                                   legit_d, legit_i);
  }
  for (int ci = split; ci < static_cast<int>(k_); ++ci) {
    insert_probe_candidate_dynamic(centroid_dists_[static_cast<size_t>(ci)], ci, fraud_count,
                                   fraud_d, fraud_i);
  }

  int li = 0;
  int fi = 0;
  int out = 0;
  while (out < total && (li < legit_count || fi < fraud_count)) {
    const bool take_fraud =
        fi < fraud_count &&
        (li >= legit_count || fraud_d[static_cast<size_t>(fi)] < legit_d[static_cast<size_t>(li)]);
    if (take_fraud) {
      probes_[static_cast<size_t>(out++)] = fraud_i[static_cast<size_t>(fi++)];
    } else {
      probes_[static_cast<size_t>(out++)] = legit_i[static_cast<size_t>(li++)];
    }
  }
}

template <int N>
void NativeIVF::select_top_radius_bound() {
  std::array<float, N> top_d;
  std::array<int, N> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const int k = static_cast<int>(k_);
  for (int ci = 0; ci < k; ++ci) {
    const float center_sq = centroid_dists_[static_cast<size_t>(ci)];
    const float radius = cluster_max_radii_[static_cast<size_t>(ci)];
    float score = 0.0f;
    if (center_sq > 0.0f) {
      const float gap = std::sqrt(center_sq) - radius;
      if (gap > 0.0f) score = gap * gap;
    }
    insert_probe_candidate<N>(score, ci, top_d, top_i);
  }

  for (int i = 0; i < N; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

template <int Prefix, int Total>
void NativeIVF::select_tail_after_prefix() {
  static_assert(Prefix > 0);
  static_assert(Total > Prefix);
  static_assert(Total <= kMaxProbe);
  constexpr int Tail = Total - Prefix;

  std::array<float, Tail> top_d;
  std::array<int, Tail> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const float prefix_cutoff =
      centroid_dists_[static_cast<size_t>(probes_[static_cast<size_t>(Prefix - 1)])];
  const __m256 cutoff_v = _mm256_set1_ps(prefix_cutoff);
  const int k = static_cast<int>(k_);
  int ci = 0;
  alignas(32) float tmp[8];
  for (; ci + 8 <= k; ci += 8) {
    const __m256 dist = _mm256_load_ps(centroid_dists_.data() + ci);
    const __m256 ge_cutoff = _mm256_cmp_ps(dist, cutoff_v, _CMP_GE_OQ);
    const __m256 lt_worst =
        _mm256_cmp_ps(dist, _mm256_set1_ps(top_d[static_cast<size_t>(Tail - 1)]), _CMP_LT_OQ);
    int mask = _mm256_movemask_ps(_mm256_and_ps(ge_cutoff, lt_worst));
    if (mask == 0) continue;

    _mm256_store_ps(tmp, dist);
    while (mask) {
      const int bit = __builtin_ctz(static_cast<unsigned>(mask));
      mask &= mask - 1;
      const int idx = ci + bit;
      const float d = tmp[bit];
      if (d == prefix_cutoff) {
        bool is_prefix = false;
        for (int i = 0; i < Prefix; ++i) {
          if (probes_[static_cast<size_t>(i)] == idx) {
            is_prefix = true;
            break;
          }
        }
        if (is_prefix) continue;
      }
      insert_probe_candidate<Tail>(d, idx, top_d, top_i);
    }
  }
  for (; ci < k; ++ci) {
    const float d = centroid_dists_[static_cast<size_t>(ci)];
    if (d < prefix_cutoff || d >= top_d[static_cast<size_t>(Tail - 1)]) continue;
    if (d == prefix_cutoff) {
      bool is_prefix = false;
      for (int i = 0; i < Prefix; ++i) {
        if (probes_[static_cast<size_t>(i)] == ci) {
          is_prefix = true;
          break;
        }
      }
      if (is_prefix) continue;
    }
    insert_probe_candidate<Tail>(d, ci, top_d, top_i);
  }

  for (int i = 0; i < Tail; ++i) {
    probes_[static_cast<size_t>(Prefix + i)] = top_i[static_cast<size_t>(i)];
  }
}

void NativeIVF::select_top20_seeded8() {
  std::array<float, kExpandedProbe> top_d;
  std::array<int, kExpandedProbe> top_i;
  std::array<int, kQuickProbe> seed_i;

  for (int i = 0; i < kQuickProbe; ++i) {
    const int idx = probes_[static_cast<size_t>(i)];
    seed_i[static_cast<size_t>(i)] = idx;
    top_i[static_cast<size_t>(i)] = idx;
    top_d[static_cast<size_t>(i)] = centroid_dists_[static_cast<size_t>(idx)];
  }
  for (int i = kQuickProbe; i < kExpandedProbe; ++i) {
    top_i[static_cast<size_t>(i)] = 0;
    top_d[static_cast<size_t>(i)] = kInitialTopDist;
  }

  const int k = static_cast<int>(k_);
  int ci = 0;
  alignas(32) float tmp[8];
  for (; ci + 8 <= k; ci += 8) {
    const __m256 dist = _mm256_load_ps(centroid_dists_.data() + ci);
    int mask = _mm256_movemask_ps(
        _mm256_cmp_ps(dist, _mm256_set1_ps(top_d[kExpandedProbe - 1]), _CMP_LT_OQ));
    if (mask == 0) continue;

    _mm256_store_ps(tmp, dist);
    while (mask) {
      const int bit = __builtin_ctz(static_cast<unsigned>(mask));
      mask &= mask - 1;
      const int idx = ci + bit;
      if (is_seed_probe(idx, seed_i)) continue;
      insert_probe_candidate<kExpandedProbe>(tmp[bit], idx, top_d, top_i);
    }
  }
  for (; ci < k; ++ci) {
    if (is_seed_probe(ci, seed_i)) continue;
    insert_probe_candidate<kExpandedProbe>(centroid_dists_[static_cast<size_t>(ci)], ci, top_d,
                                           top_i);
  }

  for (int i = 0; i < kExpandedProbe; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
}

void NativeIVF::select_top_probes(int n) {
  if (classsplit_select_) {
    select_classsplit_probes(n);
    return;
  }
  switch (n) {
    case 4:
      select_top_masked<4>();
      return;
    case 5:
      select_top_masked<5>();
      return;
    case 6:
      select_top_masked<6>();
      return;
    case 8:
      select_top_masked<8>();
      return;
    case 10:
      select_top_masked<10>();
      return;
    case 12:
      select_top_masked<12>();
      return;
    case 16:
      select_top_masked<16>();
      return;
    case 20:
      select_top_masked<20>();
      return;
    case 24:
      select_top_masked<24>();
      return;
    case 28:
      select_top_masked<28>();
      return;
    case 32:
      select_top_masked<32>();
      return;
    case 48:
      select_top_masked<48>();
      return;
    case 64:
      select_top_masked<64>();
      return;
    case 96:
      select_top_masked<96>();
      return;
    case 128:
      select_top_masked<128>();
      return;
    case 129:
      select_top_masked<129>();
      return;
    case 132:
      select_top_masked<132>();
      return;
    case 134:
      select_top_masked<134>();
      return;
    case 136:
      select_top_masked<136>();
      return;
    case 144:
      select_top_masked<144>();
      return;
    case 152:
      select_top_masked<152>();
      return;
    case 160:
      select_top_masked<160>();
      return;
    case 168:
      select_top_masked<168>();
      return;
    case 192:
      select_top_masked<192>();
      return;
    case 224:
      select_top_masked<224>();
      return;
    case 256:
      select_top_masked<256>();
      return;
    case 512:
      select_top_masked<512>();
      return;
    default:
      select_top_masked<512>();
      return;
  }
}

void NativeIVF::select_rescore_probes_exact(int n) {
  if (!rescore_radius_probes_ || cluster_max_radii_.size() != k_) {
    if (quick_probe_ == kQuickProbe && n == kExpandedProbe) {
      select_top20_seeded8();
    } else {
      select_top_probes(n);
    }
    return;
  }

  switch (n) {
    case 20:
      select_top_radius_bound<20>();
      return;
    case 24:
      select_top_radius_bound<24>();
      return;
    case 28:
      select_top_radius_bound<28>();
      return;
    case 32:
      select_top_radius_bound<32>();
      return;
    case 48:
      select_top_radius_bound<48>();
      return;
    case 64:
      select_top_radius_bound<64>();
      return;
    case 80:
      select_top_radius_bound<80>();
      return;
    case 88:
      select_top_radius_bound<88>();
      return;
    case 96:
      select_top_radius_bound<96>();
      return;
    case 128:
      select_top_radius_bound<128>();
      return;
    case 129:
      select_top_radius_bound<129>();
      return;
    default:
      select_top_probes(n);
      return;
  }
}

bool NativeIVF::can_use_centroid_graph(int n) const {
  if (!centroid_graph_rescore_ || centroid_neighbors_.empty() || n > centroid_graph_max_probe_) {
    return false;
  }
  const int theoretical = quick_probe_ + centroid_graph_seed_count_ * centroid_graph_neighbors_;
  return theoretical >= n;
}

uint16_t NativeIVF::next_centroid_graph_epoch() {
  ++centroid_graph_epoch_;
  if (centroid_graph_epoch_ == 0) {
    centroid_graph_seen_.fill(0);
    centroid_graph_epoch_ = 1;
  }
  return centroid_graph_epoch_;
}

void NativeIVF::mark_centroid_graph_seen(int idx, uint16_t epoch) {
  if (idx >= 0 && idx < static_cast<int>(centroid_graph_seen_.size())) {
    centroid_graph_seen_[static_cast<size_t>(idx)] = epoch;
  }
}

bool NativeIVF::centroid_graph_is_seen(int idx, uint16_t epoch) const {
  return idx >= 0 && idx < static_cast<int>(centroid_graph_seen_.size()) &&
         centroid_graph_seen_[static_cast<size_t>(idx)] == epoch;
}

bool NativeIVF::select_rescore_probes_graph(const float* q, int n) {
  if (!can_use_centroid_graph(n)) return false;

  std::array<float, 512> top_d;
  std::array<int, 512> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const uint16_t epoch = next_centroid_graph_epoch();
  int candidates = 0;
  auto add_candidate = [&](int ci) {
    if (ci < 0 || ci >= static_cast<int>(k_) || centroid_graph_is_seen(ci, epoch)) return;
    mark_centroid_graph_seen(ci, epoch);
    ++candidates;
    const float dist = centroid_distance_sq(q, ci);
    centroid_dists_[static_cast<size_t>(ci)] = dist;
    insert_probe_candidate_dynamic(dist, ci, n, top_d, top_i);
  };

  for (int i = 0; i < quick_probe_; ++i) {
    add_candidate(probes_[static_cast<size_t>(i)]);
  }
  const int seeds = std::min(centroid_graph_seed_count_, quick_probe_);
  for (int i = 0; i < seeds; ++i) {
    const int seed = probes_[static_cast<size_t>(i)];
    const size_t base = static_cast<size_t>(seed) * static_cast<size_t>(centroid_graph_neighbors_);
    for (int j = 0; j < centroid_graph_neighbors_; ++j) {
      add_candidate(static_cast<int>(centroid_neighbors_[base + static_cast<size_t>(j)]));
    }
  }

  if (candidates < n || top_d[static_cast<size_t>(n - 1)] >= kInitialTopDist) {
    return false;
  }
  for (int i = 0; i < n; ++i) {
    probes_[static_cast<size_t>(i)] = top_i[static_cast<size_t>(i)];
  }
  return true;
}

bool NativeIVF::select_second_chance_probes_graph(const float* q, int from_probe, int to_probe) {
  if (!can_use_centroid_graph(to_probe) || from_probe >= to_probe) return false;

  const int tail = to_probe - from_probe;
  std::array<float, 512> top_d;
  std::array<int, 512> top_i;
  top_d.fill(kInitialTopDist);
  top_i.fill(0);

  const uint16_t epoch = next_centroid_graph_epoch();
  for (int i = 0; i < from_probe; ++i) {
    mark_centroid_graph_seen(probes_[static_cast<size_t>(i)], epoch);
  }

  int candidates = 0;
  auto add_candidate = [&](int ci) {
    if (ci < 0 || ci >= static_cast<int>(k_) || centroid_graph_is_seen(ci, epoch)) return;
    mark_centroid_graph_seen(ci, epoch);
    ++candidates;
    const float dist = centroid_distance_sq(q, ci);
    centroid_dists_[static_cast<size_t>(ci)] = dist;
    insert_probe_candidate_dynamic(dist, ci, tail, top_d, top_i);
  };

  const int seeds = std::min(centroid_graph_seed_count_, quick_probe_);
  for (int i = 0; i < seeds; ++i) {
    const int seed = probes_[static_cast<size_t>(i)];
    const size_t base = static_cast<size_t>(seed) * static_cast<size_t>(centroid_graph_neighbors_);
    for (int j = 0; j < centroid_graph_neighbors_; ++j) {
      add_candidate(static_cast<int>(centroid_neighbors_[base + static_cast<size_t>(j)]));
    }
  }

  if (candidates < tail || top_d[static_cast<size_t>(tail - 1)] >= kInitialTopDist) {
    return false;
  }
  for (int i = 0; i < tail; ++i) {
    probes_[static_cast<size_t>(from_probe + i)] = top_i[static_cast<size_t>(i)];
  }
  return true;
}

void NativeIVF::select_rescore_probes(const float* q, int n, bool& full_refreshed) {
  if (select_rescore_probes_graph(q, n)) return;
  if (rescore_prefilter_select_ && centroid_prefilter_ && !full_refreshed && n == 28 &&
      !rescore_radius_probes_ && !centroid_dot_distance_) {
    centroid_dists_prefilter_target<28>(q);
    return;
  }
  if (fused_top_select_ && (centroid_prefilter_ || centroid_jl_prefilter_) && !full_refreshed &&
      centroid_prefilter_probe_ < n && !rescore_radius_probes_ && !centroid_dot_distance_) {
    centroid_dists_select_top_dispatch(q, n);
    full_refreshed = true;
    return;
  }
  ensure_centroid_dists_for(q, n, full_refreshed);
  select_rescore_probes_exact(n);
}

void NativeIVF::select_second_chance_probes(const float* q, int from_probe, int to_probe,
                                            bool& full_refreshed) {
  if (select_second_chance_probes_graph(q, from_probe, to_probe)) return;
  if (rescore_tail_select_ && full_refreshed && !rescore_radius_probes_ && from_probe == 28 &&
      to_probe == 96) {
    select_tail_after_prefix<28, 96>();
    return;
  }
  ensure_centroid_dists_for(q, to_probe, full_refreshed);
  select_rescore_probes_exact(to_probe);
}

void NativeIVF::reset_top() {
  top_dist_.fill(kInitialTopDist);
  top_label_.fill(0);
  top_slot_.fill(-1);
}

void NativeIVF::scan_probes(const float* q, int from, int to) {
  if (kEnableResidual8Index && residual8_index_) {
    scan_probes_residual8(q, from, to);
    return;
  }

  int worst = 0;
  alignas(32) __m256 scale_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    scale_by_dim[d] = _mm256_set1_ps(vector_scale(d));
  }

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    const uint32_t start = offsets_[static_cast<size_t>(probe)];
    const uint32_t end = offsets_[static_cast<size_t>(probe + 1)];

    for (uint32_t block = start; block < end; ++block) {
      const int16_t* base = blocks_.data() + static_cast<size_t>(block) * kBlockStride;
      __m256 low = _mm256_setzero_ps();
      __m256 high = _mm256_setzero_ps();

      for (int d = 0; d < 6; ++d) {
        const int16_t* row = base + d * kVectorsPerBlock;
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
        const __m256 scale = scale_by_dim[d];
        __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);
        __m256 vf_high = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);
        const __m256 qv = _mm256_set1_ps(q[d]);
        const __m256 dl = _mm256_sub_ps(vf_low, qv);
        const __m256 dh = _mm256_sub_ps(vf_high, qv);
        low = _mm256_fmadd_ps(dl, dl, low);
        high = _mm256_fmadd_ps(dh, dh, high);
      }

      const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);
      int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
      int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
      if ((mask_low | mask_high) == 0) continue;

      const bool low_alive = mask_low != 0;
      const bool high_alive = mask_high != 0;
      for (int d = 6; d < kDim; ++d) {
        const int16_t* row = base + d * kVectorsPerBlock;
        const __m256 qv = _mm256_set1_ps(q[d]);
        if (low_alive) {
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
          const __m256 scale = scale_by_dim[d];
          __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);
          const __m256 dl = _mm256_sub_ps(vf_low, qv);
          low = _mm256_fmadd_ps(dl, dl, low);
        }
        if (high_alive) {
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
          const __m256 scale = scale_by_dim[d];
          __m256 vf_high =
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);
          const __m256 dh = _mm256_sub_ps(vf_high, qv);
          high = _mm256_fmadd_ps(dh, dh, high);
        }
      }

      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
      if ((mask_low | mask_high) == 0) continue;

      const uint8_t* label_base = labels_.data() + static_cast<size_t>(block) * kVectorsPerBlock;
      if (mask_low) {
        _mm256_store_ps(dist_low, low);
        while (mask_low) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
          mask_low &= mask_low - 1;
          update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);
        }
      }
      if (mask_high) {
        _mm256_store_ps(dist_high, high);
        while (mask_high) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
          mask_high &= mask_high - 1;
          update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);
        }
      }
    }
  }
}

void NativeIVF::scan_probes_residual8(const float* q, int from, int to) {
  int worst = 0;
  const __m256 invalid = _mm256_set1_ps(kInitialTopDist);

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    const uint32_t start = offsets_[static_cast<size_t>(probe)];
    const uint32_t end = offsets_[static_cast<size_t>(probe + 1)];

    for (uint32_t block = start; block < end; ++block) {
      const int8_t* base = residual8_blocks_.data() + static_cast<size_t>(block) * kBlockStride;
      __m256 low = _mm256_setzero_ps();
      __m256 high = _mm256_setzero_ps();
      __m256 pad_low = _mm256_setzero_ps();
      __m256 pad_high = _mm256_setzero_ps();

      for (int d = 0; d < 6; ++d) {
        const int8_t* row = base + d * kVectorsPerBlock;
        const float scale_low = residual8_scale(block, d, probe, 0);
        const float scale_high = residual8_scale(block, d, probe, 1);
        const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
        accumulate_residual8_dim(row, scale_low, scale_high, q_center, d == 0, low, high,
                                 pad_low, pad_high);
      }

      low = _mm256_blendv_ps(low, invalid, pad_low);
      high = _mm256_blendv_ps(high, invalid, pad_high);

      const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);
      int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
      int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
      if ((mask_low | mask_high) == 0) continue;

      const bool low_alive = mask_low != 0;
      const bool high_alive = mask_high != 0;
      for (int d = 6; d < kDim; ++d) {
        const int8_t* row = base + d * kVectorsPerBlock;
        const float scale_low = residual8_scale(block, d, probe, 0);
        const float scale_high = residual8_scale(block, d, probe, 1);
        const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
        if (low_alive || high_alive) {
          accumulate_residual8_dim(row, scale_low, scale_high, q_center, false, low, high,
                                   pad_low, pad_high);
        }
      }

      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
      if ((mask_low | mask_high) == 0) continue;

      const uint8_t* label_base = labels_.data() + static_cast<size_t>(block) * kVectorsPerBlock;
      if (mask_low) {
        _mm256_store_ps(dist_low, low);
        while (mask_low) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
          mask_low &= mask_low - 1;
          update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);
        }
      }
      if (mask_high) {
        _mm256_store_ps(dist_high, high);
        while (mask_high) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
          mask_high &= mask_high - 1;
          update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);
        }
      }
    }
  }
}

bool NativeIVF::block_slot_is_padding(size_t base, int slot) const {
  if (kEnableResidual8Index && residual8_index_) {
    return residual8_blocks_[base + static_cast<size_t>(slot)] == kResidual8Padding;
  }
  return is_padding_slot(blocks_, base, slot);
}

float NativeIVF::block_value(uint32_t centroid, size_t base, int dim, int slot) const {
  const size_t idx = base + static_cast<size_t>(dim) * kVectorsPerBlock +
                     static_cast<size_t>(slot);
  if (kEnableResidual8Index && residual8_index_) {
    const uint32_t block = static_cast<uint32_t>(base / kBlockStride);
    const float c = centroids_[static_cast<size_t>(dim) * k_ + centroid];
    const int half = slot >= 8 ? 1 : 0;
    const float scale = residual8_scale(block, dim, static_cast<int>(centroid), half);
    return c + static_cast<float>(residual8_blocks_[idx]) * scale;
  }
  return static_cast<float>(blocks_[idx]) * vector_scale(dim);
}

float NativeIVF::vector_scale(int dim) const {
  return vector_scales_[static_cast<size_t>(dim)];
}

float NativeIVF::vector_inv_scale(int dim) const {
  return vector_inv_scales_[static_cast<size_t>(dim)];
}

float NativeIVF::residual8_scale(uint32_t block, int dim, int probe, int half) const {
  if (residual8_half_scales_) {
    return residual8_scales_[(static_cast<size_t>(block) * kDim + static_cast<size_t>(dim)) * 2u +
                             static_cast<size_t>(half)];
  }
  if (residual8_block_scales_) {
    return residual8_scales_[static_cast<size_t>(block) * kDim + static_cast<size_t>(dim)];
  }
  return residual8_scales_[static_cast<size_t>(dim) * k_ + static_cast<size_t>(probe)];
}

void NativeIVF::build_residual8_prefilter() {
  const int dims = residual8_filter_dims_;
  if (dims <= 0 || dims > kDim || blocks_.empty() || k_ == 0) {
    residual8_filter_blocks_.clear();
    residual8_filter_scales_.clear();
    residual8_prefilter_ = false;
    residual8_hybrid_repair_ = false;
    return;
  }

  residual8_filter_blocks_.assign(
      static_cast<size_t>(total_blocks_) * static_cast<size_t>(dims) * kVectorsPerBlock,
      kResidual8Padding);
  residual8_filter_scales_.assign(static_cast<size_t>(total_blocks_) * static_cast<size_t>(dims),
                                  0.0f);

  for (uint32_t ci = 0; ci < k_; ++ci) {
    const uint32_t start = offsets_[static_cast<size_t>(ci)];
    const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
    for (uint32_t block = start; block < end; ++block) {
      const size_t src_base = static_cast<size_t>(block) * kBlockStride;
      const size_t dst_base =
          static_cast<size_t>(block) * static_cast<size_t>(dims) * kVectorsPerBlock;
      for (int d = 0; d < dims; ++d) {
        float max_abs = 0.0f;
        const float centroid = centroids_[static_cast<size_t>(d) * k_ + ci];
        for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
          if (is_padding_slot(blocks_, src_base, lane)) continue;
          const size_t src_idx = src_base + static_cast<size_t>(d) * kVectorsPerBlock +
                                 static_cast<size_t>(lane);
          const float value = static_cast<float>(blocks_[src_idx]) * vector_scale(d);
          const float residual = value - centroid;
          const float abs_value = std::fabs(residual);
          if (abs_value > max_abs) max_abs = abs_value;
        }
        float scale = max_abs / 126.0f;
        if (!(scale > 0.0f) || !std::isfinite(scale)) scale = vector_scale(d);
        residual8_filter_scales_[static_cast<size_t>(block) * static_cast<size_t>(dims) +
                                 static_cast<size_t>(d)] = scale;
        int8_t* dst_row = residual8_filter_blocks_.data() + dst_base +
                          static_cast<size_t>(d) * kVectorsPerBlock;
        for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
          if (is_padding_slot(blocks_, src_base, lane)) {
            dst_row[lane] = kResidual8Padding;
            continue;
          }
          const size_t src_idx = src_base + static_cast<size_t>(d) * kVectorsPerBlock +
                                 static_cast<size_t>(lane);
          const float value = static_cast<float>(blocks_[src_idx]) * vector_scale(d);
          dst_row[lane] = quantize_residual8(value - centroid, scale);
        }
      }
    }
  }
}

bool NativeIVF::residual8_prefilter_rejects(uint32_t block, int probe, const float* q,
                                            float worst_sq) const {
  if (!residual8_prefilter_ || residual8_filter_blocks_.empty() ||
      worst_sq >= kInitialTopDist) {
    return false;
  }
  const int dims = residual8_prefilter_dims_;
  const int stride_dims = residual8_filter_dims_;
  if (dims <= 0 || dims > stride_dims) return false;
  const size_t base =
      static_cast<size_t>(block) * static_cast<size_t>(stride_dims) * kVectorsPerBlock;
  __m256 low = _mm256_setzero_ps();
  __m256 high = _mm256_setzero_ps();
  __m256 pad_low = _mm256_setzero_ps();
  __m256 pad_high = _mm256_setzero_ps();
  for (int d = 0; d < dims; ++d) {
    const int8_t* row = residual8_filter_blocks_.data() + base +
                        static_cast<size_t>(d) * kVectorsPerBlock;
    const float scale =
        residual8_filter_scales_[static_cast<size_t>(block) * static_cast<size_t>(stride_dims) +
                                 static_cast<size_t>(d)];
    const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
    accumulate_residual8_dim(row, scale, scale, q_center, d == 0, low, high,
                             pad_low, pad_high);
  }

  const __m256 invalid = _mm256_set1_ps(kInitialTopDist);
  low = _mm256_blendv_ps(low, invalid, pad_low);
  high = _mm256_blendv_ps(high, invalid, pad_high);
  const __m256 threshold = _mm256_set1_ps(worst_sq * residual8_prefilter_slack_);
  const int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, threshold, _CMP_LT_OQ));
  const int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, threshold, _CMP_LT_OQ));
  return (mask_low | mask_high) == 0;
}

void NativeIVF::build_binary_signatures() {
  if (blocks_.empty() || k_ == 0 || total_blocks_ == 0) {
    binary_signatures_.clear();
    binary_prefilter_ = false;
    return;
  }
  binary_signatures_.assign(static_cast<size_t>(total_blocks_) * kVectorsPerBlock, 0);
  for (uint32_t ci = 0; ci < k_; ++ci) {
    const uint32_t start = offsets_[static_cast<size_t>(ci)];
    const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
    for (uint32_t block = start; block < end; ++block) {
      const size_t base = static_cast<size_t>(block) * kBlockStride;
      uint16_t* sig = binary_signatures_.data() + static_cast<size_t>(block) * kVectorsPerBlock;
      for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
        if (is_padding_slot(blocks_, base, lane)) {
          sig[lane] = 0xffffu;
          continue;
        }
        uint16_t bits = 0;
        for (int d = 0; d < kDim; ++d) {
          const size_t idx = base + static_cast<size_t>(d) * kVectorsPerBlock +
                             static_cast<size_t>(lane);
          const float value = static_cast<float>(blocks_[idx]) * vector_scale(d);
          const float centroid = centroids_[static_cast<size_t>(d) * k_ + ci];
          if (value >= centroid) {
            bits = static_cast<uint16_t>(bits | (uint16_t{1} << d));
          }
        }
        sig[lane] = bits;
      }
    }
  }
}

uint16_t NativeIVF::binary_query_signature(const float* q, int probe) const {
  uint16_t bits = 0;
  for (int d = 0; d < kDim; ++d) {
    const float centroid =
        centroids_[static_cast<size_t>(d) * k_ + static_cast<size_t>(probe)];
    if (q[d] >= centroid) {
      bits = static_cast<uint16_t>(bits | (uint16_t{1} << d));
    }
  }
  return bits;
}

bool NativeIVF::binary_prefilter_rejects(int probe, uint32_t block, uint16_t query_signature,
                                         bool quick_call) {
  if (!binary_prefilter_enabled_for(probe)) return false;
  const uint16_t* sig =
      binary_signatures_.data() + static_cast<size_t>(block) * kVectorsPerBlock;
  const uint32_t threshold = static_cast<uint32_t>(binary_prefilter_threshold_for(probe));
  if (profile_enabled_) {
    if (quick_call) {
      ++profile_.quick_binary_checks;
    } else {
      ++profile_.rescore_binary_checks;
    }
    if (binary_prefilter_profile_) {
      const size_t idx = static_cast<size_t>(probe);
      ++profile_.binary_probe_checks[idx];
    }
  }
  for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
    const uint16_t bits = sig[lane];
    if (bits == 0xffffu) continue;
    const uint32_t distance =
        static_cast<uint32_t>(__builtin_popcount(static_cast<unsigned>(bits ^ query_signature)));
    if (distance <= threshold) return false;
  }
  if (profile_enabled_) {
    if (quick_call) {
      ++profile_.quick_binary_skipped_blocks;
    } else {
      ++profile_.rescore_binary_skipped_blocks;
    }
    if (binary_prefilter_profile_) {
      const size_t idx = static_cast<size_t>(probe);
      ++profile_.binary_probe_skipped[idx];
    }
  }
  return true;
}

void NativeIVF::build_block_radii() {
  if (use_block_bounds_) {
    block_min_radii_.assign(static_cast<size_t>(total_blocks_), 0.0f);
    block_max_radii_.assign(static_cast<size_t>(total_blocks_), 0.0f);
    cluster_max_radii_.assign(static_cast<size_t>(k_), 0.0f);
  } else {
    block_min_radii_.clear();
    block_max_radii_.clear();
    cluster_max_radii_.clear();
  }
  if (remaining_repair_bbox_) {
    cluster_bbox_min_.assign(static_cast<size_t>(k_) * kBBoxStride, 0.0f);
    cluster_bbox_max_.assign(static_cast<size_t>(k_) * kBBoxStride, 0.0f);
  } else {
    cluster_bbox_min_.clear();
    cluster_bbox_max_.clear();
  }
  if (use_block_bbox_) {
    block_bbox_min_.assign(static_cast<size_t>(total_blocks_) * kBBoxStride, 0.0f);
    block_bbox_max_.assign(static_cast<size_t>(total_blocks_) * kBBoxStride, 0.0f);
  } else {
    block_bbox_min_.clear();
    block_bbox_max_.clear();
  }
  if (use_hot_bbox_) {
    block_hot_min_.assign(static_cast<size_t>(total_blocks_) * kHotBBoxStride, 0);
    block_hot_max_.assign(static_cast<size_t>(total_blocks_) * kHotBBoxStride, 0);
  } else {
    block_hot_min_.clear();
    block_hot_max_.clear();
  }
  if (use_center_bound_) {
    block_center_.assign(static_cast<size_t>(total_blocks_) * kHotBBoxStride, 0.0f);
    block_center_radius_.assign(static_cast<size_t>(total_blocks_), 0.0f);
  } else {
    block_center_.clear();
    block_center_radius_.clear();
  }

  for (uint32_t ci = 0; ci < k_; ++ci) {
    const uint32_t start = offsets_[static_cast<size_t>(ci)];
    const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
    float* cluster_bbox_min = nullptr;
    float* cluster_bbox_max = nullptr;
    if (remaining_repair_bbox_) {
      const size_t cluster_bbox_base = static_cast<size_t>(ci) * kBBoxStride;
      cluster_bbox_min = cluster_bbox_min_.data() + cluster_bbox_base;
      cluster_bbox_max = cluster_bbox_max_.data() + cluster_bbox_base;
      for (int d = 0; d < kBBoxStride; ++d) {
        cluster_bbox_min[d] = d < kDim ? INFINITY : 0.0f;
        cluster_bbox_max[d] = d < kDim ? -INFINITY : 0.0f;
      }
    }
    for (uint32_t block = start; block < end; ++block) {
      const size_t base = static_cast<size_t>(block) * kBlockStride;
      const size_t bbox_base = static_cast<size_t>(block) * kBBoxStride;
      const size_t hot_base = static_cast<size_t>(block) * kHotBBoxStride;
      float min_dist = INFINITY;
      float max_dist = 0.0f;
      std::array<float, kBBoxStride> bbox_min{};
      std::array<float, kBBoxStride> bbox_max{};
      std::array<int16_t, kHotBBoxStride> hot_min{};
      std::array<int16_t, kHotBBoxStride> hot_max{};
      std::array<float, kHotBBoxStride> center_sum{};
      int center_count = 0;
      if (use_block_bbox_) {
        bbox_min.fill(0.0f);
        bbox_max.fill(0.0f);
        for (int d = 0; d < kDim; ++d) {
          bbox_min[static_cast<size_t>(d)] = INFINITY;
          bbox_max[static_cast<size_t>(d)] = -INFINITY;
        }
      }
      if (use_hot_bbox_) {
        hot_min.fill(0);
        hot_max.fill(0);
        for (int d = 0; d < hot_bbox_dims_; ++d) {
          hot_min[static_cast<size_t>(d)] = 32767;
          hot_max[static_cast<size_t>(d)] = -32768;
        }
      }

      for (int slot = 0; slot < kVectorsPerBlock; ++slot) {
        if (block_slot_is_padding(base, slot)) continue;
        ++center_count;
        float dist = 0.0f;
        for (int d = 0; d < kDim; ++d) {
          const float ref = block_value(ci, base, d, slot);
          if (use_hot_bbox_ && d < hot_bbox_dims_) {
            int raw = static_cast<int>(std::lrint(ref * vector_inv_scale(d)));
            if (raw < -32768) raw = -32768;
            if (raw > 32767) raw = 32767;
            if (raw < hot_min[static_cast<size_t>(d)]) hot_min[static_cast<size_t>(d)] = raw;
            if (raw > hot_max[static_cast<size_t>(d)]) hot_max[static_cast<size_t>(d)] = raw;
          }
          if (use_block_bbox_) {
            if (ref < bbox_min[static_cast<size_t>(d)]) bbox_min[static_cast<size_t>(d)] = ref;
            if (ref > bbox_max[static_cast<size_t>(d)]) bbox_max[static_cast<size_t>(d)] = ref;
          }
          if (remaining_repair_bbox_) {
            if (ref < cluster_bbox_min[d]) cluster_bbox_min[d] = ref;
            if (ref > cluster_bbox_max[d]) cluster_bbox_max[d] = ref;
          }
          if (use_center_bound_ && d < center_bound_dims_) {
            center_sum[static_cast<size_t>(d)] += ref;
          }
          if (use_block_bounds_) {
            const float diff = ref - centroids_[static_cast<size_t>(d) * k_ + ci];
            dist += diff * diff;
          }
        }
        if (use_block_bounds_) {
          min_dist = std::min(min_dist, dist);
          max_dist = std::max(max_dist, dist);
        }
      }

      if (use_center_bound_) {
        const size_t center_base = static_cast<size_t>(block) * kHotBBoxStride;
        if (center_count > 0) {
          const float inv_count = 1.0f / static_cast<float>(center_count);
          for (int d = 0; d < center_bound_dims_; ++d) {
            block_center_[center_base + static_cast<size_t>(d)] =
                center_sum[static_cast<size_t>(d)] * inv_count;
          }

          float max_center_dist = 0.0f;
          for (int slot = 0; slot < kVectorsPerBlock; ++slot) {
            if (block_slot_is_padding(base, slot)) continue;
            float dist = 0.0f;
            for (int d = 0; d < center_bound_dims_; ++d) {
              const float ref = block_value(ci, base, d, slot);
              const float diff = ref - block_center_[center_base + static_cast<size_t>(d)];
              dist += diff * diff;
            }
            if (dist > max_center_dist) max_center_dist = dist;
          }
          block_center_radius_[static_cast<size_t>(block)] = std::sqrt(max_center_dist);
        }
      }

      if (use_block_bounds_) {
        if (!std::isfinite(min_dist)) min_dist = 0.0f;
        const float min_radius = std::sqrt(min_dist);
        const float max_radius = std::sqrt(max_dist);
        block_min_radii_[static_cast<size_t>(block)] = min_radius;
        block_max_radii_[static_cast<size_t>(block)] = max_radius;
        if (max_radius > cluster_max_radii_[static_cast<size_t>(ci)]) {
          cluster_max_radii_[static_cast<size_t>(ci)] = max_radius;
        }
      }
      if (use_block_bbox_) {
        for (int d = 0; d < kBBoxStride; ++d) {
          float mn = bbox_min[static_cast<size_t>(d)];
          float mx = bbox_max[static_cast<size_t>(d)];
          if (!std::isfinite(mn)) mn = 0.0f;
          if (!std::isfinite(mx)) mx = mn;
          block_bbox_min_[bbox_base + static_cast<size_t>(d)] = mn;
          block_bbox_max_[bbox_base + static_cast<size_t>(d)] = mx;
        }
      }
      if (use_hot_bbox_) {
        for (int d = 0; d < kHotBBoxStride; ++d) {
          int16_t mn = hot_min[static_cast<size_t>(d)];
          int16_t mx = hot_max[static_cast<size_t>(d)];
          if (mn == 32767 && mx == -32768) {
            mn = 0;
            mx = 0;
          }
          block_hot_min_[hot_base + static_cast<size_t>(d)] = mn;
          block_hot_max_[hot_base + static_cast<size_t>(d)] = mx;
        }
      }
    }
    if (remaining_repair_bbox_) {
      for (int d = 0; d < kDim; ++d) {
        if (!std::isfinite(cluster_bbox_min[d])) cluster_bbox_min[d] = 0.0f;
        if (!std::isfinite(cluster_bbox_max[d])) cluster_bbox_max[d] = cluster_bbox_min[d];
      }
    }
  }
}

float NativeIVF::block_bbox_lower_bound(const float* q, uint32_t block) const {
  const size_t base = static_cast<size_t>(block) * kBBoxStride;
  const float* mn = block_bbox_min_.data() + base;
  const float* mx = block_bbox_max_.data() + base;
  const __m256 zero = _mm256_setzero_ps();
  const __m256 q0 = _mm256_load_ps(q);
  const __m256 mn0 = _mm256_loadu_ps(mn);
  const __m256 mx0 = _mm256_loadu_ps(mx);

  const __m256 below0 = _mm256_sub_ps(mn0, q0);
  const __m256 above0 = _mm256_sub_ps(q0, mx0);
  const __m256 d0 = _mm256_max_ps(zero, _mm256_max_ps(below0, above0));
  __m256 sq = _mm256_mul_ps(d0, d0);

  if (bbox_dims_ > 8) {
    const __m256 q1 = _mm256_load_ps(q + 8);
    const __m256 mn1 = _mm256_loadu_ps(mn + 8);
    const __m256 mx1 = _mm256_loadu_ps(mx + 8);
    const __m256 below1 = _mm256_sub_ps(mn1, q1);
    const __m256 above1 = _mm256_sub_ps(q1, mx1);
    const __m256 d1 = _mm256_max_ps(zero, _mm256_max_ps(below1, above1));
    sq = _mm256_fmadd_ps(d1, d1, sq);
  }

  const __m128 lo = _mm256_castps256_ps128(sq);
  const __m128 hi = _mm256_extractf128_ps(sq, 1);
  __m128 sum = _mm_add_ps(lo, hi);
  sum = _mm_hadd_ps(sum, sum);
  sum = _mm_hadd_ps(sum, sum);
  return _mm_cvtss_f32(sum);
}

float NativeIVF::cluster_bbox_lower_bound(const float* q, uint32_t centroid) const {
  const size_t base = static_cast<size_t>(centroid) * kBBoxStride;
  const float* mn = cluster_bbox_min_.data() + base;
  const float* mx = cluster_bbox_max_.data() + base;
  float sum = 0.0f;
  for (int d = 0; d < kDim; ++d) {
    float delta = 0.0f;
    if (q[d] < mn[d]) {
      delta = mn[d] - q[d];
    } else if (q[d] > mx[d]) {
      delta = q[d] - mx[d];
    }
    sum += delta * delta;
  }
  return sum;
}

float NativeIVF::block_hot_bbox_lower_bound(const float* q, uint32_t block) const {
  alignas(32) std::array<float, kHotBBoxStride> q_scaled{};
  for (int d = 0; d < hot_bbox_dims_; ++d) {
    q_scaled[static_cast<size_t>(d)] = q[d] * vector_inv_scale(d);
  }
  return block_hot_bbox_lower_bound_scaled(q_scaled.data(), block);
}

float NativeIVF::block_hot_bbox_lower_bound_scaled(const float* q_scaled, uint32_t block) const {
  const size_t base = static_cast<size_t>(block) * kHotBBoxStride;
  const int16_t* mn = block_hot_min_.data() + base;
  const int16_t* mx = block_hot_max_.data() + base;
  const __m128i mn16 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(mn));
  const __m128i mx16 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(mx));
  const __m256 lo = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(mn16));
  const __m256 hi = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(mx16));
  const __m256 qv = _mm256_load_ps(q_scaled);
  const __m256 zero = _mm256_setzero_ps();
  const __m256 below = _mm256_sub_ps(lo, qv);
  const __m256 above = _mm256_sub_ps(qv, hi);
  const __m256 delta = _mm256_max_ps(zero, _mm256_max_ps(below, above));
  const __m256 sq = _mm256_mul_ps(delta, delta);
  const __m128 sum128 =
      _mm_add_ps(_mm256_castps256_ps128(sq), _mm256_extractf128_ps(sq, 1));
  const __m128 sum64 = _mm_hadd_ps(sum128, sum128);
  const __m128 sum32 = _mm_hadd_ps(sum64, sum64);
  return _mm_cvtss_f32(sum32) * kVectorScaleSq;
}

float NativeIVF::block_center_distance_sq(const float* q8, uint32_t block) const {
  const size_t base = static_cast<size_t>(block) * kHotBBoxStride;
  const __m256 center = _mm256_loadu_ps(block_center_.data() + base);
  const __m256 qv = _mm256_load_ps(q8);
  const __m256 diff = _mm256_sub_ps(center, qv);
  const __m256 sq = _mm256_mul_ps(diff, diff);
  const __m128 sum128 =
      _mm_add_ps(_mm256_castps256_ps128(sq), _mm256_extractf128_ps(sq, 1));
  const __m128 sum64 = _mm_hadd_ps(sum128, sum128);
  const __m128 sum32 = _mm_hadd_ps(sum64, sum64);
  return _mm_cvtss_f32(sum32);
}

void NativeIVF::bound_probe_range(uint32_t& start, uint32_t& end, float center_dist_sq,
                                  float worst_dist_sq, float* center_dist_out) const {
  const float center_dist = std::sqrt(center_dist_sq);
  if (center_dist_out) *center_dist_out = center_dist;

  if (start >= end || worst_dist_sq >= kInitialTopDist || block_min_radii_.empty() ||
      block_min_radii_.size() != block_max_radii_.size()) {
    return;
  }

  const float best_dist = std::sqrt(worst_dist_sq);
  const float lower_radius = center_dist - best_dist;
  const float upper_radius = center_dist + best_dist;

  if (lower_radius > 0.0f) {
    uint32_t lo = start;
    uint32_t hi = end;
    while (lo < hi) {
      const uint32_t mid = (lo + hi) >> 1;
      if (block_max_radii_[static_cast<size_t>(mid)] < lower_radius) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    start = lo;
  }

  uint32_t lo = start;
  uint32_t hi = end;
  while (lo < hi) {
    const uint32_t mid = (lo + hi) >> 1;
    if (block_min_radii_[static_cast<size_t>(mid)] <= upper_radius) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  end = lo;
}

uint32_t NativeIVF::find_radius_pivot(uint32_t start, uint32_t end, float center_dist) const {
  if (start + 1 >= end) return start;

  uint32_t lo = start;
  uint32_t hi = end;
  while (lo < hi) {
    const uint32_t mid = (lo + hi) >> 1;
    if (block_max_radii_[static_cast<size_t>(mid)] < center_dist) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  if (lo >= end) return end - 1;
  if (lo == start) return start;

  const uint32_t prev = lo - 1;
  const float prev_mid =
      (block_min_radii_[static_cast<size_t>(prev)] + block_max_radii_[static_cast<size_t>(prev)]) *
      0.5f;
  const float cur_mid =
      (block_min_radii_[static_cast<size_t>(lo)] + block_max_radii_[static_cast<size_t>(lo)]) *
      0.5f;
  return std::fabs(center_dist - prev_mid) <= std::fabs(cur_mid - center_dist) ? prev : lo;
}

void NativeIVF::scan_quick8_radial(const float* q) {
  int worst = 0;
  const __m256 scale = _mm256_set1_ps(kVectorScale);
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d]);
  }

#define NATIVE_IVF_SCAN_QUICK8_BLOCK(BLOCK_VALUE)                                                   \
  do {                                                                                              \
    const uint32_t scan_block = (BLOCK_VALUE);                                                      \
    if (residual8_prefilter_quick_label_ && residual8_prefilter_enabled_for(probe)) {                \
      const bool residual8_reject = residual8_prefilter_rejects(                                    \
          scan_block, probe, q, top_dist_[static_cast<size_t>(worst)]);                              \
      record_quick_residual8_prefilter(probe, residual8_reject);                                    \
      if (residual8_reject) {                                                                        \
        break;                                                                                      \
      }                                                                                             \
    }                                                                                               \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;          \
    __m256 low = _mm256_setzero_ps();                                                               \
    __m256 high = _mm256_setzero_ps();                                                              \
                                                                                                    \
    for (int d = 0; d < 6; ++d) {                                                                   \
      const int16_t* row = base + d * kVectorsPerBlock;                                             \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));               \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));          \
      const __m256 vf_low =                                                                         \
          _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);                 \
      const __m256 vf_high =                                                                        \
          _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);                \
      const __m256 qv = qv_by_dim[d];                                                               \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                  \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                 \
      low = _mm256_fmadd_ps(dl, dl, low);                                                           \
      high = _mm256_fmadd_ps(dh, dh, high);                                                         \
    }                                                                                               \
                                                                                                    \
    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);                   \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                     \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                   \
    if ((mask_low | mask_high) != 0) {                                                              \
      const bool low_alive = mask_low != 0;                                                         \
      const bool high_alive = mask_high != 0;                                                       \
      for (int d = 6; d < kDim; ++d) {                                                              \
        const int16_t* row = base + d * kVectorsPerBlock;                                           \
        const __m256 qv = qv_by_dim[d];                                                             \
        if (low_alive) {                                                                            \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));           \
          const __m256 vf_low =                                                                     \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);             \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                              \
          low = _mm256_fmadd_ps(dl, dl, low);                                                       \
        }                                                                                           \
        if (high_alive) {                                                                           \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));      \
          const __m256 vf_high =                                                                    \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);            \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                             \
          high = _mm256_fmadd_ps(dh, dh, high);                                                     \
        }                                                                                           \
      }                                                                                             \
                                                                                                    \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;       \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;    \
      if ((mask_low | mask_high) != 0) {                                                            \
        const uint8_t* label_base =                                                                 \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                   \
        if (mask_low) {                                                                             \
          _mm256_store_ps(dist_low, low);                                                           \
          while (mask_low) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                         \
            mask_low &= mask_low - 1;                                                               \
            update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);         \
          }                                                                                         \
        }                                                                                           \
        if (mask_high) {                                                                            \
          _mm256_store_ps(dist_high, high);                                                         \
          while (mask_high) {                                                                       \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                        \
            mask_high &= mask_high - 1;                                                             \
            update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);    \
          }                                                                                         \
        }                                                                                           \
      }                                                                                             \
    }                                                                                               \
  } while (0)

  if (quick_dynamic_bounds_) {
    for (int pi = 0; pi < kQuickProbe; ++pi) {
      const int probe = probes_[static_cast<size_t>(pi)];
      uint32_t start = offsets_[static_cast<size_t>(probe)];
      uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
      float center_dist = 0.0f;
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
      if (start >= end) continue;

      for (uint32_t block = start; block < end; ++block) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        NATIVE_IVF_SCAN_QUICK8_BLOCK(block);
      }
    }
  } else {
    for (int pi = 0; pi < kQuickProbe; ++pi) {
      const int probe = probes_[static_cast<size_t>(pi)];
      uint32_t start = offsets_[static_cast<size_t>(probe)];
      uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)]);
      for (uint32_t block = start; block < end; ++block) {
        NATIVE_IVF_SCAN_QUICK8_BLOCK(block);
      }
    }
  }

#undef NATIVE_IVF_SCAN_QUICK8_BLOCK
}

void NativeIVF::scan_quick8_labelskip_radial(const float* q) {
  int worst = 0;
  alignas(32) __m256 scale_by_dim[kDim];
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    scale_by_dim[d] = _mm256_set1_ps(vector_scale(d));
    qv_by_dim[d] = _mm256_set1_ps(q[d]);
  }

  bool cached_label_skip_active = false;
  uint16_t cached_label_skip_mask = 0;
  auto cluster_skip_matches = [&](int probe) {
    if (!label_skip_cluster_ || !cached_label_skip_active || cluster_label_pure_.empty()) {
      return false;
    }
    const int8_t pure = cluster_label_pure_[static_cast<size_t>(probe)];
    return (cached_label_skip_mask == 0 && pure == 0) ||
           (cached_label_skip_mask == 0xffffu && pure == 1);
  };
  auto refresh_label_skip_cache = [&]() {
    cached_label_skip_active = false;
    const float worst_dist = top_dist_[static_cast<size_t>(worst)];
    if (worst_dist >= kInitialTopDist) return;
    if (label_skip_max_worst_ > 0.0f && worst_dist > label_skip_max_worst_) return;
    const uint8_t label = fraud_label(top_label_[0]);
    if (fraud_label(top_label_[1]) != label || fraud_label(top_label_[2]) != label ||
        fraud_label(top_label_[3]) != label || fraud_label(top_label_[4]) != label) {
      return;
    }
    cached_label_skip_mask = label == 0 ? 0 : 0xffffu;
    cached_label_skip_active = true;
  };

#define NATIVE_IVF_SCAN_QUICK8_LABEL_BLOCK(BLOCK_VALUE)                                             \
  do {                                                                                              \
    const uint32_t scan_block = (BLOCK_VALUE);                                                      \
    if (residual8_prefilter_quick_label_ && residual8_prefilter_enabled_for(probe)) {                \
      const bool residual8_reject = residual8_prefilter_rejects(                                    \
          scan_block, probe, q, top_dist_[static_cast<size_t>(worst)]);                              \
      record_quick_residual8_prefilter(probe, residual8_reject);                                    \
      if (residual8_reject) {                                                                        \
        break;                                                                                      \
      }                                                                                             \
    }                                                                                               \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;          \
    __m256 low = _mm256_setzero_ps();                                                               \
    __m256 high = _mm256_setzero_ps();                                                              \
    bool top_changed = false;                                                                       \
    if (profile_enabled_) ++profile_.quick_blocks_scanned;                                          \
                                                                                                    \
    for (int d = 0; d < 6; ++d) {                                                                   \
      const int16_t* row = base + d * kVectorsPerBlock;                                             \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));               \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));          \
      const __m256 scale = scale_by_dim[d];                                                         \
      const __m256 vf_low =                                                                         \
          _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);                 \
      const __m256 vf_high =                                                                        \
          _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);                \
      const __m256 qv = qv_by_dim[d];                                                               \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                  \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                 \
      low = _mm256_fmadd_ps(dl, dl, low);                                                           \
      high = _mm256_fmadd_ps(dh, dh, high);                                                         \
    }                                                                                               \
                                                                                                    \
    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);                   \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                     \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                   \
    if (profile_enabled_) {                                                                         \
      const int stage1_low_lanes = __builtin_popcount(static_cast<unsigned>(mask_low));             \
      const int stage1_high_lanes = __builtin_popcount(static_cast<unsigned>(mask_high));           \
      profile_.quick_stage1_low_lanes += static_cast<uint64_t>(stage1_low_lanes);                   \
      profile_.quick_stage1_high_lanes += static_cast<uint64_t>(stage1_high_lanes);                 \
      if ((mask_low | mask_high) == 0) {                                                            \
        ++profile_.quick_stage1_dead_blocks;                                                        \
        ++profile_.quick_stage1_low_dead_halves;                                                    \
        ++profile_.quick_stage1_high_dead_halves;                                                   \
      } else {                                                                                      \
        ++profile_.quick_stage1_alive_blocks;                                                       \
        if (mask_low == 0) ++profile_.quick_stage1_low_dead_halves;                                 \
        if (mask_high == 0) ++profile_.quick_stage1_high_dead_halves;                               \
        if (mask_low != 0 && mask_high != 0) ++profile_.quick_stage1_both_alive_halves;             \
      }                                                                                             \
    }                                                                                               \
    if ((mask_low | mask_high) != 0) {                                                              \
      const bool low_alive = mask_low != 0;                                                         \
      const bool high_alive = mask_high != 0;                                                       \
      for (int d = 6; d < kDim; ++d) {                                                              \
        const int16_t* row = base + d * kVectorsPerBlock;                                           \
        const __m256 qv = qv_by_dim[d];                                                             \
        if (low_alive) {                                                                            \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));           \
          const __m256 scale = scale_by_dim[d];                                                     \
          const __m256 vf_low =                                                                     \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);             \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                              \
          low = _mm256_fmadd_ps(dl, dl, low);                                                       \
        }                                                                                           \
        if (high_alive) {                                                                           \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));      \
          const __m256 scale = scale_by_dim[d];                                                     \
          const __m256 vf_high =                                                                    \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);            \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                             \
          high = _mm256_fmadd_ps(dh, dh, high);                                                     \
        }                                                                                           \
      }                                                                                             \
                                                                                                    \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;       \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;    \
      if (profile_enabled_) {                                                                       \
        const int final_lanes = __builtin_popcount(static_cast<unsigned>(mask_low)) +               \
                                __builtin_popcount(static_cast<unsigned>(mask_high));                \
        if (final_lanes == 0) {                                                                      \
          ++profile_.quick_final_dead_blocks;                                                       \
        } else {                                                                                    \
          profile_.quick_final_lanes += static_cast<uint64_t>(final_lanes);                         \
        }                                                                                           \
      }                                                                                             \
      if ((mask_low | mask_high) != 0) {                                                            \
        const uint8_t* label_base =                                                                 \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                   \
        if (mask_low) {                                                                             \
          _mm256_store_ps(dist_low, low);                                                           \
          while (mask_low) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                         \
            mask_low &= mask_low - 1;                                                               \
            const int slot = static_cast<int>(scan_block * kVectorsPerBlock + bit);                 \
            const bool accepted =                                                                   \
                (local_repair_ || local_fraud_graph_enabled_)                                       \
                    ? update_candidate_slot(dist_low[bit], label_base[bit], slot, top_dist_,        \
                                            top_label_, top_slot_, worst)                           \
                    : update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_,       \
                                       worst);                                                       \
            if (profile_enabled_ && accepted) ++profile_.quick_top5_accepts;                        \
            top_changed |= accepted;                                                                \
          }                                                                                         \
        }                                                                                           \
        if (mask_high) {                                                                            \
          _mm256_store_ps(dist_high, high);                                                         \
          while (mask_high) {                                                                       \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                        \
            mask_high &= mask_high - 1;                                                             \
            const int slot = static_cast<int>(scan_block * kVectorsPerBlock + 8 + bit);             \
            const bool accepted =                                                                   \
                (local_repair_ || local_fraud_graph_enabled_)                                       \
                    ? update_candidate_slot(dist_high[bit], label_base[8 + bit], slot, top_dist_,   \
                                            top_label_, top_slot_, worst)                           \
                    : update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, \
                                       worst);                                                       \
            if (profile_enabled_ && accepted) ++profile_.quick_top5_accepts;                        \
            top_changed |= accepted;                                                                \
          }                                                                                         \
        }                                                                                           \
        if (top_changed) refresh_label_skip_cache();                                                \
      }                                                                                             \
    }                                                                                               \
  } while (0)

  for (int pi = 0; pi < kQuickProbe; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    const uint32_t raw_start = start;
    const uint32_t raw_end = end;
    if (profile_enabled_) {
      profile_.quick_probe_blocks += static_cast<uint64_t>(raw_end - raw_start);
    }
    if (cluster_skip_matches(probe)) {
      if (profile_enabled_) {
        profile_.quick_label_skipped_blocks += static_cast<uint64_t>(end - start);
      }
      continue;
    }
    float center_dist = 0.0f;
    bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                      top_dist_[static_cast<size_t>(worst)],
                      quick_periodic_upper_bound_ ? &center_dist : nullptr);
    if (profile_enabled_) {
      const uint64_t raw_blocks = static_cast<uint64_t>(raw_end - raw_start);
      const uint64_t live_blocks = static_cast<uint64_t>(end - start);
      profile_.quick_bound_skipped_blocks += raw_blocks - live_blocks;
      if (live_blocks == 0) ++profile_.quick_bound_empty_probes;
    }
    const bool binary_probe_enabled = binary_prefilter_enabled_for(probe);
    const uint16_t binary_qsig =
        binary_probe_enabled ? binary_query_signature(q, probe) : uint16_t{0};
    uint32_t next_upper_check = start;
    const uint32_t upper_check_step = static_cast<uint32_t>(quick_periodic_upper_step_);
    if (cached_label_skip_active && label_skip_sparse_ &&
        !label_skip_nonzero_offsets_.empty()) {
      const bool skip_zero = cached_label_skip_mask == 0;
      const std::vector<uint32_t>& sparse_offsets =
          skip_zero ? label_skip_nonzero_offsets_ : label_skip_notfull_offsets_;
      const std::vector<uint32_t>& sparse_blocks =
          skip_zero ? label_skip_nonzero_blocks_ : label_skip_notfull_blocks_;
      const size_t sparse_begin = sparse_offsets[static_cast<size_t>(probe)];
      const size_t sparse_end = sparse_offsets[static_cast<size_t>(probe + 1)];
      const auto it =
          std::lower_bound(sparse_blocks.begin() + static_cast<std::ptrdiff_t>(sparse_begin),
                           sparse_blocks.begin() + static_cast<std::ptrdiff_t>(sparse_end), start);
      const auto stop = std::lower_bound(
          it, sparse_blocks.begin() + static_cast<std::ptrdiff_t>(sparse_end), end);
      const size_t live_begin = static_cast<size_t>(it - sparse_blocks.begin());
      const size_t live_end = static_cast<size_t>(stop - sparse_blocks.begin());
      if (profile_enabled_) {
        const uint64_t scanned = static_cast<uint64_t>(live_end - live_begin);
        profile_.quick_label_skipped_blocks += static_cast<uint64_t>(end - start) - scanned;
      }
      for (size_t live_idx = live_begin; live_idx < live_end; ++live_idx) {
        const uint32_t block = sparse_blocks[live_idx];
        if (quick_periodic_upper_bound_ && block >= next_upper_check) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) {
            if (profile_enabled_) {
              ++profile_.quick_dynamic_high_breaks;
              profile_.quick_dynamic_high_skipped_blocks += static_cast<uint64_t>(end - block);
            }
            break;
          }
          next_upper_check = block + upper_check_step;
        }
        if (binary_probe_enabled && binary_prefilter_rejects(probe, block, binary_qsig, true)) {
          continue;
        }
        NATIVE_IVF_SCAN_QUICK8_LABEL_BLOCK(block);
      }
      continue;
    }
    for (uint32_t block = start; block < end; ++block) {
      if (quick_periodic_upper_bound_ && block >= next_upper_check) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) {
          if (profile_enabled_) {
            ++profile_.quick_dynamic_high_breaks;
            profile_.quick_dynamic_high_skipped_blocks += static_cast<uint64_t>(end - block);
          }
          break;
        }
        next_upper_check = block + upper_check_step;
      }
      if (cached_label_skip_active) {
        const uint16_t block_mask = block_label_masks_[static_cast<size_t>(block)];
        if (block_mask == cached_label_skip_mask) {
          if (profile_enabled_) ++profile_.quick_label_skipped_blocks;
          continue;
        }
      }
      if (binary_probe_enabled && binary_prefilter_rejects(probe, block, binary_qsig, true)) {
        continue;
      }
      NATIVE_IVF_SCAN_QUICK8_LABEL_BLOCK(block);
      if (cluster_skip_matches(probe)) {
        if (profile_enabled_ && block + 1 < end) {
          profile_.quick_label_skipped_blocks += static_cast<uint64_t>(end - block - 1);
        }
        break;
      }
    }
  }

#undef NATIVE_IVF_SCAN_QUICK8_LABEL_BLOCK
}

void NativeIVF::scan_probes_bounded_active_scaled(const float* q, int from, int to) {
  const int dims = active_dims_;
  if (dims >= kDim) {
    scan_probes_bounded(q, from, to, false);
    return;
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool quick_call = from == 0 && to == quick_probe_;
  int worst = 0;
  constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < dims; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
  }

  const bool label_skip_enabled = label_skip_ && !block_label_masks_.empty();
  const bool label_skip_cached = label_skip_enabled && label_skip_cached_;
  bool cached_label_skip_active = false;
  uint16_t cached_label_skip_mask = 0;
  auto refresh_label_skip_cache = [&]() {
    cached_label_skip_active = false;
    const float worst_dist = top_dist_[static_cast<size_t>(worst)];
    if (worst_dist >= kInitialTopDist) return;
    if (label_skip_max_worst_ > 0.0f && worst_dist > label_skip_max_worst_) return;
    const uint8_t label = fraud_label(top_label_[0]);
    if (fraud_label(top_label_[1]) != label || fraud_label(top_label_[2]) != label ||
        fraud_label(top_label_[3]) != label || fraud_label(top_label_[4]) != label) {
      return;
    }
    cached_label_skip_mask = label == 0 ? 0 : 0xffffu;
    cached_label_skip_active = true;
  };
  auto cluster_skip_matches = [&](int probe) {
    if (!label_skip_cluster_ || !cached_label_skip_active || cluster_label_pure_.empty()) {
      return false;
    }
    const int8_t pure = cluster_label_pure_[static_cast<size_t>(probe)];
    return (cached_label_skip_mask == 0 && pure == 0) ||
           (cached_label_skip_mask == 0xffffu && pure == 1);
  };

#define NATIVE_IVF_SCAN_ACTIVE_BLOCK(BLOCK_VALUE)                                                    \
  do {                                                                                               \
    const uint32_t scan_block = (BLOCK_VALUE);                                                       \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;           \
    __m256 low = _mm256_setzero_ps();                                                                \
    __m256 high = _mm256_setzero_ps();                                                               \
    bool top_changed = false;                                                                        \
    if (profile_enabled_) ++profile_.quick_blocks_scanned;                                           \
                                                                                                     \
    const int stage_dims = dims < 6 ? dims : 6;                                                       \
    for (int d = 0; d < stage_dims; ++d) {                                                           \
      const int16_t* row = base + d * kVectorsPerBlock;                                              \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));                \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));           \
      const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                      \
      const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                    \
      const __m256 qv = qv_by_dim[d];                                                                \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                   \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                  \
      low = _mm256_fmadd_ps(dl, dl, low);                                                            \
      high = _mm256_fmadd_ps(dh, dh, high);                                                          \
    }                                                                                                \
                                                                                                     \
    const __m256 worst_v =                                                                           \
        _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);                         \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                      \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                    \
    if ((mask_low | mask_high) != 0) {                                                               \
      const bool low_alive = mask_low != 0;                                                          \
      const bool high_alive = mask_high != 0;                                                        \
      for (int d = stage_dims; d < dims; ++d) {                                                      \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m256 qv = qv_by_dim[d];                                                              \
        if (low_alive) {                                                                             \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));            \
          const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                  \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                               \
          low = _mm256_fmadd_ps(dl, dl, low);                                                        \
        }                                                                                            \
        if (high_alive) {                                                                            \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));       \
          const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                              \
          high = _mm256_fmadd_ps(dh, dh, high);                                                      \
        }                                                                                            \
      }                                                                                              \
                                                                                                     \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;        \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;     \
      if ((mask_low | mask_high) != 0) {                                                             \
        const uint8_t* label_base =                                                                  \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                    \
        if (mask_low) {                                                                              \
          _mm256_store_ps(dist_low, low);                                                            \
          while (mask_low) {                                                                         \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                          \
            mask_low &= mask_low - 1;                                                                \
            top_changed |= update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit],         \
                                            top_dist_, top_label_, worst);                           \
          }                                                                                          \
        }                                                                                            \
        if (mask_high) {                                                                             \
          _mm256_store_ps(dist_high, high);                                                          \
          while (mask_high) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                         \
            mask_high &= mask_high - 1;                                                              \
            top_changed |= update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit],   \
                                            top_dist_, top_label_, worst);                           \
          }                                                                                          \
        }                                                                                            \
        if (top_changed && label_skip_cached) refresh_label_skip_cache();                            \
      }                                                                                              \
    }                                                                                                \
  } while (0)

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    const uint32_t raw_start = start;
    const uint32_t raw_end = end;
    if (profile_enabled_ && quick_call) {
      profile_.quick_probe_blocks += static_cast<uint64_t>(raw_end - raw_start);
    }
    if (cluster_skip_matches(probe)) {
      if (profile_enabled_) {
        const uint64_t skipped = static_cast<uint64_t>(end - start);
        if (quick_call) {
          profile_.quick_label_skipped_blocks += skipped;
        } else {
          profile_.rescore_label_skipped_blocks += skipped;
        }
      }
      continue;
    }

    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (profile_enabled_ && quick_call && radial_enabled) {
      const uint64_t raw_blocks = static_cast<uint64_t>(raw_end - raw_start);
      const uint64_t live_blocks = static_cast<uint64_t>(end - start);
      profile_.quick_bound_skipped_blocks += raw_blocks - live_blocks;
      if (live_blocks == 0) ++profile_.quick_bound_empty_probes;
    }
    if (start >= end) continue;

    for (uint32_t block = start; block < end; ++block) {
      if (quick_dynamic_bounds_ && radial_enabled) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
      }
      if (label_skip_enabled) {
        const bool skip_by_label =
            label_skip_cached
                ? (cached_label_skip_active &&
                   block_label_masks_[static_cast<size_t>(block)] == cached_label_skip_mask)
                : label_skip_rejects(block, worst);
        if (skip_by_label) {
          if (profile_enabled_) {
            if (quick_call) {
              ++profile_.quick_label_skipped_blocks;
            } else {
              ++profile_.rescore_label_skipped_blocks;
            }
          }
          continue;
        }
      }
      NATIVE_IVF_SCAN_ACTIVE_BLOCK(block);
      if (cluster_skip_matches(probe)) {
        if (profile_enabled_ && block + 1 < end) {
          const uint64_t skipped = static_cast<uint64_t>(end - block - 1);
          if (quick_call) {
            profile_.quick_label_skipped_blocks += skipped;
          } else {
            profile_.rescore_label_skipped_blocks += skipped;
          }
        }
        break;
      }
    }
  }

#undef NATIVE_IVF_SCAN_ACTIVE_BLOCK
}

void NativeIVF::scan_probes_bounded(const float* q, int from, int to, bool use_bbox) {
  if (kEnableResidual8Index && residual8_index_) {
    scan_probes_bounded_residual8(q, from, to, use_bbox);
    return;
  }
  if (active_dims_ < kDim) {
    scan_probes_bounded_active_scaled(q, from, to);
    return;
  }
  if (scan_scaled_) {
    scan_probes_bounded_scaled(q, from, to, use_bbox);
    return;
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool bbox_enabled = use_bbox && use_block_bbox_ && !block_bbox_min_.empty();
  const bool quick_call = from == 0 && to == quick_probe_;
  const bool hot_bbox_enabled = use_hot_bbox_ && !block_hot_min_.empty() &&
                                (quick_call ? quick_hot_bbox_ : rescore_hot_bbox_);
  const bool center_phase_enabled = quick_call || from >= center_bound_min_from_;
  const bool center_bound_enabled = use_center_bound_ && !block_center_.empty() &&
                                    center_phase_enabled &&
                                    (quick_call ? quick_center_bound_ : rescore_center_bound_);
  if (!radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    scan_probes(q, from, to);
    return;
  }

  if (quick8_specialized_ && quick_call && quick_probe_ == kQuickProbe &&
      radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled &&
      !quick_radius_order_ && !scan_stage8_) {
    scan_quick8_radial(q);
    return;
  }

  if (quick8_label_skip_specialized_ && quick_call && quick_probe_ == kQuickProbe &&
      radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled &&
      !quick_radius_order_ && !quick_dynamic_bounds_ && !scan_stage8_ && label_skip_ &&
      label_skip_cached_ && !block_label_masks_.empty()) {
    scan_quick8_labelskip_radial(q);
    return;
  }

  if (radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled &&
      !quick_radius_order_) {
    int worst = 0;
    const __m256 scale = _mm256_set1_ps(kVectorScale);
    alignas(32) float dist_low[8];
    alignas(32) float dist_high[8];
    alignas(32) __m256 qv_by_dim[kDim];
	    for (int d = 0; d < kDim; ++d) {
	      qv_by_dim[d] = _mm256_set1_ps(q[d]);
	    }
	    const bool label_skip_enabled = label_skip_ && !block_label_masks_.empty();
	    const bool label_skip_cached = label_skip_enabled && label_skip_cached_;
	    bool cached_label_skip_active = false;
	    uint16_t cached_label_skip_mask = 0;
	    auto refresh_label_skip_cache = [&]() {
	      cached_label_skip_active = false;
	      const float worst_dist = top_dist_[static_cast<size_t>(worst)];
	      if (worst_dist >= kInitialTopDist) return;
	      if (label_skip_max_worst_ > 0.0f && worst_dist > label_skip_max_worst_) return;
	      const uint8_t label = fraud_label(top_label_[0]);
	      if (fraud_label(top_label_[1]) != label || fraud_label(top_label_[2]) != label ||
	          fraud_label(top_label_[3]) != label || fraud_label(top_label_[4]) != label) {
	        return;
	      }
	      cached_label_skip_mask = label == 0 ? 0 : 0xffffu;
	      cached_label_skip_active = true;
	    };
#define NATIVE_IVF_SCAN_RADIAL_BLOCK(BLOCK_VALUE)                                                   \
	    do {                                                                                             \
	      const uint32_t scan_block = (BLOCK_VALUE);                                                     \
	      const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;         \
	      __m256 low = _mm256_setzero_ps();                                                              \
	      __m256 high = _mm256_setzero_ps();                                                             \
	      bool top_changed = false;                                                                      \
                                                                                                     \
      for (int d = 0; d < 6; ++d) {                                                                  \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));              \
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));         \
        const __m256 vf_low =                                                                        \
            _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);                \
        const __m256 vf_high =                                                                       \
            _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);               \
        const __m256 qv = qv_by_dim[d];                                                              \
        const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                 \
        const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                \
        low = _mm256_fmadd_ps(dl, dl, low);                                                          \
        high = _mm256_fmadd_ps(dh, dh, high);                                                        \
      }                                                                                              \
                                                                                                     \
      const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);                  \
      int mask_low =                                                                                 \
          rescore_radius_order_                                                                      \
              ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LE_OQ))                          \
              : _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                         \
      int mask_high =                                                                                \
          rescore_radius_order_                                                                      \
              ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LE_OQ))                         \
              : _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                        \
      if ((mask_low | mask_high) != 0) {                                                             \
        bool low_alive = mask_low != 0;                                                              \
        bool high_alive = mask_high != 0;                                                            \
        int tail_start = 6;                                                                          \
        if (scan_stage8_) {                                                                          \
          for (int d = 6; d < 8; ++d) {                                                              \
            const int16_t* row = base + d * kVectorsPerBlock;                                        \
            const __m256 qv = qv_by_dim[d];                                                          \
            if (low_alive) {                                                                         \
              const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));        \
              const __m256 vf_low =                                                                  \
                  _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);          \
              const __m256 dl = _mm256_sub_ps(vf_low, qv);                                           \
              low = _mm256_fmadd_ps(dl, dl, low);                                                    \
            }                                                                                        \
            if (high_alive) {                                                                        \
              const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));   \
              const __m256 vf_high =                                                                 \
                  _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);         \
              const __m256 dh = _mm256_sub_ps(vf_high, qv);                                          \
              high = _mm256_fmadd_ps(dh, dh, high);                                                  \
            }                                                                                        \
          }                                                                                          \
          mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;    \
          mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0; \
          low_alive = mask_low != 0;                                                                 \
          high_alive = mask_high != 0;                                                               \
          tail_start = 8;                                                                            \
        }                                                                                            \
        if ((mask_low | mask_high) != 0) {                                                           \
        for (int d = tail_start; d < kDim; ++d) {                                                    \
          const int16_t* row = base + d * kVectorsPerBlock;                                          \
          const __m256 qv = qv_by_dim[d];                                                            \
          if (low_alive) {                                                                           \
            const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));          \
            const __m256 vf_low =                                                                    \
                _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);            \
            const __m256 dl = _mm256_sub_ps(vf_low, qv);                                             \
            low = _mm256_fmadd_ps(dl, dl, low);                                                      \
          }                                                                                          \
          if (high_alive) {                                                                          \
            const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));     \
            const __m256 vf_high =                                                                   \
                _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);           \
            const __m256 dh = _mm256_sub_ps(vf_high, qv);                                            \
            high = _mm256_fmadd_ps(dh, dh, high);                                                    \
          }                                                                                          \
        }                                                                                            \
                                                                                                     \
        mask_low = low_alive                                                                         \
                       ? (rescore_radius_order_                                                      \
                              ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LE_OQ))          \
                              : _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)))         \
                       : 0;                                                                          \
        mask_high = high_alive                                                                       \
                        ? (rescore_radius_order_                                                     \
                               ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LE_OQ))        \
                               : _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)))       \
                        : 0;                                                                         \
        }                                                                                            \
        if ((mask_low | mask_high) != 0) {                                                           \
          const uint8_t* label_base =                                                                \
              labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                  \
          if (mask_low) {                                                                            \
            _mm256_store_ps(dist_low, low);                                                          \
	            while (mask_low) {                                                                       \
	              const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                        \
	              mask_low &= mask_low - 1;                                                             \
	              top_changed |=                                                                         \
	                  update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);    \
	            }                                                                                        \
	          }                                                                                          \
	          if (mask_high) {                                                                           \
            _mm256_store_ps(dist_high, high);                                                        \
	            while (mask_high) {                                                                      \
	              const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                       \
	              mask_high &= mask_high - 1;                                                            \
	              top_changed |= update_candidate(dist_high[bit], label_base[8 + bit], top_dist_,        \
	                                              top_label_, worst);                                    \
	            }                                                                                        \
	          }                                                                                          \
	          if (top_changed && label_skip_cached) refresh_label_skip_cache();                           \
	        }                                                                                            \
	      }                                                                                              \
	    } while (0)

    for (int pi = from; pi < to; ++pi) {
      const int probe = probes_[static_cast<size_t>(pi)];
      uint32_t start = offsets_[static_cast<size_t>(probe)];
      uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
      if (label_skip_cluster_ && label_skip_cached && cached_label_skip_active &&
          !cluster_label_pure_.empty()) {
        const int8_t pure = cluster_label_pure_[static_cast<size_t>(probe)];
        if ((cached_label_skip_mask == 0 && pure == 0) ||
            (cached_label_skip_mask == 0xffffu && pure == 1)) {
          if (profile_enabled_) {
            const uint64_t skipped = static_cast<uint64_t>(end - start);
            if (quick_call) {
              profile_.quick_label_skipped_blocks += skipped;
            } else {
              profile_.rescore_label_skipped_blocks += skipped;
            }
          }
          continue;
        }
      }
      float center_dist = 0.0f;
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
      if (start >= end) continue;
      const bool binary_probe_enabled = binary_prefilter_enabled_for(probe);
      const uint16_t binary_qsig =
          binary_probe_enabled ? binary_query_signature(q, probe) : uint16_t{0};

	      if (label_skip_enabled) {
	        for (uint32_t block = start; block < end; ++block) {
          if (quick_dynamic_bounds_) {
            const float worst_sq = top_dist_[static_cast<size_t>(worst)];
            const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
            if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
            const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
            if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
          }
	          const bool skip_by_label =
	              label_skip_cached
	                  ? (cached_label_skip_active &&
	                     block_label_masks_[static_cast<size_t>(block)] == cached_label_skip_mask)
	                  : label_skip_rejects(block, worst);
	          if (skip_by_label) {
            if (profile_enabled_) {
              if (quick_call) {
                ++profile_.quick_label_skipped_blocks;
              } else {
                ++profile_.rescore_label_skipped_blocks;
              }
            }
            continue;
          }
          if (binary_probe_enabled &&
              binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
            continue;
          }
          NATIVE_IVF_SCAN_RADIAL_BLOCK(block);
        }
      } else if (prefetch_blocks_) {
        for (uint32_t block = start; block < end; ++block) {
          if (block + 1 < end) {
            __builtin_prefetch(blocks_.data() + static_cast<size_t>(block + 1) * kBlockStride,
                               0, 1);
          }
          if (quick_dynamic_bounds_) {
            const float worst_sq = top_dist_[static_cast<size_t>(worst)];
            const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
            if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
            const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
            if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
          }
          if (binary_probe_enabled &&
              binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
            continue;
          }
          NATIVE_IVF_SCAN_RADIAL_BLOCK(block);
        }
      } else {
        for (uint32_t block = start; block < end; ++block) {
          if (quick_dynamic_bounds_) {
            const float worst_sq = top_dist_[static_cast<size_t>(worst)];
            const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
            if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
            const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
            if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
          }
          if (binary_probe_enabled &&
              binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
            continue;
          }
          NATIVE_IVF_SCAN_RADIAL_BLOCK(block);
        }
      }
    }

#undef NATIVE_IVF_SCAN_RADIAL_BLOCK
    return;
  }

  int worst = 0;
  const __m256 scale = _mm256_set1_ps(kVectorScale);
  alignas(32) std::array<float, kBBoxStride> bbox_query{};
  const float* bbox_q = nullptr;
  if (bbox_enabled) {
    for (int d = 0; d < kDim; ++d) bbox_query[static_cast<size_t>(d)] = q[d];
    bbox_q = bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> hot_bbox_query{};
  const float* hot_bbox_q = nullptr;
  if (hot_bbox_enabled) {
    for (int d = 0; d < hot_bbox_dims_; ++d) {
      hot_bbox_query[static_cast<size_t>(d)] = q[d] * kVectorInvScale;
    }
    hot_bbox_q = hot_bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }
  auto bbox_rejects = [&](uint32_t block) {
    if (!bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_bbox_checks;
    if (block_bbox_lower_bound(bbox_q, block) >= worst_sq) {
      if (profile_enabled_) ++profile_.rescore_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) {
      if (quick_call) {
        ++profile_.quick_center_checks;
      } else {
        ++profile_.rescore_center_checks;
      }
    }
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      if (profile_enabled_) {
        if (quick_call) {
          ++profile_.quick_center_skipped_blocks;
        } else {
          ++profile_.rescore_center_skipped_blocks;
        }
      }
      return true;
    }
    return false;
  };
  auto hot_bbox_rejects = [&](uint32_t block) {
    if (!hot_bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) {
      if (quick_call) {
        ++profile_.quick_hot_bbox_checks;
      } else {
        ++profile_.rescore_hot_bbox_checks;
      }
    }
    if (block_hot_bbox_lower_bound_scaled(hot_bbox_q, block) >= worst_sq) {
      if (profile_enabled_) {
        if (quick_call) {
          ++profile_.quick_hot_bbox_skipped_blocks;
        } else {
          ++profile_.rescore_hot_bbox_skipped_blocks;
        }
      }
      return true;
    }
    return false;
  };

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d]);
  }

#define NATIVE_IVF_SCAN_BOUND_BLOCK(BLOCK_VALUE)                                                     \
  do {                                                                                               \
    const uint32_t scan_block = (BLOCK_VALUE);                                                       \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;           \
    __m256 low = _mm256_setzero_ps();                                                                \
    __m256 high = _mm256_setzero_ps();                                                               \
                                                                                                     \
    for (int d = 0; d < 6; ++d) {                                                                    \
      const int16_t* row = base + d * kVectorsPerBlock;                                              \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));                \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));           \
      __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);      \
      __m256 vf_high = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);    \
      const __m256 qv = qv_by_dim[d];                                                                \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                   \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                  \
      low = _mm256_fmadd_ps(dl, dl, low);                                                            \
      high = _mm256_fmadd_ps(dh, dh, high);                                                          \
    }                                                                                                \
                                                                                                     \
    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);                    \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                      \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                    \
    if ((mask_low | mask_high) != 0) {                                                               \
      const bool low_alive = mask_low != 0;                                                          \
      const bool high_alive = mask_high != 0;                                                        \
      for (int d = 6; d < kDim; ++d) {                                                               \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m256 qv = qv_by_dim[d];                                                              \
        if (low_alive) {                                                                             \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));            \
          __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);  \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                               \
          low = _mm256_fmadd_ps(dl, dl, low);                                                        \
        }                                                                                            \
        if (high_alive) {                                                                            \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));       \
          __m256 vf_high =                                                                           \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);             \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                              \
          high = _mm256_fmadd_ps(dh, dh, high);                                                      \
        }                                                                                            \
      }                                                                                              \
                                                                                                     \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;        \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;     \
      if ((mask_low | mask_high) != 0) {                                                             \
        const uint8_t* label_base =                                                                        \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                    \
        if (mask_low) {                                                                              \
          _mm256_store_ps(dist_low, low);                                                            \
          while (mask_low) {                                                                         \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                          \
            mask_low &= mask_low - 1;                                                                \
            update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);          \
          }                                                                                          \
        }                                                                                            \
        if (mask_high) {                                                                             \
          _mm256_store_ps(dist_high, high);                                                          \
          while (mask_high) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                         \
            mask_high &= mask_high - 1;                                                              \
            update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);     \
          }                                                                                          \
        }                                                                                            \
      }                                                                                              \
    }                                                                                                \
  } while (0)

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;
    const bool binary_probe_enabled = binary_prefilter_enabled_for(probe);
    const uint16_t binary_qsig =
        binary_probe_enabled ? binary_query_signature(q, probe) : uint16_t{0};

    if (!quick_radius_order_ || end - start < 4) {
      for (uint32_t block = start; block < end; ++block) {
        if (binary_probe_enabled &&
            binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
          continue;
        }
        if (quick_dynamic_bounds_ && radial_enabled) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        }
        if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_BOUND_BLOCK(block);
      }
      continue;
    }

    const uint32_t pivot = find_radius_pivot(start, end, center_dist);
    int32_t left = static_cast<int32_t>(pivot);
    uint32_t right = pivot + 1;
    const int32_t start_i = static_cast<int32_t>(start);
    while (left >= start_i || right < end) {
      if (left >= start_i) {
        const uint32_t block = static_cast<uint32_t>(left);
        if (binary_probe_enabled &&
            binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
          --left;
          continue;
        }
        if (!quick_dynamic_bounds_ || !radial_enabled) {
          if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
            --left;
            continue;
          }
          NATIVE_IVF_SCAN_BOUND_BLOCK(block);
        } else {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (!((upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq))) {
            if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
              --left;
              continue;
            }
            NATIVE_IVF_SCAN_BOUND_BLOCK(block);
          }
        }
        --left;
      }
      if (right < end) {
        const uint32_t block = right;
        if (binary_probe_enabled &&
            binary_prefilter_rejects(probe, block, binary_qsig, quick_call)) {
          ++right;
          continue;
        }
        if (!quick_dynamic_bounds_ || !radial_enabled) {
          if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
            ++right;
            continue;
          }
          NATIVE_IVF_SCAN_BOUND_BLOCK(block);
        } else {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (!((upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq))) {
            if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
              ++right;
              continue;
            }
            NATIVE_IVF_SCAN_BOUND_BLOCK(block);
          }
        }
        ++right;
      }
    }
  }

#undef NATIVE_IVF_SCAN_BOUND_BLOCK
}

void NativeIVF::scan_probes_bounded_scaled(const float* q, int from, int to, bool use_bbox) {
  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool bbox_enabled = use_bbox && use_block_bbox_ && !block_bbox_min_.empty();
  const bool quick_call = from == 0 && to == quick_probe_;
  const bool hot_bbox_enabled = use_hot_bbox_ && !block_hot_min_.empty() &&
                                (quick_call ? quick_hot_bbox_ : rescore_hot_bbox_);
  const bool center_phase_enabled = quick_call || from >= center_bound_min_from_;
  const bool center_bound_enabled = use_center_bound_ && !block_center_.empty() &&
                                    center_phase_enabled &&
                                    (quick_call ? quick_center_bound_ : rescore_center_bound_);
  if (!radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    scan_probes(q, from, to);
    return;
  }

  if (radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled &&
      !quick_radius_order_) {
    int worst = 0;
    constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
    alignas(32) float dist_low[8];
    alignas(32) float dist_high[8];
    alignas(32) __m256 qv_by_dim[kDim];
    for (int d = 0; d < kDim; ++d) {
      qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
    }

#define NATIVE_IVF_SCAN_RADIAL_SCALED_BLOCK(BLOCK_VALUE)                                            \
    do {                                                                                            \
      const uint32_t scan_block = (BLOCK_VALUE);                                                    \
      const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;        \
      __m256 low = _mm256_setzero_ps();                                                             \
      __m256 high = _mm256_setzero_ps();                                                            \
                                                                                                    \
      for (int d = 0; d < 6; ++d) {                                                                 \
        const int16_t* row = base + d * kVectorsPerBlock;                                           \
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));             \
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));        \
        const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                   \
        const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                 \
        const __m256 qv = qv_by_dim[d];                                                             \
        const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                \
        const __m256 dh = _mm256_sub_ps(vf_high, qv);                                               \
        low = _mm256_fmadd_ps(dl, dl, low);                                                         \
        high = _mm256_fmadd_ps(dh, dh, high);                                                       \
      }                                                                                             \
                                                                                                    \
      const __m256 worst_v =                                                                        \
          _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);                      \
      int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                   \
      int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                 \
      if ((mask_low | mask_high) != 0) {                                                            \
        const bool low_alive = mask_low != 0;                                                       \
        const bool high_alive = mask_high != 0;                                                     \
        for (int d = 6; d < kDim; ++d) {                                                            \
          const int16_t* row = base + d * kVectorsPerBlock;                                         \
          const __m256 qv = qv_by_dim[d];                                                           \
          if (low_alive) {                                                                          \
            const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));         \
            const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));               \
            const __m256 dl = _mm256_sub_ps(vf_low, qv);                                            \
            low = _mm256_fmadd_ps(dl, dl, low);                                                     \
          }                                                                                         \
          if (high_alive) {                                                                         \
            const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));    \
            const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));             \
            const __m256 dh = _mm256_sub_ps(vf_high, qv);                                           \
            high = _mm256_fmadd_ps(dh, dh, high);                                                   \
          }                                                                                         \
        }                                                                                           \
                                                                                                    \
        mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;     \
        mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;  \
        if ((mask_low | mask_high) != 0) {                                                          \
          const uint8_t* label_base =                                                               \
              labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                 \
          if (mask_low) {                                                                           \
            _mm256_store_ps(dist_low, low);                                                         \
            while (mask_low) {                                                                      \
              const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                       \
              mask_low &= mask_low - 1;                                                            \
              update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit], top_dist_,          \
                               top_label_, worst);                                                 \
            }                                                                                       \
          }                                                                                         \
          if (mask_high) {                                                                          \
            _mm256_store_ps(dist_high, high);                                                       \
            while (mask_high) {                                                                     \
              const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                      \
              mask_high &= mask_high - 1;                                                          \
              update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit], top_dist_,     \
                               top_label_, worst);                                                 \
            }                                                                                       \
          }                                                                                         \
        }                                                                                           \
      }                                                                                             \
    } while (0)

    for (int pi = from; pi < to; ++pi) {
      const int probe = probes_[static_cast<size_t>(pi)];
      uint32_t start = offsets_[static_cast<size_t>(probe)];
      uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
      float center_dist = 0.0f;
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
      if (start >= end) continue;

      for (uint32_t block = start; block < end; ++block) {
        if (quick_dynamic_bounds_) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        }
        NATIVE_IVF_SCAN_RADIAL_SCALED_BLOCK(block);
      }
    }

#undef NATIVE_IVF_SCAN_RADIAL_SCALED_BLOCK
    return;
  }

  int worst = 0;
  constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
  alignas(32) std::array<float, kBBoxStride> bbox_query{};
  const float* bbox_q = nullptr;
  if (bbox_enabled) {
    for (int d = 0; d < kDim; ++d) bbox_query[static_cast<size_t>(d)] = q[d];
    bbox_q = bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> hot_bbox_query{};
  const float* hot_bbox_q = nullptr;
  if (hot_bbox_enabled) {
    for (int d = 0; d < hot_bbox_dims_; ++d) {
      hot_bbox_query[static_cast<size_t>(d)] = q[d] * kVectorInvScale;
    }
    hot_bbox_q = hot_bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }
  auto bbox_rejects = [&](uint32_t block) {
    if (!bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_bbox_checks;
    if (block_bbox_lower_bound(bbox_q, block) >= worst_sq) {
      if (profile_enabled_) ++profile_.rescore_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) {
      if (quick_call) {
        ++profile_.quick_center_checks;
      } else {
        ++profile_.rescore_center_checks;
      }
    }
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      if (profile_enabled_) {
        if (quick_call) {
          ++profile_.quick_center_skipped_blocks;
        } else {
          ++profile_.rescore_center_skipped_blocks;
        }
      }
      return true;
    }
    return false;
  };
  auto hot_bbox_rejects = [&](uint32_t block) {
    if (!hot_bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) {
      if (quick_call) {
        ++profile_.quick_hot_bbox_checks;
      } else {
        ++profile_.rescore_hot_bbox_checks;
      }
    }
    if (block_hot_bbox_lower_bound_scaled(hot_bbox_q, block) >= worst_sq) {
      if (profile_enabled_) {
        if (quick_call) {
          ++profile_.quick_hot_bbox_skipped_blocks;
        } else {
          ++profile_.rescore_hot_bbox_skipped_blocks;
        }
      }
      return true;
    }
    return false;
  };

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
  }

#define NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(BLOCK_VALUE)                                             \
  do {                                                                                              \
    const uint32_t scan_block = (BLOCK_VALUE);                                                      \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;          \
    __m256 low = _mm256_setzero_ps();                                                               \
    __m256 high = _mm256_setzero_ps();                                                              \
                                                                                                    \
    for (int d = 0; d < 6; ++d) {                                                                   \
      const int16_t* row = base + d * kVectorsPerBlock;                                             \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));               \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));          \
      const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                    \
      const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                  \
      const __m256 qv = qv_by_dim[d];                                                               \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                  \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                 \
      low = _mm256_fmadd_ps(dl, dl, low);                                                           \
      high = _mm256_fmadd_ps(dh, dh, high);                                                         \
    }                                                                                               \
                                                                                                    \
    const __m256 worst_v =                                                                          \
        _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);                        \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                     \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                   \
    if ((mask_low | mask_high) != 0) {                                                              \
      const bool low_alive = mask_low != 0;                                                         \
      const bool high_alive = mask_high != 0;                                                       \
      for (int d = 6; d < kDim; ++d) {                                                              \
        const int16_t* row = base + d * kVectorsPerBlock;                                           \
        const __m256 qv = qv_by_dim[d];                                                             \
        if (low_alive) {                                                                            \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));           \
          const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                              \
          low = _mm256_fmadd_ps(dl, dl, low);                                                       \
        }                                                                                           \
        if (high_alive) {                                                                           \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));      \
          const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));              \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                             \
          high = _mm256_fmadd_ps(dh, dh, high);                                                     \
        }                                                                                           \
      }                                                                                             \
                                                                                                    \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;       \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;    \
      if ((mask_low | mask_high) != 0) {                                                            \
        const uint8_t* label_base =                                                                 \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                   \
        if (mask_low) {                                                                             \
          _mm256_store_ps(dist_low, low);                                                           \
          while (mask_low) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                         \
            mask_low &= mask_low - 1;                                                               \
            update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit], top_dist_, top_label_,\
                             worst);                                                               \
          }                                                                                         \
        }                                                                                           \
        if (mask_high) {                                                                            \
          _mm256_store_ps(dist_high, high);                                                         \
          while (mask_high) {                                                                       \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                        \
            mask_high &= mask_high - 1;                                                             \
            update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit], top_dist_,      \
                             top_label_, worst);                                                   \
          }                                                                                         \
        }                                                                                           \
      }                                                                                             \
    }                                                                                               \
  } while (0)

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;

    if (!quick_radius_order_ || end - start < 4) {
      for (uint32_t block = start; block < end; ++block) {
        if (quick_dynamic_bounds_ && radial_enabled) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        }
        if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(block);
      }
      continue;
    }

    const uint32_t pivot = find_radius_pivot(start, end, center_dist);
    int32_t left = static_cast<int32_t>(pivot);
    uint32_t right = pivot + 1;
    const int32_t start_i = static_cast<int32_t>(start);
    while (left >= start_i || right < end) {
      if (left >= start_i) {
        const uint32_t block = static_cast<uint32_t>(left);
        if (!quick_dynamic_bounds_ || !radial_enabled) {
          if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
            --left;
            continue;
          }
          NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(block);
        } else {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (!((upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq))) {
            if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
              --left;
              continue;
            }
            NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(block);
          }
        }
        --left;
      }
      if (right < end) {
        const uint32_t block = right;
        if (!quick_dynamic_bounds_ || !radial_enabled) {
          if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
            ++right;
            continue;
          }
          NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(block);
        } else {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (!((upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq))) {
            if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) {
              ++right;
              continue;
            }
            NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED(block);
          }
        }
        ++right;
      }
    }
  }

#undef NATIVE_IVF_SCAN_BOUND_BLOCK_SCALED
}

void NativeIVF::scan_probes_bounded_residual8(const float* q, int from, int to, bool use_bbox) {
  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool bbox_enabled = use_bbox && use_block_bbox_ && !block_bbox_min_.empty();
  const bool quick_call = from == 0 && to == quick_probe_;
  const bool center_phase_enabled = quick_call || from >= center_bound_min_from_;
  const bool center_bound_enabled = use_center_bound_ && !block_center_.empty() &&
                                    center_phase_enabled &&
                                    (quick_call ? quick_center_bound_ : rescore_center_bound_);
  if (!radial_enabled && !bbox_enabled && !center_bound_enabled) {
    scan_probes_residual8(q, from, to);
    return;
  }

  int worst = 0;
  const __m256 invalid = _mm256_set1_ps(kInitialTopDist);
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) std::array<float, kBBoxStride> bbox_query{};
  const float* bbox_q = nullptr;
  if (bbox_enabled) {
    for (int d = 0; d < kDim; ++d) bbox_query[static_cast<size_t>(d)] = q[d];
    bbox_q = bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }

  auto bbox_rejects = [&](uint32_t block) {
    if (!bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (block_bbox_lower_bound(bbox_q, block) >= worst_sq) return true;
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    return block_center_distance_sq(center_q, block) >= threshold * threshold;
  };

  auto scan_block = [&](uint32_t scan_block, int probe) {
    const int8_t* base = residual8_blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;
    __m256 low = _mm256_setzero_ps();
    __m256 high = _mm256_setzero_ps();
    __m256 pad_low = _mm256_setzero_ps();
    __m256 pad_high = _mm256_setzero_ps();

    for (int d = 0; d < 6; ++d) {
      const int8_t* row = base + d * kVectorsPerBlock;
      const float scale_low = residual8_scale(scan_block, d, probe, 0);
      const float scale_high = residual8_scale(scan_block, d, probe, 1);
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale_low, scale_high, q_center, d == 0, low, high,
                               pad_low, pad_high);
    }
    low = _mm256_blendv_ps(low, invalid, pad_low);
    high = _mm256_blendv_ps(high, invalid, pad_high);

    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
    if ((mask_low | mask_high) == 0) return;

    const bool low_alive = mask_low != 0;
    const bool high_alive = mask_high != 0;
    for (int d = 6; d < kDim; ++d) {
      const int8_t* row = base + d * kVectorsPerBlock;
      const float scale_low = residual8_scale(scan_block, d, probe, 0);
      const float scale_high = residual8_scale(scan_block, d, probe, 1);
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale_low, scale_high, q_center, false, low, high,
                               pad_low, pad_high);
    }

    mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
    mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
    if ((mask_low | mask_high) == 0) return;

    const uint8_t* label_base = labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;
    if (mask_low) {
      _mm256_store_ps(dist_low, low);
      while (mask_low) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
        mask_low &= mask_low - 1;
        update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);
      }
    }
    if (mask_high) {
      _mm256_store_ps(dist_high, high);
      while (mask_high) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
        mask_high &= mask_high - 1;
        update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);
      }
    }
  };

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;

    if (!quick_radius_order_ || end - start < 4) {
      for (uint32_t block = start; block < end; ++block) {
        if (quick_dynamic_bounds_ && radial_enabled) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        }
        if (center_rejects(block) || bbox_rejects(block)) continue;
        scan_block(block, probe);
      }
      continue;
    }

    const uint32_t pivot = find_radius_pivot(start, end, center_dist);
    int32_t left = static_cast<int32_t>(pivot);
    uint32_t right = pivot + 1;
    const int32_t start_i = static_cast<int32_t>(start);
    while (left >= start_i || right < end) {
      if (left >= start_i) {
        const uint32_t block = static_cast<uint32_t>(left);
        bool skip = false;
        if (quick_dynamic_bounds_ && radial_enabled) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          skip = (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                 (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq);
        }
        if (!skip && !(center_rejects(block) || bbox_rejects(block))) scan_block(block, probe);
        --left;
      }
      if (right < end) {
        const uint32_t block = right;
        bool skip = false;
        if (quick_dynamic_bounds_ && radial_enabled) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          skip = (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) ||
                 (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq);
        }
        if (!skip && !(center_rejects(block) || bbox_rejects(block))) scan_block(block, probe);
        ++right;
      }
    }
  }
}

void NativeIVF::scan_rescore_residual8_hybrid_repair(const float* q, int from, int to) {
  if (residual8_filter_blocks_.empty() || residual8_filter_scales_.empty() ||
      residual8_filter_dims_ < kDim || blocks_.empty()) {
    scan_probes(q, from, to);
    return;
  }

  const int candidate_count = std::min(residual8_hybrid_candidates_, 512);
  std::array<float, 512> cand_dist{};
  std::array<int, 512> cand_slot{};
  cand_dist.fill(kInitialTopDist);
  cand_slot.fill(-1);

  int exact_worst = 0;
  for (int i = 1; i < 5; ++i) {
    if (top_dist_[static_cast<size_t>(i)] > top_dist_[static_cast<size_t>(exact_worst)]) {
      exact_worst = i;
    }
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const __m256 invalid = _mm256_set1_ps(kInitialTopDist);
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];

  auto scan_block_approx = [&](uint32_t scan_block, int probe) {
    const size_t base = static_cast<size_t>(scan_block) *
                        static_cast<size_t>(residual8_filter_dims_) * kVectorsPerBlock;
    __m256 low = _mm256_setzero_ps();
    __m256 high = _mm256_setzero_ps();
    __m256 pad_low = _mm256_setzero_ps();
    __m256 pad_high = _mm256_setzero_ps();

    for (int d = 0; d < 6; ++d) {
      const int8_t* row = residual8_filter_blocks_.data() + base +
                          static_cast<size_t>(d) * kVectorsPerBlock;
      const float scale =
          residual8_filter_scales_[static_cast<size_t>(scan_block) *
                                       static_cast<size_t>(residual8_filter_dims_) +
                                   static_cast<size_t>(d)];
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale, scale, q_center, d == 0, low, high,
                               pad_low, pad_high);
    }

    low = _mm256_blendv_ps(low, invalid, pad_low);
    high = _mm256_blendv_ps(high, invalid, pad_high);
    const __m256 approx_worst_v =
        _mm256_set1_ps(cand_dist[static_cast<size_t>(candidate_count - 1)]);
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, approx_worst_v, _CMP_LT_OQ));
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, approx_worst_v, _CMP_LT_OQ));
    if ((mask_low | mask_high) == 0) return;

    const bool low_alive = mask_low != 0;
    const bool high_alive = mask_high != 0;
    for (int d = 6; d < kDim; ++d) {
      const int8_t* row = residual8_filter_blocks_.data() + base +
                          static_cast<size_t>(d) * kVectorsPerBlock;
      const float scale =
          residual8_filter_scales_[static_cast<size_t>(scan_block) *
                                       static_cast<size_t>(residual8_filter_dims_) +
                                   static_cast<size_t>(d)];
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale, scale, q_center, false, low, high,
                               pad_low, pad_high);
    }

    const __m256 final_worst_v =
        _mm256_set1_ps(cand_dist[static_cast<size_t>(candidate_count - 1)]);
    mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, final_worst_v, _CMP_LT_OQ)) : 0;
    mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, final_worst_v, _CMP_LT_OQ)) : 0;
    if ((mask_low | mask_high) == 0) return;

    const int slot_base = static_cast<int>(scan_block * kVectorsPerBlock);
    if (mask_low) {
      _mm256_store_ps(dist_low, low);
      while (mask_low) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
        mask_low &= mask_low - 1;
        insert_probe_candidate_dynamic(dist_low[bit], slot_base + bit, candidate_count,
                                       cand_dist, cand_slot);
      }
    }
    if (mask_high) {
      _mm256_store_ps(dist_high, high);
      while (mask_high) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
        mask_high &= mask_high - 1;
        insert_probe_candidate_dynamic(dist_high[bit], slot_base + 8 + bit, candidate_count,
                                       cand_dist, cand_slot);
      }
    }
  };

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_distance_sq(q, probe),
                        top_dist_[static_cast<size_t>(exact_worst)], &center_dist);
    }
    if (start >= end) continue;

    for (uint32_t block = start; block < end; ++block) {
      if (quick_dynamic_bounds_ && radial_enabled) {
        const float worst_sq = top_dist_[static_cast<size_t>(exact_worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
      }
      scan_block_approx(block, probe);
    }
  }

  int repaired_worst = exact_worst;
  for (int i = 0; i < candidate_count; ++i) {
    const int slot = cand_slot[static_cast<size_t>(i)];
    if (slot < 0) break;
    const uint32_t block = static_cast<uint32_t>(slot / kVectorsPerBlock);
    const int lane = slot & (kVectorsPerBlock - 1);
    const size_t block_base = static_cast<size_t>(block) * kBlockStride;
    if (is_padding_slot(blocks_, block_base, lane)) continue;

    float dist = 0.0f;
    const int16_t* base = blocks_.data() + block_base;
    for (int d = 0; d < kDim; ++d) {
      const float value =
          static_cast<float>(base[static_cast<size_t>(d) * kVectorsPerBlock +
                                  static_cast<size_t>(lane)]) *
          vector_scale(d);
      const float diff = value - q[d];
      dist += diff * diff;
    }
    update_candidate(dist, labels_[static_cast<size_t>(slot)], top_dist_, top_label_,
                     repaired_worst);
  }
}

void NativeIVF::scan_rescore_bounded(const float* q, int from, int to) {
  if (kEnableResidual8Index && residual8_index_) {
    scan_rescore_bounded_residual8(q, from, to);
    return;
  }
  if (residual8_hybrid_repair_ && to >= residual8_hybrid_min_probe_) {
    scan_rescore_residual8_hybrid_repair(q, from, to);
    return;
  }
  if (rescore_scaled_) {
    scan_rescore_bounded_scaled(q, from, to);
    return;
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool hot_bbox_enabled = rescore_hot_bbox_ && use_hot_bbox_ && !block_hot_min_.empty();
  const bool center_bound_enabled =
      rescore_center_bound_ && use_center_bound_ && !block_center_.empty() &&
      from >= center_bound_min_from_;
  if (!radial_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    scan_probes(q, from, to);
    return;
  }

  int worst = 0;
  alignas(32) __m256 scale_by_dim[kDim];
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    scale_by_dim[d] = _mm256_set1_ps(vector_scale(d));
    qv_by_dim[d] = _mm256_set1_ps(q[d]);
  }
  alignas(32) std::array<float, kHotBBoxStride> hot_bbox_query{};
  const float* hot_bbox_q = nullptr;
  if (hot_bbox_enabled) {
    for (int d = 0; d < hot_bbox_dims_; ++d) {
      hot_bbox_query[static_cast<size_t>(d)] = q[d] * kVectorInvScale;
    }
    hot_bbox_q = hot_bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }
  auto hot_bbox_rejects = [&](uint32_t block) {
    if (!hot_bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_hot_bbox_checks;
    if (block_hot_bbox_lower_bound_scaled(hot_bbox_q, block) >= worst_sq) {
      if (profile_enabled_) ++profile_.rescore_hot_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_center_checks;
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      if (profile_enabled_) ++profile_.rescore_center_skipped_blocks;
      return true;
    }
    return false;
  };

#define NATIVE_IVF_SCAN_RESCORE_BLOCK(BLOCK_VALUE)                                                   \
  do {                                                                                               \
    const uint32_t scan_block = (BLOCK_VALUE);                                                       \
    if (residual8_prefilter_rescore_ && residual8_prefilter_enabled_for(probe)) {                    \
      const bool residual8_reject = residual8_prefilter_rejects(                                    \
          scan_block, probe, q, top_dist_[static_cast<size_t>(worst)]);                              \
      record_rescore_residual8_prefilter(probe, residual8_reject);                                  \
      if (residual8_reject) {                                                                        \
        break;                                                                                      \
      }                                                                                             \
    }                                                                                               \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;           \
    __m256 low = _mm256_setzero_ps();                                                                \
    __m256 high = _mm256_setzero_ps();                                                               \
                                                                                                     \
    for (int d = 0; d < 6; ++d) {                                                                    \
      const int16_t* row = base + d * kVectorsPerBlock;                                              \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));                \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));           \
      const __m256 scale = scale_by_dim[d];                                                          \
      __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);      \
      __m256 vf_high = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);    \
      const __m256 qv = qv_by_dim[d];                                                                \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                   \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                  \
      low = _mm256_fmadd_ps(dl, dl, low);                                                            \
      high = _mm256_fmadd_ps(dh, dh, high);                                                          \
    }                                                                                                \
                                                                                                     \
    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);                    \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                      \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                    \
    if ((mask_low | mask_high) != 0) {                                                               \
      const bool low_alive = mask_low != 0;                                                          \
      const bool high_alive = mask_high != 0;                                                        \
      for (int d = 6; d < kDim; ++d) {                                                               \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m256 qv = qv_by_dim[d];                                                              \
        if (low_alive) {                                                                             \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));            \
          const __m256 scale = scale_by_dim[d];                                                       \
          __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);  \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                               \
          low = _mm256_fmadd_ps(dl, dl, low);                                                        \
        }                                                                                            \
        if (high_alive) {                                                                            \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));       \
          const __m256 scale = scale_by_dim[d];                                                       \
          __m256 vf_high =                                                                           \
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);             \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                              \
          high = _mm256_fmadd_ps(dh, dh, high);                                                      \
        }                                                                                            \
      }                                                                                              \
                                                                                                     \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;        \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;     \
      if ((mask_low | mask_high) != 0) {                                                             \
        const uint8_t* label_base =                                                                  \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                    \
        if (mask_low) {                                                                              \
          _mm256_store_ps(dist_low, low);                                                            \
          while (mask_low) {                                                                         \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                          \
            mask_low &= mask_low - 1;                                                                \
            update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);          \
          }                                                                                          \
        }                                                                                            \
        if (mask_high) {                                                                             \
          _mm256_store_ps(dist_high, high);                                                          \
          while (mask_high) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                         \
            mask_high &= mask_high - 1;                                                              \
            update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);     \
          }                                                                                          \
        }                                                                                            \
      }                                                                                              \
    }                                                                                                \
  } while (0)

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_distance_sq(q, probe),
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;

    if (quick_dynamic_bounds_ && radial_enabled) {
      for (uint32_t block = start; block < end; ++block) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        if (center_rejects(block) || hot_bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_RESCORE_BLOCK(block);
      }
    } else {
      for (uint32_t block = start; block < end; ++block) {
        if (center_rejects(block) || hot_bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_RESCORE_BLOCK(block);
      }
    }
  }

#undef NATIVE_IVF_SCAN_RESCORE_BLOCK
}

void NativeIVF::scan_rescore_bounded_scaled_block_ordered(const float* q, int from, int to) {
  struct BlockCandidate {
    float lower_bound;
    uint32_t block;
  };
  static thread_local std::array<BlockCandidate, 65536> candidates;

  size_t candidate_count = 0;
  bool overflow = false;
  for (int pi = from; pi < to && !overflow; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    const uint32_t start = offsets_[static_cast<size_t>(probe)];
    const uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    const float center_dist = std::sqrt(centroid_distance_sq(q, probe));
    for (uint32_t block = start; block < end; ++block) {
      if (candidate_count >= candidates.size()) {
        overflow = true;
        break;
      }
      float lower_bound = 0.0f;
      const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
      if (upper_delta > 0.0f) {
        lower_bound = upper_delta * upper_delta;
      } else {
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f) lower_bound = lower_delta * lower_delta;
      }
      candidates[candidate_count++] = {lower_bound, block};
    }
  }
  if (overflow) {
    const bool prev = rescore_block_order_;
    rescore_block_order_ = false;
    scan_rescore_bounded_scaled(q, from, to);
    rescore_block_order_ = prev;
    return;
  }

  std::sort(candidates.begin(),
            candidates.begin() + static_cast<std::ptrdiff_t>(candidate_count),
            [](const BlockCandidate& a, const BlockCandidate& b) {
              if (a.lower_bound != b.lower_bound) return a.lower_bound < b.lower_bound;
              return a.block < b.block;
            });

  int worst = 0;
  auto refresh_worst = [&]() {
    worst = 0;
    for (int i = 1; i < 5; ++i) {
      if (top_dist_[static_cast<size_t>(i)] > top_dist_[static_cast<size_t>(worst)]) {
        worst = i;
      }
    }
  };
  refresh_worst();

  constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
  }

  for (size_t ci = 0; ci < candidate_count; ++ci) {
    const BlockCandidate& candidate = candidates[ci];
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq < kInitialTopDist && candidate.lower_bound >= worst_sq) break;

    const uint32_t scan_block = candidate.block;
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;
    __m256 low = _mm256_setzero_ps();
    __m256 high = _mm256_setzero_ps();

    for (int d = 0; d < 6; ++d) {
      const int16_t* row = base + d * kVectorsPerBlock;
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
      const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
      const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
      const __m256 qv = qv_by_dim[d];
      const __m256 dl = _mm256_sub_ps(vf_low, qv);
      const __m256 dh = _mm256_sub_ps(vf_high, qv);
      low = _mm256_fmadd_ps(dl, dl, low);
      high = _mm256_fmadd_ps(dh, dh, high);
    }

    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
    if ((mask_low | mask_high) == 0) continue;

    const bool low_alive = mask_low != 0;
    const bool high_alive = mask_high != 0;
    for (int d = 6; d < kDim; ++d) {
      const int16_t* row = base + d * kVectorsPerBlock;
      const __m256 qv = qv_by_dim[d];
      if (low_alive) {
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
        const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
        const __m256 dl = _mm256_sub_ps(vf_low, qv);
        low = _mm256_fmadd_ps(dl, dl, low);
      }
      if (high_alive) {
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
        const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
        const __m256 dh = _mm256_sub_ps(vf_high, qv);
        high = _mm256_fmadd_ps(dh, dh, high);
      }
    }

    mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
    mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
    if ((mask_low | mask_high) == 0) continue;

    const uint8_t* label_base =
        labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;
    if (mask_low) {
      _mm256_store_ps(dist_low, low);
      while (mask_low) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
        mask_low &= mask_low - 1;
        update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit], top_dist_,
                         top_label_, worst);
      }
    }
    if (mask_high) {
      _mm256_store_ps(dist_high, high);
      while (mask_high) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
        mask_high &= mask_high - 1;
        update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit], top_dist_,
                         top_label_, worst);
      }
    }
  }
}

void NativeIVF::scan_rescore_bounded_scaled(const float* q, int from, int to) {
  if (active_dims_ < kDim) {
    scan_probes_bounded_active_scaled(q, from, to);
    return;
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool hot_bbox_enabled = rescore_hot_bbox_ && use_hot_bbox_ && !block_hot_min_.empty();
  const bool center_bound_enabled =
      rescore_center_bound_ && use_center_bound_ && !block_center_.empty() &&
      from >= center_bound_min_from_;
  if (!radial_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    scan_probes(q, from, to);
    return;
  }

  if (radial_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    if (rescore_block_order_ && !rescore_label_skip_ && !residual8_prefilter_rescore_) {
      scan_rescore_bounded_scaled_block_ordered(q, from, to);
      return;
    }
    int worst = 0;
    constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
    alignas(32) float dist_low[8];
    alignas(32) float dist_high[8];
    alignas(32) __m256 qv_by_dim[kDim];
    for (int d = 0; d < kDim; ++d) {
      qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
    }
    const bool rescore_label_skip_enabled =
        rescore_label_skip_ && label_skip_ && label_skip_cached_ && !block_label_masks_.empty();
    bool cached_rescore_label_skip_active = false;
    uint16_t cached_rescore_label_skip_mask = 0;
    auto rescore_cluster_skip_matches = [&](int probe) {
      if (!rescore_label_skip_enabled || !label_skip_cluster_ ||
          !cached_rescore_label_skip_active || cluster_label_pure_.empty()) {
        return false;
      }
      const int8_t pure = cluster_label_pure_[static_cast<size_t>(probe)];
      return (cached_rescore_label_skip_mask == 0 && pure == 0) ||
             (cached_rescore_label_skip_mask == 0xffffu && pure == 1);
    };
    auto refresh_rescore_label_skip_cache = [&]() {
      cached_rescore_label_skip_active = false;
      if (!rescore_label_skip_enabled) return;
      const float worst_dist = top_dist_[static_cast<size_t>(worst)];
      if (worst_dist >= kInitialTopDist) return;
      if (label_skip_max_worst_ > 0.0f && worst_dist > label_skip_max_worst_) return;
      const uint8_t label = fraud_label(top_label_[0]);
      if (fraud_label(top_label_[1]) != label || fraud_label(top_label_[2]) != label ||
          fraud_label(top_label_[3]) != label || fraud_label(top_label_[4]) != label) {
        return;
      }
      cached_rescore_label_skip_mask = label == 0 ? 0 : 0xffffu;
      cached_rescore_label_skip_active = true;
    };

#define NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK(BLOCK_VALUE)                                    \
    do {                                                                                             \
      const uint32_t scan_block = (BLOCK_VALUE);                                                     \
      if (residual8_prefilter_rescore_ && residual8_prefilter_enabled_for(probe)) {                  \
        const bool residual8_reject = residual8_prefilter_rejects(                                  \
            scan_block, probe, q, top_dist_[static_cast<size_t>(worst)]);                            \
        record_rescore_residual8_prefilter(probe, residual8_reject);                                \
        if (residual8_reject) {                                                                      \
          break;                                                                                    \
        }                                                                                           \
      }                                                                                             \
      const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;         \
      __m256 low = _mm256_setzero_ps();                                                              \
      __m256 high = _mm256_setzero_ps();                                                             \
      bool top_changed = false;                                                                      \
                                                                                                     \
      for (int d = 0; d < 6; ++d) {                                                                  \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));              \
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));         \
        const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                    \
        const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                  \
        const __m256 qv = qv_by_dim[d];                                                              \
        const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                 \
        const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                \
        low = _mm256_fmadd_ps(dl, dl, low);                                                          \
        high = _mm256_fmadd_ps(dh, dh, high);                                                        \
      }                                                                                              \
                                                                                                     \
      const __m256 worst_v =                                                                         \
          _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);                       \
      int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                    \
      int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                  \
      if ((mask_low | mask_high) != 0) {                                                             \
        const bool low_alive = mask_low != 0;                                                        \
        const bool high_alive = mask_high != 0;                                                      \
        for (int d = 6; d < kDim; ++d) {                                                             \
          const int16_t* row = base + d * kVectorsPerBlock;                                          \
          const __m256 qv = qv_by_dim[d];                                                            \
          if (low_alive) {                                                                           \
            const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));          \
            const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                \
            const __m256 dl = _mm256_sub_ps(vf_low, qv);                                             \
            low = _mm256_fmadd_ps(dl, dl, low);                                                      \
          }                                                                                          \
          if (high_alive) {                                                                          \
            const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));     \
            const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));              \
            const __m256 dh = _mm256_sub_ps(vf_high, qv);                                            \
            high = _mm256_fmadd_ps(dh, dh, high);                                                    \
          }                                                                                          \
        }                                                                                            \
                                                                                                     \
        mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;      \
        mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;   \
        if ((mask_low | mask_high) != 0) {                                                           \
          const uint8_t* label_base =                                                                \
              labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                  \
          if (mask_low) {                                                                            \
            _mm256_store_ps(dist_low, low);                                                          \
            while (mask_low) {                                                                       \
              const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                        \
              mask_low &= mask_low - 1;                                                             \
              const int slot = rescore_radius_order_                                                \
                                   ? tie_rank_base +                                                \
                                         static_cast<int>((scan_block - probe_begin) *               \
                                                          kVectorsPerBlock + bit)                    \
                                   : static_cast<int>(scan_block * kVectorsPerBlock + bit);          \
              top_changed |=                                                                         \
                  rescore_radius_order_                                                              \
                      ? update_candidate_slot(dist_low[bit] * kVectorScaleSq, label_base[bit], slot, \
                                              top_dist_, top_label_, top_slot_, worst)                \
                      : update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit], top_dist_, \
                                         top_label_, worst);                                         \
            }                                                                                        \
          }                                                                                          \
          if (mask_high) {                                                                           \
            _mm256_store_ps(dist_high, high);                                                        \
            while (mask_high) {                                                                      \
              const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                       \
              mask_high &= mask_high - 1;                                                            \
              const int slot = rescore_radius_order_                                                \
                                   ? tie_rank_base +                                                \
                                         static_cast<int>((scan_block - probe_begin) *               \
                                                          kVectorsPerBlock + 8 + bit)                \
                                   : static_cast<int>(scan_block * kVectorsPerBlock + 8 + bit);      \
              top_changed |=                                                                         \
                  rescore_radius_order_                                                              \
                      ? update_candidate_slot(dist_high[bit] * kVectorScaleSq,                       \
                                              label_base[8 + bit], slot, top_dist_, top_label_,      \
                                              top_slot_, worst)                                      \
                      : update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit],       \
                                         top_dist_, top_label_, worst);                              \
            }                                                                                        \
          }                                                                                          \
          if (top_changed) refresh_rescore_label_skip_cache();                                        \
        }                                                                                            \
      }                                                                                              \
    } while (0)

    for (int pi = from; pi < to; ++pi) {
      const int probe = probes_[static_cast<size_t>(pi)];
      uint32_t start = offsets_[static_cast<size_t>(probe)];
      uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
      const uint32_t probe_begin = start;
      const int tie_rank_base = (pi - from) << 20;
      if (rescore_cluster_skip_matches(probe)) {
        if (profile_enabled_) {
          profile_.rescore_label_skipped_blocks += static_cast<uint64_t>(end - start);
        }
        continue;
      }
      float center_dist = 0.0f;
      bound_probe_range(start, end, centroid_distance_sq(q, probe),
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
      if (start >= end) continue;

      if (rescore_radius_order_ && end - start >= 4) {
        const uint32_t pivot = find_radius_pivot(start, end, center_dist);
        int32_t left = static_cast<int32_t>(pivot);
        uint32_t right = pivot + 1;
        const int32_t start_i = static_cast<int32_t>(start);
        bool stop_probe = false;
        while (!stop_probe && (left >= start_i || right < end)) {
          if (left >= start_i) {
            const uint32_t block = static_cast<uint32_t>(left);
            bool skip = false;
            if (quick_dynamic_bounds_) {
              const float worst_sq = top_dist_[static_cast<size_t>(worst)];
              const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
              const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
              if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) {
                left = start_i - 1;
                skip = true;
              } else {
                skip = upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq;
              }
            }
            if (!skip) {
              if (cached_rescore_label_skip_active &&
                  block_label_masks_[static_cast<size_t>(block)] ==
                      cached_rescore_label_skip_mask) {
                if (profile_enabled_) ++profile_.rescore_label_skipped_blocks;
              } else {
                NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK(block);
                if (rescore_cluster_skip_matches(probe)) stop_probe = true;
              }
            }
            --left;
          }
          if (stop_probe) break;
          if (right < end) {
            const uint32_t block = right;
            bool skip = false;
            if (quick_dynamic_bounds_) {
              const float worst_sq = top_dist_[static_cast<size_t>(worst)];
              const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
              const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
              if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) {
                right = end;
                skip = true;
              } else {
                skip = lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq;
              }
            }
            if (!skip) {
              if (cached_rescore_label_skip_active &&
                  block_label_masks_[static_cast<size_t>(block)] ==
                      cached_rescore_label_skip_mask) {
                if (profile_enabled_) ++profile_.rescore_label_skipped_blocks;
              } else {
                NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK(block);
                if (rescore_cluster_skip_matches(probe)) stop_probe = true;
              }
            }
            ++right;
          }
        }
        continue;
      }

      if (quick_dynamic_bounds_) {
        for (uint32_t block = start; block < end; ++block) {
          const float worst_sq = top_dist_[static_cast<size_t>(worst)];
          const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
          if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
          const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
          if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
          if (cached_rescore_label_skip_active &&
              block_label_masks_[static_cast<size_t>(block)] == cached_rescore_label_skip_mask) {
            if (profile_enabled_) ++profile_.rescore_label_skipped_blocks;
            continue;
          }
          NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK(block);
          if (rescore_cluster_skip_matches(probe)) {
            if (profile_enabled_ && block + 1 < end) {
              profile_.rescore_label_skipped_blocks += static_cast<uint64_t>(end - block - 1);
            }
            break;
          }
        }
      } else {
        for (uint32_t block = start; block < end; ++block) {
          if (cached_rescore_label_skip_active &&
              block_label_masks_[static_cast<size_t>(block)] == cached_rescore_label_skip_mask) {
            if (profile_enabled_) ++profile_.rescore_label_skipped_blocks;
            continue;
          }
          NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK(block);
          if (rescore_cluster_skip_matches(probe)) {
            if (profile_enabled_ && block + 1 < end) {
              profile_.rescore_label_skipped_blocks += static_cast<uint64_t>(end - block - 1);
            }
            break;
          }
        }
      }
    }

#undef NATIVE_IVF_SCAN_RESCORE_RADIAL_SCALED_BLOCK
    return;
  }

  int worst = 0;
  constexpr float kInvScaleSq = kVectorInvScale * kVectorInvScale;
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) __m256 qv_by_dim[kDim];
  for (int d = 0; d < kDim; ++d) {
    qv_by_dim[d] = _mm256_set1_ps(q[d] * kVectorInvScale);
  }
  alignas(32) std::array<float, kHotBBoxStride> hot_bbox_query{};
  const float* hot_bbox_q = nullptr;
  if (hot_bbox_enabled) {
    for (int d = 0; d < hot_bbox_dims_; ++d) {
      hot_bbox_query[static_cast<size_t>(d)] = q[d] * kVectorInvScale;
    }
    hot_bbox_q = hot_bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }
  auto hot_bbox_rejects = [&](uint32_t block) {
    if (!hot_bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_hot_bbox_checks;
    if (block_hot_bbox_lower_bound_scaled(hot_bbox_q, block) >= worst_sq) {
      if (profile_enabled_) ++profile_.rescore_hot_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_center_checks;
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      if (profile_enabled_) ++profile_.rescore_center_skipped_blocks;
      return true;
    }
    return false;
  };

#define NATIVE_IVF_SCAN_RESCORE_BLOCK_SCALED(BLOCK_VALUE)                                            \
  do {                                                                                               \
    const uint32_t scan_block = (BLOCK_VALUE);                                                       \
    const int16_t* base = blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;           \
    __m256 low = _mm256_setzero_ps();                                                                \
    __m256 high = _mm256_setzero_ps();                                                               \
                                                                                                     \
    for (int d = 0; d < 6; ++d) {                                                                    \
      const int16_t* row = base + d * kVectorsPerBlock;                                              \
      const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));                \
      const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));           \
      const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                     \
      const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));                   \
      const __m256 qv = qv_by_dim[d];                                                                \
      const __m256 dl = _mm256_sub_ps(vf_low, qv);                                                   \
      const __m256 dh = _mm256_sub_ps(vf_high, qv);                                                  \
      low = _mm256_fmadd_ps(dl, dl, low);                                                            \
      high = _mm256_fmadd_ps(dh, dh, high);                                                          \
    }                                                                                                \
                                                                                                     \
    const __m256 worst_v =                                                                           \
        _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)] * kInvScaleSq);                         \
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));                      \
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));                    \
    if ((mask_low | mask_high) != 0) {                                                               \
      const bool low_alive = mask_low != 0;                                                          \
      const bool high_alive = mask_high != 0;                                                        \
      for (int d = 6; d < kDim; ++d) {                                                               \
        const int16_t* row = base + d * kVectorsPerBlock;                                            \
        const __m256 qv = qv_by_dim[d];                                                              \
        if (low_alive) {                                                                             \
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));            \
          const __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));                 \
          const __m256 dl = _mm256_sub_ps(vf_low, qv);                                               \
          low = _mm256_fmadd_ps(dl, dl, low);                                                        \
        }                                                                                            \
        if (high_alive) {                                                                            \
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));       \
          const __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));               \
          const __m256 dh = _mm256_sub_ps(vf_high, qv);                                              \
          high = _mm256_fmadd_ps(dh, dh, high);                                                      \
        }                                                                                            \
      }                                                                                              \
                                                                                                     \
      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;        \
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;     \
      if ((mask_low | mask_high) != 0) {                                                             \
        const uint8_t* label_base =                                                                  \
            labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;                    \
        if (mask_low) {                                                                              \
          _mm256_store_ps(dist_low, low);                                                            \
          while (mask_low) {                                                                         \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));                          \
            mask_low &= mask_low - 1;                                                                \
            update_candidate(dist_low[bit] * kVectorScaleSq, label_base[bit], top_dist_, top_label_, \
                             worst);                                                                \
          }                                                                                          \
        }                                                                                            \
        if (mask_high) {                                                                             \
          _mm256_store_ps(dist_high, high);                                                          \
          while (mask_high) {                                                                        \
            const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));                         \
            mask_high &= mask_high - 1;                                                              \
            update_candidate(dist_high[bit] * kVectorScaleSq, label_base[8 + bit], top_dist_,       \
                             top_label_, worst);                                                    \
          }                                                                                          \
        }                                                                                            \
      }                                                                                              \
    }                                                                                                \
  } while (0)

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_distance_sq(q, probe),
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;

    if (quick_dynamic_bounds_ && radial_enabled) {
      for (uint32_t block = start; block < end; ++block) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        if (center_rejects(block) || hot_bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_RESCORE_BLOCK_SCALED(block);
      }
    } else {
      for (uint32_t block = start; block < end; ++block) {
        if (center_rejects(block) || hot_bbox_rejects(block)) continue;
        NATIVE_IVF_SCAN_RESCORE_BLOCK_SCALED(block);
      }
    }
  }

#undef NATIVE_IVF_SCAN_RESCORE_BLOCK_SCALED
}

void NativeIVF::scan_rescore_bounded_residual8(const float* q, int from, int to) {
  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool center_bound_enabled =
      rescore_center_bound_ && use_center_bound_ && !block_center_.empty() &&
      from >= center_bound_min_from_;
  if (!radial_enabled && !center_bound_enabled) {
    scan_probes_residual8(q, from, to);
    return;
  }

  int worst = 0;
  const __m256 invalid = _mm256_set1_ps(kInitialTopDist);
  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }

  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    if (profile_enabled_) ++profile_.rescore_center_checks;
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      if (profile_enabled_) ++profile_.rescore_center_skipped_blocks;
      return true;
    }
    return false;
  };

  auto scan_block = [&](uint32_t scan_block, int probe) {
    const int8_t* base = residual8_blocks_.data() + static_cast<size_t>(scan_block) * kBlockStride;
    __m256 low = _mm256_setzero_ps();
    __m256 high = _mm256_setzero_ps();
    __m256 pad_low = _mm256_setzero_ps();
    __m256 pad_high = _mm256_setzero_ps();

    for (int d = 0; d < 6; ++d) {
      const int8_t* row = base + d * kVectorsPerBlock;
      const float scale_low = residual8_scale(scan_block, d, probe, 0);
      const float scale_high = residual8_scale(scan_block, d, probe, 1);
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale_low, scale_high, q_center, d == 0, low, high,
                               pad_low, pad_high);
    }
    low = _mm256_blendv_ps(low, invalid, pad_low);
    high = _mm256_blendv_ps(high, invalid, pad_high);

    const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);
    int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
    int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
    if ((mask_low | mask_high) == 0) return;

    const bool low_alive = mask_low != 0;
    const bool high_alive = mask_high != 0;
    for (int d = 6; d < kDim; ++d) {
      const int8_t* row = base + d * kVectorsPerBlock;
      const float scale_low = residual8_scale(scan_block, d, probe, 0);
      const float scale_high = residual8_scale(scan_block, d, probe, 1);
      const float q_center = q[d] - centroids_[static_cast<size_t>(d) * k_ + probe];
      accumulate_residual8_dim(row, scale_low, scale_high, q_center, false, low, high,
                               pad_low, pad_high);
    }

    mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
    mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
    if ((mask_low | mask_high) == 0) return;

    const uint8_t* label_base = labels_.data() + static_cast<size_t>(scan_block) * kVectorsPerBlock;
    if (mask_low) {
      _mm256_store_ps(dist_low, low);
      while (mask_low) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
        mask_low &= mask_low - 1;
        update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst);
      }
    }
    if (mask_high) {
      _mm256_store_ps(dist_high, high);
      while (mask_high) {
        const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
        mask_high &= mask_high - 1;
        update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_, worst);
      }
    }
  };

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    float center_dist = 0.0f;
    if (radial_enabled) {
      bound_probe_range(start, end, centroid_distance_sq(q, probe),
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }
    if (start >= end) continue;

    if (quick_dynamic_bounds_ && radial_enabled) {
      for (uint32_t block = start; block < end; ++block) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) break;
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) continue;
        if (center_rejects(block)) continue;
        scan_block(block, probe);
      }
    } else {
      for (uint32_t block = start; block < end; ++block) {
        if (center_rejects(block)) continue;
        scan_block(block, probe);
      }
    }
  }
}

void NativeIVF::scan_probes_bounded_profile(const float* q, int from, int to) {
  if (kEnableResidual8Index && residual8_index_) {
    scan_probes_bounded_residual8(q, from, to, quick_block_bbox_);
    return;
  }

  const bool radial_enabled = use_block_bounds_ && !block_min_radii_.empty();
  const bool bbox_enabled = quick_block_bbox_ && use_block_bbox_ && !block_bbox_min_.empty();
  const bool hot_bbox_enabled = quick_hot_bbox_ && use_hot_bbox_ && !block_hot_min_.empty();
  const bool center_bound_enabled =
      quick_center_bound_ && use_center_bound_ && !block_center_.empty();
  if (!radial_enabled && !bbox_enabled && !hot_bbox_enabled && !center_bound_enabled) {
    scan_probes(q, from, to);
    return;
  }

  int worst = 0;
  const __m256 scale = _mm256_set1_ps(kVectorScale);
  alignas(32) std::array<float, kBBoxStride> bbox_query{};
  const float* bbox_q = nullptr;
  if (bbox_enabled) {
    for (int d = 0; d < kDim; ++d) bbox_query[static_cast<size_t>(d)] = q[d];
    bbox_q = bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> hot_bbox_query{};
  const float* hot_bbox_q = nullptr;
  if (hot_bbox_enabled) {
    for (int d = 0; d < hot_bbox_dims_; ++d) {
      hot_bbox_query[static_cast<size_t>(d)] = q[d] * kVectorInvScale;
    }
    hot_bbox_q = hot_bbox_query.data();
  }
  alignas(32) std::array<float, kHotBBoxStride> center_query{};
  const float* center_q = nullptr;
  if (center_bound_enabled) {
    for (int d = 0; d < center_bound_dims_; ++d) {
      center_query[static_cast<size_t>(d)] = q[d];
    }
    center_q = center_query.data();
  }
  auto bbox_rejects = [&](uint32_t block) {
    if (!bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    ++profile_.quick_bbox_checks;
    if (block_bbox_lower_bound(bbox_q, block) >= worst_sq) {
      ++profile_.quick_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  auto hot_bbox_rejects = [&](uint32_t block) {
    if (!hot_bbox_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    ++profile_.quick_hot_bbox_checks;
    if (block_hot_bbox_lower_bound_scaled(hot_bbox_q, block) >= worst_sq) {
      ++profile_.quick_hot_bbox_skipped_blocks;
      return true;
    }
    return false;
  };
  float cached_worst_sq = -1.0f;
  float cached_worst_dist = 0.0f;
  auto center_rejects = [&](uint32_t block) {
    if (!center_bound_enabled) return false;
    const float worst_sq = top_dist_[static_cast<size_t>(worst)];
    if (worst_sq >= kInitialTopDist) return false;
    ++profile_.quick_center_checks;
    if (worst_sq != cached_worst_sq) {
      cached_worst_sq = worst_sq;
      cached_worst_dist = std::sqrt(worst_sq);
    }
    const float radius = block_center_radius_[static_cast<size_t>(block)];
    const float threshold = cached_worst_dist + radius;
    if (block_center_distance_sq(center_q, block) >= threshold * threshold) {
      ++profile_.quick_center_skipped_blocks;
      return true;
    }
    return false;
  };

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    const uint32_t original_start = offsets_[static_cast<size_t>(probe)];
    const uint32_t original_end = offsets_[static_cast<size_t>(probe + 1)];
    uint32_t start = original_start;
    uint32_t end = original_end;
    float center_dist = 0.0f;

    if (radial_enabled) {
      bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                        top_dist_[static_cast<size_t>(worst)], &center_dist);
    }

    const uint64_t probe_blocks = static_cast<uint64_t>(original_end - original_start);
    const uint64_t scanned_blocks = start < end ? static_cast<uint64_t>(end - start) : 0;
    profile_.quick_probe_blocks += probe_blocks;
    profile_.quick_bound_skipped_blocks += probe_blocks - scanned_blocks;
    if (start >= end) {
      ++profile_.quick_bound_empty_probes;
      continue;
    }

    for (uint32_t block = start; block < end; ++block) {
      if (quick_dynamic_bounds_ && radial_enabled) {
        const float worst_sq = top_dist_[static_cast<size_t>(worst)];
        const float upper_delta = block_min_radii_[static_cast<size_t>(block)] - center_dist;
        if (upper_delta > 0.0f && upper_delta * upper_delta >= worst_sq) {
          profile_.quick_dynamic_high_skipped_blocks += static_cast<uint64_t>(end - block);
          ++profile_.quick_dynamic_high_breaks;
          break;
        }
        const float lower_delta = center_dist - block_max_radii_[static_cast<size_t>(block)];
        if (lower_delta > 0.0f && lower_delta * lower_delta >= worst_sq) {
          ++profile_.quick_dynamic_low_skipped_blocks;
          ++profile_.quick_dynamic_low_events;
          continue;
        }
      }
      if (center_rejects(block) || hot_bbox_rejects(block) || bbox_rejects(block)) continue;
      ++profile_.quick_blocks_scanned;
      const int16_t* base = blocks_.data() + static_cast<size_t>(block) * kBlockStride;
      __m256 low = _mm256_setzero_ps();
      __m256 high = _mm256_setzero_ps();

      for (int d = 0; d < 6; ++d) {
        const int16_t* row = base + d * kVectorsPerBlock;
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
        __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);
        __m256 vf_high = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);
        const __m256 qv = _mm256_set1_ps(q[d]);
        const __m256 dl = _mm256_sub_ps(vf_low, qv);
        const __m256 dh = _mm256_sub_ps(vf_high, qv);
        low = _mm256_fmadd_ps(dl, dl, low);
        high = _mm256_fmadd_ps(dh, dh, high);
      }

      const __m256 worst_v = _mm256_set1_ps(top_dist_[static_cast<size_t>(worst)]);
      int mask_low = _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ));
      int mask_high = _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ));
      if (mask_low == 0) ++profile_.quick_stage1_low_dead_halves;
      if (mask_high == 0) ++profile_.quick_stage1_high_dead_halves;
      if (mask_low != 0 && mask_high != 0) ++profile_.quick_stage1_both_alive_halves;
      if ((mask_low | mask_high) == 0) {
        ++profile_.quick_stage1_dead_blocks;
        continue;
      }

      ++profile_.quick_stage1_alive_blocks;
      profile_.quick_stage1_low_lanes +=
          static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned>(mask_low)));
      profile_.quick_stage1_high_lanes +=
          static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned>(mask_high)));
      const bool low_alive = mask_low != 0;
      const bool high_alive = mask_high != 0;
      for (int d = 6; d < kDim; ++d) {
        const int16_t* row = base + d * kVectorsPerBlock;
        const __m256 qv = _mm256_set1_ps(q[d]);
        if (low_alive) {
          const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
          __m256 vf_low = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low)), scale);
          const __m256 dl = _mm256_sub_ps(vf_low, qv);
          low = _mm256_fmadd_ps(dl, dl, low);
        }
        if (high_alive) {
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
          __m256 vf_high =
              _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high)), scale);
          const __m256 dh = _mm256_sub_ps(vf_high, qv);
          high = _mm256_fmadd_ps(dh, dh, high);
        }
      }

      mask_low = low_alive ? _mm256_movemask_ps(_mm256_cmp_ps(low, worst_v, _CMP_LT_OQ)) : 0;
      mask_high = high_alive ? _mm256_movemask_ps(_mm256_cmp_ps(high, worst_v, _CMP_LT_OQ)) : 0;
      if ((mask_low | mask_high) == 0) {
        ++profile_.quick_final_dead_blocks;
        continue;
      }

      profile_.quick_final_lanes +=
          static_cast<uint64_t>(__builtin_popcount(static_cast<unsigned>(mask_low)) +
                                __builtin_popcount(static_cast<unsigned>(mask_high)));
      const uint8_t* label_base = labels_.data() + static_cast<size_t>(block) * kVectorsPerBlock;
      if (mask_low) {
        _mm256_store_ps(dist_low, low);
        while (mask_low) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_low));
          mask_low &= mask_low - 1;
          if (update_candidate(dist_low[bit], label_base[bit], top_dist_, top_label_, worst)) {
            ++profile_.quick_top5_accepts;
          }
        }
      }
      if (mask_high) {
        _mm256_store_ps(dist_high, high);
        while (mask_high) {
          const int bit = __builtin_ctz(static_cast<unsigned>(mask_high));
          mask_high &= mask_high - 1;
          if (update_candidate(dist_high[bit], label_base[8 + bit], top_dist_, top_label_,
                               worst)) {
            ++profile_.quick_top5_accepts;
          }
        }
      }
    }
  }
}

void NativeIVF::refresh_selected_centroid_dists(const float* q, int probe_count) {
  for (int pi = 0; pi < probe_count; ++pi) {
    const int ci = probes_[static_cast<size_t>(pi)];
    centroid_dists_[static_cast<size_t>(ci)] = centroid_distance_sq(q, ci);
  }
}

void NativeIVF::ensure_centroid_dists_for(const float* q, int probe_count, bool& full_refreshed) {
  if (classsplit_select_) {
    full_refreshed = true;
    return;
  }
  if ((!centroid_prefilter_ && !centroid_jl_prefilter_) || full_refreshed ||
      centroid_prefilter_probe_ >= probe_count) {
    return;
  }
  centroid_dists(q);
  full_refreshed = true;
}

void NativeIVF::build_centroid_norms() {
  centroid_norms_.assign(static_cast<size_t>(k_), 0.0f);
  for (uint32_t ci = 0; ci < k_; ++ci) {
    float sum = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      const float c = centroids_[static_cast<size_t>(d) * k_ + ci];
      sum += c * c;
    }
    centroid_norms_[static_cast<size_t>(ci)] = sum;
  }
}

void NativeIVF::build_centroid_projection() {
  centroid_jl_.assign(static_cast<size_t>(centroid_jl_dims_) * static_cast<size_t>(k_), 0.0f);
  for (int pd = 0; pd < centroid_jl_dims_; ++pd) {
    float* out = centroid_jl_.data() + static_cast<size_t>(pd) * k_;
    for (uint32_t ci = 0; ci < k_; ++ci) {
      float sum = 0.0f;
      for (int d = 0; d < kDim; ++d) {
        sum += static_cast<float>(kJLProjection[static_cast<size_t>(pd)][static_cast<size_t>(d)]) *
               centroids_[static_cast<size_t>(d) * k_ + ci];
      }
      out[ci] = sum * kJLProjectionScale;
    }
  }
}

void NativeIVF::build_centroid_neighbors() {
  if (k_ <= 1 || centroid_graph_neighbors_ <= 0) {
    centroid_neighbors_.clear();
    centroid_graph_rescore_ = false;
    return;
  }

  const int k = static_cast<int>(k_);
  const int neighbors = std::min(centroid_graph_neighbors_, k - 1);
  centroid_graph_neighbors_ = neighbors;
  centroid_neighbors_.assign(static_cast<size_t>(k) * static_cast<size_t>(neighbors), 0);

  std::array<float, 64> top_d{};
  std::array<int, 64> top_i{};
  for (int ci = 0; ci < k; ++ci) {
    top_d.fill(kInitialTopDist);
    top_i.fill(0);
    for (int cj = 0; cj < k; ++cj) {
      if (cj == ci) continue;
      float dist = 0.0f;
      for (int d = 0; d < kDim; ++d) {
        const float diff = centroids_[static_cast<size_t>(d) * k_ + static_cast<size_t>(ci)] -
                           centroids_[static_cast<size_t>(d) * k_ + static_cast<size_t>(cj)];
        dist += diff * diff;
      }
      if (dist >= top_d[static_cast<size_t>(neighbors - 1)]) continue;
      int pos = neighbors - 1;
      while (pos > 0 && dist < top_d[static_cast<size_t>(pos - 1)]) {
        top_d[static_cast<size_t>(pos)] = top_d[static_cast<size_t>(pos - 1)];
        top_i[static_cast<size_t>(pos)] = top_i[static_cast<size_t>(pos - 1)];
        --pos;
      }
      top_d[static_cast<size_t>(pos)] = dist;
      top_i[static_cast<size_t>(pos)] = cj;
    }
    const size_t out = static_cast<size_t>(ci) * static_cast<size_t>(neighbors);
    for (int i = 0; i < neighbors; ++i) {
      centroid_neighbors_[out + static_cast<size_t>(i)] =
          static_cast<uint16_t>(top_i[static_cast<size_t>(i)]);
    }
  }
}

float NativeIVF::centroid_distance_sq(const float* q, int ci) const {
  if (centroid_dot_distance_ && centroid_norms_.size() == k_) {
    return centroid_distance_sq_dot(q, ci);
  }
  return centroid_distance_sq_diff(q, ci);
}

float NativeIVF::centroid_distance_sq_diff(const float* q, int ci) const {
  float sum = 0.0f;
  for (int d = 0; d < kDim; ++d) {
    const float diff = centroids_[static_cast<size_t>(d) * k_ + static_cast<size_t>(ci)] - q[d];
    sum += diff * diff;
  }
  return sum;
}

float NativeIVF::centroid_distance_sq_dot(const float* q, int ci) const {
  float dot = 0.0f;
  float q_norm = 0.0f;
  for (int d = 0; d < kDim; ++d) {
    const float qv = q[d];
    dot += centroids_[static_cast<size_t>(d) * k_ + static_cast<size_t>(ci)] * qv;
    q_norm += qv * qv;
  }
  float dist = centroid_norms_[static_cast<size_t>(ci)] + q_norm - 2.0f * dot;
  if (dist < 0.0f) dist = 0.0f;
  return dist;
}

void NativeIVF::quantize_query(const float* q) {
  for (int i = 0; i < kDim; ++i) {
    const float inv_scale = vector_inv_scale(i);
    int x = static_cast<int>(q[i] * inv_scale + 0.5f);
    if (q[i] < 0.0f) {
      x = static_cast<int>(q[i] * inv_scale - 0.5f);
    }
    if (x < -32768) {
      x = -32768;
    } else if (x > 32767) {
      x = 32767;
    }
    quantized_[static_cast<size_t>(i)] =
        static_cast<float>(static_cast<int16_t>(x)) * vector_scale(i);
  }
}

int16_t NativeIVF::exact_kd_value(uint32_t slot, int dim) const {
  const uint32_t block = slot / kVectorsPerBlock;
  const int lane = static_cast<int>(slot % kVectorsPerBlock);
  const size_t idx = static_cast<size_t>(block) * kBlockStride +
                     static_cast<size_t>(dim) * kVectorsPerBlock +
                     static_cast<size_t>(lane);
  return blocks_[idx];
}

void NativeIVF::build_exact_kd_quantile_cuts() {
  exact_kd_amount_cuts_.fill(0);
  exact_kd_dow_cuts_.fill(0);

  std::vector<int16_t> amount_values;
  std::vector<int16_t> dow_values;
  amount_values.reserve(labels_.size());
  dow_values.reserve(labels_.size());

  for (uint32_t block = 0; block < total_blocks_; ++block) {
    const size_t base = static_cast<size_t>(block) * kBlockStride;
    for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
      if (is_padding_slot(blocks_, base, lane)) continue;
      const uint32_t slot = block * kVectorsPerBlock + static_cast<uint32_t>(lane);
      amount_values.push_back(exact_kd_value(slot, 0));
      dow_values.push_back(exact_kd_value(slot, 4));
    }
  }

  auto fill_cuts = [](std::vector<int16_t>& values, int16_t* cuts, size_t cut_count) {
    if (values.empty()) return;
    std::sort(values.begin(), values.end());
    const size_t n = values.size();
    const size_t buckets = cut_count + 1;
    for (size_t i = 0; i < cut_count; ++i) {
      size_t idx = ((i + 1) * n) / buckets;
      if (idx >= n) idx = n - 1;
      cuts[i] = values[idx];
    }
  };

  fill_cuts(amount_values, exact_kd_amount_cuts_.data(), exact_kd_amount_cuts_.size());
  fill_cuts(dow_values, exact_kd_dow_cuts_.data(), exact_kd_dow_cuts_.size());
}

uint32_t NativeIVF::exact_kd_bucket(int16_t value, const int16_t* cuts, int cut_count) const {
  uint32_t bucket = 0;
  for (int i = 0; i < cut_count; ++i) {
    if (value > cuts[i]) {
      ++bucket;
    } else {
      break;
    }
  }
  return bucket;
}

uint32_t NativeIVF::exact_kd_partition_key_from_slot(uint32_t slot) const {
  auto value = [&](int dim) {
    return static_cast<float>(exact_kd_value(slot, dim)) * vector_scale(dim);
  };
  if (exact_kd_partition_scheme_ == 3) {
    const uint32_t amount =
        exact_kd_bucket(exact_kd_value(slot, 0), exact_kd_amount_cuts_.data(),
                        static_cast<int>(exact_kd_amount_cuts_.size()));
    const uint32_t dow =
        exact_kd_bucket(exact_kd_value(slot, 4), exact_kd_dow_cuts_.data(),
                        static_cast<int>(exact_kd_dow_cuts_.size()));
    return amount | (dow << 4);
  }
  if (exact_kd_partition_scheme_ == 2) {
    auto bucket = [](float v, int max_value) {
      int out = static_cast<int>(v * static_cast<float>(max_value) + 0.5f);
      if (out < 0) out = 0;
      if (out > max_value) out = max_value;
      return static_cast<uint32_t>(out);
    };
    const uint32_t is_online = value(9) > 0.0f ? 1u : 0u;
    const uint32_t card_present = value(10) > 0.0f ? 1u : 0u;
    const uint32_t unknown_merchant = value(11) > 0.0f ? 1u : 0u;
    const uint32_t sent = value(5) >= 0.0f ? 1u : 0u;
    const uint32_t hour = bucket(value(3), 23);
    const uint32_t day = bucket(value(4), 6);
    return (is_online << 12) | (card_present << 11) | (unknown_merchant << 10) |
           (sent << 9) | (sent << 8) | (hour << 3) | day;
  }

  uint32_t key = 0;
  if (value(5) >= 0.0f) key |= 1u << 0;
  if (value(9) > 0.0f) key |= 1u << 1;
  if (value(10) > 0.0f) key |= 1u << 2;
  if (value(11) > 0.0f) key |= 1u << 3;
  const float mcc = value(12);
  if (mcc > 0.6143f) {
    key |= 3u << 4;
  } else if (mcc > 0.4095f) {
    key |= 2u << 4;
  } else if (mcc > 0.2047f) {
    key |= 1u << 4;
  }
  if (value(2) > 0.4096f) key |= 1u << 6;
  if (value(8) > 0.2048f) key |= 1u << 7;
  return key;
}

uint32_t NativeIVF::exact_kd_partition_key_from_q16(
    const std::array<int16_t, kDim>& q16) const {
  auto value = [&](int dim) {
    return static_cast<float>(q16[static_cast<size_t>(dim)]) * vector_scale(dim);
  };
  if (exact_kd_partition_scheme_ == 3) {
    const uint32_t amount =
        exact_kd_bucket(q16[0], exact_kd_amount_cuts_.data(),
                        static_cast<int>(exact_kd_amount_cuts_.size()));
    const uint32_t dow =
        exact_kd_bucket(q16[4], exact_kd_dow_cuts_.data(),
                        static_cast<int>(exact_kd_dow_cuts_.size()));
    return amount | (dow << 4);
  }
  if (exact_kd_partition_scheme_ == 2) {
    auto bucket = [](float v, int max_value) {
      int out = static_cast<int>(v * static_cast<float>(max_value) + 0.5f);
      if (out < 0) out = 0;
      if (out > max_value) out = max_value;
      return static_cast<uint32_t>(out);
    };
    const uint32_t is_online = value(9) > 0.0f ? 1u : 0u;
    const uint32_t card_present = value(10) > 0.0f ? 1u : 0u;
    const uint32_t unknown_merchant = value(11) > 0.0f ? 1u : 0u;
    const uint32_t sent = value(5) >= 0.0f ? 1u : 0u;
    const uint32_t hour = bucket(value(3), 23);
    const uint32_t day = bucket(value(4), 6);
    return (is_online << 12) | (card_present << 11) | (unknown_merchant << 10) |
           (sent << 9) | (sent << 8) | (hour << 3) | day;
  }

  uint32_t key = 0;
  if (value(5) >= 0.0f) key |= 1u << 0;
  if (value(9) > 0.0f) key |= 1u << 1;
  if (value(10) > 0.0f) key |= 1u << 2;
  if (value(11) > 0.0f) key |= 1u << 3;
  const float mcc = value(12);
  if (mcc > 0.6143f) {
    key |= 3u << 4;
  } else if (mcc > 0.4095f) {
    key |= 2u << 4;
  } else if (mcc > 0.2047f) {
    key |= 1u << 4;
  }
  if (value(2) > 0.4096f) key |= 1u << 6;
  if (value(8) > 0.2048f) key |= 1u << 7;
  return key;
}

bool NativeIVF::build_exact_kd() {
  exact_kd_slots_.clear();
  exact_kd_nodes_.clear();
  exact_kd_partition_roots_.fill(-1);
  exact_kd_partition_counts_.fill(0);
  exact_kd_populated_partitions_.clear();
  if (blocks_.empty() || total_blocks_ == 0) return false;
  if (exact_kd_partition_scheme_ == 3) build_exact_kd_quantile_cuts();

  if (!exact_kd_partitioned_) {
    exact_kd_slots_.reserve(labels_.size());
    for (uint32_t block = 0; block < total_blocks_; ++block) {
      const size_t base = static_cast<size_t>(block) * kBlockStride;
      for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
        if (is_padding_slot(blocks_, base, lane)) continue;
        exact_kd_slots_.push_back(block * kVectorsPerBlock + static_cast<uint32_t>(lane));
      }
    }
    if (exact_kd_slots_.empty()) return false;

    const size_t expected_nodes =
        (exact_kd_slots_.size() / static_cast<size_t>(exact_kd_leaf_size_)) * 2 + 1;
    exact_kd_nodes_.reserve(expected_nodes);
    const uint32_t root = build_exact_kd_node(0, static_cast<uint32_t>(exact_kd_slots_.size()));
    exact_kd_partition_roots_[0] = static_cast<int32_t>(root);
    exact_kd_partition_counts_[0] = static_cast<uint32_t>(exact_kd_slots_.size());
    exact_kd_populated_partitions_.push_back(0);
    return !exact_kd_nodes_.empty();
  }

  std::array<uint32_t, kExactKDMaxPartitionCount> counts{};
  uint32_t slot_count = 0;
  for (uint32_t block = 0; block < total_blocks_; ++block) {
    const size_t base = static_cast<size_t>(block) * kBlockStride;
    for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
      if (is_padding_slot(blocks_, base, lane)) continue;
      const uint32_t slot = block * kVectorsPerBlock + static_cast<uint32_t>(lane);
      ++counts[static_cast<size_t>(exact_kd_partition_key_from_slot(slot))];
      ++slot_count;
    }
  }
  if (slot_count == 0) return false;

  std::array<uint32_t, kExactKDMaxPartitionCount + 1> offsets{};
  for (int i = 0; i < exact_kd_partition_count_; ++i) {
    offsets[static_cast<size_t>(i + 1)] =
        offsets[static_cast<size_t>(i)] + counts[static_cast<size_t>(i)];
  }

  exact_kd_slots_.assign(static_cast<size_t>(slot_count), 0);
  auto cursor = offsets;
  for (uint32_t block = 0; block < total_blocks_; ++block) {
    const size_t base = static_cast<size_t>(block) * kBlockStride;
    for (int lane = 0; lane < kVectorsPerBlock; ++lane) {
      if (is_padding_slot(blocks_, base, lane)) continue;
      const uint32_t slot = block * kVectorsPerBlock + static_cast<uint32_t>(lane);
      const uint32_t key = exact_kd_partition_key_from_slot(slot);
      exact_kd_slots_[static_cast<size_t>(cursor[static_cast<size_t>(key)]++)] = slot;
    }
  }

  const size_t expected_nodes =
      (exact_kd_slots_.size() / static_cast<size_t>(exact_kd_leaf_size_)) * 2 +
      static_cast<size_t>(exact_kd_partition_count_);
  exact_kd_nodes_.reserve(expected_nodes);
  for (uint32_t key = 0; key < static_cast<uint32_t>(exact_kd_partition_count_); ++key) {
    const uint32_t count = counts[static_cast<size_t>(key)];
    if (count == 0) continue;
    const uint32_t start = offsets[static_cast<size_t>(key)];
    const uint32_t root = build_exact_kd_node(start, start + count);
    exact_kd_partition_roots_[static_cast<size_t>(key)] = static_cast<int32_t>(root);
    exact_kd_partition_counts_[static_cast<size_t>(key)] = count;
    exact_kd_populated_partitions_.push_back(static_cast<uint16_t>(key));
  }
  return !exact_kd_nodes_.empty();
}

uint32_t NativeIVF::build_exact_kd_node(uint32_t start, uint32_t end) {
  ExactKDNode node{};
  node.start = start;
  node.count = end - start;
  node.min.fill(32767);
  node.max.fill(-32768);

  for (uint32_t i = start; i < end; ++i) {
    const uint32_t slot = exact_kd_slots_[i];
    for (int d = 0; d < kDim; ++d) {
      const int16_t v = exact_kd_value(slot, d);
      if (v < node.min[static_cast<size_t>(d)]) node.min[static_cast<size_t>(d)] = v;
      if (v > node.max[static_cast<size_t>(d)]) node.max[static_cast<size_t>(d)] = v;
    }
  }
  const uint32_t node_idx = static_cast<uint32_t>(exact_kd_nodes_.size());
  exact_kd_nodes_.push_back(node);

  if (node.count <= static_cast<uint32_t>(exact_kd_leaf_size_)) {
    if (exact_kd_leaf_block_simd_) {
      auto begin = exact_kd_slots_.begin();
      std::sort(begin + start, begin + end);
    }
    return node_idx;
  }

  int split_dim = 0;
  int best_width = -1;
  for (int d = 0; d < kDim; ++d) {
    const int width = static_cast<int>(node.max[static_cast<size_t>(d)]) -
                      static_cast<int>(node.min[static_cast<size_t>(d)]);
    if (width > best_width) {
      best_width = width;
      split_dim = d;
    }
  }
  if (best_width <= 0) {
    return node_idx;
  }

  const uint32_t mid = start + node.count / 2;
  auto begin = exact_kd_slots_.begin();
  std::nth_element(begin + start, begin + mid, begin + end,
                   [this, split_dim](uint32_t a, uint32_t b) {
                     const int16_t av = exact_kd_value(a, split_dim);
                     const int16_t bv = exact_kd_value(b, split_dim);
                     return av == bv ? a < b : av < bv;
                   });

  const uint32_t left = build_exact_kd_node(start, mid);
  const uint32_t right = build_exact_kd_node(mid, end);
  exact_kd_nodes_[node_idx].left = static_cast<int32_t>(left);
  exact_kd_nodes_[node_idx].right = static_cast<int32_t>(right);
  exact_kd_nodes_[node_idx].start = 0;
  exact_kd_nodes_[node_idx].count = 0;
  return node_idx;
}

int64_t NativeIVF::exact_kd_lower_bound(const std::array<int16_t, kDim>& q16,
                                        const ExactKDNode& node) const {
  int64_t sum = 0;
  for (int d = 0; d < kDim; ++d) {
    const int qv = q16[static_cast<size_t>(d)];
    int diff = 0;
    const int lo = node.min[static_cast<size_t>(d)];
    const int hi = node.max[static_cast<size_t>(d)];
    if (qv < lo) {
      diff = lo - qv;
    } else if (qv > hi) {
      diff = qv - hi;
    }
    sum += static_cast<int64_t>(diff) * static_cast<int64_t>(diff);
  }
  return sum;
}

int NativeIVF::classify_exact_kd(const float* q) {
  if (exact_kd_nodes_.empty()) return -1;

  const uint64_t t0 = profile_enabled_ ? now_ns() : 0;
  std::array<int16_t, kDim> q16{};
  for (int d = 0; d < kDim; ++d) {
    const float scaled = q[d] * vector_inv_scale(d);
    int x = scaled < 0.0f ? static_cast<int>(scaled - 0.5f) : static_cast<int>(scaled + 0.5f);
    if (x < -32768) x = -32768;
    if (x > 32767) x = 32767;
    q16[static_cast<size_t>(d)] = static_cast<int16_t>(x);
  }

  std::array<int64_t, 5> top_dist{};
  std::array<uint8_t, 5> top_label{};
  top_dist.fill(static_cast<int64_t>(0x7fffffffffffffffLL));
  top_label.fill(0);
  int top_count = 0;
  int worst = 0;

  auto refresh_worst = [&]() {
    worst = 0;
    for (int i = 1; i < top_count; ++i) {
      if (top_dist[static_cast<size_t>(i)] > top_dist[static_cast<size_t>(worst)]) {
        worst = i;
      }
    }
  };

  auto update_top = [&](int64_t dist, uint8_t label) {
    if (top_count < 5) {
      top_dist[static_cast<size_t>(top_count)] = dist;
      top_label[static_cast<size_t>(top_count)] = label;
      ++top_count;
      refresh_worst();
      return;
    }
    if (dist >= top_dist[static_cast<size_t>(worst)]) return;
    top_dist[static_cast<size_t>(worst)] = dist;
    top_label[static_cast<size_t>(worst)] = label;
    refresh_worst();
  };

  auto scan_leaf_block_simd = [&](uint32_t start, uint32_t end, uint64_t& visited_slots) {
    alignas(32) std::array<int64_t, kVectorsPerBlock> dist_buf{};
    uint32_t i = start;
    while (i < end) {
      const uint32_t first_slot = exact_kd_slots_[i];
      const uint32_t block = first_slot / kVectorsPerBlock;
      uint32_t lane_mask = 0;
      do {
        const uint32_t slot = exact_kd_slots_[i];
        if (slot / kVectorsPerBlock != block) break;
        lane_mask |= 1u << (slot & (kVectorsPerBlock - 1));
        ++i;
      } while (i < end);

      const size_t base = static_cast<size_t>(block) * kBlockStride;
      const int lane_count = __builtin_popcount(lane_mask);
      visited_slots += static_cast<uint64_t>(lane_count);
      if (lane_count < exact_kd_leaf_block_simd_min_lanes_) {
        uint32_t scalar_mask = lane_mask;
        while (scalar_mask) {
          const int lane = __builtin_ctz(scalar_mask);
          scalar_mask &= scalar_mask - 1;
          int64_t dist = 0;
          for (int d = 0; d < kDim; ++d) {
            const size_t idx = base + static_cast<size_t>(d) * kVectorsPerBlock +
                               static_cast<size_t>(lane);
            const int diff = static_cast<int>(blocks_[idx]) -
                             static_cast<int>(q16[static_cast<size_t>(d)]);
            dist += static_cast<int64_t>(diff) * static_cast<int64_t>(diff);
          }
          const uint32_t slot = block * kVectorsPerBlock + static_cast<uint32_t>(lane);
          update_top(dist, labels_[static_cast<size_t>(slot)]);
        }
        continue;
      }

      __m256i acc0 = _mm256_setzero_si256();
      __m256i acc1 = _mm256_setzero_si256();
      __m256i acc2 = _mm256_setzero_si256();
      __m256i acc3 = _mm256_setzero_si256();
      for (int d = 0; d < kDim; ++d) {
        const int16_t* row = blocks_.data() + base + static_cast<size_t>(d) * kVectorsPerBlock;
        const __m256i qv = _mm256_set1_epi32(static_cast<int>(q16[static_cast<size_t>(d)]));
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
        const __m256i val_low = _mm256_cvtepi16_epi32(raw_low);
        const __m256i val_high = _mm256_cvtepi16_epi32(raw_high);
        const __m256i diff_low = _mm256_sub_epi32(val_low, qv);
        const __m256i diff_high = _mm256_sub_epi32(val_high, qv);
        const __m256i sq_low = _mm256_mullo_epi32(diff_low, diff_low);
        const __m256i sq_high = _mm256_mullo_epi32(diff_high, diff_high);
        acc0 = _mm256_add_epi64(
            acc0, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(sq_low)));
        acc1 = _mm256_add_epi64(
            acc1, _mm256_cvtepi32_epi64(_mm256_extracti128_si256(sq_low, 1)));
        acc2 = _mm256_add_epi64(
            acc2, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(sq_high)));
        acc3 = _mm256_add_epi64(
            acc3, _mm256_cvtepi32_epi64(_mm256_extracti128_si256(sq_high, 1)));
      }
      _mm256_store_si256(reinterpret_cast<__m256i*>(dist_buf.data()), acc0);
      _mm256_store_si256(reinterpret_cast<__m256i*>(dist_buf.data() + 4), acc1);
      _mm256_store_si256(reinterpret_cast<__m256i*>(dist_buf.data() + 8), acc2);
      _mm256_store_si256(reinterpret_cast<__m256i*>(dist_buf.data() + 12), acc3);

      while (lane_mask) {
        const int lane = __builtin_ctz(lane_mask);
        lane_mask &= lane_mask - 1;
        const uint32_t slot = block * kVectorsPerBlock + static_cast<uint32_t>(lane);
        update_top(dist_buf[static_cast<size_t>(lane)], labels_[static_cast<size_t>(slot)]);
      }
    }
  };

  struct StackEntry {
    uint32_t node;
    int64_t lower_bound;
  };
  std::array<StackEntry, 1024> stack{};

  uint64_t visited_nodes = 0;
  uint64_t visited_leaves = 0;
  uint64_t visited_slots = 0;

  auto search_root = [&](uint32_t root, int64_t root_lb) {
    if (top_count == 5 && root_lb >= top_dist[static_cast<size_t>(worst)]) return;

    int stack_len = 0;
    stack[stack_len++] = {root, root_lb};

    while (stack_len > 0) {
      const StackEntry cur = stack[--stack_len];
      if (top_count == 5 && cur.lower_bound >= top_dist[static_cast<size_t>(worst)]) continue;

      const ExactKDNode& node = exact_kd_nodes_[cur.node];
      ++visited_nodes;
      if (node.left < 0) {
        ++visited_leaves;
        const uint32_t end = node.start + node.count;
        if (exact_kd_leaf_block_simd_) {
          scan_leaf_block_simd(node.start, end, visited_slots);
          continue;
        }
        for (uint32_t i = node.start; i < end; ++i) {
          const uint32_t slot = exact_kd_slots_[i];
          int64_t dist = 0;
          for (int d = 0; d < kDim; ++d) {
            const int diff = static_cast<int>(exact_kd_value(slot, d)) -
                             static_cast<int>(q16[static_cast<size_t>(d)]);
            dist += static_cast<int64_t>(diff) * static_cast<int64_t>(diff);
          }
          ++visited_slots;
          update_top(dist, labels_[static_cast<size_t>(slot)]);
        }
        continue;
      }

      const uint32_t left_idx = static_cast<uint32_t>(node.left);
      const uint32_t right_idx = static_cast<uint32_t>(node.right);
      const int64_t left_lb = exact_kd_lower_bound(q16, exact_kd_nodes_[left_idx]);
      const int64_t right_lb = exact_kd_lower_bound(q16, exact_kd_nodes_[right_idx]);
      const bool left_first = left_lb <= right_lb;
      const uint32_t near_idx = left_first ? left_idx : right_idx;
      const uint32_t far_idx = left_first ? right_idx : left_idx;
      const int64_t near_lb = left_first ? left_lb : right_lb;
      const int64_t far_lb = left_first ? right_lb : left_lb;

      auto push_if_needed = [&](uint32_t idx, int64_t lb) {
        if (top_count == 5 && lb >= top_dist[static_cast<size_t>(worst)]) return;
        if (stack_len < static_cast<int>(stack.size())) {
          stack[stack_len++] = {idx, lb};
        }
      };
      push_if_needed(far_idx, far_lb);
      push_if_needed(near_idx, near_lb);
    }
  };

  if (exact_kd_partitioned_) {
    const uint32_t primary_key = exact_kd_partition_key_from_q16(q16);
    if (primary_key < static_cast<uint32_t>(exact_kd_partition_count_)) {
      const int32_t primary_root = exact_kd_partition_roots_[static_cast<size_t>(primary_key)];
      if (primary_root >= 0) {
        const uint32_t root = static_cast<uint32_t>(primary_root);
        search_root(root, exact_kd_lower_bound(q16, exact_kd_nodes_[root]));
      }
    }

    struct RootCandidate {
      uint32_t root;
      int64_t lower_bound;
    };
    std::array<RootCandidate, kExactKDMaxPartitionCount> candidates{};
    size_t candidate_count = 0;
    for (uint16_t key : exact_kd_populated_partitions_) {
      if (key == primary_key) continue;
      const int32_t root_i = exact_kd_partition_roots_[static_cast<size_t>(key)];
      if (root_i < 0) continue;
      const uint32_t root = static_cast<uint32_t>(root_i);
      const int64_t lb = exact_kd_lower_bound(q16, exact_kd_nodes_[root]);
      if (top_count == 5 && lb >= top_dist[static_cast<size_t>(worst)]) continue;
      candidates[candidate_count++] = {root, lb};
    }
    std::sort(candidates.begin(), candidates.begin() + static_cast<std::ptrdiff_t>(candidate_count),
              [](const RootCandidate& a, const RootCandidate& b) {
                return a.lower_bound < b.lower_bound;
              });
    for (size_t i = 0; i < candidate_count; ++i) {
      const RootCandidate candidate = candidates[i];
      if (top_count == 5 &&
          candidate.lower_bound >= top_dist[static_cast<size_t>(worst)]) {
        break;
      }
      search_root(candidate.root, candidate.lower_bound);
    }
  } else {
    search_root(0, exact_kd_lower_bound(q16, exact_kd_nodes_[0]));
  }

  int frauds = 0;
  for (int i = 0; i < top_count; ++i) {
    frauds += static_cast<int>(fraud_label(top_label[static_cast<size_t>(i)]));
  }

  if (profile_enabled_) {
    const uint64_t elapsed = now_ns() - t0;
    ++profile_.exact_kd_calls;
    profile_.exact_kd_ns += elapsed;
    if (elapsed > profile_.exact_kd_max_ns) profile_.exact_kd_max_ns = elapsed;
    profile_.exact_kd_nodes += visited_nodes;
    profile_.exact_kd_leaves += visited_leaves;
    profile_.exact_kd_slots += visited_slots;
  }
  return frauds;
}

bool NativeIVF::should_exact_kd_repair(int count) const {
  return exact_kd_repair_ && exact_kd_enabled_ && !exact_kd_nodes_.empty() &&
         count >= exact_kd_repair_min_ && count <= exact_kd_repair_max_;
}

bool NativeIVF::should_exact_kd_repair_context(int fast, int count, bool would_rescore,
                                               int primary_centroid, float quick_margin,
                                               float quick_worst, float centroid_gap_max,
                                               float centroid_gap_last) const {
  if (!should_exact_kd_repair(count)) return false;
  if (!exact_kd_repair_policy_enabled_) return true;
  if (fast < 0 || fast >= kFraudCount || count < 0 || count >= kFraudCount ||
      primary_centroid < 0 || primary_centroid >= static_cast<int>(k_)) {
    return false;
  }
  const size_t idx =
      (((static_cast<size_t>(fast) * kFraudCount + static_cast<size_t>(count)) * 2u +
        (would_rescore ? 1u : 0u)) *
       static_cast<size_t>(kMaxCentroids)) +
      static_cast<size_t>(primary_centroid);
  if (idx >= exact_kd_repair_policy_.size()) return false;
  const ExactKDRepairRule& rule = exact_kd_repair_policy_[idx];
  if (rule.enabled == 0) return false;
  if (rule.min_margin > -0.5f && quick_margin < rule.min_margin) return false;
  if (rule.max_worst > 0.0f && quick_worst > rule.max_worst) return false;
  if (rule.min_gap > 0.0f && centroid_gap_max < rule.min_gap) return false;
  if (rule.max_gap > 0.0f && centroid_gap_max > rule.max_gap) return false;
  if (rule.min_last_gap > 0.0f && centroid_gap_last < rule.min_last_gap) return false;
  if (rule.max_last_gap > 0.0f && centroid_gap_last > rule.max_last_gap) return false;
  return true;
}

void NativeIVF::local_repair_top(const float* q) {
  if (!local_repair_ || blocks_.empty()) return;

  int worst = 0;
  for (int i = 1; i < 5; ++i) {
    if (top_dist_[static_cast<size_t>(i)] > top_dist_[static_cast<size_t>(worst)]) {
      worst = i;
    }
  }

  if (profile_enabled_) ++profile_.local_repair_calls;
  const int before_fast = count_frauds();

  for (int top_idx = 0; top_idx < 5; ++top_idx) {
    const int seed_slot = top_slot_[static_cast<size_t>(top_idx)];
    if (seed_slot < 0) continue;
    const uint32_t seed_block = static_cast<uint32_t>(seed_slot / kVectorsPerBlock);

    int probe = -1;
    for (int pi = 0; pi < quick_probe_; ++pi) {
      const int candidate_probe = probes_[static_cast<size_t>(pi)];
      if (seed_block >= offsets_[static_cast<size_t>(candidate_probe)] &&
          seed_block < offsets_[static_cast<size_t>(candidate_probe + 1)]) {
        probe = candidate_probe;
        break;
      }
    }
    if (probe < 0) continue;

    const int cluster_begin =
        static_cast<int>(offsets_[static_cast<size_t>(probe)] * kVectorsPerBlock);
    const int cluster_end =
        static_cast<int>(offsets_[static_cast<size_t>(probe + 1)] * kVectorsPerBlock);
    const int begin = std::max(cluster_begin, seed_slot - local_repair_window_);
    const int end = std::min(cluster_end, seed_slot + local_repair_window_ + 1);

    for (int slot = begin; slot < end; ++slot) {
      const uint32_t block = static_cast<uint32_t>(slot / kVectorsPerBlock);
      const int lane = slot & (kVectorsPerBlock - 1);
      const size_t base = static_cast<size_t>(block) * kBlockStride;
      if (block_slot_is_padding(base, lane)) continue;
      if (profile_enabled_) ++profile_.local_repair_candidates;

      float dist = 0.0f;
      for (int d = 0; d < kDim; ++d) {
        const float diff = block_value(static_cast<uint32_t>(probe), base, d, lane) - q[d];
        dist += diff * diff;
      }
      if (update_candidate_slot(dist, labels_[static_cast<size_t>(slot)], slot, top_dist_,
                                top_label_, top_slot_, worst)) {
        if (profile_enabled_) ++profile_.local_repair_accepts;
      }
    }
  }

  if (profile_enabled_ && count_frauds() != before_fast) {
    ++profile_.local_repair_fast_changed;
  }
}

void NativeIVF::local_fraud_graph_repair_top(const float* q) {
  if (!local_fraud_graph_enabled_ || local_fraud_graph_.empty() || blocks_.empty()) return;

  int worst = 0;
  for (int i = 1; i < 5; ++i) {
    if (top_dist_[static_cast<size_t>(i)] > top_dist_[static_cast<size_t>(worst)]) {
      worst = i;
    }
  }

  if (profile_enabled_) ++profile_.local_fraud_graph_calls;
  const int before_fast = count_frauds();
  std::array<int, 5> seen_slots{};
  seen_slots.fill(-1);
  int seen_count = 0;

  for (int top_idx = 0; top_idx < 5; ++top_idx) {
    const int seed_slot = top_slot_[static_cast<size_t>(top_idx)];
    if (seed_slot < 0 || seed_slot >= static_cast<int>(local_fraud_graph_.size())) continue;

    const uint32_t seed_block = static_cast<uint32_t>(seed_slot / kVectorsPerBlock);
    int probe = -1;
    for (int pi = 0; pi < quick_probe_; ++pi) {
      const int candidate_probe = probes_[static_cast<size_t>(pi)];
      if (seed_block >= offsets_[static_cast<size_t>(candidate_probe)] &&
          seed_block < offsets_[static_cast<size_t>(candidate_probe + 1)]) {
        probe = candidate_probe;
        break;
      }
    }
    if (probe < 0) continue;

    const uint16_t local_neighbor =
        local_fraud_graph_[static_cast<size_t>(seed_slot)];
    if (local_neighbor == UINT16_MAX) continue;

    const int cluster_begin =
        static_cast<int>(offsets_[static_cast<size_t>(probe)] * kVectorsPerBlock);
    const int cluster_end =
        static_cast<int>(offsets_[static_cast<size_t>(probe + 1)] * kVectorsPerBlock);
    const int slot = cluster_begin + static_cast<int>(local_neighbor);
    if (slot < cluster_begin || slot >= cluster_end ||
        slot >= static_cast<int>(local_fraud_graph_.size())) {
      continue;
    }
    bool duplicate = false;
    for (int i = 0; i < seen_count; ++i) {
      if (seen_slots[static_cast<size_t>(i)] == slot) {
        duplicate = true;
        break;
      }
    }
    if (duplicate) continue;
    if (seen_count < static_cast<int>(seen_slots.size())) {
      seen_slots[static_cast<size_t>(seen_count++)] = slot;
    }

    const uint32_t block = static_cast<uint32_t>(slot / kVectorsPerBlock);
    const int lane = slot & (kVectorsPerBlock - 1);
    const size_t base = static_cast<size_t>(block) * kBlockStride;
    if (block_slot_is_padding(base, lane)) continue;
    if (fraud_label(labels_[static_cast<size_t>(slot)]) == 0) continue;
    if (profile_enabled_) ++profile_.local_fraud_graph_candidates;

    float dist = 0.0f;
    for (int d = 0; d < kDim; ++d) {
      const float diff = block_value(static_cast<uint32_t>(probe), base, d, lane) - q[d];
      dist += diff * diff;
    }
    if (update_candidate_slot(dist, labels_[static_cast<size_t>(slot)], slot, top_dist_,
                              top_label_, top_slot_, worst)) {
      if (profile_enabled_) ++profile_.local_fraud_graph_accepts;
    }
  }

  if (profile_enabled_ && count_frauds() != before_fast) {
    ++profile_.local_fraud_graph_fast_changed;
  }
}

int NativeIVF::count_frauds() const {
  return static_cast<int>(fraud_label(top_label_[0])) +
         static_cast<int>(fraud_label(top_label_[1])) +
         static_cast<int>(fraud_label(top_label_[2])) +
         static_cast<int>(fraud_label(top_label_[3])) +
         static_cast<int>(fraud_label(top_label_[4]));
}

bool NativeIVF::top_has_reference_borderline() const {
  return is_reference_borderline_label(top_label_[0]) ||
         is_reference_borderline_label(top_label_[1]) ||
         is_reference_borderline_label(top_label_[2]) ||
         is_reference_borderline_label(top_label_[3]) ||
         is_reference_borderline_label(top_label_[4]);
}

float NativeIVF::quick_best_dist() const {
  float best = top_dist_[0];
  for (int i = 1; i < 5; ++i) {
    const float dist = top_dist_[static_cast<size_t>(i)];
    if (dist < best) best = dist;
  }
  return best;
}

float NativeIVF::quick_worst_dist() const {
  float worst = top_dist_[0];
  for (int i = 1; i < 5; ++i) {
    const float dist = top_dist_[static_cast<size_t>(i)];
    if (dist > worst) worst = dist;
  }
  return worst;
}

float NativeIVF::quick_label_margin(int fast) const {
  if (fast <= 0 || fast >= 5) return -1.0f;
  const uint8_t majority_label = fast >= 3 ? 1 : 0;
  float majority_worst = -1.0f;
  float minority_best = kInitialTopDist;
  for (int i = 0; i < 5; ++i) {
    const float dist = top_dist_[static_cast<size_t>(i)];
    if (fraud_label(top_label_[static_cast<size_t>(i)]) == majority_label) {
      if (dist > majority_worst) majority_worst = dist;
    } else if (dist < minority_best) {
      minority_best = dist;
    }
  }
  if (majority_worst < 0.0f || minority_best >= kInitialTopDist) return -1.0f;
  return minority_best - majority_worst;
}

bool NativeIVF::label_skip_rejects(uint32_t block, int worst) const {
  const float worst_dist = top_dist_[static_cast<size_t>(worst)];
  if (worst_dist >= kInitialTopDist) return false;
  if (label_skip_max_worst_ > 0.0f && worst_dist > label_skip_max_worst_) return false;

  const uint8_t label = fraud_label(top_label_[0]);
  if (fraud_label(top_label_[1]) != label || fraud_label(top_label_[2]) != label ||
      fraud_label(top_label_[3]) != label || fraud_label(top_label_[4]) != label) {
    return false;
  }

  const uint16_t mask = block_label_masks_[static_cast<size_t>(block)];
  return label == 0 ? mask == 0 : mask == 0xffffu;
}

float NativeIVF::quick_centroid_gap_max() const {
  if (quick_probe_ < 8) return 0.0f;
  float best_gap = 0.0f;
  for (int i = 3; i < 7; ++i) {
    const float gap = quick_centroid_gap(i);
    if (gap > best_gap) best_gap = gap;
  }
  return best_gap;
}

float NativeIVF::quick_centroid_gap(int from_probe) const {
  if (from_probe < 0 || from_probe + 1 >= quick_probe_) return 0.0f;
  const float prev = centroid_dists_[static_cast<size_t>(probes_[from_probe])];
  if (prev <= 1.0e-12f) return 0.0f;
  const float next = centroid_dists_[static_cast<size_t>(probes_[from_probe + 1])];
  return next / prev;
}

void NativeIVF::trace_quick_decision(uint64_t call, int fast, bool will_rescore) const {
  if (!trace_enabled_) return;
  float best = top_dist_[0];
  float worst = top_dist_[0];
  for (int i = 1; i < 5; ++i) {
    const float dist = top_dist_[static_cast<size_t>(i)];
    if (dist < best) best = dist;
    if (dist > worst) worst = dist;
  }
  const float margin = quick_label_margin(fast);
  const float c4 = centroid_dists_[static_cast<size_t>(probes_[3])];
  const float c5 = centroid_dists_[static_cast<size_t>(probes_[4])];
  const float c6 = centroid_dists_[static_cast<size_t>(probes_[5])];
  const float c7 = centroid_dists_[static_cast<size_t>(probes_[6])];
  const float c8 = centroid_dists_[static_cast<size_t>(probes_[7])];
  std::fprintf(stderr,
               "[native-ivf-trace] call=%llu id=%.*s fast=%d will_rescore=%d "
               "best=%.9g worst=%.9g spread=%.9g margin=%.9g "
               "c45=%.9g c56=%.9g c67=%.9g c78=%.9g "
               "labels=%u%u%u%u%u d0=%.9g d1=%.9g d2=%.9g d3=%.9g d4=%.9g\n",
               static_cast<unsigned long long>(call),
               static_cast<int>(trace_label_len_),
               trace_label_ ? trace_label_ : "",
               fast, will_rescore ? 1 : 0, best, worst,
               worst - best, margin, c4 > 1.0e-12f ? c5 / c4 : 9999.0f,
               c5 > 1.0e-12f ? c6 / c5 : 9999.0f, c6 > 1.0e-12f ? c7 / c6 : 9999.0f,
               c7 > 1.0e-12f ? c8 / c7 : 9999.0f,
               static_cast<unsigned>(top_label_[0]), static_cast<unsigned>(top_label_[1]),
               static_cast<unsigned>(top_label_[2]), static_cast<unsigned>(top_label_[3]),
               static_cast<unsigned>(top_label_[4]), top_dist_[0], top_dist_[1], top_dist_[2],
               top_dist_[3], top_dist_[4]);
}

void NativeIVF::trace_second_chance(uint64_t call, int fast, int first_out, bool will_extend,
                                    int from_probe, int to_probe) const {
  if (!trace_enabled_) return;
  float best = top_dist_[0];
  float worst = top_dist_[0];
  for (int i = 1; i < 5; ++i) {
    const float dist = top_dist_[static_cast<size_t>(i)];
    if (dist < best) best = dist;
    if (dist > worst) worst = dist;
  }
  std::fprintf(stderr,
               "[native-ivf-trace-second] call=%llu id=%.*s fast=%d first=%d extend=%d "
               "from=%d to=%d best=%.9g worst=%.9g spread=%.9g margin=%.9g "
               "labels=%u%u%u%u%u d0=%.9g d1=%.9g d2=%.9g d3=%.9g d4=%.9g\n",
               static_cast<unsigned long long>(call),
               static_cast<int>(trace_label_len_),
               trace_label_ ? trace_label_ : "",
               fast, first_out, will_extend ? 1 : 0, from_probe, to_probe, best, worst,
               worst - best, quick_label_margin(first_out),
               static_cast<unsigned>(top_label_[0]), static_cast<unsigned>(top_label_[1]),
               static_cast<unsigned>(top_label_[2]), static_cast<unsigned>(top_label_[3]),
               static_cast<unsigned>(top_label_[4]), top_dist_[0], top_dist_[1], top_dist_[2],
               top_dist_[3], top_dist_[4]);
}

void NativeIVF::trace_final_decision(uint64_t call, int out) const {
  if (!trace_enabled_) return;
  std::fprintf(stderr, "[native-ivf-trace-final] call=%llu id=%.*s out=%d\n",
               static_cast<unsigned long long>(call),
               static_cast<int>(trace_label_len_),
               trace_label_ ? trace_label_ : "", out);
}

void NativeIVF::record_rescore_margin_profile(int fast, int out, float margin) {
  if (!margin_profile_enabled_ || fast < 0 ||
      fast >= static_cast<int>(profile_.margin_rescore_observed.size())) {
    return;
  }
  const size_t idx = static_cast<size_t>(fast);
  const bool approval_changed = approved_decision(out) != approved_decision(fast);
  ++profile_.margin_rescore_observed[idx];
  if (approval_changed) ++profile_.margin_rescore_changed[idx];
  if (margin < 0.0f || !std::isfinite(margin)) return;
  for (size_t i = 0; i < kMarginProfileThresholds.size(); ++i) {
    if (margin < kMarginProfileThresholds[i]) continue;
    ++profile_.margin_candidate_skips[idx][i];
    if (!approval_changed) {
      ++profile_.margin_safe_skips[idx][i];
    } else {
      ++profile_.margin_bad_skips[idx][i];
    }
  }
}

void NativeIVF::record_rescore_centroid_profile(int primary_centroid, int fast, int out) {
  if (!centroid_flip_profile_enabled_ || primary_centroid < 0 ||
      primary_centroid >= static_cast<int>(profile_.centroid_rescore_observed.size())) {
    return;
  }
  const size_t idx = static_cast<size_t>(primary_centroid);
  const bool approval_changed = approved_decision(out) != approved_decision(fast);
  ++profile_.centroid_rescore_observed[idx];
  if (approval_changed) {
    ++profile_.centroid_rescore_approval_changed[idx];
  }
  if (fast >= 0 && fast < 6) {
    const size_t pair_idx = static_cast<size_t>(fast) * static_cast<size_t>(kMaxCentroids) + idx;
    ++profile_.centroid_fast_rescore_observed[pair_idx];
    if (approval_changed) ++profile_.centroid_fast_rescore_approval_changed[pair_idx];
  }
}

bool NativeIVF::centroid_policy_skips_rescore(int fast, int primary_centroid) const {
  if (!centroid_rescore_skip_ || fast < 0 || fast > 5 || primary_centroid < 0 ||
      primary_centroid >= static_cast<int>(k_)) {
    return false;
  }
  const size_t idx = static_cast<size_t>(fast) * static_cast<size_t>(kMaxCentroids) +
                     static_cast<size_t>(primary_centroid);
  const CentroidRescoreSkipRule& rule = centroid_rescore_skip_policy_[idx];
  if (rule.enabled == 0) return false;
  if (rule.min_margin > -0.5f && quick_label_margin(fast) < rule.min_margin) return false;
  if (rule.max_worst > 0.0f && quick_worst_dist() > rule.max_worst) return false;
  if (rule.min_gap > 0.0f || rule.max_gap > 0.0f) {
    const float gap = quick_centroid_gap_max();
    if (rule.min_gap > 0.0f && gap < rule.min_gap) return false;
    if (rule.max_gap > 0.0f && gap > rule.max_gap) return false;
  }
  if (rule.min_last_gap > 0.0f || rule.max_last_gap > 0.0f) {
    const float last_gap = quick_centroid_gap(6);
    if (rule.min_last_gap > 0.0f && last_gap < rule.min_last_gap) return false;
    if (rule.max_last_gap > 0.0f && last_gap > rule.max_last_gap) return false;
  }
  return true;
}

bool NativeIVF::should_rescore(int fast, int primary_centroid) const {
  if (rescore_disabled_) return false;
  if (fast < rescore_min_ || fast > rescore_max_) {
    return false;
  }
  if (centroid_policy_skips_rescore(fast, primary_centroid)) {
    return false;
  }
  if (fast >= 0 && fast < static_cast<int>(rescore_skip_worst_.size())) {
    const float worst_threshold = rescore_skip_worst_[static_cast<size_t>(fast)];
    if (worst_threshold > 0.0f && quick_worst_dist() < worst_threshold) {
      return false;
    }
    const float margin_threshold = rescore_skip_margin_[static_cast<size_t>(fast)];
    if (margin_threshold > 0.0f && quick_label_margin(fast) >= margin_threshold) {
      return false;
    }
    if (fast == 5 && rescore5_min_centroid_gap_ > 0.0f &&
        quick_centroid_gap_max() >= rescore5_min_centroid_gap_) {
      return false;
    }
    if (fast == 5 && rescore5_min_last_centroid_gap_ > 0.0f &&
        quick_centroid_gap(6) >= rescore5_min_last_centroid_gap_) {
      return false;
    }
    if (fast == 5 && rescore5_max_centroid_gap_ > 0.0f &&
        quick_centroid_gap_max() <= rescore5_max_centroid_gap_) {
      return false;
    }
    if (fast == 5 && rescore5_max_last_centroid_gap_ > 0.0f &&
        quick_centroid_gap(6) <= rescore5_max_last_centroid_gap_) {
      return false;
    }
    if (fast == 5 && rescore5_max_best_ > 0.0f && quick_best_dist() <= rescore5_max_best_) {
      return false;
    }
  }
  return true;
}

bool NativeIVF::fast3_approve_rule_matches(int fast) const {
  if (!fast3_approve_rule_ || fast != 3) return false;
  const float worst = quick_worst_dist();
  const float margin = quick_label_margin(fast);
  if (!std::isfinite(worst) || !std::isfinite(margin)) return false;
  return worst <= fast3_approve_max_worst_ && margin <= fast3_approve_max_margin_;
}

int NativeIVF::rescore_probe_count(int fast) const {
  if (fast == 5 && rescore5_probe_ > 0) return rescore5_probe_;
  return expanded_probe_;
}

int NativeIVF::phase_rescore_probe_count(int fast, int full_probe_count) const {
  if (!rescore234_two_phase_ || fast < 2 || fast > 4) return full_probe_count;
  if (rescore234_probe_ <= quick_probe_ || rescore234_probe_ >= full_probe_count) {
    return full_probe_count;
  }
  return rescore234_probe_;
}

bool NativeIVF::should_rescore234_extend(int fast, int current_probe_count, int full_probe_count,
                                         int rescore_result) const {
  return rescore234_two_phase_ && fast >= 2 && fast <= 4 && current_probe_count < full_probe_count &&
         rescore_result >= rescore234_second_min_ &&
         rescore_result <= rescore234_second_max_;
}

bool NativeIVF::should_second_chance(int fast, int current_probe_count, int rescore_result) const {
  if (fast != 5 || second_chance_probe_ <= current_probe_count ||
      rescore_result < second_chance_min_ || rescore_result > second_chance_max_) {
    return false;
  }
  if (rescore_result >= 0 &&
      rescore_result < static_cast<int>(second_chance_min_worst_.size())) {
    const float min_worst = second_chance_min_worst_[static_cast<size_t>(rescore_result)];
    const float max_worst = second_chance_max_worst_[static_cast<size_t>(rescore_result)];
    if (min_worst > 0.0f || max_worst > 0.0f) {
      const float worst = quick_worst_dist();
      if (min_worst > 0.0f && worst < min_worst) return false;
      if (max_worst > 0.0f && worst > max_worst) return false;
    }
  }
  return true;
}

bool NativeIVF::should_remaining_repair(int count, bool full_centroid_dists) const {
  const bool count_in_range = count >= remaining_repair_min_ && count <= remaining_repair_max_;
  const bool borderline_trigger = reference_borderline_repair_ && top_has_reference_borderline();
  if (!remaining_repair_ || (!count_in_range && !borderline_trigger)) {
    return false;
  }
  if (remaining_repair_bbox_) {
    return cluster_bbox_min_.size() == static_cast<size_t>(k_) * kBBoxStride &&
           cluster_bbox_max_.size() == static_cast<size_t>(k_) * kBBoxStride;
  }
  if (cluster_max_radii_.size() != static_cast<size_t>(k_) || centroid_dot_distance_) {
    return false;
  }
  return full_centroid_dists || (!centroid_prefilter_ && !centroid_jl_prefilter_);
}

bool NativeIVF::rescore_early_unambig_done(int count) const {
  return count < rescore_early_min_ || count > rescore_early_max_;
}

float NativeIVF::rescore_probe_lower_bound(int probe) const {
  if (probe < 0 || probe >= static_cast<int>(k_) ||
      cluster_max_radii_.size() != static_cast<size_t>(k_) || centroid_dot_distance_) {
    return kInitialTopDist;
  }
  const float center_sq = centroid_dists_[static_cast<size_t>(probe)];
  if (center_sq <= 0.0f) return 0.0f;
  const float gap = std::sqrt(center_sq) - cluster_max_radii_[static_cast<size_t>(probe)];
  return gap > 0.0f ? gap * gap : 0.0f;
}

void NativeIVF::scan_rescore_range_linear(const float* q, int from_probe, int to_probe,
                                          bool centroid_dists_ready) {
  if (from_probe >= to_probe) return;
  if (rescore_bounds_) {
    if (rescore_specialized_ && !rescore_block_bbox_) {
      scan_rescore_bounded(q, from_probe, to_probe);
    } else {
      if (!centroid_dists_ready) refresh_selected_centroid_dists(q, to_probe);
      scan_probes_bounded(q, from_probe, to_probe, rescore_block_bbox_);
    }
  } else {
    scan_probes(q, from_probe, to_probe);
  }
}

void NativeIVF::scan_rescore_range(const float* q, int from_probe, int to_probe,
                                   bool centroid_dists_ready) {
  if (from_probe >= to_probe) return;
  const int count = to_probe - from_probe;
  if (!rescore_order_lower_bound_ || count <= 1 ||
      cluster_max_radii_.size() != static_cast<size_t>(k_) || centroid_dot_distance_) {
    scan_rescore_range_linear(q, from_probe, to_probe, centroid_dists_ready);
    return;
  }

  if (rescore_bounds_ && !(rescore_specialized_ && !rescore_block_bbox_) &&
      !centroid_dists_ready) {
    refresh_selected_centroid_dists(q, to_probe);
    centroid_dists_ready = true;
  }

  struct OrderedProbe {
    float lower_bound;
    float center_dist;
    int probe;
  };
  std::array<OrderedProbe, kMaxProbe> ordered{};
  std::array<int, kMaxProbe> original{};
  for (int pi = from_probe; pi < to_probe; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    original[static_cast<size_t>(pi - from_probe)] = probe;
    ordered[static_cast<size_t>(pi - from_probe)] = {
        rescore_probe_lower_bound(probe),
        centroid_dists_[static_cast<size_t>(probe)],
        probe,
    };
  }
  std::sort(ordered.begin(), ordered.begin() + count,
            [](const OrderedProbe& a, const OrderedProbe& b) {
              if (a.lower_bound != b.lower_bound) return a.lower_bound < b.lower_bound;
              return a.center_dist < b.center_dist;
            });
  for (int i = 0; i < count; ++i) {
    probes_[static_cast<size_t>(from_probe + i)] = ordered[static_cast<size_t>(i)].probe;
  }
  scan_rescore_range_linear(q, from_probe, to_probe, centroid_dists_ready);
  for (int i = 0; i < count; ++i) {
    probes_[static_cast<size_t>(from_probe + i)] = original[static_cast<size_t>(i)];
  }
}

int NativeIVF::rescore_quantized_prepared(int probe_count) {
  reset_top();
  if (!rescore_early_unambig_ || !rescore_early_primary_) {
    scan_rescore_range(quantized_.data(), 0, probe_count, false);
    return count_frauds();
  }

  const bool centroid_dists_ready =
      rescore_bounds_ && !(rescore_specialized_ && !rescore_block_bbox_);
  if (centroid_dists_ready) refresh_selected_centroid_dists(quantized_.data(), probe_count);
  int out = 0;
  for (int pi = 0; pi < probe_count; ++pi) {
    scan_rescore_range(quantized_.data(), pi, pi + 1, centroid_dists_ready);
    out = count_frauds();
    if (pi + 1 >= rescore_early_min_probe_ && rescore_early_unambig_done(out)) {
      return out;
    }
  }
  return out;
}

int NativeIVF::rescore_quantized_extend_prepared(int from_probe, int to_probe) {
  if (from_probe >= to_probe) return count_frauds();
  if (!rescore_early_unambig_) {
    scan_rescore_range(quantized_.data(), from_probe, to_probe, false);
    return count_frauds();
  }

  const bool centroid_dists_ready =
      rescore_bounds_ && !(rescore_specialized_ && !rescore_block_bbox_);
  if (centroid_dists_ready) refresh_selected_centroid_dists(quantized_.data(), to_probe);
  int out = count_frauds();
  for (int pi = from_probe; pi < to_probe; ++pi) {
    scan_rescore_range(quantized_.data(), pi, pi + 1, centroid_dists_ready);
    out = count_frauds();
    if (pi + 1 >= rescore_early_min_probe_ && rescore_early_unambig_done(out)) {
      return out;
    }
  }
  return out;
}

int NativeIVF::remaining_repair_prepared(const float* q_scan, int scanned_probe_count,
                                         bool full_centroid_dists) {
  int out = count_frauds();
  if (!should_remaining_repair(out, full_centroid_dists)) return out;
  const bool started_from_reference_borderline =
      reference_borderline_repair_ && top_has_reference_borderline();

  scanned_probe_count = std::max(0, std::min(scanned_probe_count, kMaxProbe));
  const int candidate_count =
      std::max(1, std::min(remaining_repair_candidates_, kMaxRepairCandidates));
  std::array<uint8_t, kMaxCentroids> scanned{};
  for (int i = 0; i < scanned_probe_count; ++i) {
    const int probe = probes_[static_cast<size_t>(i)];
    if (probe >= 0 && probe < static_cast<int>(k_)) scanned[static_cast<size_t>(probe)] = 1;
  }

  std::array<float, kMaxRepairCandidates> cand_dist{};
  std::array<int, kMaxRepairCandidates> cand_probe{};
  cand_dist.fill(kInitialTopDist);
  cand_probe.fill(-1);

  auto current_worst_dist = [&]() {
    float worst = top_dist_[0];
    for (int i = 1; i < 5; ++i) {
      if (top_dist_[static_cast<size_t>(i)] > worst) {
        worst = top_dist_[static_cast<size_t>(i)];
      }
    }
    return worst;
  };

  const float initial_worst = current_worst_dist();
  if (initial_worst >= kInitialTopDist) return out;

  for (int ci = 0; ci < static_cast<int>(k_); ++ci) {
    if (scanned[static_cast<size_t>(ci)]) continue;
    if (offsets_[static_cast<size_t>(ci)] >= offsets_[static_cast<size_t>(ci + 1)]) continue;
    const float lb = remaining_repair_bbox_
                         ? cluster_bbox_lower_bound(q_scan, static_cast<uint32_t>(ci))
                         : rescore_probe_lower_bound(ci);
    if (lb < initial_worst) {
      insert_probe_candidate_dynamic(lb, ci, candidate_count, cand_dist, cand_probe);
    }
  }

  const int saved_probe0 = probes_[0];
  for (int i = 0; i < candidate_count; ++i) {
    const int probe = cand_probe[static_cast<size_t>(i)];
    if (probe < 0) break;
    const float worst = current_worst_dist();
    if (worst < kInitialTopDist && cand_dist[static_cast<size_t>(i)] >= worst) break;
    probes_[0] = probe;
    scan_rescore_range(q_scan, 0, 1, true);
    out = count_frauds();
    if (started_from_reference_borderline && !top_has_reference_borderline()) break;
    if (out < remaining_repair_min_ || out > remaining_repair_max_) break;
  }
  probes_[0] = saved_probe0;
  return out;
}

int NativeIVF::rescore_quantized(const float* q, int probe_count) {
  if (!rescore_quantized_) {
    reset_top();
    if (!rescore_early_unambig_ || !rescore_early_primary_) {
      if (rescore_bounds_) {
        scan_probes_bounded(q, 0, probe_count, rescore_block_bbox_);
      } else {
        scan_probes(q, 0, probe_count);
      }
      return count_frauds();
    }
    if (rescore_bounds_) refresh_selected_centroid_dists(q, probe_count);
    int out = 0;
    for (int pi = 0; pi < probe_count; ++pi) {
      if (rescore_bounds_) {
        scan_probes_bounded(q, pi, pi + 1, rescore_block_bbox_);
      } else {
        scan_probes(q, pi, pi + 1);
      }
      out = count_frauds();
      if (pi + 1 >= rescore_early_min_probe_ && rescore_early_unambig_done(out)) {
        return out;
      }
    }
    return out;
  }

  quantize_query(q);
  return rescore_quantized_prepared(probe_count);
}

NativeIVF::DecisionDebug NativeIVF::classify_debug(const float* q) {
  DecisionDebug dbg{};
  if (k_ == 0) return dbg;
  if (rh26_index_) {
    int16_t q16[kDim];
    __m256i vq[kRH26Pairs];
    rh26_prepare_query_pairs(q, q16, vq);

    int nprobe = rh26_nprobe_;
    nprobe = std::max(1, std::min(nprobe, static_cast<int>(k_)));

    uint32_t probes[256];
    uint32_t probe_dists[256];
    const int probe_count = nprobe;
    rh26_find_top_centroids(vq, probes, probe_count);
    for (int i = 0; i < probe_count; ++i) {
      probe_dists[i] = rh26_centroid_distance(vq, probes[i]);
    }

    const int initial_probe =
        rh26_initial_nprobe_ > 0 ? std::min(rh26_initial_nprobe_, nprobe) : nprobe;
    uint32_t quick_dists[5] = {UINT32_MAX, UINT32_MAX, UINT32_MAX, UINT32_MAX,
                               UINT32_MAX};
    uint8_t quick_labels[5] = {};
    uint32_t quick_max = UINT32_MAX;
    const RH26Top5View top{quick_dists, quick_labels, quick_max};
    int scanned_probe_count = 0;
    auto scan_ranked_range = [&](int from, int to) {
      to = std::min(to, probe_count);
      for (int i = from; i < to; ++i) {
        rh26_scan_cluster(vq, probes[i], quick_dists, quick_labels, quick_max);
      }
      if (to > scanned_probe_count) scanned_probe_count = to;
    };
    scan_ranked_range(0, initial_probe);

    for (int i = 0; i < 5; ++i) {
      dbg.quick_labels[static_cast<size_t>(i)] = quick_labels[i];
      dbg.quick_dists[static_cast<size_t>(i)] = static_cast<float>(quick_dists[i]);
    }
    dbg.fast = top.fraud_count();
    dbg.primary_centroid = static_cast<int>(probes[0]);
    dbg.quick_best = static_cast<float>(quick_dists[0]);
    dbg.quick_worst = static_cast<float>(quick_dists[4]);
    dbg.quick_margin = quick_dists[4] > quick_dists[0]
                           ? static_cast<float>(quick_dists[4] - quick_dists[0])
                           : 0.0f;
    if (probe_dists[0] > 0 && probe_count > 4) {
      float best_gap = 0.0f;
      for (int i = 3; i + 1 < std::min(probe_count, 7); ++i) {
        if (probe_dists[i] == 0) continue;
        const float gap = static_cast<float>(probe_dists[i + 1]) /
                          static_cast<float>(probe_dists[i]);
        if (gap > best_gap) best_gap = gap;
      }
      dbg.centroid_gap_max = best_gap;
    }
    if (probe_count > 7 && probe_dists[6] > 0) {
      dbg.centroid_gap_last =
          static_cast<float>(probe_dists[7]) / static_cast<float>(probe_dists[6]);
    }

    dbg.would_rescore = true;

    RH26TopSummary summary = top.summarize();
    const bool force_initial_full =
        rh26_initial_full_policy_enabled_ && initial_probe < nprobe &&
        probes[0] < kMaxCentroids &&
        rh26_initial_full_policy_[static_cast<size_t>(probes[0])] != 0;
    if (initial_probe < nprobe &&
        (summary.has_borderline ||
         summary.in_repair_band(rh26_repair_min_, rh26_repair_max_) ||
         force_initial_full)) {
      scan_ranked_range(initial_probe, nprobe);
      summary = top.summarize();
    }

    if (rh26_expand_nprobe_ > nprobe &&
        rh26_expand_policy_matches(summary.frauds, probes[0])) {
      const int scanned_before_expand = scanned_probe_count;
      const int expanded_nprobe =
          std::min(rh26_expand_nprobe_, static_cast<int>(k_));
      uint32_t expanded_probes[256];
      rh26_find_top_centroids(vq, expanded_probes, expanded_nprobe);
      for (int i = 0; i < expanded_nprobe; ++i) {
        bool already_scanned = false;
        for (int j = 0; j < scanned_before_expand; ++j) {
          if (expanded_probes[i] == probes[j]) {
            already_scanned = true;
            break;
          }
        }
        if (!already_scanned) {
          rh26_scan_cluster(vq, expanded_probes[i], quick_dists, quick_labels, quick_max);
        }
      }
      for (int i = 0; i < expanded_nprobe; ++i) {
        probes[i] = expanded_probes[i];
      }
      nprobe = expanded_nprobe;
      scanned_probe_count = expanded_nprobe;
      dbg.rh26_expanded = true;
      summary = top.summarize();
    }

    bool should_repair = summary.has_borderline;
    if (should_repair && summary.frauds == 5 && rh26_repair5_skip_q0_min_ > 0.0f &&
        q[0] >= rh26_repair5_skip_q0_min_ &&
        (!rh26_repair5_skip_requires_no_expand_ || !dbg.rh26_expanded)) {
      should_repair = false;
    }
    if (should_repair && summary.frauds == 5 && rh26_repair5_policy_enabled_) {
      const uint32_t primary = probes[0];
      should_repair = primary < kMaxCentroids && rh26_repair5_policy_[primary] != 0;
    }
    if (should_repair) {
      rh26_repair(vq, probes, scanned_probe_count, quick_dists, quick_labels, quick_max);
      dbg.rh26_repaired = true;
    }

    for (int i = 0; i < 5; ++i) {
      dbg.rescore_labels[static_cast<size_t>(i)] = quick_labels[i];
      dbg.rescore_dists[static_cast<size_t>(i)] = static_cast<float>(quick_dists[i]);
    }
    dbg.rh26_final_nprobe = nprobe;
    dbg.out = top.fraud_count();
    return dbg;
  }
  const float* raw_q = q;
  struct QuickProbeGuard {
    NativeIVF* self = nullptr;
    int saved = -1;
    ~QuickProbeGuard() {
      if (self != nullptr && saved > 0) self->quick_probe_ = saved;
    }
  } quick_guard;
  if (query_borderline_quick_select_ &&
      !(exact_kd_primary_ && exact_kd_enabled_ && !exact_kd_nodes_.empty())) {
    const int selected_quick_probe = query_is_borderline(raw_q)
                                         ? query_borderline_quick_probe_
                                         : query_obvious_quick_probe_;
    if (selected_quick_probe > 0 && selected_quick_probe != quick_probe_) {
      quick_guard = QuickProbeGuard{this, quick_probe_};
      quick_probe_ = std::clamp(selected_quick_probe, 1, kMaxProbe);
      if (expanded_probe_ < quick_probe_) quick_probe_ = quick_guard.saved;
    }
  }
  q = order_query(q);
  auto apply_exact_repair = [&]() {
    dbg.exact_kd_repaired = true;
    dbg.exact_kd_out = classify_exact_kd(q);
    return dbg.exact_kd_out;
  };
  if (exact_kd_primary_ && exact_kd_enabled_ && !exact_kd_nodes_.empty()) {
    dbg.fast = classify_exact_kd(q);
    dbg.out = dbg.fast;
    return dbg;
  }

  centroid_dists_select8(q);
  if (quick_probe_ != kQuickProbe) select_top_probes(quick_probe_);

  reset_top();
  scan_probes_bounded(q, 0, quick_probe_, quick_block_bbox_);
  int fast = count_frauds();
  if (local_repair_ && fast >= local_repair_min_fast_ && fast <= local_repair_max_fast_) {
    local_repair_top(q);
    fast = count_frauds();
  }
  if (local_fraud_graph_enabled_ && fast >= local_fraud_graph_min_fast_ &&
      fast <= local_fraud_graph_max_fast_) {
    local_fraud_graph_repair_top(q);
    fast = count_frauds();
  }
  const int primary_centroid = probes_[0];
  dbg.fast = fast;
  dbg.primary_centroid = primary_centroid;
  dbg.quick_best = quick_best_dist();
  dbg.quick_worst = quick_worst_dist();
  dbg.quick_margin = quick_label_margin(fast);
  dbg.centroid_gap_max = quick_centroid_gap_max();
  dbg.centroid_gap_last = quick_centroid_gap(6);
  for (int i = 0; i < 5; ++i) {
    dbg.quick_labels[static_cast<size_t>(i)] = top_label_[static_cast<size_t>(i)];
    dbg.quick_dists[static_cast<size_t>(i)] = top_dist_[static_cast<size_t>(i)];
  }
  if (fast3_approve_rule_matches(fast)) {
    dbg.out = 0;
    dbg.would_rescore = false;
    dbg.rescore_labels = dbg.quick_labels;
    dbg.rescore_dists = dbg.quick_dists;
    return dbg;
  }
  const bool will_rescore = should_rescore(fast, primary_centroid);
  dbg.would_rescore = will_rescore;
  if (will_rescore && rescore_tree_enabled_) {
    const int tree_score = predict_rescore_tree_from_query(raw_q);
    if (tree_score >= 0 && tree_score <= 5) {
      dbg.out = tree_score;
      dbg.rescore_labels = dbg.quick_labels;
      dbg.rescore_dists = dbg.quick_dists;
      return dbg;
    }
  }
  if (!will_rescore) {
    dbg.out = fast;
    if (remaining_repair_ && reference_borderline_repair_ && top_has_reference_borderline()) {
      dbg.pre_repair = dbg.out;
      dbg.out = remaining_repair_prepared(q, quick_probe_, false);
    }
    if (should_exact_kd_repair_context(fast, dbg.out, false, primary_centroid,
                                       dbg.quick_margin, dbg.quick_worst,
                                       dbg.centroid_gap_max, dbg.centroid_gap_last)) {
      if (dbg.pre_repair < 0) dbg.pre_repair = dbg.out;
      dbg.out = apply_exact_repair();
    }
    for (int i = 0; i < 5; ++i) {
      dbg.rescore_labels[static_cast<size_t>(i)] = top_label_[static_cast<size_t>(i)];
      dbg.rescore_dists[static_cast<size_t>(i)] = top_dist_[static_cast<size_t>(i)];
    }
    return dbg;
  }

  const int full_probe_count = rescore_probe_count(fast);
  int probe_count = phase_rescore_probe_count(fast, full_probe_count);
  bool full_centroid_dists = false;
  select_rescore_probes(q, probe_count, full_centroid_dists);
  int out = 0;
  if (rescore_quantized_) {
    quantize_query(q);
    out = rescore_quantized_prepared(probe_count);
    if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
      select_rescore_probes(q, full_probe_count, full_centroid_dists);
      out = rescore_quantized_prepared(full_probe_count);
      probe_count = full_probe_count;
    }
    if (should_second_chance(fast, probe_count, out)) {
      select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
      out = rescore_quantized_extend_prepared(probe_count, second_chance_probe_);
      probe_count = second_chance_probe_;
    }
  } else {
    out = rescore_quantized(q, probe_count);
    if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
      select_rescore_probes(q, full_probe_count, full_centroid_dists);
      out = rescore_quantized(q, full_probe_count);
      probe_count = full_probe_count;
    }
    if (should_second_chance(fast, probe_count, out)) {
      select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
      out = rescore_quantized(q, second_chance_probe_);
      probe_count = second_chance_probe_;
    }
  }
  if (remaining_repair_) {
    out = remaining_repair_prepared(rescore_quantized_ ? quantized_.data() : q,
                                    probe_count, full_centroid_dists);
  }
  if (should_exact_kd_repair_context(fast, out, true, primary_centroid,
                                     dbg.quick_margin, dbg.quick_worst,
                                     dbg.centroid_gap_max, dbg.centroid_gap_last)) {
    dbg.pre_repair = out;
    out = apply_exact_repair();
  }
  dbg.out = out;
  for (int i = 0; i < 5; ++i) {
    dbg.rescore_labels[static_cast<size_t>(i)] = top_label_[static_cast<size_t>(i)];
    dbg.rescore_dists[static_cast<size_t>(i)] = top_dist_[static_cast<size_t>(i)];
  }
  return dbg;
}

int NativeIVF::classify(const float* q) {
  if (k_ == 0) return -1;
  if (rh26_index_) return classify_rh26(q);
  const float* raw_q = q;
  if (query_borderline_quick_select_ &&
      !(exact_kd_primary_ && exact_kd_enabled_ && !exact_kd_nodes_.empty())) {
    const int selected_quick_probe = query_is_borderline(raw_q)
                                         ? query_borderline_quick_probe_
                                         : query_obvious_quick_probe_;
    if (selected_quick_probe > 0 && selected_quick_probe != quick_probe_) {
      return classify_with_quick_probe(raw_q, selected_quick_probe);
    }
  }
  q = order_query(q);
  if (exact_kd_primary_ && exact_kd_enabled_ && !exact_kd_nodes_.empty()) {
    return classify_exact_kd(q);
  }
  if (!profile_enabled_) {
    centroid_dists_select8(q);
    if (quick_probe_ != kQuickProbe) select_top_probes(quick_probe_);

    reset_top();
    scan_probes_bounded(q, 0, quick_probe_, quick_block_bbox_);
    int fast = count_frauds();
    if (local_repair_ && fast >= local_repair_min_fast_ && fast <= local_repair_max_fast_) {
      local_repair_top(q);
      fast = count_frauds();
    }
    if (local_fraud_graph_enabled_ && fast >= local_fraud_graph_min_fast_ &&
        fast <= local_fraud_graph_max_fast_) {
      local_fraud_graph_repair_top(q);
      fast = count_frauds();
    }
    const int primary_centroid = probes_[0];
    const bool need_exact_features = exact_kd_repair_feature_policy_enabled_;
    const float exact_quick_margin = need_exact_features ? quick_label_margin(fast) : -1.0f;
    const float exact_quick_worst = need_exact_features ? quick_worst_dist() : 0.0f;
    const float exact_centroid_gap_max =
        need_exact_features ? quick_centroid_gap_max() : 0.0f;
    const float exact_centroid_gap_last =
        need_exact_features ? quick_centroid_gap(6) : 0.0f;
    const uint64_t trace_call = trace_enabled_ ? ++trace_calls_ : 0;
    if (fast3_approve_rule_matches(fast)) {
      if (trace_enabled_) trace_final_decision(trace_call, 0);
      return 0;
    }
    const bool will_rescore = should_rescore(fast, primary_centroid);
    if (trace_enabled_ && fast >= 2) trace_quick_decision(trace_call, fast, will_rescore);
    if (!will_rescore) {
      int out = fast;
      if (remaining_repair_ && reference_borderline_repair_ && top_has_reference_borderline()) {
        out = remaining_repair_prepared(q, quick_probe_, false);
      }
      if (should_exact_kd_repair_context(fast, out, false, primary_centroid,
                                         exact_quick_margin, exact_quick_worst,
                                         exact_centroid_gap_max, exact_centroid_gap_last)) {
        out = classify_exact_kd(q);
      }
      if (trace_enabled_ && out >= 2) trace_final_decision(trace_call, out);
      return out;
    }
    if (rescore_tree_enabled_) {
      int tree_score = predict_rescore_tree_from_query(raw_q);
      if (tree_score >= 0 && tree_score <= 5) {
        if (should_exact_kd_repair_context(fast, tree_score, true, primary_centroid,
                                           exact_quick_margin, exact_quick_worst,
                                           exact_centroid_gap_max, exact_centroid_gap_last)) {
          tree_score = classify_exact_kd(q);
        }
        if (trace_enabled_) trace_final_decision(trace_call, tree_score);
        return tree_score;
      }
    }

    const int full_probe_count = rescore_probe_count(fast);
    int probe_count = phase_rescore_probe_count(fast, full_probe_count);
    bool full_centroid_dists = false;
    select_rescore_probes(q, probe_count, full_centroid_dists);
    int out = 0;
    if (rescore_quantized_) {
      quantize_query(q);
      out = rescore_quantized_prepared(probe_count);
      if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
        select_rescore_probes(q, full_probe_count, full_centroid_dists);
        out = rescore_quantized_prepared(full_probe_count);
        probe_count = full_probe_count;
      }
      const bool will_second = should_second_chance(fast, probe_count, out);
      if (trace_enabled_ && fast == 5) {
        trace_second_chance(trace_call, fast, out, will_second, probe_count, second_chance_probe_);
      }
      if (will_second) {
        select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
        out = rescore_quantized_extend_prepared(probe_count, second_chance_probe_);
        probe_count = second_chance_probe_;
      }
    } else {
      out = rescore_quantized(q, probe_count);
      if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
        select_rescore_probes(q, full_probe_count, full_centroid_dists);
        out = rescore_quantized(q, full_probe_count);
        probe_count = full_probe_count;
      }
      const bool will_second = should_second_chance(fast, probe_count, out);
      if (trace_enabled_ && fast == 5) {
        trace_second_chance(trace_call, fast, out, will_second, probe_count, second_chance_probe_);
      }
      if (will_second) {
        select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
        out = rescore_quantized(q, second_chance_probe_);
        probe_count = second_chance_probe_;
      }
    }
    if (remaining_repair_) {
      out = remaining_repair_prepared(rescore_quantized_ ? quantized_.data() : q,
                                      probe_count, full_centroid_dists);
    }
    if (should_exact_kd_repair_context(fast, out, true, primary_centroid,
                                       exact_quick_margin, exact_quick_worst,
                                       exact_centroid_gap_max, exact_centroid_gap_last)) {
      out = classify_exact_kd(q);
    }
    if (trace_enabled_) trace_final_decision(trace_call, out);
    return out;
  }

  const uint64_t t0 = now_ns();
  centroid_dists_select8(q);
  const uint64_t t1 = now_ns();
  const uint64_t t2 = now_ns();

  reset_top();
  if (quick_probe_ != kQuickProbe) select_top_probes(quick_probe_);
  if (profile_actual_scan_) {
    scan_probes_bounded(q, 0, quick_probe_, quick_block_bbox_);
  } else {
    scan_probes_bounded_profile(q, 0, quick_probe_);
  }
  int fast = count_frauds();
  if (local_repair_ && fast >= local_repair_min_fast_ && fast <= local_repair_max_fast_) {
    local_repair_top(q);
    fast = count_frauds();
  }
  if (local_fraud_graph_enabled_ && fast >= local_fraud_graph_min_fast_ &&
      fast <= local_fraud_graph_max_fast_) {
    local_fraud_graph_repair_top(q);
    fast = count_frauds();
  }
  const uint64_t t3 = now_ns();
  const uint64_t quick_scan_ns = t3 - t2;
  ++profile_.calls;
  if (fast >= 0 && fast < static_cast<int>(profile_.quick_fraud_counts.size())) {
    ++profile_.quick_fraud_counts[static_cast<size_t>(fast)];
  }
  profile_.centroid_ns += t1 - t0;
  profile_.select8_ns += t2 - t1;
  profile_.quick_scan_ns += quick_scan_ns;
  record_profile_ns(quick_scan_ns, profile_.quick_scan_bins, profile_.max_quick_scan_ns);
  const bool need_exact_features = exact_kd_repair_feature_policy_enabled_;
  const float quick_margin =
      (margin_profile_enabled_ || need_exact_features) ? quick_label_margin(fast) : -1.0f;
  const float exact_quick_worst = need_exact_features ? quick_worst_dist() : 0.0f;
  const float exact_centroid_gap_max =
      need_exact_features ? quick_centroid_gap_max() : 0.0f;
  const float exact_centroid_gap_last =
      need_exact_features ? quick_centroid_gap(6) : 0.0f;
  const int primary_centroid = probes_[0];
  const uint64_t trace_call = trace_enabled_ ? ++trace_calls_ : 0;
  if (fast3_approve_rule_matches(fast)) {
    const uint64_t total_ns = now_ns() - t0;
    ++profile_.quick_only;
    profile_.total_ns += total_ns;
    record_profile_ns(total_ns, profile_.total_bins, profile_.max_total_ns);
    if (trace_enabled_) trace_final_decision(trace_call, 0);
    return 0;
  }
  const bool will_rescore = should_rescore(fast, primary_centroid);
  if (trace_enabled_ && fast >= 2) trace_quick_decision(trace_call, fast, will_rescore);
  if (!will_rescore) {
    int out = fast;
    if (remaining_repair_ && reference_borderline_repair_ && top_has_reference_borderline()) {
      out = remaining_repair_prepared(q, quick_probe_, false);
    }
    if (should_exact_kd_repair_context(fast, out, false, primary_centroid,
                                       quick_margin, exact_quick_worst,
                                       exact_centroid_gap_max,
                                       exact_centroid_gap_last)) {
      out = classify_exact_kd(q);
    }
    const uint64_t total_ns = now_ns() - t0;
    ++profile_.quick_only;
    profile_.total_ns += total_ns;
    record_profile_ns(total_ns, profile_.total_bins, profile_.max_total_ns);
    if (trace_enabled_ && out >= 2) trace_final_decision(trace_call, out);
    return out;
  }
  if (rescore_tree_enabled_) {
    int tree_score = predict_rescore_tree_from_query(raw_q);
    if (tree_score >= 0 && tree_score <= 5) {
      if (should_exact_kd_repair_context(fast, tree_score, true, primary_centroid,
                                         quick_margin, exact_quick_worst,
                                         exact_centroid_gap_max,
                                         exact_centroid_gap_last)) {
        tree_score = classify_exact_kd(q);
      }
      const uint64_t total_ns = now_ns() - t0;
      ++profile_.rescore_tree;
      profile_.total_ns += total_ns;
      record_profile_ns(total_ns, profile_.total_bins, profile_.max_total_ns);
      if (trace_enabled_) trace_final_decision(trace_call, tree_score);
      return tree_score;
    }
  }

  const uint64_t t4 = now_ns();
  const int full_probe_count = rescore_probe_count(fast);
  int probe_count = phase_rescore_probe_count(fast, full_probe_count);
  bool full_centroid_dists = false;
  select_rescore_probes(q, probe_count, full_centroid_dists);
  const uint64_t t5 = now_ns();
  int out = 0;
  if (rescore_quantized_) {
    quantize_query(q);
    out = rescore_quantized_prepared(probe_count);
    if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
      select_rescore_probes(q, full_probe_count, full_centroid_dists);
      out = rescore_quantized_prepared(full_probe_count);
      probe_count = full_probe_count;
    }
    const bool will_second = should_second_chance(fast, probe_count, out);
    if (trace_enabled_ && fast == 5) {
      trace_second_chance(trace_call, fast, out, will_second, probe_count, second_chance_probe_);
    }
    if (will_second) {
      select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
      out = rescore_quantized_extend_prepared(probe_count, second_chance_probe_);
      probe_count = second_chance_probe_;
    }
  } else {
    out = rescore_quantized(q, probe_count);
    if (should_rescore234_extend(fast, probe_count, full_probe_count, out)) {
      select_rescore_probes(q, full_probe_count, full_centroid_dists);
      out = rescore_quantized(q, full_probe_count);
      probe_count = full_probe_count;
    }
    const bool will_second = should_second_chance(fast, probe_count, out);
    if (trace_enabled_ && fast == 5) {
      trace_second_chance(trace_call, fast, out, will_second, probe_count, second_chance_probe_);
    }
    if (will_second) {
      select_second_chance_probes(q, probe_count, second_chance_probe_, full_centroid_dists);
      out = rescore_quantized(q, second_chance_probe_);
      probe_count = second_chance_probe_;
    }
  }
  if (remaining_repair_) {
    out = remaining_repair_prepared(rescore_quantized_ ? quantized_.data() : q,
                                    probe_count, full_centroid_dists);
  }
  if (should_exact_kd_repair_context(fast, out, true, primary_centroid,
                                     quick_margin, exact_quick_worst,
                                     exact_centroid_gap_max,
                                     exact_centroid_gap_last)) {
    out = classify_exact_kd(q);
  }
  const uint64_t t6 = now_ns();
  const uint64_t rescore_ns = t6 - t5;
  const uint64_t total_ns = t6 - t0;
  record_rescore_margin_profile(fast, out, quick_margin);
  record_rescore_centroid_profile(primary_centroid, fast, out);
  ++profile_.rescore;
  if (fast >= 0 && fast < static_cast<int>(profile_.rescore_fraud_counts.size())) {
    const size_t idx = static_cast<size_t>(fast);
    ++profile_.rescore_fraud_counts[idx];
    profile_.rescore_ns_by_fast[idx] += rescore_ns;
    if (rescore_ns > profile_.max_rescore_ns_by_fast[idx]) {
      profile_.max_rescore_ns_by_fast[idx] = rescore_ns;
    }
    if (out >= 0 && out < static_cast<int>(profile_.rescore_result_by_fast[idx].size())) {
      ++profile_.rescore_result_by_fast[idx][static_cast<size_t>(out)];
    }
  }
  profile_.select20_ns += t5 - t4;
  profile_.rescore_ns += rescore_ns;
  profile_.total_ns += total_ns;
  record_profile_ns(rescore_ns, profile_.rescore_bins, profile_.max_rescore_ns);
  record_profile_ns(total_ns, profile_.total_bins, profile_.max_total_ns);
  if (trace_enabled_) trace_final_decision(trace_call, out);
  return out;
}

int NativeIVF::classify_with_quick_probe(const float* q, int quick_probe_override) {
  if (quick_probe_override <= 0 || quick_probe_override == quick_probe_) {
    return classify(q);
  }
  const int saved_quick_probe = quick_probe_;
  quick_probe_ = std::clamp(quick_probe_override, 1, kMaxProbe);
  if (expanded_probe_ < quick_probe_) quick_probe_ = saved_quick_probe;
  const int out = classify(q);
  quick_probe_ = saved_quick_probe;
  return out;
}
