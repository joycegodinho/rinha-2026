#include "native_ivf.hpp"

#include <immintrin.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <fstream>
#include <time.h>

namespace {

volatile uint64_t g_native_ivf_warmup_sink = 0;
constexpr int kNativeDim = 14;
constexpr int kNativeVectorsPerBlock = 16;

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
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

inline void update_candidate(float dist, uint8_t label, std::array<float, 5>& top_dist,
                             std::array<uint8_t, 5>& top_label, int& worst) {
  if (dist >= top_dist[static_cast<size_t>(worst)]) return;
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
               "avg_centroid_ns=%llu avg_select8_ns=%llu avg_quick_scan_ns=%llu "
               "avg_select20_ns=%llu avg_rescore_ns=%llu\n",
               static_cast<unsigned long long>(calls),
               static_cast<unsigned long long>(profile_.quick_only),
               static_cast<unsigned long long>(profile_.rescore),
               static_cast<unsigned long long>(profile_.centroid_ns / calls),
               static_cast<unsigned long long>(profile_.select8_ns / calls),
               static_cast<unsigned long long>(profile_.quick_scan_ns / calls),
               static_cast<unsigned long long>(profile_.select20_ns / rescore),
               static_cast<unsigned long long>(profile_.rescore_ns / rescore));
}

bool NativeIVF::load(const char* path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    error_ = "could not open index";
    return false;
  }

  char magic[4];
  if (!read_exact(in, magic, 4) || std::memcmp(magic, "IVF1", 4) != 0) {
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

  offsets_.resize(static_cast<size_t>(k_) + 1);
  if (!read_exact(in, offsets_.data(), offsets_.size())) {
    error_ = "could not read offsets";
    return false;
  }
  total_blocks_ = offsets_[k_];
  const size_t padded_n = static_cast<size_t>(total_blocks_) * kVectorsPerBlock;

  labels_.resize(padded_n);
  if (!read_exact(in, labels_.data(), labels_.size())) {
    error_ = "could not read labels";
    return false;
  }

  blocks_.resize(static_cast<size_t>(total_blocks_) * kBlockStride);
  if (!read_exact(in, blocks_.data(), blocks_.size())) {
    error_ = "could not read blocks";
    return false;
  }

  const char* bounds_env = std::getenv("NATIVE_IVF_BLOCK_BOUNDS");
  use_block_bounds_ = !(bounds_env && std::strcmp(bounds_env, "0") == 0);
  if (use_block_bounds_) {
    build_block_radii();
  }

  return true;
}

void NativeIVF::warmup() {
  uint64_t acc = 0;
  acc += touch_pages(centroids_);
  acc += touch_pages(offsets_);
  acc += touch_pages(labels_);
  acc += touch_pages(blocks_);

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

void NativeIVF::centroid_dists_select8(const float* q) {
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

void NativeIVF::reset_top() {
  top_dist_.fill(kInitialTopDist);
  top_label_.fill(0);
}

void NativeIVF::scan_probes(const float* q, int from, int to) {
  const __m256 scale = _mm256_set1_ps(kVectorScale);
  int worst = 0;

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
        __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
        __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
        vf_low = _mm256_mul_ps(vf_low, scale);
        vf_high = _mm256_mul_ps(vf_high, scale);
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
          __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
          vf_low = _mm256_mul_ps(vf_low, scale);
          const __m256 dl = _mm256_sub_ps(vf_low, qv);
          low = _mm256_fmadd_ps(dl, dl, low);
        }
        if (high_alive) {
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
          __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
          vf_high = _mm256_mul_ps(vf_high, scale);
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

void NativeIVF::build_block_radii() {
  block_min_radii_.assign(static_cast<size_t>(total_blocks_), 0.0f);
  block_max_radii_.assign(static_cast<size_t>(total_blocks_), 0.0f);

  for (uint32_t ci = 0; ci < k_; ++ci) {
    const uint32_t start = offsets_[static_cast<size_t>(ci)];
    const uint32_t end = offsets_[static_cast<size_t>(ci + 1)];
    for (uint32_t block = start; block < end; ++block) {
      const size_t base = static_cast<size_t>(block) * kBlockStride;
      float min_dist = INFINITY;
      float max_dist = 0.0f;

      for (int slot = 0; slot < kVectorsPerBlock; ++slot) {
        if (is_padding_slot(blocks_, base, slot)) continue;
        float dist = 0.0f;
        for (int d = 0; d < kDim; ++d) {
          const float ref = static_cast<float>(
                                blocks_[base + static_cast<size_t>(d) * kVectorsPerBlock +
                                        static_cast<size_t>(slot)]) *
                            kVectorScale;
          const float diff = ref - centroids_[static_cast<size_t>(d) * k_ + ci];
          dist += diff * diff;
        }
        min_dist = std::min(min_dist, dist);
        max_dist = std::max(max_dist, dist);
      }

      if (!std::isfinite(min_dist)) min_dist = 0.0f;
      block_min_radii_[static_cast<size_t>(block)] = std::sqrt(min_dist);
      block_max_radii_[static_cast<size_t>(block)] = std::sqrt(max_dist);
    }
  }
}

void NativeIVF::bound_probe_range(uint32_t& start, uint32_t& end, float center_dist_sq,
                                  float worst_dist_sq) const {
  if (start >= end || worst_dist_sq >= kInitialTopDist || block_min_radii_.empty() ||
      block_min_radii_.size() != block_max_radii_.size()) {
    return;
  }

  const float center_dist = std::sqrt(center_dist_sq);
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

void NativeIVF::scan_probes_bounded(const float* q, int from, int to) {
  if (!use_block_bounds_ || block_min_radii_.empty()) {
    scan_probes(q, from, to);
    return;
  }

  const __m256 scale = _mm256_set1_ps(kVectorScale);
  int worst = 0;

  alignas(32) float dist_low[8];
  alignas(32) float dist_high[8];

  for (int pi = from; pi < to; ++pi) {
    const int probe = probes_[static_cast<size_t>(pi)];
    uint32_t start = offsets_[static_cast<size_t>(probe)];
    uint32_t end = offsets_[static_cast<size_t>(probe + 1)];
    bound_probe_range(start, end, centroid_dists_[static_cast<size_t>(probe)],
                      top_dist_[static_cast<size_t>(worst)]);
    if (start >= end) continue;

    for (uint32_t block = start; block < end; ++block) {
      const int16_t* base = blocks_.data() + static_cast<size_t>(block) * kBlockStride;
      __m256 low = _mm256_setzero_ps();
      __m256 high = _mm256_setzero_ps();

      for (int d = 0; d < 6; ++d) {
        const int16_t* row = base + d * kVectorsPerBlock;
        const __m128i raw_low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row));
        const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
        __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
        __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
        vf_low = _mm256_mul_ps(vf_low, scale);
        vf_high = _mm256_mul_ps(vf_high, scale);
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
          __m256 vf_low = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_low));
          vf_low = _mm256_mul_ps(vf_low, scale);
          const __m256 dl = _mm256_sub_ps(vf_low, qv);
          low = _mm256_fmadd_ps(dl, dl, low);
        }
        if (high_alive) {
          const __m128i raw_high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + 8));
          __m256 vf_high = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw_high));
          vf_high = _mm256_mul_ps(vf_high, scale);
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

int NativeIVF::count_frauds() const {
  return static_cast<int>(top_label_[0] + top_label_[1] + top_label_[2] + top_label_[3] + top_label_[4]);
}

int NativeIVF::rescore_quantized(const float* q) {
  for (int i = 0; i < kDim; ++i) {
    int x = static_cast<int>(q[i] * 10000.0f + 0.5f);
    if (q[i] < 0.0f) {
      x = static_cast<int>(q[i] * 10000.0f - 0.5f);
    }
    if (x < -32768) {
      x = -32768;
    } else if (x > 32767) {
      x = 32767;
    }
    quantized_[static_cast<size_t>(i)] = static_cast<float>(static_cast<int16_t>(x)) * kVectorScale;
  }

  reset_top();
  scan_probes(quantized_.data(), 0, kExpandedProbe);
  return count_frauds();
}

int NativeIVF::classify(const float* q) {
  if (k_ == 0) return -1;
  if (!profile_enabled_) {
    centroid_dists_select8(q);

    reset_top();
    scan_probes_bounded(q, 0, kQuickProbe);
    const int fast = count_frauds();
    if (fast != 2 && fast != 3) {
      return fast;
    }

    select_top_masked<kExpandedProbe>();
    return rescore_quantized(q);
  }

  const uint64_t t0 = now_ns();
  centroid_dists_select8(q);
  const uint64_t t1 = now_ns();
  const uint64_t t2 = now_ns();

  reset_top();
  scan_probes_bounded(q, 0, kQuickProbe);
  const uint64_t t3 = now_ns();
  const int fast = count_frauds();
  ++profile_.calls;
  profile_.centroid_ns += t1 - t0;
  profile_.select8_ns += t2 - t1;
  profile_.quick_scan_ns += t3 - t2;
  if (fast != 2 && fast != 3) {
    ++profile_.quick_only;
    return fast;
  }

  const uint64_t t4 = now_ns();
  select_top_masked<kExpandedProbe>();
  const uint64_t t5 = now_ns();
  int out = rescore_quantized(q);
  const uint64_t t6 = now_ns();
  ++profile_.rescore;
  profile_.select20_ns += t5 - t4;
  profile_.rescore_ns += t6 - t5;
  return out;
}
