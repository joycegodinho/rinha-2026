#include "native_ivf.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

namespace {

constexpr int kDim = 14;
constexpr int kFastCount = 6;
constexpr int kMaxCentroids = 4096;

struct PairStats {
  uint64_t observed = 0;
  uint64_t oracle_bad_approval = 0;
  uint64_t rescore_bad_approval = 0;
  uint64_t rescore_changed_approval = 0;
};

struct FeatureRow {
  float margin = -1.0f;
  float worst = 0.0f;
  float gap = 0.0f;
  float last_gap = 0.0f;
  bool quick_bad = false;
};

struct Options {
  const char* index_path = "service/index.bin";
  const char* oracle_path = nullptr;
  const char* output_path = nullptr;
  const char* dump_rescore_path = nullptr;
  const char* dump_rescore_debug_path = nullptr;
  const char* dump_exact_repair_debug_path = nullptr;
  int min_observed = 2;
  bool exact_count = false;
  bool feature_rules = false;
};

bool approved(int count) {
  return count < 3;
}

bool parse_bool_arg(const char* value) {
  return std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
         std::strcmp(value, "yes") == 0;
}

bool parse_options(int argc, char** argv, Options& options) {
  for (int i = 1; i < argc; ++i) {
    const char* arg = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) return nullptr;
      return argv[++i];
    };
    if (std::strcmp(arg, "--index") == 0) {
      options.index_path = next();
    } else if (std::strcmp(arg, "--oracle") == 0) {
      options.oracle_path = next();
    } else if (std::strcmp(arg, "--output") == 0) {
      options.output_path = next();
    } else if (std::strcmp(arg, "--dump-rescore-oracle") == 0) {
      options.dump_rescore_path = next();
    } else if (std::strcmp(arg, "--dump-rescore-debug") == 0) {
      options.dump_rescore_debug_path = next();
    } else if (std::strcmp(arg, "--dump-exact-repair-debug") == 0) {
      options.dump_exact_repair_debug_path = next();
    } else if (std::strcmp(arg, "--min-observed") == 0) {
      const char* value = next();
      options.min_observed = value ? std::atoi(value) : 0;
    } else if (std::strcmp(arg, "--exact-count") == 0) {
      options.exact_count = true;
    } else if (std::strcmp(arg, "--approval-only") == 0) {
      options.exact_count = false;
    } else if (std::strcmp(arg, "--exact-count-value") == 0) {
      const char* value = next();
      options.exact_count = value && parse_bool_arg(value);
    } else if (std::strcmp(arg, "--feature-rules") == 0) {
      options.feature_rules = true;
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", arg);
      return false;
    }
    if ((std::strcmp(arg, "--index") == 0 || std::strcmp(arg, "--oracle") == 0 ||
         std::strcmp(arg, "--output") == 0 ||
         std::strcmp(arg, "--dump-rescore-oracle") == 0 ||
         std::strcmp(arg, "--dump-rescore-debug") == 0 ||
         std::strcmp(arg, "--dump-exact-repair-debug") == 0 ||
         std::strcmp(arg, "--min-observed") == 0 ||
         std::strcmp(arg, "--exact-count-value") == 0) &&
        (!argv[i] || argv[i][0] == '-')) {
      std::fprintf(stderr, "missing value for %s\n", arg);
      return false;
    }
  }
  if (!options.oracle_path ||
      (!options.output_path && !options.dump_rescore_path &&
       !options.dump_rescore_debug_path && !options.dump_exact_repair_debug_path) ||
      !options.index_path || options.min_observed <= 0) {
    std::fprintf(stderr,
                 "usage: ivf-oracle-policy --oracle PATH --output PATH "
                 "[--index service/index.bin] [--min-observed N] [--exact-count] "
                 "[--feature-rules] [--dump-rescore-oracle PATH] "
                 "[--dump-rescore-debug PATH] [--dump-exact-repair-debug PATH]\n");
    return false;
  }
  return true;
}

enum class CondKind {
  MarginMin = 0,
  WorstMax = 1,
  GapMin = 2,
  GapMax = 3,
  LastGapMin = 4,
  LastGapMax = 5,
};

struct RuleCandidate {
  bool valid = false;
  int cond_a = -1;
  int cond_b = -1;
  float threshold_a = 0.0f;
  float threshold_b = 0.0f;
  uint64_t covered = 0;
};

float cond_value(const FeatureRow& row, int cond) {
  switch (static_cast<CondKind>(cond)) {
    case CondKind::MarginMin:
      return row.margin;
    case CondKind::WorstMax:
      return row.worst;
    case CondKind::GapMin:
    case CondKind::GapMax:
      return row.gap;
    case CondKind::LastGapMin:
    case CondKind::LastGapMax:
      return row.last_gap;
  }
  return 0.0f;
}

bool cond_passes(const FeatureRow& row, int cond, float threshold) {
  const float value = cond_value(row, cond);
  if (!std::isfinite(value)) return false;
  switch (static_cast<CondKind>(cond)) {
    case CondKind::MarginMin:
      return value >= threshold;
    case CondKind::WorstMax:
      return value <= threshold;
    case CondKind::GapMin:
      return value >= threshold;
    case CondKind::GapMax:
      return value <= threshold;
    case CondKind::LastGapMin:
      return value >= threshold;
    case CondKind::LastGapMax:
      return value <= threshold;
  }
  return false;
}

bool cond_is_usable_value(int cond, float value) {
  if (!std::isfinite(value)) return false;
  if (cond == static_cast<int>(CondKind::MarginMin)) return value >= 0.0f;
  return value > 0.0f;
}

bool same_feature(int a, int b) {
  if (a == b) return true;
  if ((a == static_cast<int>(CondKind::GapMin) && b == static_cast<int>(CondKind::GapMax)) ||
      (a == static_cast<int>(CondKind::GapMax) && b == static_cast<int>(CondKind::GapMin))) {
    return false;
  }
  if ((a == static_cast<int>(CondKind::LastGapMin) &&
       b == static_cast<int>(CondKind::LastGapMax)) ||
      (a == static_cast<int>(CondKind::LastGapMax) &&
       b == static_cast<int>(CondKind::LastGapMin))) {
    return false;
  }
  return cond_value(FeatureRow{}, a) == cond_value(FeatureRow{}, b) && false;
}

bool selected_by_rule(const FeatureRow& row, const RuleCandidate& rule) {
  if (!rule.valid) return false;
  if (rule.cond_a >= 0 && !cond_passes(row, rule.cond_a, rule.threshold_a)) return false;
  if (rule.cond_b >= 0 && !cond_passes(row, rule.cond_b, rule.threshold_b)) return false;
  return true;
}

void consider_candidate(const std::vector<FeatureRow>& rows, RuleCandidate candidate,
                        int min_observed, RuleCandidate& best) {
  uint64_t covered = 0;
  uint64_t bad = 0;
  for (const FeatureRow& row : rows) {
    if (!selected_by_rule(row, candidate)) continue;
    ++covered;
    if (row.quick_bad) ++bad;
  }
  if (covered < static_cast<uint64_t>(min_observed) || bad != 0) return;
  candidate.covered = covered;
  if (!best.valid || candidate.covered > best.covered ||
      (candidate.covered == best.covered && candidate.cond_b < 0 && best.cond_b >= 0)) {
    best = candidate;
  }
}

RuleCandidate find_feature_rule(const std::vector<FeatureRow>& rows, int min_observed) {
  RuleCandidate best{};
  for (int cond = 0; cond < 6; ++cond) {
    std::vector<float> thresholds;
    thresholds.reserve(rows.size());
    for (const FeatureRow& row : rows) {
      const float value = cond_value(row, cond);
      if (cond_is_usable_value(cond, value)) thresholds.push_back(value);
    }
    std::sort(thresholds.begin(), thresholds.end());
    thresholds.erase(std::unique(thresholds.begin(), thresholds.end()), thresholds.end());
    for (float threshold : thresholds) {
      consider_candidate(rows, RuleCandidate{true, cond, -1, threshold, 0.0f, 0},
                         min_observed, best);
    }
  }

  for (int cond_a = 0; cond_a < 6; ++cond_a) {
    std::vector<float> thresholds_a;
    thresholds_a.reserve(rows.size());
    for (const FeatureRow& row : rows) {
      const float value = cond_value(row, cond_a);
      if (cond_is_usable_value(cond_a, value)) thresholds_a.push_back(value);
    }
    std::sort(thresholds_a.begin(), thresholds_a.end());
    thresholds_a.erase(std::unique(thresholds_a.begin(), thresholds_a.end()), thresholds_a.end());
    for (int cond_b = cond_a + 1; cond_b < 6; ++cond_b) {
      if (same_feature(cond_a, cond_b)) continue;
      std::vector<float> thresholds_b;
      thresholds_b.reserve(rows.size());
      for (const FeatureRow& row : rows) {
        const float value = cond_value(row, cond_b);
        if (cond_is_usable_value(cond_b, value)) thresholds_b.push_back(value);
      }
      std::sort(thresholds_b.begin(), thresholds_b.end());
      thresholds_b.erase(std::unique(thresholds_b.begin(), thresholds_b.end()),
                         thresholds_b.end());
      for (float threshold_a : thresholds_a) {
        for (float threshold_b : thresholds_b) {
          consider_candidate(rows,
                             RuleCandidate{true, cond_a, cond_b, threshold_a, threshold_b, 0},
                             min_observed, best);
        }
      }
    }
  }
  return best;
}

void apply_cond_to_fields(int cond, float threshold, float& min_margin, float& max_worst,
                          float& min_gap, float& max_gap, float& min_last_gap,
                          float& max_last_gap) {
  switch (static_cast<CondKind>(cond)) {
    case CondKind::MarginMin:
      min_margin = threshold;
      break;
    case CondKind::WorstMax:
      max_worst = threshold;
      break;
    case CondKind::GapMin:
      min_gap = threshold;
      break;
    case CondKind::GapMax:
      max_gap = threshold;
      break;
    case CondKind::LastGapMin:
      min_last_gap = threshold;
      break;
    case CondKind::LastGapMax:
      max_last_gap = threshold;
      break;
  }
}

}  // namespace

int main(int argc, char** argv) {
  Options options{};
  if (!parse_options(argc, argv, options)) return 2;

  NativeIVF ivf;
  if (!ivf.load(options.index_path)) {
    std::fprintf(stderr, "could not load index %s: %s\n", options.index_path,
                 ivf.error().c_str());
    return 1;
  }

  std::ifstream in(options.oracle_path);
  if (!in) {
    std::fprintf(stderr, "could not open oracle %s\n", options.oracle_path);
    return 1;
  }

  std::array<PairStats, kFastCount * kMaxCentroids> pair_stats{};
  std::array<std::vector<FeatureRow>, kFastCount * kMaxCentroids> feature_rows{};
  std::array<uint64_t, kFastCount> rescore_by_fast{};
  std::array<uint64_t, kFastCount> oracle_bad_by_fast{};
  std::array<uint64_t, kFastCount> rescore_bad_by_fast{};
  uint64_t rows = 0;
  uint64_t skipped = 0;
  uint64_t quick_bad_total = 0;
  uint64_t final_bad_total = 0;
  uint64_t would_rescore = 0;
  uint64_t rh26_expanded = 0;
  uint64_t rh26_repaired = 0;
  uint64_t exact_repair = 0;
  uint64_t exact_repair_changed = 0;
  uint64_t exact_repair_pre_bad = 0;
  uint64_t exact_repair_bad = 0;
  uint64_t exact_repair_fixed = 0;
  uint64_t exact_repair_hurt = 0;
  std::ofstream dump_rescore;
  std::ofstream dump_debug;
  std::ofstream dump_exact_repair_debug;
  if (options.dump_rescore_path) {
    dump_rescore.open(options.dump_rescore_path);
    if (!dump_rescore) {
      std::fprintf(stderr, "could not write %s\n", options.dump_rescore_path);
      return 1;
    }
    dump_rescore << "# label fraud_count f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13"
                 << " # dumped_from=" << options.oracle_path << "\n";
  }
  if (options.dump_rescore_debug_path) {
    dump_debug.open(options.dump_rescore_debug_path);
    if (!dump_debug) {
      std::fprintf(stderr, "could not write %s\n", options.dump_rescore_debug_path);
      return 1;
    }
    dump_debug.precision(9);
    dump_debug
        << "# label fraud_count oracle_approved fast out primary_centroid quick_bad "
           "rescore_bad rescore_changed quick_best quick_worst quick_margin "
           "centroid_gap_max centroid_gap_last "
           "q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 "
           "ql0 ql1 ql2 ql3 ql4 qd0 qd1 qd2 qd3 qd4 "
           "rl0 rl1 rl2 rl3 rl4 rd0 rd1 rd2 rd3 rd4 "
           "rh26_final_nprobe rh26_expanded rh26_repaired"
        << " # dumped_from=" << options.oracle_path << "\n";
  }
  if (options.dump_exact_repair_debug_path) {
    dump_exact_repair_debug.open(options.dump_exact_repair_debug_path);
    if (!dump_exact_repair_debug) {
      std::fprintf(stderr, "could not write %s\n", options.dump_exact_repair_debug_path);
      return 1;
    }
    dump_exact_repair_debug.precision(9);
    dump_exact_repair_debug
        << "# label fraud_count oracle_approved fast pre_repair exact_kd_out out "
           "pre_bad exact_bad repair_changed repair_fixed repair_hurt would_rescore "
           "primary_centroid quick_best quick_worst quick_margin centroid_gap_max "
           "centroid_gap_last "
           "q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 "
           "ql0 ql1 ql2 ql3 ql4 qd0 qd1 qd2 qd3 qd4 "
           "rl0 rl1 rl2 rl3 rl4 rd0 rd1 rd2 rd3 rd4"
        << " # dumped_from=" << options.oracle_path << "\n";
  }

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') continue;
    const char* p = line.c_str();
    char* end = nullptr;
    const long oracle_label = std::strtol(p, &end, 10);
    if (end == p) {
      ++skipped;
      continue;
    }
    p = end;
    const long oracle_count = std::strtol(p, &end, 10);
    if (end == p || oracle_count < 0 || oracle_count > 5) {
      ++skipped;
      continue;
    }
    p = end;
    std::array<float, kDim> q{};
    bool ok = true;
    for (int d = 0; d < kDim; ++d) {
      q[static_cast<size_t>(d)] = std::strtof(p, &end);
      if (end == p) {
        ok = false;
        break;
      }
      p = end;
    }
    if (!ok) {
      ++skipped;
      continue;
    }

    ++rows;
    const NativeIVF::DecisionDebug dbg = ivf.classify_debug(q.data());
    if (dbg.rh26_expanded) ++rh26_expanded;
    if (dbg.rh26_repaired) ++rh26_repaired;
    const bool row_quick_bad =
        options.exact_count ? (dbg.fast != oracle_count)
                            : (approved(dbg.fast) != approved(static_cast<int>(oracle_count)));
    const bool row_final_bad =
        options.exact_count ? (dbg.out != oracle_count)
                            : (approved(dbg.out) != approved(static_cast<int>(oracle_count)));
    if (row_quick_bad) ++quick_bad_total;
    if (row_final_bad) ++final_bad_total;
    if (dbg.exact_kd_repaired) {
      ++exact_repair;
      const bool pre_bad =
          options.exact_count ? (dbg.pre_repair != oracle_count)
                              : (approved(dbg.pre_repair) !=
                                 approved(static_cast<int>(oracle_count)));
      const bool exact_bad =
          options.exact_count ? (dbg.out != oracle_count)
                              : (approved(dbg.out) != approved(static_cast<int>(oracle_count)));
      const bool repair_changed = dbg.pre_repair != dbg.exact_kd_out;
      const bool repair_fixed = pre_bad && !exact_bad;
      const bool repair_hurt = !pre_bad && exact_bad;
      if (repair_changed) ++exact_repair_changed;
      if (pre_bad) ++exact_repair_pre_bad;
      if (exact_bad) ++exact_repair_bad;
      if (repair_fixed) ++exact_repair_fixed;
      if (repair_hurt) ++exact_repair_hurt;
      if (dump_exact_repair_debug) {
        dump_exact_repair_debug << oracle_label << ' ' << oracle_count << ' '
                                << (approved(static_cast<int>(oracle_count)) ? 1 : 0) << ' '
                                << dbg.fast << ' ' << dbg.pre_repair << ' '
                                << dbg.exact_kd_out << ' ' << dbg.out << ' '
                                << (pre_bad ? 1 : 0) << ' ' << (exact_bad ? 1 : 0) << ' '
                                << (repair_changed ? 1 : 0) << ' '
                                << (repair_fixed ? 1 : 0) << ' '
                                << (repair_hurt ? 1 : 0) << ' '
                                << (dbg.would_rescore ? 1 : 0) << ' '
                                << dbg.primary_centroid << ' ' << dbg.quick_best << ' '
                                << dbg.quick_worst << ' ' << dbg.quick_margin << ' '
                                << dbg.centroid_gap_max << ' ' << dbg.centroid_gap_last;
        for (float value : q) {
          dump_exact_repair_debug << ' ' << value;
        }
        for (uint8_t label : dbg.quick_labels) {
          dump_exact_repair_debug << ' ' << static_cast<unsigned>(label);
        }
        for (float value : dbg.quick_dists) {
          dump_exact_repair_debug << ' ' << value;
        }
        for (uint8_t label : dbg.rescore_labels) {
          dump_exact_repair_debug << ' ' << static_cast<unsigned>(label);
        }
        for (float value : dbg.rescore_dists) {
          dump_exact_repair_debug << ' ' << value;
        }
        dump_exact_repair_debug << "\n";
      }
    }
    if (!dbg.would_rescore || dbg.fast < 0 || dbg.fast >= kFastCount ||
        dbg.primary_centroid < 0 || dbg.primary_centroid >= kMaxCentroids) {
      continue;
    }

    ++would_rescore;
    if (dump_rescore) {
      dump_rescore << line << "\n";
    }
    ++rescore_by_fast[static_cast<size_t>(dbg.fast)];
    const size_t pair_idx = static_cast<size_t>(dbg.fast) * kMaxCentroids +
                            static_cast<size_t>(dbg.primary_centroid);
    PairStats& stats = pair_stats[pair_idx];
    ++stats.observed;

    const bool quick_bad =
        options.exact_count ? (dbg.fast != oracle_count)
                            : (approved(dbg.fast) != approved(static_cast<int>(oracle_count)));
    const bool rescore_bad =
        options.exact_count ? (dbg.out != oracle_count)
                            : (approved(dbg.out) != approved(static_cast<int>(oracle_count)));
    const bool rescore_changed =
        approved(dbg.out) != approved(dbg.fast);
    if (quick_bad) {
      ++stats.oracle_bad_approval;
      ++oracle_bad_by_fast[static_cast<size_t>(dbg.fast)];
    }
    if (rescore_bad) {
      ++stats.rescore_bad_approval;
      ++rescore_bad_by_fast[static_cast<size_t>(dbg.fast)];
    }
    if (rescore_changed) ++stats.rescore_changed_approval;
    if (dump_debug) {
      dump_debug << oracle_label << ' ' << oracle_count << ' '
                 << (approved(static_cast<int>(oracle_count)) ? 1 : 0) << ' '
                 << dbg.fast << ' ' << dbg.out << ' ' << dbg.primary_centroid << ' '
                 << (quick_bad ? 1 : 0) << ' ' << (rescore_bad ? 1 : 0) << ' '
                 << (rescore_changed ? 1 : 0) << ' ' << dbg.quick_best << ' '
                 << dbg.quick_worst << ' ' << dbg.quick_margin << ' '
                 << dbg.centroid_gap_max << ' ' << dbg.centroid_gap_last;
      for (float value : q) {
        dump_debug << ' ' << value;
      }
      for (uint8_t label : dbg.quick_labels) {
        dump_debug << ' ' << static_cast<unsigned>(label);
      }
      for (float value : dbg.quick_dists) {
        dump_debug << ' ' << value;
      }
      for (uint8_t label : dbg.rescore_labels) {
        dump_debug << ' ' << static_cast<unsigned>(label);
      }
      for (float value : dbg.rescore_dists) {
        dump_debug << ' ' << value;
      }
      dump_debug << ' ' << dbg.rh26_final_nprobe << ' '
                 << (dbg.rh26_expanded ? 1 : 0) << ' '
                 << (dbg.rh26_repaired ? 1 : 0);
      dump_debug << "\n";
    }
    if (options.feature_rules) {
      feature_rows[pair_idx].push_back(FeatureRow{dbg.quick_margin, dbg.quick_worst,
                                                  dbg.centroid_gap_max,
                                                  dbg.centroid_gap_last, quick_bad});
    }
  }

  std::ofstream out;
  if (options.output_path) {
    out.open(options.output_path);
    if (!out) {
      std::fprintf(stderr, "could not write %s\n", options.output_path);
      return 1;
    }
    out << "# generated_by=ivf-oracle-policy oracle=" << options.oracle_path
        << " min_observed=" << options.min_observed
        << " mode=" << (options.exact_count ? "exact_count" : "approval")
        << " feature_rules=" << (options.feature_rules ? "1" : "0") << "\n";
    if (options.feature_rules) {
      out << "# fast centroid min_margin max_worst min_gap max_gap min_last_gap max_last_gap "
             "# observed covered oracle_bad rescore_bad rescore_changed\n";
    } else {
      out << "# fast centroid observed oracle_bad rescore_bad rescore_changed\n";
    }
  }

  uint64_t loaded_pairs = 0;
  uint64_t covered = 0;
  for (int fast = 0; fast < kFastCount; ++fast) {
    for (int centroid = 0; centroid < kMaxCentroids; ++centroid) {
      const size_t idx = static_cast<size_t>(fast) * kMaxCentroids + static_cast<size_t>(centroid);
      const PairStats& stats = pair_stats[idx];
      if (stats.observed < static_cast<uint64_t>(options.min_observed)) continue;
      uint64_t rule_covered = stats.observed;
      if (!options.feature_rules) {
        if (stats.oracle_bad_approval != 0) continue;
        if (out) {
          out << fast << ' ' << centroid << " # " << stats.observed << ' '
              << stats.oracle_bad_approval << ' ' << stats.rescore_bad_approval << ' '
              << stats.rescore_changed_approval << "\n";
        }
      } else {
        const RuleCandidate rule = find_feature_rule(feature_rows[idx], options.min_observed);
        if (!rule.valid) continue;
        float min_margin = -1.0f;
        float max_worst = 0.0f;
        float min_gap = 0.0f;
        float max_gap = 0.0f;
        float min_last_gap = 0.0f;
        float max_last_gap = 0.0f;
        apply_cond_to_fields(rule.cond_a, rule.threshold_a, min_margin, max_worst, min_gap,
                             max_gap, min_last_gap, max_last_gap);
        if (rule.cond_b >= 0) {
          apply_cond_to_fields(rule.cond_b, rule.threshold_b, min_margin, max_worst, min_gap,
                               max_gap, min_last_gap, max_last_gap);
        }
        rule_covered = rule.covered;
        if (out) {
          out << fast << ' ' << centroid << ' ' << min_margin << ' ' << max_worst << ' '
              << min_gap << ' ' << max_gap << ' ' << min_last_gap << ' ' << max_last_gap
              << " # " << stats.observed << ' ' << rule_covered << ' '
              << stats.oracle_bad_approval << ' ' << stats.rescore_bad_approval << ' '
              << stats.rescore_changed_approval << "\n";
        }
      }
      ++loaded_pairs;
      covered += rule_covered;
    }
  }
  if (out) out.close();
  if (dump_rescore) dump_rescore.close();
  if (dump_debug) dump_debug.close();
  if (dump_exact_repair_debug) dump_exact_repair_debug.close();

  std::fprintf(stderr,
               "oracle_policy rows=%llu skipped=%llu quick_bad_total=%llu "
               "final_bad_total=%llu would_rescore=%llu rh26_expanded=%llu "
               "rh26_repaired=%llu pairs=%llu "
               "covered=%llu exact_repair=%llu exact_repair_changed=%llu "
               "exact_repair_pre_bad=%llu exact_repair_bad=%llu "
               "exact_repair_fixed=%llu exact_repair_hurt=%llu "
               "output=%s dump_rescore=%s dump_debug=%s dump_exact_repair_debug=%s\n",
               static_cast<unsigned long long>(rows),
               static_cast<unsigned long long>(skipped),
               static_cast<unsigned long long>(quick_bad_total),
               static_cast<unsigned long long>(final_bad_total),
               static_cast<unsigned long long>(would_rescore),
               static_cast<unsigned long long>(rh26_expanded),
               static_cast<unsigned long long>(rh26_repaired),
               static_cast<unsigned long long>(loaded_pairs),
               static_cast<unsigned long long>(covered),
               static_cast<unsigned long long>(exact_repair),
               static_cast<unsigned long long>(exact_repair_changed),
               static_cast<unsigned long long>(exact_repair_pre_bad),
               static_cast<unsigned long long>(exact_repair_bad),
               static_cast<unsigned long long>(exact_repair_fixed),
               static_cast<unsigned long long>(exact_repair_hurt),
               options.output_path ? options.output_path : "",
               options.dump_rescore_path ? options.dump_rescore_path : "",
               options.dump_rescore_debug_path ? options.dump_rescore_debug_path : "",
               options.dump_exact_repair_debug_path ? options.dump_exact_repair_debug_path : "");
  for (int fast = 0; fast < kFastCount; ++fast) {
    if (rescore_by_fast[static_cast<size_t>(fast)] == 0) continue;
    std::fprintf(stderr,
                 "oracle_policy fast=%d observed=%llu quick_bad=%llu rescore_bad=%llu\n",
                 fast,
                 static_cast<unsigned long long>(rescore_by_fast[static_cast<size_t>(fast)]),
                 static_cast<unsigned long long>(oracle_bad_by_fast[static_cast<size_t>(fast)]),
                 static_cast<unsigned long long>(rescore_bad_by_fast[static_cast<size_t>(fast)]));
  }
  return 0;
}
