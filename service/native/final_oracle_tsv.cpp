#include "native_vector.hpp"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <string_view>

namespace {

constexpr int kDim = 14;

struct Options {
  const char* input_path = "test-final/test-data.json";
  const char* output_path = "test-final/final-oracle-native.tsv";
};

bool parse_options(int argc, char** argv, Options& options) {
  for (int i = 1; i < argc; ++i) {
    const char* arg = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) return nullptr;
      return argv[++i];
    };
    if (std::strcmp(arg, "--input") == 0) {
      options.input_path = next();
    } else if (std::strcmp(arg, "--output") == 0) {
      options.output_path = next();
    } else {
      std::fprintf(stderr, "unknown arg: %s\n", arg);
      return false;
    }
    if ((!options.input_path || options.input_path[0] == '\0') ||
        (!options.output_path || options.output_path[0] == '\0')) {
      std::fprintf(stderr, "missing value for %s\n", arg);
      return false;
    }
  }
  return true;
}

bool read_file(const char* path, std::string& out) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  in.seekg(0, std::ios::end);
  const std::streamoff size = in.tellg();
  if (size < 0) return false;
  in.seekg(0, std::ios::beg);
  out.resize(static_cast<size_t>(size));
  if (!out.empty()) {
    in.read(out.data(), static_cast<std::streamsize>(out.size()));
  }
  return static_cast<bool>(in);
}

size_t skip_ws(std::string_view s, size_t pos) {
  while (pos < s.size()) {
    const char c = s[pos];
    if (c != ' ' && c != '\n' && c != '\r' && c != '\t') break;
    ++pos;
  }
  return pos;
}

size_t find_matching_object(std::string_view s, size_t open_pos) {
  if (open_pos >= s.size() || s[open_pos] != '{') return std::string_view::npos;
  int depth = 0;
  bool in_string = false;
  bool escape = false;
  for (size_t i = open_pos; i < s.size(); ++i) {
    const char c = s[i];
    if (in_string) {
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == '"') {
      in_string = true;
      continue;
    }
    if (c == '{') {
      ++depth;
    } else if (c == '}') {
      --depth;
      if (depth == 0) return i + 1;
    }
  }
  return std::string_view::npos;
}

bool parse_bool_after(std::string_view s, std::string_view key, size_t from,
                      bool& out, size_t& end_pos) {
  size_t pos = s.find(key, from);
  if (pos == std::string_view::npos) return false;
  pos += key.size();
  pos = skip_ws(s, pos);
  if (pos + 4 <= s.size() && s.compare(pos, 4, "true") == 0) {
    out = true;
    end_pos = pos + 4;
    return true;
  }
  if (pos + 5 <= s.size() && s.compare(pos, 5, "false") == 0) {
    out = false;
    end_pos = pos + 5;
    return true;
  }
  return false;
}

bool parse_double_after(const std::string& data, std::string_view key, size_t from,
                        double& out, size_t& end_pos) {
  std::string_view s(data);
  size_t pos = s.find(key, from);
  if (pos == std::string_view::npos) return false;
  pos += key.size();
  pos = skip_ws(s, pos);
  char* end = nullptr;
  out = std::strtod(data.c_str() + pos, &end);
  if (end == data.c_str() + pos) return false;
  end_pos = static_cast<size_t>(end - data.c_str());
  return true;
}

int score_to_count(double score) {
  int count = static_cast<int>(std::llround(score * 5.0));
  if (count < 0) return 0;
  if (count > 5) return 5;
  return count;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    std::fprintf(stderr, "usage: final-oracle-tsv --input PATH --output PATH\n");
    return 2;
  }

  std::string data;
  if (!read_file(options.input_path, data)) {
    std::fprintf(stderr, "could not read %s\n", options.input_path);
    return 1;
  }

  std::ofstream out(options.output_path);
  if (!out) {
    std::fprintf(stderr, "could not write %s\n", options.output_path);
    return 1;
  }
  out.precision(9);
  out << "# label fraud_count f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13\n";

  constexpr std::string_view kRequestKey = "\"request\":";
  constexpr std::string_view kApprovedKey = "\"expected_approved\":";
  constexpr std::string_view kScoreKey = "\"expected_fraud_score\":";
  std::string_view view(data);

  uint64_t rows = 0;
  uint64_t vector_failures = 0;
  uint64_t approval_mismatches = 0;
  size_t pos = 0;
  while (true) {
    size_t req_key = view.find(kRequestKey, pos);
    if (req_key == std::string_view::npos) break;
    size_t req_start = skip_ws(view, req_key + kRequestKey.size());
    size_t req_end = find_matching_object(view, req_start);
    if (req_end == std::string_view::npos) {
      std::fprintf(stderr, "could not find request object end near byte %zu\n", req_start);
      return 1;
    }

    bool expected_approved = false;
    size_t approved_end = 0;
    if (!parse_bool_after(view, kApprovedKey, req_end, expected_approved, approved_end)) {
      std::fprintf(stderr, "could not parse expected_approved after byte %zu\n", req_end);
      return 1;
    }
    double expected_score = 0.0;
    size_t score_end = 0;
    if (!parse_double_after(data, kScoreKey, approved_end, expected_score, score_end)) {
      std::fprintf(stderr, "could not parse expected_fraud_score after byte %zu\n",
                   approved_end);
      return 1;
    }

    const int count = score_to_count(expected_score);
    if ((count < 3) != expected_approved) ++approval_mismatches;

    float q[kDim]{};
    const auto* body = reinterpret_cast<const uint8_t*>(data.data() + req_start);
    const size_t body_len = req_end - req_start;
    if (!build_fraud_vector_cpp(body, body_len, q)) {
      ++vector_failures;
      pos = score_end;
      continue;
    }

    out << (count >= 3 ? 1 : 0) << ' ' << count;
    for (float value : q) {
      out << ' ' << value;
    }
    out << '\n';
    ++rows;
    pos = score_end;
  }

  out.close();
  std::fprintf(stderr,
               "final_oracle_tsv rows=%llu vector_failures=%llu approval_mismatches=%llu "
               "input=%s output=%s\n",
               static_cast<unsigned long long>(rows),
               static_cast<unsigned long long>(vector_failures),
               static_cast<unsigned long long>(approval_mismatches),
               options.input_path, options.output_path);
  return vector_failures == 0 ? 0 : 1;
}
