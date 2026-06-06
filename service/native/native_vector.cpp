#include "native_vector.hpp"

#include <array>
#include <cstdint>
#include <cstring>

#include "decision_tree.hpp"

namespace {

constexpr int kDim = 16;
constexpr float kMaxAmount = 10000.0f;
constexpr float kMaxInstallments = 12.0f;
constexpr float kAmountVsAvgRatio = 10.0f;
constexpr float kMaxMinutes = 1440.0f;
constexpr float kMaxKm = 1000.0f;
constexpr float kMaxTxCount24h = 20.0f;
constexpr float kMaxMerchantAvgAmount = 10000.0f;
constexpr float kRatioFraudThreshold = 0.06951915f;

constexpr std::array<int, 32> kMarch2026Weekday = {
    0,
    6, 0, 1, 2, 3, 4, 5,
    6, 0, 1, 2, 3, 4, 5,
    6, 0, 1, 2, 3, 4, 5,
    6, 0, 1, 2, 3, 4, 5,
    6, 0, 1,
};

enum KeyID {
  keyUnknown = 0,
  keyKnownMerchants,
  keyCustomer,
  keyMerchant,
  keyAmount,
  keyInstallments,
  keyAvgAmount,
  keyTxCount24h,
  keyKmFromHome,
  keyIsOnline,
  keyCardPresent,
  keyMCC,
  keyID,
  keyRequestedAt,
  keyTimestamp,
  keyKmFromCurrent,
  keyLastTransaction,
};

struct Slice {
  const uint8_t* ptr = nullptr;
  size_t len = 0;
};

struct Parsed {
  float amount = 0.0f;
  float installments = 0.0f;
  float avg_amount = 0.0f;
  float merchant_avg = 0.0f;
  float tx_count = 0.0f;
  float km_home = 0.0f;
  float km_last = 0.0f;
  bool is_online = false;
  bool card_present = false;
  bool has_last_tx = false;
  Slice merchant_id{};
  Slice mcc{};
  Slice curr_ts{};
  Slice last_ts{};
  std::array<Slice, 32> known{};
  int known_count = 0;
};

inline float clampf(float x) {
  if (x < 0.0f) return 0.0f;
  if (x > 1.0f) return 1.0f;
  return x;
}

inline bool eq_lit(const uint8_t* data, size_t len, const char* lit, size_t lit_len) {
  return len == lit_len && std::memcmp(data, lit, lit_len) == 0;
}

template <size_t N>
inline bool eq_lit(const uint8_t* data, size_t len, const char (&lit)[N]) {
  return eq_lit(data, len, lit, N - 1);
}

uint64_t pack_upto8(Slice s) {
  uint64_t v = 0;
  const size_t n = s.len < 8 ? s.len : 8;
  std::memcpy(&v, s.ptr, n);
  return v;
}

void add_known_merchant(Parsed& p, Slice s) {
  if (p.known_count >= static_cast<int>(p.known.size())) return;
  p.known[static_cast<size_t>(p.known_count++)] = s;
}

KeyID json_key_id(const uint8_t* key, size_t len) {
  switch (len) {
    case 2:
      if (key[0] == 'i' && key[1] == 'd') return keyID;
      break;
    case 3:
      if (key[0] == 'm' && key[1] == 'c' && key[2] == 'c') return keyMCC;
      break;
    case 6:
      if (eq_lit(key, len, "amount")) return keyAmount;
      break;
    case 8:
      if (key[0] == 'c' && eq_lit(key, len, "customer")) return keyCustomer;
      if (key[0] == 'm' && eq_lit(key, len, "merchant")) return keyMerchant;
      break;
    case 9:
      if (key[0] == 'i' && eq_lit(key, len, "is_online")) return keyIsOnline;
      if (key[0] == 't' && eq_lit(key, len, "timestamp")) return keyTimestamp;
      break;
    case 10:
      if (eq_lit(key, len, "avg_amount")) return keyAvgAmount;
      break;
    case 12:
      switch (key[0]) {
        case 'i':
          if (eq_lit(key, len, "installments")) return keyInstallments;
          break;
        case 't':
          if (eq_lit(key, len, "tx_count_24h")) return keyTxCount24h;
          break;
        case 'k':
          if (eq_lit(key, len, "km_from_home")) return keyKmFromHome;
          break;
        case 'c':
          if (eq_lit(key, len, "card_present")) return keyCardPresent;
          break;
        case 'r':
          if (eq_lit(key, len, "requested_at")) return keyRequestedAt;
          break;
      }
      break;
    case 15:
      if (key[0] == 'k' && eq_lit(key, len, "known_merchants")) return keyKnownMerchants;
      if (key[0] == 'k' && eq_lit(key, len, "km_from_current")) return keyKmFromCurrent;
      break;
    case 16:
      if (eq_lit(key, len, "last_transaction")) return keyLastTransaction;
      break;
  }
  return keyUnknown;
}

template <size_t N>
bool find_after(const uint8_t* body, size_t len, size_t& i, const char (&lit)[N]) {
  constexpr size_t lit_len = N - 1;
  if (lit_len == 0 || i + lit_len > len) return false;
  const uint8_t first = static_cast<uint8_t>(lit[0]);
  for (; i + lit_len <= len; ++i) {
    if (body[i] == first && std::memcmp(body + i, lit, lit_len) == 0) {
      i += lit_len;
      return true;
    }
  }
  return false;
}

template <size_t N>
bool consume_lit(const uint8_t* body, size_t len, size_t& i, const char (&lit)[N]) {
  constexpr size_t lit_len = N - 1;
  if (i + lit_len > len) return false;
  if (std::memcmp(body + i, lit, lit_len) != 0) return false;
  i += lit_len;
  return true;
}

float parse_float_fast(const uint8_t* body, size_t len, size_t& i) {
  float sign = 1.0f;
  if (i < len && body[i] == '-') {
    sign = -1.0f;
    ++i;
  }

  float int_part = 0.0f;
  while (i < len && body[i] >= '0' && body[i] <= '9') {
    int_part = int_part * 10.0f + static_cast<float>(body[i] - '0');
    ++i;
  }

  float frac = 0.0f;
  float base = 0.1f;
  if (i < len && body[i] == '.') {
    ++i;
    while (i < len && body[i] >= '0' && body[i] <= '9') {
      frac += static_cast<float>(body[i] - '0') * base;
      base *= 0.1f;
      ++i;
    }
  }
  return sign * (int_part + frac);
}

bool parse_string_at_quote(const uint8_t* body, size_t len, size_t& i, Slice& out) {
  while (i < len && body[i] != '"') ++i;
  if (i >= len) return false;
  const size_t start = ++i;
  while (i < len && body[i] != '"') ++i;
  if (i >= len) return false;
  out = {body + start, i - start};
  ++i;
  return true;
}

bool parse_string_after_open_quote(const uint8_t* body, size_t len, size_t& i, Slice& out) {
  const size_t start = i;
  while (i < len && body[i] != '"') ++i;
  if (i >= len) return false;
  out = {body + start, i - start};
  ++i;
  return true;
}

bool parse_known_array(const uint8_t* body, size_t len, size_t& i, Parsed& p) {
  p.known_count = 0;
  while (i < len) {
    while (i < len && (body[i] == ' ' || body[i] == '\n' || body[i] == ',')) ++i;
    if (i >= len) return false;
    if (body[i] == ']') {
      ++i;
      return true;
    }
    if (body[i] != '"') {
      ++i;
      continue;
    }
    Slice s;
    if (!parse_string_at_quote(body, len, i, s)) return false;
    add_known_merchant(p, s);
  }
  return false;
}

bool parse_known_array_direct(const uint8_t* body, size_t len, size_t& i, Parsed& p) {
  p.known_count = 0;
  if (i >= len) return false;
  if (body[i] == ']') {
    ++i;
    return true;
  }

  for (;;) {
    if (i >= len || body[i] != '"') return false;
    const size_t start = ++i;
    while (i < len && body[i] != '"') ++i;
    if (i >= len) return false;
    add_known_merchant(p, {body + start, i - start});
    ++i;
    if (i >= len) return false;
    if (body[i] == ',') {
      ++i;
      continue;
    }
    if (body[i] == ']') {
      ++i;
      return true;
    }
    return false;
  }
}

int parse_minute_of_day(Slice ts) {
  if (ts.len < 16) return -1;
  const auto* t = ts.ptr;
  const int hour = static_cast<int>(t[11] - '0') * 10 + static_cast<int>(t[12] - '0');
  return hour * 60 + static_cast<int>(t[14] - '0') * 10 + static_cast<int>(t[15] - '0');
}

void parse_date(Slice ts, int& y, int& m, int& d) {
  const auto* t = ts.ptr;
  y = static_cast<int>(t[0] - '0') * 1000 +
      static_cast<int>(t[1] - '0') * 100 +
      static_cast<int>(t[2] - '0') * 10 +
      static_cast<int>(t[3] - '0');
  m = static_cast<int>(t[5] - '0') * 10 + static_cast<int>(t[6] - '0');
  d = static_cast<int>(t[8] - '0') * 10 + static_cast<int>(t[9] - '0');
}

int day_of_week(int y, int m, int d) {
  if (m < 3) {
    m += 12;
    --y;
  }
  const int k = y % 100;
  const int j = y / 100;
  const int h = (d + (13 * (m + 1)) / 5 + k + k / 4 + j / 4 + 5 * j) % 7;
  return (h + 5) % 7;
}

int day_of_week_fast(Slice ts) {
  if (ts.len >= 10 &&
      ts.ptr[0] == '2' && ts.ptr[1] == '0' && ts.ptr[2] == '2' && ts.ptr[3] == '6' &&
      ts.ptr[5] == '0' && ts.ptr[6] == '3') {
    const int d = static_cast<int>(ts.ptr[8] - '0') * 10 + static_cast<int>(ts.ptr[9] - '0');
    if (d >= 1 && d <= 31) return kMarch2026Weekday[static_cast<size_t>(d)];
  }
  int y = 0, m = 0, d = 0;
  parse_date(ts, y, m, d);
  return day_of_week(y, m, d);
}

int days_from_civil(int y, int m, int d) {
  if (m <= 2) --y;
  const int era = y / 400;
  const int yoe = y - era * 400;
  const int mp = m > 2 ? m - 3 : m + 9;
  const int doy = (153 * mp + 2) / 5 + d - 1;
  const int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe;
}

int64_t days_from_civil_epoch(int64_t y, int64_t m, int64_t d) {
  int64_t year = y;
  int64_t month = m;
  if (month <= 2) --year;
  const int64_t era = (year >= 0 ? year : year - 399) / 400;
  const int64_t yoe = year - era * 400;
  const int64_t month_adj = month > 2 ? month - 3 : month + 9;
  const int64_t doy = (153 * month_adj + 2) / 5 + d - 1;
  const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

bool parse_utc_epoch(Slice ts, int& hour_out, int& weekday_out, int64_t& epoch_seconds_out) {
  if (ts.len < 19 || ts.ptr == nullptr) return false;
  const auto* t = ts.ptr;
  if (t[4] != '-' || t[7] != '-' || t[10] != 'T' || t[13] != ':' || t[16] != ':') {
    return false;
  }
  const int year = static_cast<int>(t[0] - '0') * 1000 +
                   static_cast<int>(t[1] - '0') * 100 +
                   static_cast<int>(t[2] - '0') * 10 +
                   static_cast<int>(t[3] - '0');
  const int month = static_cast<int>(t[5] - '0') * 10 + static_cast<int>(t[6] - '0');
  const int day = static_cast<int>(t[8] - '0') * 10 + static_cast<int>(t[9] - '0');
  const int hour = static_cast<int>(t[11] - '0') * 10 + static_cast<int>(t[12] - '0');
  const int minute = static_cast<int>(t[14] - '0') * 10 + static_cast<int>(t[15] - '0');
  const int second = static_cast<int>(t[17] - '0') * 10 + static_cast<int>(t[18] - '0');
  if (month < 1 || month > 12 || day < 1 || day > 31 ||
      hour > 23 || minute > 59 || second > 59) {
    return false;
  }
  const int64_t days = days_from_civil_epoch(year, month, day);
  int64_t weekday = (days + 3) % 7;
  if (weekday < 0) weekday += 7;
  hour_out = hour;
  weekday_out = static_cast<int>(weekday);
  epoch_seconds_out = days * 86400 + static_cast<int64_t>(hour) * 3600 +
                      static_cast<int64_t>(minute) * 60 + second;
  return true;
}

int parse_absolute_minute(Slice ts) {
  int y = 0, m = 0, d = 0;
  parse_date(ts, y, m, d);
  return days_from_civil(y, m, d) * 1440 + parse_minute_of_day(ts);
}

float lookup_mcc(Slice mcc) {
  if (mcc.len < 4) return 0.5f;
  const auto* k = mcc.ptr;
  if (k[0] < '0' || k[0] > '9' || k[1] < '0' || k[1] > '9' ||
      k[2] < '0' || k[2] > '9' || k[3] < '0' || k[3] > '9') {
    return 0.5f;
  }
  const int idx = static_cast<int>(k[0] - '0') * 1000 +
                  static_cast<int>(k[1] - '0') * 100 +
                  static_cast<int>(k[2] - '0') * 10 +
                  static_cast<int>(k[3] - '0');
  switch (idx) {
    case 5411: return 0.15f;
    case 5812: return 0.30f;
    case 5912: return 0.20f;
    case 5944: return 0.45f;
    case 7801: return 0.80f;
    case 7802: return 0.75f;
    case 7995: return 0.85f;
    case 4511: return 0.35f;
    case 5311: return 0.25f;
    default: return 0.5f;
  }
}

float is_unknown_merchant(Slice merchant, const Parsed& p) {
  if (merchant.ptr == nullptr || merchant.len == 0) return 1.0f;
  if (merchant.len == 8) {
    const uint64_t m = pack_upto8(merchant);
    for (int i = 0; i < p.known_count; ++i) {
      const Slice k = p.known[static_cast<size_t>(i)];
      if (k.len == 8 && pack_upto8(k) == m) return 0.0f;
    }
    return 1.0f;
  }

  for (int i = 0; i < p.known_count; ++i) {
    const Slice k = p.known[static_cast<size_t>(i)];
    if (k.len == merchant.len && std::memcmp(k.ptr, merchant.ptr, merchant.len) == 0) {
      return 0.0f;
    }
  }
  return 1.0f;
}

bool merchant_known(const Parsed& p) {
  return is_unknown_merchant(p.merchant_id, p) == 0.0f;
}

bool is_mcc(Slice mcc, const char* value) {
  return mcc.len == 4 && std::memcmp(mcc.ptr, value, 4) == 0;
}

bool is_safe_mcc(Slice mcc) {
  return is_mcc(mcc, "5411") || is_mcc(mcc, "5812") ||
         is_mcc(mcc, "5912") || is_mcc(mcc, "5311");
}

bool is_risky_mcc(Slice mcc) {
  return is_mcc(mcc, "7995") || is_mcc(mcc, "7801") || is_mcc(mcc, "7802");
}

bool is_extreme_fraud_pattern(const Parsed& p, bool known, float amount_ratio) {
  return !known &&
         p.amount >= 3000.0f &&
         amount_ratio >= 5.0f &&
         p.installments >= 7.0f &&
         p.tx_count >= 10.0f &&
         p.km_home >= 400.0f;
}

int tree_score_from_parsed(const Parsed& p, bool known, float amount_ratio) {
  int hour = 0;
  int weekday = 0;
  int64_t requested_epoch = 0;
  if (!parse_utc_epoch(p.curr_ts, hour, weekday, requested_epoch)) {
    return -1;
  }

  float minutes_since_last = -1.0f;
  float km_from_last = -1.0f;
  float last_null = 1.0f;
  if (p.has_last_tx && p.last_ts.len != 0) {
    int last_hour = 0;
    int last_weekday = 0;
    int64_t last_epoch = 0;
    if (!parse_utc_epoch(p.last_ts, last_hour, last_weekday, last_epoch)) {
      return -1;
    }
    int64_t delta = requested_epoch - last_epoch;
    if (delta < 0) delta = 0;
    minutes_since_last = clampf(static_cast<float>(delta) / 60.0f / kMaxMinutes);
    km_from_last = clampf(p.km_last / kMaxKm);
    last_null = 0.0f;
  }

  float features[TREE_FEATURE_COUNT];
  features[0] = clampf(p.amount / kMaxAmount);
  features[1] = clampf(p.installments / kMaxInstallments);
  features[2] = clampf(amount_ratio / kAmountVsAvgRatio);
  features[3] = static_cast<float>(hour) / 23.0f;
  features[4] = static_cast<float>(weekday) / 6.0f;
  features[5] = minutes_since_last;
  features[6] = km_from_last;
  features[7] = clampf(p.km_home / kMaxKm);
  features[8] = clampf(p.tx_count / kMaxTxCount24h);
  features[9] = p.is_online ? 1.0f : 0.0f;
  features[10] = p.card_present ? 1.0f : 0.0f;
  features[11] = known ? 0.0f : 1.0f;
  features[12] = lookup_mcc(p.mcc);
  features[13] = clampf(p.merchant_avg / kMaxMerchantAvgAmount);
  features[14] = last_null;
  features[15] = p.amount;
  features[16] = p.avg_amount;
  features[17] = amount_ratio;
  features[18] = p.tx_count;
  features[19] = p.km_home;
  features[20] = p.merchant_avg;

  return tree_predict_score(features);
}

int try_fast_path(const Parsed& p, int mode) {
  const bool known = merchant_known(p);
  const float safe_avg = p.avg_amount > 0.0f ? p.avg_amount : 1.0f;
  const float amount_ratio = p.amount / safe_avg;

  if ((mode & kNativeFastPathLegit) != 0 &&
      p.amount <= 500.0f &&
      p.amount <= safe_avg * 0.50001f &&
      p.installments <= 3.0f &&
      p.tx_count <= 5.0f &&
      known &&
      p.km_home <= 50.0f &&
      is_safe_mcc(p.mcc)) {
    return 0;
  }

  if ((mode & kNativeFastPathExtremeFraud) != 0 &&
      is_extreme_fraud_pattern(p, known, amount_ratio)) {
    return 5;
  }

  if ((mode & kNativeFastPathFraud) != 0 &&
      p.amount >= 5000.0f &&
      p.installments >= 5.0f &&
      p.tx_count >= 6.0f &&
      !known &&
      p.km_home >= 150.0f &&
      is_risky_mcc(p.mcc)) {
    return 5;
  }

  if ((mode & kNativeFastPathFraudOffline) != 0 &&
      !p.is_online &&
      p.amount >= 5000.0f &&
      p.installments >= 5.0f &&
      p.tx_count >= 6.0f &&
      !known &&
      p.km_home >= 150.0f &&
      is_risky_mcc(p.mcc)) {
    return 5;
  }

  if ((mode & kNativeFastPathTree) != 0) {
    const int tree_score = tree_score_from_parsed(p, known, amount_ratio);
    if (tree_score >= 0) {
      if ((mode & kNativeFastPathTreeFraudUnknown) != 0 && tree_score >= 3 && known) {
        return -1;
      }
      return tree_score;
    }
  }

  return -1;
}

int tier_fraud_score(const Parsed& p) {
  const bool known = merchant_known(p);
  const float safe_avg = p.avg_amount > 0.0f ? p.avg_amount : 1.0f;
  const float amount_ratio = p.amount / safe_avg;

  if (p.amount <= 500.0f &&
      p.amount <= safe_avg * 0.50001f &&
      p.installments <= 3.0f &&
      p.tx_count <= 5.0f &&
      known &&
      p.km_home <= 50.0f &&
      is_safe_mcc(p.mcc)) {
    return 0;
  }

  if (p.amount >= 5000.0f &&
      p.installments >= 5.0f &&
      p.tx_count >= 6.0f &&
      !known &&
      p.km_home >= 150.0f &&
      is_risky_mcc(p.mcc)) {
    return 5;
  }

  const int tree_score = tree_score_from_parsed(p, known, amount_ratio);
  if (tree_score >= 0) return tree_score;
  return clampf(amount_ratio / kAmountVsAvgRatio) > kRatioFraudThreshold ? 5 : 0;
}

bool parse_ordered_k6_body(const uint8_t* body, size_t len, Parsed& p) {
  size_t i = 0;
  if (!find_after(body, len, i, "\"transaction\":{\"amount\":")) return false;
  p.amount = parse_float_fast(body, len, i);
  if (!find_after(body, len, i, "\"installments\":")) return false;
  p.installments = parse_float_fast(body, len, i);
  if (!find_after(body, len, i, "\"requested_at\":\"")) return false;
  if (!parse_string_after_open_quote(body, len, i, p.curr_ts)) return false;

  if (!find_after(body, len, i, "\"customer\":{\"avg_amount\":")) return false;
  p.avg_amount = parse_float_fast(body, len, i);
  if (!find_after(body, len, i, "\"tx_count_24h\":")) return false;
  p.tx_count = parse_float_fast(body, len, i);
  if (!find_after(body, len, i, "\"known_merchants\":[")) return false;
  if (!parse_known_array(body, len, i, p)) return false;

  if (!find_after(body, len, i, "\"merchant\":{\"id\":\"")) return false;
  if (!parse_string_after_open_quote(body, len, i, p.merchant_id)) return false;
  if (!find_after(body, len, i, "\"mcc\":\"")) return false;
  if (!parse_string_after_open_quote(body, len, i, p.mcc)) return false;
  if (!find_after(body, len, i, "\"avg_amount\":")) return false;
  p.merchant_avg = parse_float_fast(body, len, i);

  if (!find_after(body, len, i, "\"terminal\":{\"is_online\":")) return false;
  if (i >= len) return false;
  p.is_online = body[i] == 't';
  i += p.is_online ? 4 : 5;
  if (!find_after(body, len, i, "\"card_present\":")) return false;
  if (i >= len) return false;
  p.card_present = body[i] == 't';
  i += p.card_present ? 4 : 5;
  if (!find_after(body, len, i, "\"km_from_home\":")) return false;
  p.km_home = parse_float_fast(body, len, i);

  if (!find_after(body, len, i, "\"last_transaction\":")) return false;
  if (i >= len) return false;
  if (body[i] == 'n') {
    p.has_last_tx = false;
    return true;
  }
  if (body[i] != '{') return false;
  p.has_last_tx = true;
  if (!find_after(body, len, i, "\"timestamp\":\"")) return false;
  if (!parse_string_after_open_quote(body, len, i, p.last_ts)) return false;
  if (!find_after(body, len, i, "\"km_from_current\":")) return false;
  p.km_last = parse_float_fast(body, len, i);
  return true;
}

bool parse_string_value(const uint8_t* body, size_t len, size_t& i, Slice& out) {
  const size_t start = i;
  while (i < len && body[i] != '"') ++i;
  if (i >= len) return false;
  out = {body + start, i - start};
  ++i;
  return true;
}

bool skip_string_value(const uint8_t* body, size_t len, size_t& i) {
  while (i < len && body[i] != '"') ++i;
  if (i >= len) return false;
  ++i;
  return true;
}

bool parse_ordered_k6_body_direct(const uint8_t* body, size_t len, Parsed& p) {
  size_t i = 0;
  if (!consume_lit(body, len, i, "{\"id\":\"")) return false;
  if (!skip_string_value(body, len, i)) return false;
  if (!consume_lit(body, len, i, ",\"transaction\":{\"amount\":")) return false;
  p.amount = parse_float_fast(body, len, i);
  if (!consume_lit(body, len, i, ",\"installments\":")) return false;
  p.installments = parse_float_fast(body, len, i);
  if (!consume_lit(body, len, i, ",\"requested_at\":\"")) return false;
  if (!parse_string_value(body, len, i, p.curr_ts)) return false;

  if (!consume_lit(body, len, i, "},\"customer\":{\"avg_amount\":")) return false;
  p.avg_amount = parse_float_fast(body, len, i);
  if (!consume_lit(body, len, i, ",\"tx_count_24h\":")) return false;
  p.tx_count = parse_float_fast(body, len, i);
  if (!consume_lit(body, len, i, ",\"known_merchants\":[")) return false;
  if (!parse_known_array_direct(body, len, i, p)) return false;

  if (!consume_lit(body, len, i, "},\"merchant\":{\"id\":\"")) return false;
  if (!parse_string_value(body, len, i, p.merchant_id)) return false;
  if (!consume_lit(body, len, i, ",\"mcc\":\"")) return false;
  if (!parse_string_value(body, len, i, p.mcc)) return false;
  if (!consume_lit(body, len, i, ",\"avg_amount\":")) return false;
  p.merchant_avg = parse_float_fast(body, len, i);

  if (!consume_lit(body, len, i, "},\"terminal\":{\"is_online\":")) return false;
  if (i >= len) return false;
  p.is_online = body[i] == 't';
  i += p.is_online ? 4 : 5;
  if (!consume_lit(body, len, i, ",\"card_present\":")) return false;
  if (i >= len) return false;
  p.card_present = body[i] == 't';
  i += p.card_present ? 4 : 5;
  if (!consume_lit(body, len, i, ",\"km_from_home\":")) return false;
  p.km_home = parse_float_fast(body, len, i);

  if (!consume_lit(body, len, i, "},\"last_transaction\":")) return false;
  if (i >= len) return false;
  if (body[i] == 'n') {
    p.has_last_tx = false;
    return consume_lit(body, len, i, "null}");
  }
  if (body[i] != '{') return false;
  ++i;
  p.has_last_tx = true;
  if (!consume_lit(body, len, i, "\"timestamp\":\"")) return false;
  if (!parse_string_value(body, len, i, p.last_ts)) return false;
  if (!consume_lit(body, len, i, ",\"km_from_current\":")) return false;
  p.km_last = parse_float_fast(body, len, i);
  return true;
}

bool parse_generic_body(const uint8_t* body, size_t len, Parsed& p) {
  enum { secNone = 0, secCustomer, secMerchant };
  enum FieldSeen : uint32_t {
    seenAmount = 1u << 0,
    seenInstallments = 1u << 1,
    seenCustomerAvg = 1u << 2,
    seenMerchantAvg = 1u << 3,
    seenTxCount = 1u << 4,
    seenKmHome = 1u << 5,
    seenIsOnline = 1u << 6,
    seenCardPresent = 1u << 7,
    seenMcc = 1u << 8,
    seenMerchantId = 1u << 9,
    seenRequestedAt = 1u << 10,
    seenKnownMerchants = 1u << 11,
    seenLastTransaction = 1u << 12,
    seenLastTimestamp = 1u << 13,
    seenKmCurrent = 1u << 14,
  };
  int section = secNone;
  uint32_t seen = 0;
  size_t i = 0;

  while (i < len) {
    if (body[i] != '"') {
      ++i;
      continue;
    }
    const size_t key_start = ++i;
    while (i < len && body[i] != '"') ++i;
    if (i >= len) break;
    const uint8_t* key = body + key_start;
    const size_t key_len = i - key_start;
    ++i;
    while (i < len && body[i] != ':') ++i;
    if (i >= len) break;
    ++i;
    while (i < len && body[i] == ' ') ++i;

    switch (json_key_id(key, key_len)) {
      case keyKnownMerchants:
        while (i < len && body[i] != '[') ++i;
        if (i >= len) return false;
        ++i;
        if (!parse_known_array(body, len, i, p)) return false;
        seen |= seenKnownMerchants;
        break;
      case keyCustomer:
        section = secCustomer;
        break;
      case keyMerchant:
        section = secMerchant;
        break;
      case keyAmount:
        p.amount = parse_float_fast(body, len, i);
        seen |= seenAmount;
        break;
      case keyInstallments:
        p.installments = parse_float_fast(body, len, i);
        seen |= seenInstallments;
        break;
      case keyAvgAmount: {
        const float val = parse_float_fast(body, len, i);
        if (section == secCustomer) {
          p.avg_amount = val;
          seen |= seenCustomerAvg;
        } else if (section == secMerchant) {
          p.merchant_avg = val;
          seen |= seenMerchantAvg;
        }
        break;
      }
      case keyTxCount24h:
        p.tx_count = parse_float_fast(body, len, i);
        seen |= seenTxCount;
        break;
      case keyKmFromHome:
        p.km_home = parse_float_fast(body, len, i);
        seen |= seenKmHome;
        break;
      case keyIsOnline:
        p.is_online = i < len && body[i] == 't';
        i += p.is_online ? 4 : 5;
        seen |= seenIsOnline;
        break;
      case keyCardPresent:
        p.card_present = i < len && body[i] == 't';
        i += p.card_present ? 4 : 5;
        seen |= seenCardPresent;
        break;
      case keyMCC:
        if (!parse_string_at_quote(body, len, i, p.mcc)) return false;
        seen |= seenMcc;
        break;
      case keyID:
        if (section == secMerchant && !parse_string_at_quote(body, len, i, p.merchant_id)) {
          return false;
        }
        if (section == secMerchant) seen |= seenMerchantId;
        break;
      case keyRequestedAt:
        if (!parse_string_at_quote(body, len, i, p.curr_ts)) return false;
        seen |= seenRequestedAt;
        break;
      case keyTimestamp:
        if (!parse_string_at_quote(body, len, i, p.last_ts)) return false;
        seen |= seenLastTimestamp;
        break;
      case keyKmFromCurrent:
        p.km_last = parse_float_fast(body, len, i);
        seen |= seenKmCurrent;
        break;
      case keyLastTransaction:
        seen |= seenLastTransaction;
        if (i < len && body[i] == 'n') {
          p.has_last_tx = false;
          i += 4;
        } else {
          p.has_last_tx = true;
        }
        break;
      default:
        break;
    }
    ++i;
  }
  constexpr uint32_t required =
      seenAmount | seenInstallments | seenCustomerAvg | seenMerchantAvg | seenTxCount |
      seenKmHome | seenIsOnline | seenCardPresent | seenMcc | seenMerchantId |
      seenRequestedAt | seenKnownMerchants | seenLastTransaction;
  if ((seen & required) != required) return false;
  if (p.has_last_tx && (seen & (seenLastTimestamp | seenKmCurrent)) !=
                           (seenLastTimestamp | seenKmCurrent)) {
    return false;
  }
  return true;
}

void fill_vector(const Parsed& p, float* out) {
  for (int k = 0; k < kDim; ++k) out[k] = 0.0f;

  out[0] = clampf(p.amount / kMaxAmount);
  out[1] = clampf(p.installments / kMaxInstallments);
  out[2] = p.avg_amount > 0.0f ? clampf((p.amount / p.avg_amount) / kAmountVsAvgRatio) : 0.0f;

  int curr_minute = -1;
  int curr_abs_minute = -1;
  if (p.curr_ts.len >= 16) {
    curr_minute = parse_minute_of_day(p.curr_ts);
    curr_abs_minute = parse_absolute_minute(p.curr_ts);
    out[3] = static_cast<float>(curr_minute / 60) / 23.0f;
    out[4] = static_cast<float>(day_of_week_fast(p.curr_ts)) / 6.0f;
  }

  if (!p.has_last_tx || p.last_ts.len == 0 || curr_abs_minute < 0) {
    out[5] = -1.0f;
    out[6] = -1.0f;
  } else {
    int diff = curr_abs_minute - parse_absolute_minute(p.last_ts);
    if (diff < 0) diff = 0;
    out[5] = clampf(static_cast<float>(diff) / kMaxMinutes);
    out[6] = clampf(p.km_last / kMaxKm);
  }

  out[7] = clampf(p.km_home / kMaxKm);
  out[8] = clampf(p.tx_count / kMaxTxCount24h);
  if (p.is_online) out[9] = 1.0f;
  if (p.card_present) out[10] = 1.0f;
  out[11] = is_unknown_merchant(p.merchant_id, p);
  out[12] = lookup_mcc(p.mcc);
  out[13] = clampf(p.merchant_avg / kMaxMerchantAvgAmount);
}

bool parse_body(const uint8_t* body, size_t len, Parsed& parsed) {
  if (parse_ordered_k6_body_direct(body, len, parsed)) return true;
  parsed = Parsed{};
  if (parse_ordered_k6_body(body, len, parsed)) return true;
  parsed = Parsed{};
  return parse_generic_body(body, len, parsed);
}

}  // namespace

bool build_fraud_vector_cpp(const uint8_t* body, size_t len, float* out) {
  if (body == nullptr || out == nullptr || len == 0) return false;
  Parsed parsed;
  if (!parse_body(body, len, parsed)) return false;
  fill_vector(parsed, out);
  return true;
}

bool build_fraud_vector_cpp_with_fast_path(const uint8_t* body, size_t len, float* out,
                                           int fast_path_mode, int* fast_score_out) {
  return build_fraud_vector_cpp_with_tier_path(body, len, out, fast_path_mode, false,
                                               fast_score_out);
}

bool build_fraud_vector_cpp_with_fast_route(const uint8_t* body, size_t len, float* out,
                                            int direct_fast_path_mode,
                                            int route_fast_path_mode,
                                            int* direct_fast_score_out,
                                            int* route_fast_score_out) {
  if (direct_fast_score_out != nullptr) *direct_fast_score_out = -1;
  if (route_fast_score_out != nullptr) *route_fast_score_out = -1;
  if (body == nullptr || out == nullptr || len == 0) return false;

  Parsed parsed;
  if (!parse_body(body, len, parsed)) return false;

  if (direct_fast_path_mode != 0 && direct_fast_score_out != nullptr) {
    const int direct = try_fast_path(parsed, direct_fast_path_mode);
    *direct_fast_score_out = direct;
    if (direct >= 0) return true;
  }

  if (route_fast_path_mode != 0 && route_fast_score_out != nullptr) {
    *route_fast_score_out = try_fast_path(parsed, route_fast_path_mode);
  }

  fill_vector(parsed, out);
  return true;
}

bool build_fraud_vector_cpp_with_tier_path(const uint8_t* body, size_t len, float* out,
                                           int fast_path_mode, bool tier_path,
                                           int* fast_score_out) {
  if (fast_score_out != nullptr) *fast_score_out = -1;
  if (body == nullptr || out == nullptr || len == 0) return false;

  Parsed parsed;
  if (!parse_body(body, len, parsed)) return false;

  if (tier_path && fast_score_out != nullptr) {
    *fast_score_out = tier_fraud_score(parsed);
    return true;
  }

  if (fast_path_mode != 0 && fast_score_out != nullptr) {
    const int fast = try_fast_path(parsed, fast_path_mode);
    *fast_score_out = fast;
    if (fast >= 0) return true;
  }

  fill_vector(parsed, out);
  return true;
}
