#include "native_vector.hpp"

#include <array>
#include <cstdint>
#include <cstring>

namespace {

constexpr int kDim = 16;
constexpr float kMaxAmount = 10000.0f;
constexpr float kMaxInstallments = 12.0f;
constexpr float kAmountVsAvgRatio = 10.0f;
constexpr float kMaxMinutes = 1440.0f;
constexpr float kMaxKm = 1000.0f;
constexpr float kMaxTxCount24h = 20.0f;
constexpr float kMaxMerchantAvgAmount = 10000.0f;

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
    if (p.known_count < static_cast<int>(p.known.size())) {
      p.known[static_cast<size_t>(p.known_count++)] = s;
    }
  }
  return false;
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

uint64_t pack_upto8(Slice s) {
  uint64_t v = 0;
  const size_t n = s.len < 8 ? s.len : 8;
  std::memcpy(&v, s.ptr, n);
  return v;
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

bool parse_generic_body(const uint8_t* body, size_t len, Parsed& p) {
  enum { secNone = 0, secCustomer, secMerchant };
  int section = secNone;
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
        break;
      case keyCustomer:
        section = secCustomer;
        break;
      case keyMerchant:
        section = secMerchant;
        break;
      case keyAmount:
        p.amount = parse_float_fast(body, len, i);
        break;
      case keyInstallments:
        p.installments = parse_float_fast(body, len, i);
        break;
      case keyAvgAmount: {
        const float val = parse_float_fast(body, len, i);
        if (section == secCustomer) p.avg_amount = val;
        else if (section == secMerchant) p.merchant_avg = val;
        break;
      }
      case keyTxCount24h:
        p.tx_count = parse_float_fast(body, len, i);
        break;
      case keyKmFromHome:
        p.km_home = parse_float_fast(body, len, i);
        break;
      case keyIsOnline:
        p.is_online = i < len && body[i] == 't';
        i += p.is_online ? 4 : 5;
        break;
      case keyCardPresent:
        p.card_present = i < len && body[i] == 't';
        i += p.card_present ? 4 : 5;
        break;
      case keyMCC:
        if (!parse_string_at_quote(body, len, i, p.mcc)) return false;
        break;
      case keyID:
        if (section == secMerchant && !parse_string_at_quote(body, len, i, p.merchant_id)) {
          return false;
        }
        break;
      case keyRequestedAt:
        if (!parse_string_at_quote(body, len, i, p.curr_ts)) return false;
        break;
      case keyTimestamp:
        if (!parse_string_at_quote(body, len, i, p.last_ts)) return false;
        break;
      case keyKmFromCurrent:
        p.km_last = parse_float_fast(body, len, i);
        break;
      case keyLastTransaction:
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
  return true;
}

void fill_vector(const Parsed& p, float* out) {
  for (int k = 0; k < kDim; ++k) out[k] = 0.0f;

  out[0] = clampf(p.amount / kMaxAmount);
  out[1] = clampf(p.installments / kMaxInstallments);
  out[2] = p.avg_amount > 0.0f ? clampf((p.amount / p.avg_amount) / kAmountVsAvgRatio) : 0.0f;

  int curr_minute = -1;
  if (p.curr_ts.len >= 16) {
    curr_minute = parse_minute_of_day(p.curr_ts);
    out[3] = static_cast<float>(curr_minute / 60) / 23.0f;
    out[4] = static_cast<float>(day_of_week_fast(p.curr_ts)) / 6.0f;
  }

  if (!p.has_last_tx || p.last_ts.len == 0 || curr_minute < 0) {
    out[5] = -1.0f;
    out[6] = -1.0f;
  } else {
    int diff = curr_minute - parse_minute_of_day(p.last_ts);
    if (diff < 0) diff += 1440;
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

}  // namespace

bool build_fraud_vector_cpp(const uint8_t* body, size_t len, float* out) {
  if (body == nullptr || out == nullptr || len == 0) return false;
  Parsed parsed;
  if (!parse_ordered_k6_body(body, len, parsed)) {
    parsed = Parsed{};
    if (!parse_generic_body(body, len, parsed)) return false;
  }
  fill_vector(parsed, out);
  return true;
}
