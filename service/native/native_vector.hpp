#pragma once

#include <cstddef>
#include <cstdint>

constexpr int kNativeFastPathLegit = 1;
constexpr int kNativeFastPathFraud = 2;
constexpr int kNativeFastPathExtremeFraud = 4;
constexpr int kNativeFastPathTree = 8;
constexpr int kNativeFastPathTreeFraudUnknown = 16;
constexpr int kNativeFastPathFraudOffline = 32;

bool build_fraud_vector_cpp(const uint8_t* body, size_t len, float* out);
bool build_fraud_vector_cpp_with_fast_path(const uint8_t* body, size_t len, float* out,
                                           int fast_path_mode, int* fast_score_out);
bool build_fraud_vector_cpp_with_fast_route(const uint8_t* body, size_t len, float* out,
                                            int direct_fast_path_mode,
                                            int route_fast_path_mode,
                                            int* direct_fast_score_out,
                                            int* route_fast_score_out);
bool build_fraud_vector_cpp_with_tier_path(const uint8_t* body, size_t len, float* out,
                                           int fast_path_mode, bool tier_path,
                                           int* fast_score_out);
