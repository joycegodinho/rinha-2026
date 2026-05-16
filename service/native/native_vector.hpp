#pragma once

#include <cstddef>
#include <cstdint>

bool build_fraud_vector_cpp(const uint8_t* body, size_t len, float* out);
