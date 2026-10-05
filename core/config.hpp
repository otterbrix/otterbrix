#pragma once

// Detect thread sanitizer: GCC defines __SANITIZE_THREAD__, Clang uses __has_feature
#ifndef OTTERBRIX_TSAN_ENABLED
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define OTTERBRIX_TSAN_ENABLED
#endif
#elif defined(__SANITIZE_THREAD__)
#define OTTERBRIX_TSAN_ENABLED
#endif
#endif

// Detect address sanitizer: GCC defines __SANITIZE_ADDRESS__, MSVC _ADDRESS_SANITIZER, clang answers
// only __has_feature(address_sanitizer).
#ifndef OTTERBRIX_ADDRESS_SANITIZER
#if defined(__SANITIZE_ADDRESS__) || defined(_ADDRESS_SANITIZER)
#define OTTERBRIX_ADDRESS_SANITIZER
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define OTTERBRIX_ADDRESS_SANITIZER
#endif
#endif
#endif
