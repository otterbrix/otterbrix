#pragma once

// Detect thread sanitizer: GCC defines __SANITIZE_THREAD__, Clang uses __has_feature
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define OTTERBRIX_TSAN_ENABLED
#endif
#elif defined(__SANITIZE_THREAD__)
#define OTTERBRIX_TSAN_ENABLED
#endif
