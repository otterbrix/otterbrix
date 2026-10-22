#include "../otterbrix.h"

size_t otterbrix_header_is_c(void);

size_t otterbrix_header_is_c(void) { return sizeof(config_t) + sizeof(sql_param_t) + sizeof(error_message); }
