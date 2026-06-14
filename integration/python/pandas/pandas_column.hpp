#pragma once

namespace otterbrix {

enum class pandas_column_backend_t { NUMPY };

class pandas_column_t {
public:
	pandas_column_t(pandas_column_backend_t backend) : backend_(backend) {
	}
	virtual ~pandas_column_t() {
	}

public:
	pandas_column_backend_t backend() const {
		return backend_;
	}

protected:
	pandas_column_backend_t backend_;
};

} // namespace otterbrix
