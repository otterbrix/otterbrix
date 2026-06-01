#pragma once

namespace otterbrix {

enum class PandasColumnBackend { NUMPY };

class PandasColumn {
public:
	PandasColumn(PandasColumnBackend backend) : backend(backend) {
	}
	virtual ~PandasColumn() {
	}

public:
	PandasColumnBackend Backend() const {
		return backend;
	}

protected:
	PandasColumnBackend backend;
};

} // namespace otterbrix
