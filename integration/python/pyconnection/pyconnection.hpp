#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>

#include <connection_environment/connection_environment.hpp>
#include <core/string_util/case_insensitive.hpp>
#include <core/types/memory.hpp>
#include <core/types/vector.hpp>

#include <mutex>

namespace otterbrix {
    class otterbrix_t;
    class PyConnection;
    class PyResult;
    class PyRelation;

    using pyconnection_ptr = shared_ptr<PyConnection>;
    using pycursor_ptr = shared_ptr<PyConnection>;

    struct DefaultConnectionHolder {
    public:
        DefaultConnectionHolder();
        ~DefaultConnectionHolder();
    
    public:
        DefaultConnectionHolder(const DefaultConnectionHolder &other) = delete;
        DefaultConnectionHolder(DefaultConnectionHolder &&other) = delete;
        DefaultConnectionHolder &operator=(const DefaultConnectionHolder &other) = delete;
        DefaultConnectionHolder &operator=(DefaultConnectionHolder &&other) = delete;
    
    public:
        pyconnection_ptr Get();
        void Set(pyconnection_ptr conn);
    
    private:
        pyconnection_ptr connection;
        std::mutex l;
    };
    
    class Cursors {
    public:
        Cursors(); 
        ~Cursors(); 
    public:
        void AddCursor(pycursor_ptr conn);
        void ClearCursors();
    private:
        std::mutex lock;
        vector<weak_ptr<PyConnection>> cursors;
    };

    // Main class
    class PyConnection 
        : public ConnectionEnvironment
        , public enable_shared_from_this<PyConnection>
    {
    private:
        Cursors cursors;
        std::mutex py_connection_lock;
        unique_ptr<PyResult> result;
    public:
        PyConnection(const boost::intrusive_ptr<otterbrix_t>& space);
        PyConnection(const PyConnection& other);
        static pyconnection_ptr Connect(const py::object &database_p, bool read_only, 
                const py::dict &config_options);
        ~PyConnection();
        static void Initialize(py::handle& m);
    private: 
        static DefaultConnectionHolder default_connection; 
    public:
	    static pyconnection_ptr DefaultConnection();
	    static void SetDefaultConnection(pyconnection_ptr conn);
    
    public:
        static void Cleanup();
    public:
        py::list ListTables();
        
        pyconnection_ptr Enter();
        void Exit(const py::object& exc_type, const py::object& exc, 
                const py::object& traceback);

        pyconnection_ptr Begin();
        pyconnection_ptr Commit();
        pyconnection_ptr Rollback();
        pyconnection_ptr Checkpoint();
        void Close();

        pycursor_ptr Cursor();
        pycursor_ptr Execute(const py::object& query, py::object params = py::list());

        unique_ptr<PyRelation> RunQuery(const py::object& query, string alias = "", py::object params = py::list());
    public:
        unique_ptr<PyRelation> FromDF(const py::object& value);
        unique_ptr<PyRelation> FromObject(const py::object& value);
    public:
        bool HasResult() const;
        PyResult& GetResult();   
        const PyResult& GetResult() const;
        void SetResult(Result res);

    };
} // namespace otterbrix
