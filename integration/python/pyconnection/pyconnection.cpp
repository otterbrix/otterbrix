#include "pyconnection.hpp"
#include <connection_environment/relation/relation_factory.hpp>
#include <otterbrix_wrapper/pyresult.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <core/string_util/string_util.hpp>
#include <scan/python_replacement_scan.hpp>

namespace otterbrix {
    // DefaultConnectionHolder
    DefaultConnectionHolder::DefaultConnectionHolder() = default;
    DefaultConnectionHolder::~DefaultConnectionHolder() = default;

    pyconnection_ptr DefaultConnectionHolder::Get() {
        std::lock_guard<std::mutex> guard(l);
        if (!connection) {

            auto default_path = std::filesystem::absolute(ConnectionEnvironment::DEFAULT_FOLDER);
            auto space = ConnectionEnvironment::MakeSpace(default_path);
            connection = make_shared<PyConnection>(space);
        }
        return connection;
    }

    void DefaultConnectionHolder::Set(pyconnection_ptr conn) {
        std::lock_guard<std::mutex> guard(l);
        connection = conn;
    }
    
    // Cursors
    Cursors::Cursors() = default;
    Cursors::~Cursors() = default;
    
    void Cursors::AddCursor(pycursor_ptr conn) {
        std::lock_guard<std::mutex> l(lock);

        // Clean up previously created cursors
        vector<weak_ptr<PyConnection>> compacted_cursors;
        bool needs_compaction = false;
        for (auto &cur_p : cursors) {
            auto cur = cur_p.lock();
            if (!cur) {
                needs_compaction = true;
                continue;
            }
            compacted_cursors.push_back(cur_p);
        }
        if (needs_compaction) {
            cursors = std::move(compacted_cursors);
        }

        cursors.push_back(conn);
    }
    
    void Cursors::ClearCursors() {
        std::lock_guard<std::mutex> l(lock);

        for (auto &cur : cursors) {
            auto cursor = cur.lock();
            if (!cursor) {
                // The cursor has already been closed
                continue;
            }
            // This is *only* needed because we have a py::gil_scoped_release in Close, so it *needs* the GIL in order to
            // release it don't ask me why it can't just realize there is no GIL and move on
            py::gil_scoped_acquire gil;
            cursor->Close();
        }

        cursors.clear();
    }


    DefaultConnectionHolder PyConnection::default_connection;

    pyconnection_ptr PyConnection::DefaultConnection() {
        return default_connection.Get();
    }

    void PyConnection::SetDefaultConnection(pyconnection_ptr conn) {
        return default_connection.Set(std::move(conn));
    }

    PyConnection::PyConnection(const boost::intrusive_ptr<otterbrix_t>& space) 
        : ConnectionEnvironment(space) {}

    PyConnection::PyConnection(const PyConnection& other)
        : ConnectionEnvironment(other), std::enable_shared_from_this<PyConnection>(other) {}

    pyconnection_ptr PyConnection::Connect(const py::object &database_p, bool /*read_only*/,
            const py::dict & /*config_options*/) {
        string db_str;
        if (py::isinstance<py::str>(database_p)) {
            db_str = py::str(database_p);
        } else {
            throw std::runtime_error("Please provide either a str or a pathlib.Path");
        }
        std::filesystem::path path = db_str;
        if (path.is_relative()) {
            path = std::filesystem::absolute(path);
        }

        pyconnection_ptr con = nullptr;
        if (db_str == ConnectionEnvironment::DEFAULT_FOLDER) {
            con = default_connection.Get();
        } else {
            auto space = ConnectionEnvironment::MakeSpace(path);
            con = make_shared<PyConnection>(space);
            
        }
        
        return con;
    }  

    PyConnection::~PyConnection() {
        py::gil_scoped_release gil;
    }

    void PyConnection::Cleanup() {
        default_connection.Set(nullptr);
        ConnectionEnvironment::Cleanup();
    }

    py::list PyConnection::ListTables() {
        py::gil_scoped_acquire gil;
        py::list res;
        const auto& tables = ConnectionEnvironment::GetCollections();
        for (const auto& entry : tables) {
            res.append(py::str(entry));
        }
        return res;
    }

    pyconnection_ptr PyConnection::Enter() {
        return shared_from_this();
    }

    void PyConnection::Exit(const py::object& exc_type, const py::object& exc,
            const py::object& /*traceback*/) {
        this->Close();
        if (exc_type.ptr() != Py_None) {
            // Propagate the exception if any occurred
            PyErr_SetObject(exc_type.ptr(), exc.ptr());
            throw py::error_already_set();
        }
    }

    pyconnection_ptr PyConnection::Begin() {
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Commit() {
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Rollback() {
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Checkpoint() {
        return shared_from_this();
    }

    void PyConnection::Close() {
        SetResult(nullptr);
        assert(py::gil_check());
        py::gil_scoped_release release;
        SetNullConnection();
        cursors.ClearCursors();
    }

    pycursor_ptr PyConnection::Cursor() {
        pycursor_ptr res = make_shared<PyConnection>(*this);
        cursors.AddCursor(res);
        return res;
    }

    pycursor_ptr PyConnection::Execute(const py::object& query, py::object /*params*/) {
        py::gil_scoped_acquire gil;
        result = nullptr;
        if (py::isinstance<py::str>(query)) {
            SetResult(ExecuteInternal(string(py::str(query))));
        } 
        return shared_from_this();

    }

    unique_ptr<PyRelation> PyConnection::RunQuery(const py::object& query, string alias, py::object /*params*/) {
        if (alias.empty()) {
            alias = "unnamed_relation";
        }
        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this), RelationFromQuery(string(py::str(query))));

    }


    unique_ptr<PyRelation> PyConnection::FromDF(const PandasDataFrame& value) {
        string name = "df_no_idea"; 
        auto tableref = Scan::TryReplacementObject(value, name);
        assert(tableref);

        shared_ptr<Relation> relation = RelationFactory::CreateDFRelation(std::move(tableref));
        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this), relation);

    }

    unique_ptr<PyRelation> PyConnection::FromObject(const py::object& value) {
        string name = "object_no_idea"; 
        auto tableref = Scan::TryReplacementObject(value, name);
        assert(tableref);

        shared_ptr<Relation> relation = RelationFactory::CreateDFRelation(std::move(tableref));
        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this), relation);
    }

    bool PyConnection::HasResult() const {
        return result != nullptr;
    }
    
    PyResult& PyConnection::GetResult() {
        if (!result) {
            ThrowConnectionException();
        }
        return *result;
    }
    
    const PyResult& PyConnection::GetResult() const {
        if (!result) {
            ThrowConnectionException();
        }
        return *result;
    }
    
    void PyConnection::SetResult(Result res) {
        if (res) {
            result = nullptr;
        } else {
            result = nullptr;
        }
    }

} // namespace otterbrix
