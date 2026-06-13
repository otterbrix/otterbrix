#include "pyconnection.hpp"
#include <connection_environment/relation/relation_factory.hpp>
#include <otterbrix_wrapper/pyresult.hpp>
#include <otterbrix_wrapper/pyrelation.hpp>
#include <core/string_util/string_util.hpp>
#include <scan/python_replacement_scan.hpp>
#include <components/catalog/catalog_oids.hpp>

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

        // Enumerate USER tables straight from the engine catalog (pg_class) instead
        // of a local registry. pg_class layout: [0=oid, 1=relname, 2=relnamespace,
        // 3=relkind, 4=relstoragemode]. We project the columns we need and filter in
        // C++: user objects have oid >= FIRST_USER_OID (system catalog rows sit below
        // that), and only regular relations (relkind 'r') are tables.
        auto cursor = ExecuteInternal("SELECT oid, relname, relkind FROM pg_class;");
        if (!cursor || cursor->is_error() || cursor->size() == 0) {
            return res;
        }

        // Resolve the projected column positions by alias, falling back to the
        // SELECT order above if the cursor carries no aliases.
        const auto& types = cursor->type_data();
        components::cursor::index_t oid_col = 0;
        components::cursor::index_t relname_col = 1;
        components::cursor::index_t relkind_col = 2;
        for (std::size_t i = 0; i < types.size(); ++i) {
            if (!types[i].has_alias()) {
                continue;
            }
            const auto& alias = types[i].alias();
            if (alias == "oid") {
                oid_col = static_cast<components::cursor::index_t>(i);
            } else if (alias == "relname") {
                relname_col = static_cast<components::cursor::index_t>(i);
            } else if (alias == "relkind") {
                relkind_col = static_cast<components::cursor::index_t>(i);
            }
        }

        while (cursor->has_next()) {
            cursor->advance();
            auto oid_cell = cursor->value(static_cast<uint64_t>(oid_col));
            if (oid_cell.is_null() ||
                oid_cell.value<std::uint32_t>() < components::catalog::FIRST_USER_OID) {
                continue; // system catalog object
            }
            auto relkind_cell = cursor->value(static_cast<uint64_t>(relkind_col));
            if (!relkind_cell.is_null()) {
                auto relkind = relkind_cell.value<std::string_view>();
                if (!relkind.empty() && relkind.front() != 'r') {
                    continue; // not a regular table (view / matview / sequence / ...)
                }
            }
            auto relname_cell = cursor->value(static_cast<uint64_t>(relname_col));
            if (relname_cell.is_null()) {
                continue;
            }
            auto relname = relname_cell.value<std::string_view>();
            res.append(py::str(std::string(relname)));
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
        if (py::isinstance<py::str>(query)) {
            ExecuteInternal(string(py::str(query)));
        }
        return shared_from_this();

    }

    unique_ptr<PyRelation> PyConnection::RunQuery(const py::object& query, string alias, py::object /*params*/) {
        if (alias.empty()) {
            alias = "unnamed_relation";
        }
        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this), RelationFromQuery(string(py::str(query))));

    }


    unique_ptr<PyRelation> PyConnection::FromDF(const py::object& value) {
        string name = "df_no_idea";
        auto tableref = Scan::ReplacementObject(value, name);

        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this),
            RelationFactory::CreateDFRelation(std::move(tableref)));
    }

    unique_ptr<PyRelation> PyConnection::FromObject(const py::object& value) {
        string name = "object_no_idea";
        auto tableref = Scan::TryReplacementObject(value, name);
        assert(tableref);

        return make_unique<PyRelation>(static_cast<ConnectionEnvironment*>(this),
            RelationFactory::CreateDFRelation(std::move(tableref)));
    }

} // namespace otterbrix
