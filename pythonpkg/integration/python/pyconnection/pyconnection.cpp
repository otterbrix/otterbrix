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

    PyConnection::PyConnection(const PyConnection& other) : ConnectionEnvironment(other) {}

    pyconnection_ptr PyConnection::Connect(const py::object &database_p, bool read_only, 
            const py::dict &config_options) {
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
        //auto default_path = std::filesystem::absolute(ConnectionEnvironment::DEFAULT_FOLDER);
        //if (std::filesystem::equivalent(path, default_path)) {
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
            const py::object& traceback) {
        this->Close();
        if (exc_type.ptr() != Py_None) {
            // Propagate the exception if any occurred
            PyErr_SetObject(exc_type.ptr(), exc.ptr());
            throw py::error_already_set();
        }
    }

    pyconnection_ptr PyConnection::Begin() {
        ("BEGIN TRANSACTION");
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Commit() {
        ("COMMIT");
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Rollback() {
        ("ROLLBACK");
        return shared_from_this();
    }

    pyconnection_ptr PyConnection::Checkpoint() {
        ("CHECKPOINT");
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

    pycursor_ptr PyConnection::Execute(const py::object& query, py::object params) {
        py::gil_scoped_acquire gil;
        result = nullptr;
        if (py::isinstance<py::str>(query)) {
            SetResult(ExecuteInternal(string(py::str(query))));
        } 
        return shared_from_this();

    }

    unique_ptr<PyRelation> PyConnection::RunQuery(const py::object& query, string alias, py::object params) {
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
            //result = std::move(make_unique<PyResult>(this, res));
            result =  nullptr;
        } else {
            result = nullptr;
        }
    }

    // pycursor_ptr PyConnection::Execute(const py::object& query, py::object params) {
    //     py::gil_scoped_acquire gil;
    //     SetResult(nullptr);
    //     // execute->check ans->move to relation->SetResult 
    //     //SetResult(make_unique<int>(nullptr, {}));
    //
    //     return shared_from_this();
    // }

    // unique_ptr<PyRelation> PyConnection::FromDF(const PandasDataFrame &value) {
    //     string name = "df_" + string_utils::GenerateRandomName();
    //     auto tableref = PythonReplacementScan::ReplacementObject(value, name);
    //     return CreateViewRelation(std::move(tableref), name);
    // }

    // Optional<py::tuple> PyConnection::FetchOne() {
    //     if (!HasResult()) {
    //         throw std::runtime_error("No open result set");
    //     }
    //     auto& result = GetResult();
    //     return py::make_tuple(py::cast(5));//py::cast(result));//.FetchOne()
    // }

    /*pyarrow::Table PyConnection::FetchArrow(idx_t rows_per_batch) {
        if (!HasResult()) {
            throw std::runtime_error("No open result set");
        }    
        auto &result = con.GetResult();
        return result.ToArrowTable(rows_per_batch);
    }*/
    
/*    py::tuple PyConnection::test(py::array& np) {
        //return py::make_tuple(py::hasattr(np, "strides"), np.attr("strides").attr("__getitem__")(0));
       auto* resource = &(space->resource); 
        std::vector<components::table::column_definition_t> col_defs;
        col_defs.emplace_back(
                "test", components::types::logical_type::BIGINT);
        std::vector<components::types::complex_logical_type> types = {components::types::logical_type::BIGINT};
        components::vector::data_chunk_t chunk(resource, types, 4);
        chunk.set_cardinality(4);
        auto src_ptr = (int64_t*)np.data();
        chunk.data[0].set_data(reinterpret_cast<std::byte*>(src_ptr));

        core::filesystem::local_file_system_t fs; 
        auto buffer_pool =
            components::table::storage::buffer_pool_t(resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = components::table::storage::standard_buffer_manager_t(resource, fs, buffer_pool);
        auto block_manager = components::table::storage::in_memory_block_manager_t(buffer_manager, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE);


        components::table::data_table_t table(resource, block_manager, std::move(col_defs));
        components::table::table_append_state state(resource);
        table.append_lock(state); 
        table.initialize_append(state);
        table.append(chunk, state);
        table.finalize_append(state);
        
        //components::relation::LimitRelation rel(nullptr, 1, 2);
        auto relation = std::make_shared<DocumentRelation>(space, std::move(table));
        auto session = otterbrix::session_id_t();
        auto node1 = relation->GetQueryNode();
        auto cur1 = space->dispatcher()->execute_plan(session, node1);
        auto node2 = relation->Limit(2)->GetQueryNode();
        session = otterbrix::session_id_t();
        auto cur2 = space->dispatcher()->execute_plan(session, node2);


        return py::make_tuple(cur1->size(), cur2->size());
       return py::make_tuple();
    }*/
    
} // namespace otterbrix
