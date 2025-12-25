#include "wrapper_database.hpp"
#include "forward.hpp"
#include "spaces.hpp"
#include "wrapper_collection.hpp"

namespace otterbrix {

    wrapper_database::~wrapper_database() { trace(log_, "delete wrapper_database"); }

    auto wrapper_database::collection_names() -> py::list {
        py::list tmp;
        for (auto& i : collections_) {
            tmp.append(i.first);
        }
        return tmp;
    }

    bool wrapper_database::drop_collection(const std::string& collection_name) {
        debug(log_, "wrapper_database::drop_collection: {}", collection_name);
        collections_.erase(collection_name);
        auto session_tmp = otterbrix::session_id_t();
        auto result = ptr_->drop_collection(session_tmp, name_, collection_name);
        debug(log_, "wrapper_database::drop_collection {}", result->is_success());
        return result->is_success();
    }

    wrapper_collection_ptr wrapper_database::create(const std::string& collection_name) {
        debug(log_, "wrapper_database::create name collection: {}", collection_name);
        auto session_tmp = otterbrix::session_id_t();
        ptr_->create_collection(session_tmp, name_, collection_name);
        auto result = wrapper_collection_ptr(new wrapper_collection(collection_name, name_, ptr_, log_));
        debug(log_, "wrapper_client::get_or_create return wrapper_database_ptr");
        collections_.emplace(collection_name, result);
        return result;
    }

    wrapper_database::wrapper_database(const std::string& name, wrapper_dispatcher_t* ptr, log_t& log)
        : name_(name)
        , ptr_(ptr)
        , log_(log.clone()) {
        debug(log_, "wrapper_database");
    }

} // namespace otterbrix