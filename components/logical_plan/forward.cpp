#include "forward.hpp"

namespace components::logical_plan { namespace aggregate {

    operator_type get_aggregate_type(const std::string& key) {
        if (key == "count") {
            return operator_type::count;
        } else if (key == "group") {
            return operator_type::group;
        } else if (key == "limit") {
            return operator_type::limit;
        } else if (key == "match") {
            return operator_type::match;
        } else if (key == "merge") {
            return operator_type::merge;
        } else if (key == "out") {
            return operator_type::out;
        } else if (key == "project") {
            return operator_type::project;
        } else if (key == "skip") {
            return operator_type::skip;
        } else if (key == "sort") {
            return operator_type::sort;
        } else if (key == "unset") {
            return operator_type::unset;
        } else if (key == "unwind") {
            return operator_type::unwind;
        } else if (key == "finish") {
            return operator_type::finish;
        } else {
            return aggregate::operator_type::invalid;
        }
    }

}} // namespace components::logical_plan::aggregate