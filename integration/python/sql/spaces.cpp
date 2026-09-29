#include "spaces.hpp"

namespace otterbrix {

    namespace {
        core::result_wrapper_t<spaces_ptr> open_spaces(const configuration::config& config) {
            auto host = base_otterbrix_t::open(config);
            if (host.has_error()) {
                return host.error();
            }
            return spaces_ptr{new spaces(std::move(host.value()))};
        }
    } // namespace

    spaces::spaces(host_ptr host)
        : base_otterbrix_t(std::move(host)) {}

    core::result_wrapper_t<spaces_ptr> spaces::get_instance() {
        return open_spaces(configuration::config::default_config());
    }

    core::result_wrapper_t<spaces_ptr> spaces::get_instance(const std::filesystem::path& path) {
        return open_spaces(configuration::config::create_config(path));
    }

} // namespace otterbrix
