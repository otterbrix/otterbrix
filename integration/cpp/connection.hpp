#include "otterbrix.hpp"

namespace otterbrix {

    class connection_t {
    public:
        explicit connection_t(boost::intrusive_ptr<otterbrix_t> instance);
        ~connection_t();
        connection_t(const connection_t&) = delete;
        connection_t& operator=(const connection_t&) = delete;

        // void execute_async(const std::string& query);
        components::cursor::cursor_t_ptr execute(const std::string& query);
        components::cursor::cursor_t_ptr cursor();
        void close();

    private:
        const session_id_t session_{};
        boost::intrusive_ptr<otterbrix_t> instance_;
        components::cursor::cursor_t_ptr cursor_store_{nullptr};
    };

} // namespace otterbrix