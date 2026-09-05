#pragma once

#include "block_manager.hpp"
#include "buffer_manager.hpp" // complete buffer_manager_t for the inherited constructor

#include <cstdio>
#include <cstdlib>

namespace components::table::storage {

    // Block manager for TRANSIENT buffers — temporary payloads that never belong to a file
    // (standard_buffer_manager_t's `temp_block_manager_`). Owns geometry and block_handle_t
    // construction, nothing else. Every file-facing virtual is a CALLER BUG if reached (a
    // transient manager handed to something that wanted a file), so each aborts. The four that
    // return result_wrapper_t<bool> abort too rather than reporting: block_handle_t::load only
    // calls read() for a real disk id, so the value would go nowhere anyway.
    class transient_block_manager_t : public block_manager_t {
    public:
        using block_manager_t::block_manager_t;

        std::unique_ptr<block_t> convert_block(uint64_t, file_buffer_t&) override { no_io("convert_block"); }
        std::unique_ptr<block_t> create_block(uint64_t, file_buffer_t*) override { no_io("create_block"); }
        uint64_t free_block_id() override { no_io("free_block_id"); }
        uint64_t peek_free_block_id() override { no_io("peek_free_block_id"); }
        bool is_root_block(meta_block_pointer_t) override { no_io("is_root_block"); }
        void mark_as_free(uint64_t) override { no_io("mark_as_free"); }
        void mark_as_used(uint64_t) override { no_io("mark_as_used"); }
        void mark_as_modified(uint64_t) override { no_io("mark_as_modified"); }
        void increase_block_ref_count(uint64_t) override { no_io("increase_block_ref_count"); }
        uint64_t meta_block() override { no_io("meta_block"); }
        [[nodiscard]] core::result_wrapper_t<bool> read(block_t&) override { no_io("read"); }
        [[nodiscard]] core::result_wrapper_t<bool> read_blocks(file_buffer_t&, uint64_t, uint64_t) override {
            no_io("read_blocks");
        }
        [[nodiscard]] core::result_wrapper_t<bool> write(file_buffer_t&, uint64_t) override { no_io("write"); }
        [[nodiscard]] core::result_wrapper_t<bool> file_sync() override { no_io("file_sync"); }
        uint64_t total_blocks() override { no_io("total_blocks"); }
        uint64_t free_blocks() override { no_io("free_blocks"); }

    private:
        [[noreturn]] static void no_io(const char* op) {
            std::fprintf(stderr,
                         "transient_block_manager_t::%s: this manager owns buffers with no file behind them and "
                         "cannot perform file I/O. Reaching it means a transient block manager was handed to a "
                         "caller that needs a real one.\n",
                         op);
            std::fflush(stderr);
            std::abort();
        }
    };

} // namespace components::table::storage
