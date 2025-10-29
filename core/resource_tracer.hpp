#include <iostream>
#include <memory_resource>
#include <unordered_map>

static inline size_t align_to(size_t size, size_t align) { return (size + (align - 1)) / align * align; }

template<class memory_pool_t>
class memory_tracer_t : public memory_pool_t {
public:
    memory_tracer_t()
        : memory_pool_t() {}

    virtual ~memory_tracer_t() {
        std::cout << "total allocated bytes: " << allocated_ << std::endl;
        std::cout << "total deallocated bytes: " << deallocated_ << std::endl;
        std::cout << "missed pointers count: " << addresses_.size() << std::endl;
    }

    void release() { memory_pool_t::release(); }

    std::pmr::memory_resource* upstream_resource() const noexcept { return memory_pool_t::upstream_resource(); }

protected:
    size_t allocated_{0};
    size_t deallocated_{0};
    std::unordered_map<uint64_t, uint64_t> addresses_;

    void* do_allocate(size_t bytes, size_t alignment) override {
        void* ptr = memory_pool_t::do_allocate(bytes, alignment);
        size_t with_alignment = align_to(bytes, alignment);
        auto it = addresses_.find(reinterpret_cast<uint64_t>(ptr));
        if (it == addresses_.end()) {
            addresses_.emplace(reinterpret_cast<uint64_t>(ptr), with_alignment);
        } else {
            std::cout << "do_allocate: ptr exists: " << ptr << std::endl;
        }
        allocated_ += with_alignment;
        std::cout << "do_allocate: region: " << ptr << " - " << ptr + bytes << std::endl;
        std::cout << "do_allocate: bytes: " << bytes << std::endl;
        std::cout << "do_allocate: alignment: " << alignment << std::endl;
        if (with_alignment != bytes) {
            std::cout << "do_allocate: extra bytes due to alignment: " << with_alignment - bytes << std::endl;
        }
        return ptr;
    }

    void do_deallocate(void* p, size_t bytes, size_t alignment) override {
        size_t with_alignment = align_to(bytes, alignment);
        deallocated_ += with_alignment;
        auto it = addresses_.find(reinterpret_cast<uint64_t>(p));
        if (it != addresses_.end()) {
            if (it->second != with_alignment) {
                std::cout << "do_deallocate: region size error: allocated: " << it->second
                          << "; requested to deallocate: " << with_alignment << std::endl;
            }
            addresses_.erase(it);
        } else {
            std::cout << "do_deallocate: ptr does not exists: " << p << std::endl;
        }
        std::cout << "do_deallocate: region: " << p << " - " << p + bytes << std::endl;
        std::cout << "do_deallocate: bytes: " << bytes << std::endl;
        std::cout << "do_deallocate: alignment: " << alignment << std::endl;
        if (with_alignment != bytes) {
            std::cout << "do_deallocate: extra bytes due to alignment: " << with_alignment - bytes << std::endl;
        }
        memory_pool_t::do_deallocate(p, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};