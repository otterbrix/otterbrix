#pragma once

#include <string_view>

namespace core {

    class string_split_iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::string_view;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        string_split_iterator(std::string_view str, char delim, bool end = false) noexcept
            : str_(str)
            , delim_(delim)
            , end_(end) {
            if (!end_) {
                ++(*this);
            }
        }

        reference operator*() const noexcept { return current_; }

        pointer operator->() const noexcept { return &current_; }

        string_split_iterator& operator++() noexcept {
            if (end_)
                return *this;

            auto pos = str_.find(delim_);
            if (pos != std::string_view::npos) {
                current_ = str_.substr(0, pos);
                str_.remove_prefix(pos + 1);
            } else if (next_end_) {
                end_ = true;
            } else {
                current_ = str_;
                next_end_ = true;
            }
            return *this;
        }

        string_split_iterator operator++(int) noexcept {
            string_split_iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const string_split_iterator& a, const string_split_iterator& b) noexcept {
            return a.end_ == b.end_ && (a.end_ || a.str_.data() == b.str_.data());
        }

        friend bool operator!=(const string_split_iterator& a, const string_split_iterator& b) noexcept {
            return !(a == b);
        }

    private:
        std::string_view str_;
        char delim_;
        bool end_ = false;
        bool next_end_ = false;
        value_type current_;
    };

    class string_splitter {
    public:
        string_splitter(std::string_view str, char delim) noexcept
            : str_(str)
            , delim_(delim) {}

        string_split_iterator begin() const noexcept { return {str_, delim_}; }

        string_split_iterator end() const noexcept { return {str_, delim_, true}; }

    private:
        std::string_view str_;
        char delim_;
    };

} // namespace core
