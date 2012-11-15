// Copyright (C) 2012 Georgia Institute of Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// ---
// Author: Hrishikesh Amur

#ifndef SRC_MESSAGE_H
#define SRC_MESSAGE_H
#include <stdint.h>
#include <string.h>
#include <thrust/host_vector.h>

namespace gpucbt {
    class Message {
      public:
        struct MessageMerge {
            __host__ __device__ Message operator()(const Message& lhs,
                    const Message& rhs) {
                Message ret = lhs;
                ret.value_ += rhs.value_;
                return ret;
            }
        };

        __host__ __device__ Message() {}
        __host__ __device__ ~Message() {}
        const char* key() const {
            return key_;
        }
        void set_key(const char* key, uint32_t key_length) {
            strncpy(key_, key, key_length);
        }
        uint32_t value() const {
            return value_;
        }
        void set_value(const uint32_t val) {
            value_ = val;
        }
        void Merge(const Message& msg);
        bool SameKey(const Message& msg);
      private:
        char key_[16];
        uint64_t value_;
    };

    class MessageHash {
      public:
        struct MessageHashComp {
            __host__ __device__ bool operator()(
                    const MessageHash& lhs,
                    const MessageHash& rhs) {
                return lhs == rhs;
            }
        };
        uint32_t hash() const {
            return hash_;
        }
        void set_hash(const uint32_t hash) {
            hash_ = hash;
        }
        __host__ __device__ bool operator<(const MessageHash& rhs) const {
            return (hash_ < rhs.hash_);
        }
        bool operator>(const MessageHash& rhs) const { return (hash_ > rhs.hash_); }
        bool operator<=(const MessageHash& rhs) const { return (hash_ <= rhs.hash_); }
        bool operator>=(const MessageHash& rhs) const { return (hash_ >= rhs.hash_); }
        __host__ __device__ bool operator==(const MessageHash& rhs) const {
             return (hash_ == rhs.hash_);
        }
        bool operator<(uint32_t rhs) const { return (hash_ < rhs); }
        bool operator>(uint32_t rhs) const { return (hash_ > rhs); }
        bool operator<=(uint32_t rhs) const { return (hash_ <= rhs); }
        bool operator>=(uint32_t rhs) const { return (hash_ >= rhs); }
        bool operator==(uint32_t rhs) const { return (hash_ == rhs); }
      private:
        uint32_t hash_;
    };
}  // gpucbt

#endif  // SRC_MESSAGE_H
