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

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
#include "Buffer.h"
#include "snappy.h"

namespace gpucbt {
    class CompressTree;

    const uint32_t Buffer::kMaximumElements = 80000000;
    const uint32_t Buffer::kEmptyThreshold = 40000000;

    Buffer::Buffer() :
            messages_(NULL),
            hashes_(NULL),
            num_elements_(0) {
        messages_ = new Message[kMaximumElements];
        hashes_ = new MessageHash[kMaximumElements];
    }

    Buffer::~Buffer() {
        Clear();
    }

    bool Buffer::empty() const {
        return (num_elements_ == 0);
    }

    uint32_t Buffer::num_elements() const {
        return num_elements_;
    }

    void Buffer::set_num_elements(uint32_t n) {
        num_elements_ = n;
    }

    void Buffer::SetParent(Node* n) {
        node_ = n;
    }

    void Buffer::SetEmpty() {
        set_num_elements(0);
    }

    void Buffer::Clear() {
        messages_ = NULL;
        hashes_ = NULL;
        set_num_elements(0);
    }

    void Buffer::Deallocate() {
        if (messages_) {
            delete[] messages_;
            delete[] hashes_;
            messages_ = NULL;
            hashes_ = NULL;
        }
        set_num_elements(0);
    }

    void Buffer::Quicksort(uint32_t uleft, uint32_t uright) {
        int32_t i, j, stack_pointer = -1;
        int32_t left = uleft;
        int32_t right = uright;
        int32_t* rstack = new int32_t[128];

        Message swap, temp;
        Message* arr = messages_;

        MessageHash swh, tmph;

        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    swh = hashes_[j];
                    i = j - 1;
                    if (i < 0) {
                        fprintf(stderr, "Noo");
                        assert(false);
                    }
                    while (i >= left && (hashes_[i] > swh)) {
                        arr[i + 1] = arr[i];
                        hashes_[i + 1] = hashes_[i];
                        i--;
                    }
                    arr[i + 1] = swap;
                    hashes_[i + 1] = swh;
                }
                if (stack_pointer == -1) {
                    break;
                }
                right = rstack[stack_pointer--];
                left = rstack[stack_pointer--];
            } else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;

                swap = arr[median];
                arr[median] = arr[i];
                arr[i] = swap;

                swh = hashes_[median];
                hashes_[median] = hashes_[i];
                hashes_[i] = swh;

                if (hashes_[left] > hashes_[right]) {
                    swap = arr[left];
                    arr[left] = arr[right];
                    arr[right] = swap;

                    swh = hashes_[left];
                    hashes_[left] = hashes_[right];
                    hashes_[right] = swh;
                }
                if (hashes_[i] > hashes_[right]) {
                    swap = arr[i];
                    arr[i] = arr[right];
                    arr[right] = swap;

                    swh = hashes_[i];
                    hashes_[i] = hashes_[right];
                    hashes_[right] = swh;
                }
                if (hashes_[left] > hashes_[i]) {
                    swap = arr[left];
                    arr[left] = arr[i];
                    arr[i] = swap;

                    swh = hashes_[left];
                    hashes_[left] = hashes_[i];
                    hashes_[i] = swh;
                }
                temp = arr[i];
                tmph = hashes_[i];

                while (true) {
                    while (hashes_[++i] < tmph);
                    while (hashes_[--j] > tmph);
                    if (j < i) {
                        break;
                    }
                    swap = arr[i];
                    arr[i] = arr[j];
                    arr[j] = swap;

                    swh = hashes_[i];
                    hashes_[i] = hashes_[j];
                    hashes_[j] = swh;
                }
                arr[left + 1] = arr[j];
                arr[j] = temp;

                hashes_[left + 1] = hashes_[j];
                hashes_[j] = tmph;

                if (right - i + 1 >= j - left) {
                    rstack[++stack_pointer] = i;
                    rstack[++stack_pointer] = right;
                    right = j - 1;
                } else {
                    rstack[++stack_pointer] = left;
                    rstack[++stack_pointer] = j - 1;
                    left = i;
                }
            }
        }
        delete[] rstack;
    }

    // Sorting-related
    bool Buffer::Sort(bool use_gpu) {
        if (empty())
            return true;

        uint32_t num = num_elements();
        // sort elements
        if (use_gpu) {
            GPUSort(num);
        } else {
            Quicksort(0, num - 1);
        }
        return true;
    }

    bool Buffer::Aggregate(bool use_gpu) {
        bool ret;
        if (use_gpu) {
            ret = GPUAggregate();
        } else {
            ret = CPUAggregate();
        }
        return ret;
    }

    bool Buffer::CPUAggregate() {
        // initialize auxiliary buffer
        Buffer aux;

        // aggregate elements in buffer
        uint32_t lastIndex = 0;
        uint32_t aggregatedIndex = 0;
        uint32_t num = num_elements();
        for (uint32_t i = 1; i < num; ++i) {
            if (hashes_[i] == hashes_[lastIndex]) {
                // aggregate elements
                if (messages_[i].SameKey(
                            messages_[lastIndex])) {
                    messages_[lastIndex].Merge(messages_[i]);
                    continue;
                }
            }

            // we found a Message with a different key than that in
            // messages_[lastIndex]. Therefore we store the latter and update
            // lastIndex
            aux.messages_[aggregatedIndex] = messages_[lastIndex];
            aux.hashes_[aggregatedIndex] = hashes_[lastIndex];
            ++aggregatedIndex;
            lastIndex = i;
        }
        // copy the last Message;
        aux.messages_[aggregatedIndex] = messages_[lastIndex];
        aux.hashes_[aggregatedIndex] = hashes_[lastIndex];
        aggregatedIndex++;

        Deallocate();
        messages_ = aux.messages_;
        hashes_ = aux.hashes_;
        set_num_elements(aggregatedIndex);

        // Clear aux to prevent deallocation on destruction
        aux.Clear();
        return true;
    }
}
