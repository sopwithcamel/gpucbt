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
// #include "compsort.h"
// #include "rle.h"
#include "snappy.h"

namespace gpucbt {
    class CompressTree;

    const uint32_t Buffer::kMaximumElements = 16000000;
    const uint32_t Buffer::kEmptyThreshold = 8000000;

    Buffer::Buffer() :
            messages_(NULL),
            num_elements_(0) {
        messages_ = new Message[kMaximumElements];
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
        set_num_elements(0);
    }

    void Buffer::Deallocate() {
        if (messages_) {
            delete[] messages_;
            messages_ = NULL;
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

        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    i = j - 1;
                    if (i < 0) {
                        fprintf(stderr, "Noo");
                        assert(false);
                    }
                    while (i >= left && (arr[i].hash() > swap.hash())) {
                        arr[i + 1] = arr[i];
                        i--;
                    }
                    arr[i + 1] = swap;
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

                if (arr[left].hash() > arr[right].hash()) {
                    swap = arr[left];
                    arr[left] = arr[right];
                    arr[right] = swap;
                }
                if (arr[i].hash() > arr[right].hash()) {
                    swap = arr[i];
                    arr[i] = arr[right];
                    arr[right] = swap;
                }
                if (arr[left].hash() > arr[i].hash()) {
                    swap = arr[left];
                    arr[left] = arr[i];
                    arr[i] = swap;
                }
                temp = arr[i];
                while (true) {
                    while (arr[++i].hash() < temp.hash());
                    while (arr[--j].hash() > temp.hash());
                    if (j < i) {
                        break;
                    }
                    swap = arr[i];
                    arr[i] = arr[j];
                    arr[j] = swap;
                }
                arr[left + 1] = arr[j];
                arr[j] = temp;
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
    bool Buffer::Sort() {
        if (empty())
            return true;

        uint32_t num = num_elements();
        // quicksort elements
        Quicksort(0, num - 1);
        return true;
    }
}
