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

#ifndef SRC_BUFFER_H_
#define SRC_BUFFER_H_
#include <stdint.h>
#include <stdio.h>
#include "Config.h"
#include "Message.h"

namespace gpucbt {
    class Node;

    class Buffer {
        friend class Node;
        friend class CompressTree;
        friend class Compressor;

        public:

          Buffer();
          // clears all buffer state
          ~Buffer();
          uint32_t num_elements() const;
          void set_num_elements(uint32_t n);
          bool empty() const;

          void SetParent(Node* n);
          void SetEmpty();

          void Allocate(bool isLarge = false);
          // DOES NOT FREE memory. Only resets Buffer
          void Clear();
          // Frees memory buffer and resets Buffer
          void Deallocate();

          /* Sorting-related */
          void Quicksort(uint32_t left, uint32_t right);
          void GPUSort(uint32_t num);
          bool Sort(bool use_gpu = false);

          /* Aggregation-related */
          bool Aggregate(bool use_gpu = false);
          bool CPUAggregate();
          bool GPUAggregate();

        private:
          static const uint32_t kMaximumElements;
          static const uint32_t kEmptyThreshold;

          const Node* node_;

          Message* messages_;
          uint32_t* hashes_;
          uint32_t num_elements_;
          uint32_t* perm_;
    };
}
#endif  // SRC_BUFFER_H_
