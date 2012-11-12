#include <thrust/sort.h>
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>
#include <cuda.h>
#include <cuda_runtime.h>

#include "Buffer.h"
#include "Message.h"

namespace gpucbt {

    void Buffer::GPUSort(uint32_t num) {
        // initialize host vector
        thrust::device_vector<Message> d(messages_, messages_ + num);

        try {
            thrust::sort(d.begin(), d.end());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        }

        thrust::copy(d.begin(), d.end(), messages_);
    }

    bool Buffer::GPUAggregate() {
        return false;
    }
}
