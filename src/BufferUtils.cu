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
        thrust::device_vector<MessageHash> d_hash(hashes_, hashes_ + num);
        thrust::device_vector<Message> d_msg(messages_, messages_ + num);

        try {
            thrust::sort_by_key(d_hash.begin(), d_hash.end(), d_msg.begin());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        }

        thrust::copy(d_hash.begin(), d_hash.end(), hashes_);
        thrust::copy(d_msg.begin(), d_msg.end(), messages_);
    }

    bool Buffer::GPUAggregate() {
        return false;
    }
}
