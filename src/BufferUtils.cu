#include <thrust/sort.h>
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>

#include "Buffer.h"
#include "Message.h"

namespace gpucbt {

    void Buffer::GPUSort(uint32_t num) {
        // initialize host vector
        thrust::device_vector<Message> d(messages_, messages_ + num);

        // copy over vector to gpu
//        thrust::device_vector<Message> d = h;
        thrust::sort(d.begin(), d.end());

        thrust::copy(d.begin(), d.end(), messages_);
    }
}
