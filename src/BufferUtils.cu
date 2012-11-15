#include <assert.h>
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
        assert(hashes_[num - 1].hash());
        assert(sizeof(messages_[num - 1]));
        thrust::device_vector<MessageHash> d_hash(hashes_, hashes_ + num);
        thrust::device_vector<Message> d_msg(messages_, messages_ + num);

        try {
            thrust::sort_by_key(d_hash.begin(), d_hash.end(), d_msg.begin());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        } catch (thrust::system_error &e) {
            fprintf(stderr, "Some other error: %s\n", e.what());
            exit(-1);
        }

        typedef thrust::device_vector<MessageHash>::iterator it_h;
        typedef thrust::device_vector<Message>::iterator it_m;
        thrust::pair<it_h, it_m> new_end;

        try {
            new_end = thrust::reduce_by_key(d_hash.begin(), d_hash.end(),
                    d_msg.begin(), d_hash.begin(), d_msg.begin(),
                    MessageHash::MessageHashComp(),
                    Message::MessageMerge());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        } catch (thrust::system_error &e) {
            fprintf(stderr, "Some other error: %s\n", e.what());
            exit(-1);
        }

        thrust::copy(d_hash.begin(), new_end.first, hashes_);
        thrust::copy(d_msg.begin(), new_end.second, messages_);
        set_num_elements(new_end.first - d_hash.begin());
    }

    bool Buffer::GPUAggregate() {
        return false;
    }
}
