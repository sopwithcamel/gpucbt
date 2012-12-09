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
        assert(hashes_[num - 1]);
        assert(sizeof(messages_[num - 1]));

        thrust::device_vector<uint32_t> d_hash(hashes_, hashes_ + num);
        thrust::device_vector<uint32_t> d_perm(num);

        thrust::sequence(d_perm.begin(), d_perm.begin() + num);

        try {
            thrust::sort_by_key(d_hash.begin(), d_hash.end(), d_perm.begin());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        } catch (thrust::system_error &e) {
            fprintf(stderr, "Some other error: %s\n", e.what());
            exit(-1);
        }
        // copy sorted hashes back
        thrust::copy(d_hash.begin(), d_hash.end(), hashes_);

        thrust::copy(d_perm.begin(), d_perm.end(), perm_);

/*
        typedef thrust::device_vector<uint32_t>::iterator it_h;
        typedef thrust::device_vector<uint32_t>::iterator it_p;
        thrust::pair<it_h, it_p> new_end;

        try {
            new_end = thrust::reduce_by_key(d_hash.begin(), d_hash.end(),
                    d_msg.begin(), d_hash.begin(), d_msg.begin(),
                    Message::MessageMerge());
        } catch(std::bad_alloc &e) {
            fprintf(stderr, "Ran out of memory while sorting\n");
            exit(-1);
        } catch (thrust::system_error &e) {
            fprintf(stderr, "Some other error: %s\n", e.what());
            exit(-1);
        }
*/
    }

    bool Buffer::GPUAggregate() {
        return false;
    }
}
