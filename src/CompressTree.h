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

#ifndef SRC_COMPRESSTREE_H_
#define SRC_COMPRESSTREE_H_

#include <pthread.h>
#include <semaphore.h>
#include <deque>
#include <queue>
#include <vector>
#include "Config.h"
#include "Node.h"
#include "PartialAgg.h"

namespace gpucbt {
    enum EmptyType {
        ALWAYS,
        IF_FULL
    };

    class Node;
    class Emptier;
    class Compressor;
    class Merger;
    class Sorter;

    class CompressTree {
      public:
        CompressTree(uint32_t b, uint32_t buffer_size);
        ~CompressTree();

        /* Insert record into tree */
        bool insert(const uint32_t& hash, const Message& agg);
        bool bulk_insert(const uint32_t* hashes, const Message* paos,
                uint64_t num);
        /* read values */
        // returns true if there are more values to be read and false otherwise
        bool bulk_read(Message* pao_list, uint64_t& num_read, uint64_t max);
        bool nextValue(Message& msg);
        void clear();

      private:
        friend class Node;
        friend class Slave;
        friend class Emptier;
        friend class Merger;
        friend class Pager;
        friend class Sorter;
#ifdef ENABLE_COUNTERS
        friend class Monitor;
#endif
        Node* GetEmptyRootNode();
        void AddEmptyRootNode(Node* n);
        void SubmitNodeForEmptying(Node* n);
        bool RootNodeAvailable();
        bool AddLeafToEmpty(Node* node);
        bool CreateNewRoot(Node* otherChild);
        void EmptyTree();
        /* Write out all buffers to leaves. Do this before reading */
        bool FlushBuffers();
        void HandleFullLeaves();
        void StartThreads();
        void StopThreads();

      private:
        // (a,b)-tree...
        const uint32_t b_;
        uint32_t nodeCtr;
        Node* rootNode_;
        Node* inputNode_;

        std::deque<Node*> emptyRootNodes_;
        pthread_mutex_t emptyRootNodesMutex_;

        pthread_cond_t emptyRootAvailable_;

        bool allFlush_;
        bool empty_;
        EmptyType emptyType_;
        sem_t sleepSemaphore_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        uint32_t lastLeafRead_;
        uint32_t lastOffset_;
        uint32_t lastElement_;

        /* Slave-threads */
        bool threadsStarted_;
        pthread_barrier_t threadsBarrier_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        uint32_t numEvicted_;
        char* evictedBuffer_;
        uint32_t evictedBufferOffset_;
        pthread_mutex_t evictedBufferMutex_;

        /* Members for async-emptying */
        Emptier* emptier_;

        /* Sorting-related */
        Sorter* sorter_;

        /* Members for async-sorting */
        Merger* merger_;

        /* Compression-related */
        Compressor* compressor_;

#ifdef ENABLE_COUNTERS
        /* Monitor */
        Monitor* monitor_;
#endif

        /* GPU-related */
        sem_t gpu_in_use_;
    };
}

#endif  // SRC_COMPRESSTREE_H_
