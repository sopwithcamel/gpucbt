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

#ifndef SRC_NODE_H_
#define SRC_NODE_H_
#include <stdint.h>
#include <string>
#include <vector>

#include "Buffer.h"
#include "CompressTree.h"
#include "Config.h"
#include "PartialAgg.h"

namespace gpucbt {

    class CompressTree;
    class Emptier;
    class Compressor;
    class Merger;

    enum Action {
        SORT,
        MERGE,
        EMPTY,
        NONE
    };

    class Node {
        friend class CompressTree;
        friend class Buffer;
        friend class Compressor;
        friend class Emptier;
        friend class Merger;
        friend class Sorter;
        friend class Slave;
        friend class PriorityDAG;

      public:
        explicit Node(CompressTree* tree, uint32_t level);
        ~Node();
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(const Message& msg);

        // identification functions
        bool isLeaf() const;
        bool isRoot() const;

        bool isFull() const;
        uint32_t level() const;
        uint32_t id() const;

      private:
        /* Buffer handling functions */

        bool EmptyIfNecessary();
        /* Responsible for handling the spilling of the buffer. */
        bool SpillBuffer();
        /* Function: empty the buffer into the buffers in the next level.
         *  + Must be called with buffer decompressed.
         *  + Buffer will be freed after invocation.
         *  + If children buffers overflow, it recursively calls itself.
         *    until the recursion reaches the leaves. At this stage, handling
         *    the leaf buffer overflows is queued for later because this may
         *    cause splitting (recursively) up the tree which is best done
         *    when no internal nodes are over-full.
         *  + an emptyBuffer() invocation should be followed by a
         *    handleFullLeaves() call.
         */
        bool emptyBuffer();
        /* Sort the root buffer based on hash value. All other nodes can
         * aggregating by merging. */
        bool sortBuffer();
        /* Aggregate the sorted root buffer */
        bool aggregateSortedBuffer();
        /* copy contents from node's buffer into this buffer. Starting from
         * index = index, copy num elements' data.
         */
        bool CopyFromBuffer(Buffer& dest_buffer, uint32_t index,
                uint32_t num);

        /* Tree-related functions */

        /* split leaf node and return new leaf */
        Node* SplitLeaf();
        /* Add a new child to the node; the child type indicates which side
         * of the separator the child must be inserted.
         * if the number of children is more than the allowed number:
         * + first check if siblings have fewer children
         * + if not, split the node into two and call addChild recursively
         */
        bool AddChild(Node* newNode);
        /* Split non-leaf node; must be called with the buffer decompressed
         * and sorted. If called on the root, then a new root is created */
        bool SplitNonLeaf();
        //
        // management of queues
        //
        // Actually perform action. This assumes that it is ok to perform the
        // action and does not check. For example, perform(MERGE) will directly
        // sort/merge the buffer. The caller has to ensure that the buffer is
        // already decompressed.
        void perform();
        // Check if the node is currently queued up for Action act and
        // block until receipt of signal indicating completion.
        void wait(const Action& act);
        // Signal that Action act is complete.
        void done(const Action& act);
        // 
        void schedule(const Action& act);
        Action getQueueStatus();
        void setQueueStatus(const Action& act);

        /* pointer to the tree */
        CompressTree* tree_;
        /* Buffer */
        Buffer buffer_;
        pthread_mutex_t stateMutex_;
        uint32_t id_;
        /* level in the tree; 0 at leaves and increases upwards */
        uint32_t level_;
        Node* parent_;
        PartialAgg *lastPAO, *thisPAO;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint32_t separator_;

        // Queueing related status, condition variables and mutexes
        enum Action queueStatus_;
        pthread_spinlock_t queueStatusLock_;

        pthread_cond_t emptyCond_;
        pthread_mutex_t emptyMutex_;

        pthread_cond_t sortCond_;
        pthread_mutex_t sortMutex_;

        pthread_cond_t xgressCond_;
        pthread_mutex_t xgressMutex_;
    };
}

#endif  // SRC_NODE_H_
