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
#include <stdio.h>
#define __STDC_LIMIT_MACROS /* for UINT32_MAX etc. */
#include <stdint.h>
#include <stdlib.h>
#include <deque>

#include "Buffer.h"
#include "CompressTree.h"
#include "Slaves.h"

namespace gpucbt {
    CompressTree::CompressTree(uint32_t b, uint32_t buffer_size) :
            b_(b),
            nodeCtr(1),
            allFlush_(true),
            empty_(true),
            lastLeafRead_(0),
            lastOffset_(0),
            lastElement_(0),
            threadsStarted_(false) {
        pthread_cond_init(&emptyRootAvailable_, NULL);
        pthread_mutex_init(&emptyRootNodesMutex_, NULL);
        sem_init(&gpu_in_use_, 0, 1);
    }

    CompressTree::~CompressTree() {
        pthread_cond_destroy(&emptyRootAvailable_);
        pthread_mutex_destroy(&emptyRootNodesMutex_);
        pthread_barrier_destroy(&threadsBarrier_);
        sem_destroy(&sleepSemaphore_);
        sem_destroy(&gpu_in_use_);
    }

    bool CompressTree::bulk_insert(const uint32_t* hashes,
            const Message* msgs, uint64_t num) {
        bool ret = true;
        // copy buf into root node buffer
        // root node buffer always decompressed
        if (num > 0)
            allFlush_ = empty_ = false;
        if (!threadsStarted_) {
            StartThreads();
        }
        for (uint64_t i = 0; i < num; ++i) {
            if (inputNode_->isFull()) {
                // add inputNode_ to be sorted
                inputNode_->schedule(SORT);

                // get an empty root. This function can block until there are
                // empty roots available
                inputNode_ = GetEmptyRootNode();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Now inputting into node %d\n",
                        inputNode_->id());
#endif  // CT_NODE_DEBUG
            }
            ret &= inputNode_->insert(hashes[i], msgs[i]);
        }
        return ret;
    }

    bool CompressTree::insert(const uint32_t& hash, const Message& msg) {
        bool ret = bulk_insert(&hash, &msg, 1);
        return ret;
    }

    bool CompressTree::bulk_read(Message* msg_list, uint64_t& num_read,
            uint64_t max) {
        num_read = 0;
        while (num_read < max) {
            if (!(nextValue(msg_list[num_read])))
                return false;
            num_read++;
        }
        return true;
    }

    bool CompressTree::nextValue(Message& msg) {
        if (empty_)
            return false;

        if (!allFlush_) {
            FlushBuffers();
            lastLeafRead_ = 0;
            lastOffset_ = 0;
            lastElement_ = 0;

            allFlush_ = true;

            Node* curLeaf = allLeaves_[0];
            while (curLeaf->buffer_.num_elements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        msg = curLeaf->buffer_.messages_[lastElement_];
        lastElement_++;

        if (lastElement_ >= curLeaf->buffer_.num_elements()) {
            if (++lastLeafRead_ == allLeaves_.size()) {
                int all_done;                                                            
                do {                                                                     
                    usleep(100);                                                         
                    sem_getvalue(&sleepSemaphore_, &all_done);                           
                } while (all_done);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Emptying tree!\n");
#endif
                EmptyTree();
                StopThreads();
                return false;
            }
            Node *n = allLeaves_[lastLeafRead_];
            while (curLeaf->buffer_.num_elements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
            lastElement_ = 0;
        }
        return true;
    }

    void CompressTree::clear() {
        EmptyTree();
        StopThreads();
    }

    void CompressTree::EmptyTree() {
        std::deque<Node*> delList1;
        std::deque<Node*> delList2;
        delList1.push_back(rootNode_);
        while (!delList1.empty()) {
            Node* n = delList1.front();
            delList1.pop_front();
            for (uint32_t i = 0; i < n->children_.size(); ++i) {
                delList1.push_back(n->children_[i]);
            }
            delList2.push_back(n);
        }
        while (!delList2.empty()) {
            Node* n = delList2.front();
            delList2.pop_front();
            delete n;
        }
        allLeaves_.clear();
        leavesToBeEmptied_.clear();
        allFlush_ = empty_ = true;
        lastLeafRead_ = 0;
        lastOffset_ = 0;
        lastElement_ = 0;

        nodeCtr = 0;
    }

    bool CompressTree::FlushBuffers() {
        Node* curNode;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");

        emptyType_ = ALWAYS;
        inputNode_->schedule(SORT);

        int all_done;                                                            
        do {                                                                     
            usleep(100);                                                         
            sem_getvalue(&sleepSemaphore_, &all_done);                           
        } while (all_done);

        // add all leaves;
        visitQueue.push_back(rootNode_);
        while (!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();
            if (curNode->isLeaf()) {
                allLeaves_.push_back(curNode);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Pushing node %d to all-leaves\t",
                        curNode->id_);
                fprintf(stderr, "Now has: ");
                for (uint32_t i = 0; i < allLeaves_.size(); ++i) {
                    fprintf(stderr, "%d ", allLeaves_[i]->id_);
                }
                fprintf(stderr, "\n");
#endif
                continue;
            }
            for (uint32_t i = 0; i < curNode->children_.size(); ++i) {
                visitQueue.push_back(curNode->children_[i]);
            }
        }
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
        uint32_t depth = 1;
        curNode = rootNode_;
        while (curNode->children_.size() > 0) {
            depth++;
            curNode = curNode->children_[0];
        }
        fprintf(stderr, "Tree has depth: %d\n", depth);
        uint64_t numit = 0;
        for (uint64_t i = 0; i < allLeaves_.size(); ++i)
            numit += allLeaves_[i]->buffer_.num_elements();
        fprintf(stderr, "Tree has %ld elements\n", numit);
        return true;
    }

    bool CompressTree::AddLeafToEmpty(Node* node) {
        leavesToBeEmptied_.push_back(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    void CompressTree::HandleFullLeaves() {
        while (!leavesToBeEmptied_.empty()) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop_front();

            Node* newLeaf = node->SplitLeaf();

            Node *l1 = NULL, *l2 = NULL;
            if (node->isFull()) {
                l1 = node->SplitLeaf();
                assert(l1);
            }
            if (newLeaf && newLeaf->isFull()) {
                l2 = newLeaf->SplitLeaf();
                assert(l2);
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n",
                    node->id_);
#endif
            // % WHY?
            //node->setQueueStatus(NONE);
        }
    }

    Node* CompressTree::GetEmptyRootNode() {
        pthread_mutex_lock(&emptyRootNodesMutex_);
        while (emptyRootNodes_.empty()) {
#ifdef CT_NODE_DEBUG
            if (!rootNode_->buffer_.empty())
                fprintf(stderr, "inserter sleeping (buffer not empty)\n");
            else
                fprintf(stderr, "inserter sleeping (queued somewhere %d)\n",
                        emptyRootNodes_.size());
#endif

            pthread_cond_wait(&emptyRootAvailable_, &emptyRootNodesMutex_);

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "inserter fingered\n");
#endif
        }
        Node* e = emptyRootNodes_.front();
        emptyRootNodes_.pop_front();
        pthread_mutex_unlock(&emptyRootNodesMutex_);
        return e;
    }

    void CompressTree::AddEmptyRootNode(Node* n) {
        bool no_empty_nodes = false;
        pthread_mutex_lock(&emptyRootNodesMutex_);
        // check if there are no empty nodes right now
        if (emptyRootNodes_.empty())
            no_empty_nodes = true;
        emptyRootNodes_.push_back(n);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Added empty root (now has: %d)\n",
                emptyRootNodes_.size());
#endif
        // if this is the first empty node, then signal
//        if (no_empty_nodes)
        pthread_cond_signal(&emptyRootAvailable_);
        pthread_mutex_unlock(&emptyRootNodesMutex_);
    }

    bool CompressTree::RootNodeAvailable() {
        if (!rootNode_->buffer_.empty() ||
                rootNode_->getQueueStatus() != NONE)
            return false;
        return true;
    }

    void CompressTree::SubmitNodeForEmptying(Node* n) {
        // perform the switch, schedule root, add node to empty list
        Buffer temp = rootNode_->buffer_;
        rootNode_->buffer_ = n->buffer_;
        rootNode_->schedule(EMPTY);

        n->buffer_ = temp;
        temp.Clear();
        AddEmptyRootNode(n);
    }

    void CompressTree::StartThreads() {
        // create root node; initially a leaf
        rootNode_ = new Node(this, 0);
        rootNode_->separator_ = UINT32_MAX;

        inputNode_ = new Node(this, 0);
        inputNode_->separator_ = UINT32_MAX;

        uint32_t number_of_root_nodes = 4;
        for (uint32_t i = 0; i < number_of_root_nodes - 1; ++i) {
            Node* n = new Node(this, 0);
            n->separator_ = UINT32_MAX;
            emptyRootNodes_.push_back(n);
        }

        emptyType_ = IF_FULL;

        uint32_t mergerThreadCount = 4;
        uint32_t emptierThreadCount = 4;
        uint32_t sorterThreadCount = 2;

        // One for the inserter
        uint32_t threadCount = mergerThreadCount +
                emptierThreadCount + sorterThreadCount + 1;
#ifdef ENABLE_COUNTERS
        uint32_t monitorThreadCount = 1;
        threadCount += monitorThreadCount;
#endif
        pthread_barrier_init(&threadsBarrier_, NULL, threadCount);
        sem_init(&sleepSemaphore_, 0, threadCount - 1);

        sorter_ = new Sorter(this);
        sorter_->StartThreads(sorterThreadCount);

        merger_ = new Merger(this);
        merger_->StartThreads(mergerThreadCount);

        emptier_ = new Emptier(this);
        emptier_->StartThreads(emptierThreadCount);

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::StopThreads() {
        delete inputNode_;

        merger_->StopThreads();
        sorter_->StopThreads();
        emptier_->StopThreads();
        threadsStarted_ = false;
    }

    bool CompressTree::CreateNewRoot(Node* otherChild) {
        Node* newRoot = new Node(this, rootNode_->level() + 1);
        newRoot->separator_ = UINT32_MAX;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d is new root; children are %d and %d\n",
                newRoot->id_, rootNode_->id_, otherChild->id_);
#endif
        // add two children of new root
        newRoot->AddChild(rootNode_);
        newRoot->AddChild(otherChild);
        rootNode_ = newRoot;
        return true;
    }
}
