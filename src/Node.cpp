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
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <queue>
#include <vector>

#include "CompressTree.h"
#include "HashUtil.h"
#include "Node.h"
#include "Slaves.h"

namespace gpucbt {
    Node::Node(CompressTree* tree, uint32_t level) :
            tree_(tree),
            level_(level),
            parent_(NULL),
            queueStatus_(NONE) {
        id_ = tree_->nodeCtr++;
        buffer_.SetParent(this);

        pthread_mutex_init(&emptyMutex_, NULL);
        pthread_cond_init(&emptyCond_, NULL);

        pthread_mutex_init(&sortMutex_, NULL);
        pthread_cond_init(&sortCond_, NULL);

        pthread_mutex_init(&xgressMutex_, NULL);
        pthread_cond_init(&xgressCond_, NULL);

        pthread_spin_init(&queueStatusLock_, PTHREAD_PROCESS_PRIVATE);
    }

    Node::~Node() {
        pthread_mutex_destroy(&sortMutex_);
        pthread_cond_destroy(&sortCond_);

        pthread_mutex_destroy(&xgressMutex_);
        pthread_cond_destroy(&xgressCond_);

    }

    bool Node::insert(const MessageHash& hash, const Message& msg) {
        // copy into Buffer fields
        uint32_t n = buffer_.num_elements();
        buffer_.hashes_[n] = hash;
        buffer_.messages_[n] = msg;
        buffer_.set_num_elements(n + 1);
        return true;
    }

    bool Node::isLeaf() const {
        if (children_.empty())
            return true;
        return false;
    }

    bool Node::isRoot() const {
        if (parent_ == NULL)
            return true;
        return false;
    }

    bool Node::EmptyIfNecessary() {
        bool ret = true;
        if (tree_->emptyType_ == ALWAYS || isFull()) {
            ret = SpillBuffer();
        }
        return ret;
    }

    bool Node::SpillBuffer() {
        schedule(MERGE);
        return true;
    }

    bool Node::emptyBuffer() {
        uint32_t curChild = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0;

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
            /* this may be called even when buffer is not full (when flushing
             * all buffers at the end). */
            if (isFull() || isRoot()) {
                tree_->AddLeafToEmpty(this);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %u/%u\n", id_, buffer_.num_elements(),
                        Buffer::kEmptyThreshold);
#endif
            }
            return true;
        }

        if (buffer_.empty()) {
            for (curChild = 0; curChild < children_.size(); curChild++) {
                children_[curChild]->EmptyIfNecessary();
            }
        } else {
            // find the first separator strictly greater than the first element
            while (buffer_.hashes_[curElement] >=
                    children_[curChild]->separator_) {
                children_[curChild]->EmptyIfNecessary();
                curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                if (curChild >= children_.size()) {
                    fprintf(stderr,
                            "Node: %d: Can't place %u among children\n", id_,
                            l->hashes_[curElement]);
                    checkIntegrity();
                    assert(false);
                }
#endif
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node: %d: first node chosen: %d (sep: %u, \
                child: %d); first element: %u\n", id_, children_[curChild]->id_,
                    children_[curChild]->separator_, curChild,
                    buffer_.hashes_[0].hash());
#endif
            uint32_t num = buffer_.num_elements();

            while (curElement < num) {
                if (buffer_.hashes_[curElement] >=
                        children_[curChild]->separator_) {
                    /* this separator is the largest separator that is not greater
                     * than *curHash. This invariant needs to be maintained.
                     */
                    if (curElement > lastElement) {
                        // copy elements into child
                        CopyFromBuffer(children_[curChild]->buffer_,
                                lastElement, curElement - lastElement);
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Copied %u elements into node %d\n",
                                curElement - lastElement,
                                children_[curChild]->id_);
#endif
                        lastElement = curElement;
                    }
                    // skip past all separators not greater than current hash
                    while (buffer_.hashes_[curElement] >=
                            children_[curChild]->separator_) {
                        children_[curChild]->EmptyIfNecessary();
                        curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                        if (curChild >= children_.size()) {
                            assert(false);
                        }
#endif
                    }
                }
                // proceed to next element
                curElement++;
            }

            // copy remaining elements into child
            if (curElement >= lastElement) {
                // copy elements into child
                CopyFromBuffer(children_[curChild]->buffer_,
                        lastElement, curElement - lastElement);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Copied %u elements into node %d\n",
                        curElement - lastElement,
                        children_[curChild]->id_);
#endif
                children_[curChild]->EmptyIfNecessary();
                curChild++;
            }
            // empty or egress any remaining children
            while (curChild < children_.size()) {
                children_[curChild]->EmptyIfNecessary();
                curChild++;
            }

            // set buffer as empty
            if (!isRoot())
                buffer_.Deallocate();
            else
                buffer_.SetEmpty();
        }
        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            SplitNonLeaf();
        }
        return true;
    }

    bool Node::sortBuffer(bool use_gpu) {
        bool ret = buffer_.Sort(use_gpu);
        return ret;
    }

    bool Node::aggregateSortedBuffer() {
        bool ret = buffer_.Aggregate();
        return ret;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::SplitLeaf() {
        // select splitting index
        uint32_t num = buffer_.num_elements();
        uint32_t splitIndex = num / 2;
        while (buffer_.hashes_[splitIndex] ==
                buffer_.hashes_[splitIndex - 1]) {
            splitIndex++;
#ifdef ENABLE_ASSERT_CHECKS
            if (splitIndex == num) {
                assert(false);
            }
#endif
        }

        // create new leaf
        Node* newLeaf = new Node(tree_, 0);
        CopyFromBuffer(newLeaf->buffer_, splitIndex,
                num - splitIndex);
        newLeaf->separator_ = separator_;

        // modify this leaf properties

        Buffer temp;
        CopyFromBuffer(temp, 0, splitIndex);
        separator_ = buffer_.hashes_[splitIndex].hash();

        // deallocate the old buffer
        buffer_.Deallocate();

        buffer_.messages_ = temp.messages_;
        buffer_.hashes_ = temp.hashes_;
        buffer_.set_num_elements(temp.num_elements());
        // Clear temp to prevent automatic deallocation
        temp.Clear();

#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new indices: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_,
                buffer_.num_elements(), newLeaf->buffer_.num_elements(),
                separator_, newLeaf->separator_);
#endif

        // if leaf is also the root, create new root
        if (isRoot()) {
            tree_->CreateNewRoot(newLeaf);
        } else {
            parent_->AddChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::CopyFromBuffer(Buffer& dest_buffer, uint32_t index,
            uint32_t num) {
        uint32_t dest_num = dest_buffer.num_elements();
#ifdef ENABLE_ASSERT_CHECKS
        if (dest_num + num >= Buffer::kMaximumElements) {
            fprintf(stderr, "Node: %d, num_elements: %d, num_copied: %d\n",
                    id_, dest_num, num);
            assert(false);
        }
#endif
        memmove(&dest_buffer.messages_[dest_num], &buffer_.messages_[index],
                num * sizeof(Message));
        memmove(&dest_buffer.hashes_[dest_num], &buffer_.hashes_[index],
                num * sizeof(MessageHash));
        dest_buffer.set_num_elements(dest_num + num);
        return true;
    }

    bool Node::AddChild(Node* newNode) {
        uint32_t i;
        // insert separator value

        // find position of insertion
        std::vector<Node*>::iterator it = children_.begin();
        for (i = 0; i < children_.size(); ++i) {
            if (newNode->separator_ > children_[i]->separator_)
                continue;
            break;
        }
        it += i;
        children_.insert(it, newNode);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Node %d added at pos %u, [", id_,
                newNode->id_, i);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "], num children: %ld\n", children_.size());
#endif
        // set parent
        newNode->parent_ = this;

        return true;
    }

    bool Node::SplitNonLeaf() {
        // ensure node's buffer is empty
#ifdef ENABLE_ASSERT_CHECKS
        if (!buffer_.empty()) {
            fprintf(stderr, "Node %d has non-empty buffer\n", id_);
            assert(false);
        }
#endif
        // create new node
        Node* newNode = new Node(tree_, level_);
        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = (children_.size() + 1) / 2;
#ifdef ENABLE_ASSERT_CHECKS
        if (children_[newNodeChildIndex]->separator_ <=
                children_[newNodeChildIndex-1]->separator_) {
            fprintf(stderr, "%d sep is %u and %d sep is %u\n",
                    newNodeChildIndex,
                    children_[newNodeChildIndex]->separator_,
                    newNodeChildIndex-1,
                    children_[newNodeChildIndex-1]->separator_);
            assert(false);
        }
#endif
        // add children to new node
        for (uint32_t i = newNodeChildIndex; i < children_.size(); ++i) {
            newNode->children_.push_back(children_[i]);
            children_[i]->parent_ = newNode;
        }
        // set separator
        newNode->separator_ = separator_;

        // remove children from current node
        std::vector<Node*>::iterator it = children_.begin() +
                newNodeChildIndex;
        children_.erase(it, children_.end());

        // median separator from node
        separator_ = children_[children_.size()-1]->separator_;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "After split, %d: [", id_);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%u, ", children_[j]->separator_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j = 0; j < newNode->children_.size(); ++j)
            fprintf(stderr, "%u, ", newNode->children_[j]->separator_);
        fprintf(stderr, "]\n");

        fprintf(stderr, "Children, %d: [", id_);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j = 0; j < newNode->children_.size(); ++j)
            fprintf(stderr, "%d, ", newNode->children_[j]->id_);
        fprintf(stderr, "]\n");
#endif

        if (isRoot()) {
            buffer_.Deallocate();
            return tree_->CreateNewRoot(newNode);
        } else {
            return parent_->AddChild(newNode);
        }
    }

    bool Node::isFull() const {
        if (buffer_.num_elements() > Buffer::kEmptyThreshold)
            return true;
        return false;
    }

    uint32_t Node::level() const {
        return level_;
    }

    uint32_t Node::id() const {
        return id_;
    }

    Action Node::getQueueStatus() {
        pthread_spin_lock(&queueStatusLock_);
        Action ret = queueStatus_;
        pthread_spin_unlock(&queueStatusLock_);
        return ret;
    }

    void Node::setQueueStatus(const Action& act) {
        pthread_spin_lock(&queueStatusLock_);
        queueStatus_ = act;
        pthread_spin_unlock(&queueStatusLock_);
    }

    void Node::done(const Action& act) {
        switch(act) {
            case MERGE:
                {
                    // Signal that we're done sorting
                    pthread_mutex_lock(&sortMutex_);
                    pthread_cond_signal(&sortCond_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case SORT:
            case EMPTY:
                {
                }
                break;
            case NONE:
                {
                    assert(false && "Can't signal NONE");
                }
                break;
        }
    }

    void Node::schedule(const Action& act) {
        switch(act) {
            case SORT:
                {
                    setQueueStatus(SORT);
                    // add node to merger
                    tree_->sorter_->AddNode(this);
                    tree_->sorter_->Wakeup();
                }
                break;
            case MERGE:
                {
                    setQueueStatus(MERGE);
                    // add node to merger
                    tree_->merger_->AddNode(this);
                    tree_->merger_->Wakeup();
                }
                break;
            case EMPTY:
                {
                    setQueueStatus(act);
                    // add node to empty
                    tree_->emptier_->AddNode(this);
                    tree_->emptier_->Wakeup();
                }
                break;
            case NONE:
                {
                    assert(false && "Can't schedule NONE");
                }
                break;
        }
    }

    void Node::wait(const Action& act) {
        switch (act) {
            case SORT:
                break;
            case MERGE:
                {
                    pthread_mutex_lock(&sortMutex_);
                    while (getQueueStatus() == act)
                        pthread_cond_wait(&sortCond_, &sortMutex_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case EMPTY:
                {
                    pthread_mutex_lock(&emptyMutex_);
                    while (getQueueStatus() == act)
                        pthread_cond_wait(&emptyCond_, &emptyMutex_);
                    pthread_mutex_unlock(&emptyMutex_);
                }
                break;
            case NONE:
                {
                    assert(false && "Can't wait for NONE");
                }
                break;
        }
    }

    void Node::perform() {
        Action act = getQueueStatus();
        switch (act) {
            case SORT:
            case MERGE:
                {
                    bool use_gpu = id() % 2 == 0? true : false;
                    sortBuffer(use_gpu);
                    if (!use_gpu)
                        aggregateSortedBuffer();
                }
                break;
            case EMPTY:
                {
                    bool rootFlag = isRoot();
                    emptyBuffer();
                    if (isLeaf())
                        tree_->HandleFullLeaves();
                    setQueueStatus(NONE);
                    if (rootFlag) {
                        tree_->sorter_->SubmitNextNodeForEmptying();
                    }
                }
                break;
            case NONE:
                {
                    assert(false && "Can't perform NONE");
                }
                break;
        }
    }
}

