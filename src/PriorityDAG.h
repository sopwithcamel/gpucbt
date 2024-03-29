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

#ifndef SRC_EMPTYQUEUE_H_
#define SRC_EMPTYQUEUE_H_
#include <stdint.h>
#include <list>
#include <tr1/unordered_map>
#include <queue>
#include <set>
#include <vector>

namespace gpucbt {
    class Node;
    struct NodeID {
        uint32_t operator()(const Node* node) const {
            return node->id();
        }
    };
    struct NodeEqual {
        bool operator()(const Node* lhs, const Node* rhs) const {
            return (lhs->id() == rhs->id());
        }
    };

    typedef struct {
        Node* node;
        uint32_t prio; // node priority
    } NodeInfo;
    struct NodeInfoCompare {
        bool operator()(const NodeInfo* lhs, const NodeInfo* rhs) const {
            return (lhs->prio < rhs->prio);
        }
    };

    typedef std::tr1::unordered_map<Node*, std::set<uint32_t>*, NodeID,
            NodeEqual> DisabledDAG;
    typedef std::priority_queue<NodeInfo*, std::vector<NodeInfo*>,
            NodeInfoCompare> EnabledPriorityQueue;

    class PriorityDAG {
      public:
        PriorityDAG() {}
        ~PriorityDAG() {
            DisabledDAG::iterator it = disabNodes_.begin();
            for ( ; it != disabNodes_.end(); ++it) {
                delete it->second;
            }
            while (!enabNodes_.empty()) {
                NodeInfo* n = enabNodes_.top();
                enabNodes_.pop();
                delete n;
            }
        }

        // Insert element into queue. Returns true if the element is enabled to
        // empty immediately or false otherwise.
        bool insert(Node* n) {
            // check if all of the node's children have queueStatus_ >=
            // COMPRESSED (i.e. COMPRESS, PAGEOUT or NONE).
            bool canEmpty = true;
            uint32_t i, s = n->children_.size();
            std::set<uint32_t>* d = new std::set<uint32_t>();
            for (i = 0; i < s; ++i) {
                if (n->children_[i]->getQueueStatus() != NONE) {
                    canEmpty = false;
                    d->insert(n->children_[i]->id());
                }
            }
            //  If so, the node goes to the enabled queue
            if (canEmpty) {
                delete d;
    
                NodeInfo* ni = new NodeInfo();
                ni->node = n;
                ni->prio = n->level();
                enabNodes_.push(ni);
            } else { // disabled queue
                disabNodes_[n] = d; 
            }
            // There is no need to check if there is an active parent (which
            // needs to be disabled). This is because at the time the parent
            // was added, if a child hadn't begun the process of emptying, then
            // it cannot do so (begin emptying) unless the parent empties.
            // Therefore there is never a possibility of a node moving from the
            // enabled to the disabled queue.
            return canEmpty;
        }

        // Returns an enabled with maximum priority or NULL if the queue is
        // empty
        Node* pop() {
            if (enabNodes_.empty())
                return NULL;
            NodeInfo* ret = enabNodes_.top();
            enabNodes_.pop();
            Node* ret_node = ret->node;
            delete ret;
            return ret_node;
        }

        void post(Node* n) {
            // If parent is present, it must be in the disabled queue.
            // remove n from its parent's dependency list
            if (n->parent_ && n->parent_->getQueueStatus() == EMPTY) {
                std::set<uint32_t>* ch = disabNodes_[n->parent_];
                std::set<uint32_t>::iterator it = ch->find(n->id());
                if (it != ch->end()) { // found
                    ch->erase(it);
                }
                // if dependency list of parent is empty move parent to enabled
                // queue
                if (ch->empty()) {
                    NodeInfo* np = new NodeInfo();
                    np->node = n->parent_;
                    np->prio = n->parent_->level();
                    enabNodes_.push(np);

                    delete ch;
                    DisabledDAG::iterator t = disabNodes_.find(n->parent_);
                    disabNodes_.erase(t); 
                }
            }
            
        }

        bool empty() const {
            return enabNodes_.empty();
        }


        void PrintElements() {
            fprintf(stderr, "EN: has %ld els.", enabNodes_.size());
/*
            for (EnabledPriorityQueue::iterator it = enabNodes_.begin();
                    it != enabNodes_.end(); ++it) {
                if (it->node->isRoot())
                    fprintf(stderr, "%d(%d)*, ", it->node->id(), it->prio);
                else
                    fprintf(stderr, "%d(%d), ", it->node->id(), it->prio);
            }
*/
            fprintf(stderr, ", DIS: ");
            for (DisabledDAG::iterator it = disabNodes_.begin();
                    it != disabNodes_.end(); ++it) {
                if (it->first->isRoot()) {
                    fprintf(stderr, "%d(%ld)*, ", it->first->id(),
                            it->second->size());
                } else {
                    fprintf(stderr, "%d(%ld), ", it->first->id(),
                            it->second->size());
                }
            }
            fprintf(stderr, "\n");
        }

      private:
        EnabledPriorityQueue enabNodes_;
        DisabledDAG disabNodes_;
    };
}
#endif  // SRC_EMPTYQUEUE_H_
