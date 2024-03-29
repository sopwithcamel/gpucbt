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

#ifndef SRC_SLAVES_H_
#define SRC_SLAVES_H_
#include <stdint.h>
#include <deque>
#include <string>
#include <vector>

#include "CompressTree.h"
#include "Node.h"
#include "PriorityDAG.h"

namespace gpucbt {
    class CompressTree;
    class Node;

    typedef std::priority_queue<NodeInfo*, std::vector<NodeInfo*>,
            NodeInfoCompare> PriorityQueue;

    class Slave {
      public:
        explicit Slave(CompressTree* tree);
        virtual ~Slave() {}
        // Responsible for managing queueStatus
        virtual void AddNode(Node* node) = 0;
        // Returns true if there are no queued jobs and all threads are
        // sleeping; false otherwise
        virtual bool empty();
        // Returns true if there are queued jobs; false otherwise
        virtual bool More();
        virtual void Wakeup();
        virtual void WaitUntilCompletionNoticeReceived();

        void StartThreads(uint32_t num = 1);
        void StopThreads();

      protected:
        class ThreadStruct {
          public:
            ThreadStruct() {
                pthread_cond_init(&hasWork_, NULL);
                pthread_mutex_init(&mutex_, NULL);
            }
            bool Wakeup();

            uint32_t index_;
            pthread_t thread_;
            pthread_cond_t hasWork_;
            pthread_mutex_t mutex_;
          private:
            // disable copying and assignment
            ThreadStruct(const ThreadStruct& rhs);
            ThreadStruct& operator=(const ThreadStruct& rhs);
        };

        // used to pass arguments to pthread execute function
        typedef struct {
            void* context;
            ThreadStruct* desc;
        } Pthread_args;

        // check if all input is done
        virtual bool inputComplete();

        // get next node from (default: head of) queue or NULL if empty
        virtual Node* getNextNode(bool fromHead = true);

        // add node to (default: tail of) queue
        virtual bool addNodeToQueue(Node* node, uint32_t priority);

        static void* callHelper(void* context);
        // the pthread execution function. It extracts Nodes added by
        // AddNode() using getNextNode() and calls Work(). Each of these
        // functions can be specialized
        virtual void slaveRoutine(ThreadStruct* t);

        // check if someone has requested a completion signal.
        // completion is defined as all threads being asleep
        // and the queue being empty. This is requested only when
        // the insertion thread completes input and blocks waiting
        // for all Slaves to finish.
        virtual void checkSendCompletionNotice();
        virtual void setInputComplete(bool value);
        bool checkInputComplete();
        virtual void Work(Node* n) = 0;

        // Thread-mask related functions
        void setThreadSleep(uint32_t index);
        uint32_t getNumberOfSleepingThreads();

#ifdef CT_NODE_DEBUG
        // Debugging
        virtual std::string GetSlaveName() const = 0;
        virtual void PrintElements();
#endif  // CT_NODE_DEBUG

        CompressTree* const tree_;

        pthread_mutex_t completionMutex_;
        pthread_cond_t complete_;
        bool askForCompletionNotice_;

        uint32_t numThreads_;
        std::vector<ThreadStruct*> threads_;

        pthread_spinlock_t maskLock_;
        uint64_t tmask_;

        pthread_spinlock_t nodesLock_;
        // nodesLock_ protection begin
            // never use the empty() member of the deque directly.
            // instead, always use Slave::empty()
        PriorityQueue nodes_;
        bool inputComplete_;
        bool nodesEmpty_;
        // nodesLock_ protection end

      private:
        friend class Node;
    };

    class Sorter : public Slave {
      public:
        explicit Sorter(CompressTree* tree);
        ~Sorter();
        void Work(Node* n);
        void AddNode(Node* node);

      protected:
        virtual std::string GetSlaveName() const;
        void AddToSorted(Node* n);
        void SubmitNextNodeForEmptying();

      private:
        friend class Node;

        std::deque<Node*> sortedNodes_;
        pthread_mutex_t sortedNodesMutex_;
    };

    class Emptier : public Slave {
      public:
        explicit Emptier(CompressTree* tree);
        ~Emptier();
        void Work(Node* n);
        void AddNode(Node* node);
        // Returns true if there are no queued jobs and all threads are
        // sleeping; false otherwise
        bool empty();

      protected:
        // Returns true if there are queued jobs; false otherwise
        bool More();
        virtual Node* getNextNode(bool fromHead = true);
        virtual std::string GetSlaveName() const;
        void PrintElements();

      private:
        friend class Node;

        PriorityDAG queue_;
    };

    class Compressor : public Slave {
      public:
        explicit Compressor(CompressTree* tree);
        ~Compressor();
        void Work(Node* n);
        void AddNode(Node* node);

      protected:
        virtual std::string GetSlaveName() const;

      private:
        friend class Node;
    };

    class Merger : public Slave {
      public:
        explicit Merger(CompressTree* tree);
        ~Merger();
        void Work(Node* n);
        void AddNode(Node* node);

      protected:
        virtual std::string GetSlaveName() const;

      private:
        friend class Node;
    };

    class Pager : public Slave {
      public:
        explicit Pager(CompressTree* tree);
        ~Pager();
        void Work(Node* n);
        void AddNode(Node* node);

      protected:
        virtual std::string GetSlaveName() const;

      private:
        friend class Node;
    };

#ifdef ENABLE_COUNTERS
    class Monitor : public Slave {
      public:
        explicit Monitor(CompressTree* tree);
        ~Monitor();
        void Work(Node* n);
        void AddNode(Node* n);

      protected:
        virtual std::string GetSlaveName() const;

      private:
        friend class Node;
        friend class Compressor;

        uint64_t numElements;
        uint64_t numMerged;
        std::vector<uint64_t> elctr;
        std::vector<int32_t> nodeCtr;
        std::vector<int32_t> totNodeCtr;
        uint64_t actr;
        uint64_t bctr;
        uint64_t cctr;
    };
#endif
}
#endif  // SRC_SLAVES_H_
