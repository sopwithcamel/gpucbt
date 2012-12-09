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

#ifndef SERVICE_SERVER_H_
#define SERVICE_SERVER_H_
#include <pthread.h>
#include <stdint.h>

#include "CompressTree.h"
#include "Message.h"

namespace gpucbtservice {
    class CBTServer {
      public:
        static CBTServer* Instance();
        void Start();
        void Stop();
        static void* CallHelper(void*);
      private:
        const uint32_t kMessagesInsertAtTime;

        CBTServer();
        ~CBTServer();
        bool HandleMessage(const uint32_t* recv_hashes,
                const gpucbt::Message* recv_msgs, 
                uint32_t num_PAOs);
        void Run();
        void Timer();

        static CBTServer* instance_;
        bool stop_server_;
        gpucbt::CompressTree* cbt_;

        uint64_t total_messages_inserted_;
    };
} // gpucbtservice

#endif  // SERVICE_SERVER_H_
