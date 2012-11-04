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
#include <dlfcn.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <gperftools/heap-profiler.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <zmq.hpp>
#include "zhelpers.hpp"
#include <string>
#include <iostream>

#include "CompressTree.h"
#include "Message.h"
#include "Server.h"

using namespace google::protobuf::io;
using namespace std;

DEFINE_bool(timed, false, "Do a timed run");
DEFINE_bool(heapcheck, false, "Heap check");

namespace gpucbtservice {
    // Global static pointer used to ensure a single instance of the class.
    CBTServer* CBTServer::instance_ = NULL; 

    // This function is called to create an instance of the class.  Calling the
    // constructor publicly is not allowed. The constructor is private and is
    // only called by this Instance function.
    CBTServer* CBTServer::Instance() {
        if (!instance_) {   // Only allow one instance of class to be generated.
            instance_ = new CBTServer();
        }
        return instance_;
    }

    void CBTServer::Start() {
        Instance()->Run();
    }

    void CBTServer::Stop() {
        stop_server_ = true;
        fprintf(stderr, "Stopping Server...\n");

        // flushing tree
        int hash; void* ptr = reinterpret_cast<void*>(&hash);
        gpucbt::Message test;
        cbt_->nextValue(test);
        sleep(2);
        cbt_->clear();
        delete Instance();
    }

    CBTServer::CBTServer() :
            kMessagesInsertAtTime(100000),
            stop_server_(false),
            total_messages_inserted_(0) {
        uint32_t fanout = 8;
        uint32_t buffer_size = 31457280;

        cbt_ = new gpucbt::CompressTree(fanout, buffer_size);
        fprintf(stderr, "CBTServer created\n");
    }

    CBTServer::~CBTServer() {
        delete cbt_;
    }

    void CBTServer::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REP);
        socket.bind ("tcp://*:5555");

        while (!stop_server_) {
            bool ret;

            //  Wait for next request from client
            zmq::message_t request;
            socket.recv(&request);

            uint32_t num_received_messages = request.size() /
                    sizeof(gpucbt::Message);
            ret = HandleMessage((gpucbt::Message*)request.data(),
                    num_received_messages);
//            std::cout << "Recvd. " << request.size() << " sized message" << std::endl;
    
            if (ret) {
                total_messages_inserted_ += num_received_messages;
//                cout << "Inserted " << total_messages_inserted_ << endl;
            } else {
                printf("ERROR\n");
            }
                
            //  Send reply back to client
            zmq::message_t reply (5);
            memcpy((void *)reply.data(), ret? "True" : "False", 5);
            socket.send(reply);
        }
    }

    bool CBTServer::HandleMessage(const gpucbt::Message* recv_msgs,
            uint32_t num_messages) {
        uint32_t rem = num_messages;
        while (rem > 0) {
            uint32_t ins = (rem < kMessagesInsertAtTime?
                    rem : kMessagesInsertAtTime);
            assert(cbt_->bulk_insert(recv_msgs + (num_messages - rem), ins));
            rem -= ins;
        }

        return true;
    }

    void CBTServer::Timer() {
        uint64_t last_messages_inserted = 0;
        while (!stop_server_) {
            cout << (total_messages_inserted_ - last_messages_inserted) << endl;
            last_messages_inserted = total_messages_inserted_;
            sleep(1);
        }
    }

    void* CBTServer::CallHelper(void*) {
        Instance()->Timer();   
        pthread_exit(NULL);
    }
} // gpucbtservice

void INThandler(int sig) {
    gpucbtservice::CBTServer::Instance()->Stop();   
    exit(0);
}

int main (int argc, char** argv) {
    // Define gflags options
    google::ParseCommandLineFlags(&argc, &argv, true);

    // Check if we are doing a timed run
    if (FLAGS_timed) {
        pthread_t timer_thread_;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        int rc = pthread_create(&timer_thread_, &attr,
                gpucbtservice::CBTServer::CallHelper, NULL);
        if (rc) {
            cerr << "ERROR; return code from pthread_create() is "
                    << rc << endl;
            exit(-1);
        }
        sleep(1);
    }

    signal(SIGINT, INThandler);

    gpucbtservice::CBTServer::Instance()->Start();   
    return 0;
}
