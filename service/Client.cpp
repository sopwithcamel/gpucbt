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

#include <dlfcn.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>

#include <zmq.hpp>
#include "Util.h"
#include <string>
#include <iostream>

#include "Client.h"
#include "HashUtil.h"
#include "Message.h"

using namespace google::protobuf::io;

namespace gpucbtservice {

    CBTClient::CBTClient(uint32_t u, uint32_t l) :
            kNumUniqKeys(u),
            kKeyLen(l),
            kNumFillers(10000),
            kLettersInAlphabet(26),
            kMaxMessages(200000) {
        num_full_loops_ = (int)floor(Conv26(log2(kNumUniqKeys)));
        part_loop_ = (int)ceil(kNumUniqKeys / pow(26, num_full_loops_));
        GenerateFillers(kKeyLen - num_full_loops_ - 1);
    }

    CBTClient::~CBTClient() {
        for (uint32_t i = 0; i < kNumFillers; ++i)
            delete[] fillers_[i];
    }

    void CBTClient::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REQ);

        std::cout << "Connecting to CBTServer" << std::endl;
        socket.connect ("tcp://localhost:5555");

        uint32_t number_of_paos = 100000;

        for (int request_nbr = 0; ; request_nbr++) {
            std::stringstream ss;
            gpucbt::Message* msgs = new gpucbt::Message[number_of_paos];
            gpucbt::MessageHash* hashes = new gpucbt::MessageHash[number_of_paos];
            GenerateMessages(msgs, hashes, number_of_paos);
    
            uint32_t hash_size = sizeof(gpucbt::MessageHash) * number_of_paos;
            zmq::message_t hash_request(hash_size);
            memcpy((void*)hash_request.data(), (void*)hashes, hash_size);

//            std::cout << "Sending " << hash_request.size() << " sized message" << std::endl;
            socket.send(hash_request);

            //  Get the reply.
            zmq::message_t reply;
            socket.recv(&reply);
            assert(!strcmp(reinterpret_cast<char*>(reply.data()), "True"));
    
            uint32_t msg_size = sizeof(gpucbt::Message) * number_of_paos;
            zmq::message_t msg_request(msg_size);
            memcpy((void*)msg_request.data(), (void*)msgs, msg_size);

//            std::cout << "Sending " << msg_request.size() << " sized message" << std::endl;
            socket.send(msg_request);

            //  Get the reply.
            socket.recv(&reply);
            assert(!strcmp(reinterpret_cast<char*>(reply.data()), "True"));

            delete[] msgs;
            delete[] hashes;
        }
    }

    void CBTClient::GenerateFillers(uint32_t filler_len) {
        for (uint32_t i = 0; i < kNumFillers; ++i) {
            char* f = new char[filler_len + 1];
            for (uint32_t j = 0; j < filler_len; ++j)
                f[j] = 97 + rand() % kLettersInAlphabet;
            f[filler_len] = '\0';
            fillers_.push_back(f);
        }
    }

    void CBTClient::GenerateMessages(gpucbt::Message* msgs,
            gpucbt::MessageHash* hashes,
            uint32_t number_of_paos) {
        char* word = new char[kKeyLen + 1];
        assert(number_of_paos < kMaxMessages);
        for (uint32_t i = 0; i < number_of_paos; ++i) {
            for (uint32_t j=0; j < num_full_loops_; j++)
                word[j] = 97 + rand() % kLettersInAlphabet;
            word[num_full_loops_] = 97 + rand() % part_loop_;
            word[num_full_loops_ + 1] = '\0';

            uint32_t hash = HashUtil::MurmurHash(word, strlen(word), 42);
            uint32_t filler_number = hash % kNumFillers;

            strncat(word, fillers_[filler_number], kKeyLen -
                    num_full_loops_ - 1);
            hashes[i].set_hash(hash);
            msgs[i].set_key(word, kKeyLen); 
            msgs[i].set_value(1);
        }
        delete[] word;
    }
} // cbtservice


#define USAGE "%s <Number of unique keys> <Length of a key>\n"

int main (int argc, char* argv[])
{
    if (argc < 3) {
        fprintf(stdout, USAGE, argv[0]);
        exit(EXIT_FAILURE);
    }

    uint32_t uniq = atoi(argv[1]);
    uint32_t len = atoi(argv[2]);

    gpucbtservice::CBTClient* client = new gpucbtservice::CBTClient(uniq,
            len);
    client->Run();
    return 0;
}
