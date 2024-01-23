// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "brpc/uri.h"
#include "brpc/rtmp.h"

#define kMinInputLength 5
#define kMaxInputLength 1024

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < kMinInputLength || size > kMaxInputLength){
        return 1;
    }

    char *data_in = (char *)malloc(size + 1);
    memcpy(data_in, data, size);
    data_in[size] = '\0';


    std::string input(reinterpret_cast<const char*>(data), size);
    {
        brpc::URI uri;
        uri.SetHttpURL(input);
    }
    {
        butil::StringPiece host;
        butil::StringPiece vhost;
        butil::StringPiece port;
        butil::StringPiece app;
        butil::StringPiece stream_name;

        brpc::ParseRtmpURL(input, &host, &vhost, &port, &app, &stream_name);
    }

    return 0;
}
