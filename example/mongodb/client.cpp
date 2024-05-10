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

// A multi-threaded client getting keys from a memcache server constantly.

#include <cstddef>
#include <cstdint>
#include <openssl/hmac.h>
#include <stdlib.h>
#include <stdio.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/mongo_utils.h>
#include <butil/string_printf.h>
#include <brpc/channel.h>
#include <brpc/memcache.h>
#include <brpc/policy/couchbase_authenticator.h>
#include "brpc/options.pb.h"
#include "brpc/policy/mongo.pb.h"
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>


DEFINE_int32(thread_num, 10, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_bool(use_couchbase, false, "Use couchbase.");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:7017", "IP Address of server");
DEFINE_string(bucket_name, "", "Couchbase bucktet name");
DEFINE_string(bucket_password, "", "Couchbase bucket password");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_bool(dont_fail, false, "Print fatal when some call failed");
DEFINE_int32(exptime, 0, "The to-be-got data will be expired after so many seconds");
DEFINE_string(key, "hello", "The key to be get");
DEFINE_string(value, "world", "The value associated with the key");
DEFINE_int32(batch, 1, "Pipelined Operations");

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_exptime < 0) {
        FLAGS_exptime = 0;
    }

    // A Channel represents a communication line to a Server. Notice that 
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;
    
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_MONGO;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;

    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // Pipeline #batch * #thread_num SET requests into memcache so that we
    // have keys to get.
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;

    int32_t numberToReturn = 1; // Return all matching documents
    request.set_full_collection_name("myDatabase.$cmd");
    request.set_number_to_return(numberToReturn);
    bsoncxx::builder::basic::document document{};
    document.append(bsoncxx::builder::basic::kvp("isMaster", 1));
    request.set_message(butil::SerilizeBsonDocView(document));
    request.mutable_header()->set_op_code(brpc::policy::DB_QUERY);
    auto request_code = butil::GetRandomSlavePreferredRequestCode();
    cntl.set_request_code(request_code);
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }

    butil::DeSerilizeBsonDocView(response.message());
    request.set_full_collection_name("myDatabase.test");
    cntl.Reset();
    response.Clear();
    document.clear();
    request.set_message(butil::SerilizeBsonDocView(document));
    request.set_number_to_return(3);
    cntl.set_request_code(request_code);
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }
    butil::DeSerilizeBsonDocView(response.message());

//get more
    request.mutable_header()->set_op_code(brpc::policy::DB_GETMORE);
    request.set_cursor_id(response.cursor_id());
    cntl.Reset();
    cntl.set_request_code(request_code);
    response.Clear();
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }

    butil::DeSerilizeBsonDocView(response.message());

    LOG(INFO) << "memcache_client is going to quit";
    if (options.auth) {
        delete options.auth;
    }

    return 0;
}
