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
#include <memory>
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

static std::unique_ptr<butil::mongo::Client> GetClient() {
    LOG(INFO) << "before: " << bthread_self();
    std::unique_ptr<butil::mongo::Client> client(new butil::mongo::Client(FLAGS_server));
    return client;
}

butil::mongo::Client* Client() {
    static std::unique_ptr<butil::mongo::Client> client;
    if (client == nullptr) {
        client = GetClient();
    }
    return client.get();
}

static void* ClientTest(void*) {
    auto client = Client();
    auto col = (*client)["testdb"]["test"];
    bsoncxx::builder::basic::document doc;
    auto v = col.find(doc.view());
    for (auto&& doc : v) {
        LOG(INFO) << bsoncxx::to_json(doc);
    }
    return nullptr;
}

int main(int argc, char* argv[]) {
    LOG(INFO) << "main...";
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_exptime < 0) {
        FLAGS_exptime = 0;
    }
    std::vector<bthread_t> tids;
    for (int i = 0; i < 32; i++) {
        bthread_t tid;
        if (bthread_start_background(&tid, nullptr, ClientTest, nullptr) != 0) {
            LOG(ERROR) << "Fail to start thread " << i;
            return -1;
        }
        tids.push_back(tid);
    }
    for (size_t i = 0; i < tids.size(); i++) {
        bthread_join(tids[i], nullptr);
    }
    return 0;
}
