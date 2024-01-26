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

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/string_printf.h>
#include <brpc/channel.h>
#include <brpc/memcache.h>
#include <bvar/bvar.h>
#include <brpc/policy/couchbase_authenticator.h>
#include "bvar/multi_dimension.h"
#include "bvar/mico_bvar.h"

DEFINE_int32(thread_num, 10, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_bool(use_couchbase, false, "Use couchbase.");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:11211", "IP Address of server");
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

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");
butil::static_atomic<int> g_sender_count = BUTIL_STATIC_ATOMIC_INIT(0);

INIT_LATENCY_RECORDER(bthread_get, "cmd");
INIT_COUNT_RECORDER(bthread_count, "cmd");
int main(int argc, char* argv[]) {
    // butil::Timer t;
    // std::vector<std::string> vec;
    // for (int i = 0; i < 1000; i++) {
    //     vec.push_back("0x17" + std::to_string(i));
    // }
    // t.start();
    // for (int i = 0; i < 10000; i++) {
    //     for (const auto &key : vec) {
    //         GET_LATENCY_RECORDER(bthread_get, key) << i;
    //     }
    // }
    // t.stop();
    start_stat_bvar("localhost:50033");
    for (int i = 0; i < 100; i++)
    {
        SCOPED_LATENCY_RECORDER(bthread_get, "123");
        bthread_usleep(2000 * i);
        GET_COUNT_RECORDER(bthread_count, "123") << i * i * 1234;
    }
    LOG(INFO) << GET_LATENCY_RECORDER(bthread_get, "123");
    bthread_usleep(1000 * 1000 * 7);
    return 0;
}
