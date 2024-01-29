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

#define INIT_LATENCY_RECORDER(name, bvar_name, ...) \
    static thread_local butil::FlatMap<std::string, bvar::LatencyRecorder*> tls_##name##_latency_recorder; \
    static butil::FlatMap<std::string, bvar::LatencyRecorder*> name##_latency_recorder; \
    static std::mutex name##latency_recorder_mt; \
    static bvar::MultiDimension<bvar::LatencyRecorder> name##latency_recorder_bvar(bvar_name, {__VA_ARGS__}); \
    class name##_latency_recorder##InitHelper { \
    public:    \
        name##_latency_recorder##InitHelper() {    \
            tls_##name##_latency_recorder.init(512); \
            name##_latency_recorder.init(512); \
        } \
    };  \
    name##_latency_recorder##InitHelper name##_latency_recorder##initHelper;

#define GET_LATENCY_RECORDER(name, hash_key, ...) \
    [&]() -> bvar::LatencyRecorder& { \
        auto* valptr = tls_##name##_latency_recorder.seek(hash_key); \
        if (valptr != nullptr) { \
            return *(*valptr); \
        }  \
        std::lock_guard<std::mutex> lock(name##latency_recorder_mt); \
        valptr = name##_latency_recorder.seek(hash_key); \
        if (valptr == nullptr) { \
            std::list<std::string> label_values = {__VA_ARGS__}; \
            name##_latency_recorder.insert(hash_key, name##latency_recorder_bvar.get_stats(label_values)); \
        } \
        return *name##_latency_recorder[hash_key]; \
    }()


#define INIT_COUNT_RECORDER(name, bvar_name, ...) \
    static thread_local butil::FlatMap<std::string, bvar::CountRecorder*> tls_##name##_count_recorder; \
    static butil::FlatMap<std::string, bvar::CountRecorder*> name##_count_recorder; \
    static std::mutex name##count_recorder_mt; \
    static bvar::MultiDimension<bvar::CountRecorder> name##count_recorder_bvar(bvar_name, {__VA_ARGS__}); \
    class name##_count_recorder##InitHelper { \
    public:    \
        name##_count_recorder##InitHelper() {    \
            tls_##name##_count_recorder.init(512); \
            name##_count_recorder.init(512); \
        } \
    };  \
    name##_count_recorder##InitHelper name##_count_recorder##initHelper;

#define GET_COUNT_RECORDER(name, hash_key, ...) \
    [&]() -> bvar::CountRecorder& { \
        auto* valptr = tls_##name##_count_recorder.seek(hash_key); \
        if (valptr != nullptr) { \
            return *(*valptr); \
        }  \
        std::lock_guard<std::mutex> lock(name##count_recorder_mt); \
        valptr = name##_count_recorder.seek(hash_key); \
        if (valptr == nullptr) { \
            std::list<std::string> label_values = {__VA_ARGS__}; \
            name##_count_recorder.insert(hash_key, name##count_recorder_bvar.get_stats(label_values)); \
        } \
        return *name##_count_recorder[hash_key]; \
    }()


INIT_LATENCY_RECORDER(get, "mnpq", "key", "hello");
INIT_COUNT_RECORDER(get, "mnpq1", "key", "hello");
int main(int argc, char* argv[]) {
    GET_LATENCY_RECORDER(get, "1234", "ivy", "jvj") << 11;
    GET_COUNT_RECORDER(get, "1234", "ivy", "jvj") << 12;
    std::vector<std::string> exposed_vars;
    bvar::MVariable::list_exposed(&exposed_vars);
    for (const auto& ele : exposed_vars) {
        std::cout << ele << std::endl;
    }
    bthread_usleep(100000000);
    return 0;
}
