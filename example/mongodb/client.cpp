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
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
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

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");
butil::static_atomic<int> g_sender_count = BUTIL_STATIC_ATOMIC_INIT(0);

void parse_continuous_bson_data(const uint8_t* data, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        // 假设文档长度存储在前四个字节
        uint32_t doc_length = *reinterpret_cast<const uint32_t*>(data + offset);

        // 创建 BSON 视图
        bsoncxx::document::view view(data + offset, doc_length);

        auto it = view.find("payload");
        if (it != view.end()) {
            // 如果找到了 payload 字段，将其转换为字符串
            bsoncxx::types::b_binary payload = it->get_binary();
            std::string payload_str(reinterpret_cast<const char*>(payload.bytes), payload.size);
            std::cout << "payload: " << payload_str << std::endl;
        }

        // 将 BSON 转换为 JSON 并输出
        std::cout << bsoncxx::to_json(view) << std::endl;
        
        // 移动偏移量到下一个文档的起始位置
        offset += doc_length;
    }
}

static // 生成客户端随机数的函数
void generate_client_nonce(char *nonce, size_t size) {
    const char *chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < size; i++) {
        int index = rand() % (strlen(chars));
        nonce[i] = chars[index];
    }
    nonce[size] = '\0';  // 字符串结束符
}

int GenerateCredential(std::string* auth_str) {
    // butil::IOBuf buf;
    // if (!passwd_.empty()) {
    //     brpc::RedisCommandFormat(&buf, "AUTH %s", passwd_.c_str());
    // }
    // if (db_ >= 0) {
    //     brpc::RedisCommandFormat(&buf, "SELECT %d", db_);
    // }
    // *auth_str = buf.to_string();
    char client_nonce[24];
    generate_client_nonce(client_nonce, 23);  // 生成一个长度为 23 的随机数
    char first_message[128];
    snprintf(first_message, sizeof(first_message), "n,,n=myUser,r=%s", client_nonce);

    bsoncxx::builder::basic::document command;
    command.append(bsoncxx::builder::basic::kvp("saslStart", 1));
    command.append(bsoncxx::builder::basic::kvp("mechanism", "SCRAM-SHA-1"));
    command.append(bsoncxx::builder::basic::kvp("payload", bsoncxx::types::b_binary{
        bsoncxx::binary_sub_type::k_binary,
        (uint32_t)strlen(first_message),
        reinterpret_cast<const uint8_t*>(first_message)
    }));
    command.append(bsoncxx::builder::basic::kvp("autoAuthorize", 1));

    // 将 BSON 文档转换为 bson_t*
    bsoncxx::document::view_or_value view = command.view();
    // char fullnName[256];
    // snprintf(fullnName, sizeof(fullnName), "%s.%s", "myDatabase", "$cmd");
    char fullCollectionName[] = "myDatabase.$cmd"; // Ensure null-terminated string

    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
        brpc::Channel channel;
    
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_MONGO;

    if (channel.Init("0.0.0.0:7017", "", &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    // char fullCollectionName[256];
    // snprintf(fullnName, sizeof(fullnName), "%s.%s", "myDatabase", "$cmd");
    int32_t fullCollectionNameLen = strlen(fullCollectionName) + 1;
    int32_t flags = 0; // No special options
    int32_t numberToSkip = 0;
    int32_t numberToReturn = 1; // Return all matching documents
    request.set_full_collection_name(fullCollectionName, fullCollectionNameLen);
    request.set_number_to_return(numberToReturn);
    // bsoncxx::builder::stream::document document{};
    auto v = command.view();
    request.set_message((char*)v.data(), v.length());
    request.mutable_header()->set_op_code(brpc::policy::DB_QUERY);
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }

    parse_continuous_bson_data((const uint8_t*)response.message().c_str(), response.message().length());

    return 0;
}

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

    char fullCollectionName[] = "myDatabase.test"; // Ensure null-terminated string
    int32_t flags = 0; // No special options
    int32_t numberToSkip = 0;
    int32_t numberToReturn = 11; // Return all matching documents
    request.set_full_collection_name(fullCollectionName, sizeof(fullCollectionName));
    request.set_number_to_return(numberToReturn);
    bsoncxx::builder::stream::document document{};
    auto v = document.view();
    request.set_message((char*)v.data(), v.length());
    request.mutable_header()->set_op_code(brpc::policy::DB_QUERY);
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }

    parse_continuous_bson_data((const uint8_t*)response.message().c_str(), response.message().length());
    request.set_cursor_id(response.cursor_id());
    request.mutable_header()->set_op_code(brpc::policy::DB_GETMORE);
    request.set_number_to_return(7);

    cntl.Reset();
    response.Clear();
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }

    parse_continuous_bson_data((const uint8_t*)response.message().c_str(), response.message().length());
    GenerateCredential(NULL);

    LOG(INFO) << "memcache_client is going to quit";
    if (options.auth) {
        delete options.auth;
    }

    return 0;
}
