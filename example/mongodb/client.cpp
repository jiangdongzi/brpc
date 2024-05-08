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

#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include "butil/base64.h"
#include "butil/sha1.h"
#include "butil/fast_rand.h"

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

static char r[256], s[256];
static int i;
std::string encoded_nonce;
std::string first_payload_str;
int conv_id;
std::string authmsg;
std::string output_v_str;
int step = 0;
std::string salted_password_str;

#define MONGOC_SCRAM_SERVER_KEY "Server Key"
#define MONGOC_SCRAM_CLIENT_KEY "Client Key"

void parse_continuous_bson_data(const uint8_t* data, size_t length) {
    size_t offset = 0;
    step++;
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
            if (i == 0) {
                sscanf(payload_str.c_str(), "r=%[^,],s=%[^,],i=%d", r, s, &i);
                conv_id = view["conversationId"].get_int32();
                first_payload_str.swap(payload_str);
            }
            if (output_v_str.empty() && step > 3) {
                output_v_str = payload_str.substr(2);
            }
        }

        // 将 BSON 转换为 JSON 并输出
        std::cout << bsoncxx::to_json(view) << std::endl;
        
        // 移动偏移量到下一个文档的起始位置
        offset += doc_length;
    }
}

static std::string generate_client_nonce() {
    std::string nonce;
    nonce.resize(24);
    for (int i = 0; i < 24; i++) {
        nonce[i] = butil::fast_rand_less_than(256);
    }
    return nonce;
}

static std::string HMAC_SHA1(const std::string& key, const std::string& data) {
    unsigned int len = 0;
    unsigned char digest[20];
    HMAC(EVP_sha1(), key.c_str(), key.size(), (unsigned char*)data.c_str(), data.size(), digest, &len);
    std::string result((char*)digest, len);
    return result;
}

std::string SCRAM_salt_password(const std::string& password,
                                 const std::string& salt,
                                 int iterations) {
    std::string start_key = salt + std::string("\x00\x00\x00\x01", 4);
    std::string intermediate_digest;
    std::string output = intermediate_digest = HMAC_SHA1(password, start_key);
    for (int i = 2; i <= iterations; i++) {
        intermediate_digest = HMAC_SHA1(password, intermediate_digest);
        for (int k = 0; k < 20; k++) {
            output[k] ^= intermediate_digest[k];
        }
    }
    return output;
}

static void AppendBinary(bsoncxx::builder::basic::document& builder,
                         const std::string& key,
                         const std::string& value) {
    builder.append(bsoncxx::builder::basic::kvp(key, bsoncxx::types::b_binary{
        bsoncxx::binary_sub_type::k_binary,
        (uint32_t)value.size(),
        reinterpret_cast<const uint8_t*>(value.c_str())
    }));
}

int GenerateCredential1(std::string* auth_str) {
    char tmp[] = "myUser:mongo:password123";
    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)tmp, strlen(tmp), result);

    char hexOutput[(MD5_DIGEST_LENGTH * 2) + 1];
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&hexOutput[i * 2], "%02x", result[i]);
    }
    hexOutput[MD5_DIGEST_LENGTH * 2] = '\0';  // 确保字符串以NULL结尾
    char *hashed_password = hexOutput;
    std::string out_str;
    const char* user_name = "myUser";
    out_str = "n,,n=";
    out_str.append(user_name).append(",r=").append(encoded_nonce);
    authmsg.append(out_str.substr(3)).append(",").append(first_payload_str).append(",");
    out_str = "c=biws,r=";
    out_str.append(r);
    authmsg.append(out_str);
    out_str.append(",p=");
    std::string decoded_salt;
    butil::Base64Decode(s, &decoded_salt);
    salted_password_str = SCRAM_salt_password(hashed_password, decoded_salt, i);

    //generate proof
    std::string client_proof;
    client_proof.resize(20);

    const std::string client_key_str = HMAC_SHA1(salted_password_str, MONGOC_SCRAM_CLIENT_KEY);

    std::string stored_key_str = butil::SHA1HashString(client_key_str);
    const std::string client_signature_str = HMAC_SHA1(stored_key_str, authmsg);

    for (i = 0; i < 20; i++) {
        client_proof[i] = client_key_str[i] ^ client_signature_str[i];
    }
    std::string proof_base64;
    butil::Base64Encode(client_proof, &proof_base64);
    out_str.append(proof_base64);

    bsoncxx::builder::basic::document builder{};
    builder.append(bsoncxx::builder::basic::kvp("saslContinue", 1));
    builder.append(bsoncxx::builder::basic::kvp("conversationId", conv_id));
    AppendBinary(builder, "payload", out_str);

    auto v = builder.view();
    std::string fullCollectionName = "myDatabase.$cmd"; // Ensure null-terminated string

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

    request.set_full_collection_name(fullCollectionName);
    request.set_number_to_return(1);
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

int GenerateCredential(std::string* auth_str) {
    std::string client_nonce = generate_client_nonce();
    butil::Base64Encode(client_nonce, &encoded_nonce);
    std::string first_message = "n,,n=myUser,r=" + encoded_nonce;

    bsoncxx::builder::basic::document command;
    command.append(bsoncxx::builder::basic::kvp("saslStart", 1));
    command.append(bsoncxx::builder::basic::kvp("mechanism", "SCRAM-SHA-1"));
    AppendBinary(command, "payload", first_message);
    command.append(bsoncxx::builder::basic::kvp("autoAuthorize", 1));

    // 将 BSON 文档转换为 bson_t*
    bsoncxx::document::view_or_value view = command.view();
    std::string fullCollectionName = "myDatabase.$cmd";

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

    request.set_full_collection_name(fullCollectionName);
    request.set_number_to_return(1);
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

int LastAuthStep() {

    using namespace bsoncxx::builder::basic;

    // 创建一个BSON文档构建器
    document builder{};

    // 添加saslContinue和conversationId字段
    builder.append(kvp("saslContinue", 1));
    builder.append(kvp("conversationId", conv_id));

    // 添加一个空的payload字段
    // 注意：根据你的需要，如果payload应该是空的二进制数据，你可以如下设置：
    builder.append(kvp("payload", bsoncxx::types::b_binary{bsoncxx::binary_sub_type::k_binary, 0, nullptr}));
    auto v = builder.view();
    std::string fullCollectionName = "myDatabase.$cmd"; // Ensure null-terminated string

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

    request.set_full_collection_name(fullCollectionName);
    request.set_number_to_return(1);
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

int VerifyServerSign() {
    const std::string server_key_str = HMAC_SHA1(salted_password_str, MONGOC_SCRAM_SERVER_KEY);
    //authmsg hmac
    const std::string server_signature_str = HMAC_SHA1(server_key_str, authmsg);
    std::string encoded_server_signature_str;
    butil::Base64Encode(server_signature_str, &encoded_server_signature_str);
    //compare encoded_server_signature and output_v, need care length
    if (encoded_server_signature_str != output_v_str) {
        printf ("server signature is not equal\n");
    } else {
        printf ("server signature is equal\n");
    }


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

    int32_t flags = 0; // No special options
    int32_t numberToSkip = 0;
    int32_t numberToReturn = 11; // Return all matching documents
    request.set_full_collection_name("myDatabase.test");
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
    GenerateCredential1(NULL);
    VerifyServerSign();
    LastAuthStep();

    LOG(INFO) << "memcache_client is going to quit";
    if (options.auth) {
        delete options.auth;
    }

    return 0;
}
