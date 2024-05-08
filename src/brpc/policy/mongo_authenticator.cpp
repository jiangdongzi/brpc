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

#include "brpc/policy/mongo_authenticator.h"

#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/hmac.h>
#include <openssl/buffer.h>
#include "butil/base64.h"
#include "butil/iobuf.h"
#include "butil/string_printf.h"
#include "butil/sys_byteorder.h"
#include "brpc/redis_command.h"
#include "butil/base64.h"
#include "butil/sha1.h"
#include "butil/fast_rand.h"
#include "brpc/policy/mongo.pb.h"
#include <brpc/channel.h>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>

namespace brpc {
namespace policy {

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

#define MONGOC_SCRAM_SERVER_KEY "Server Key"
#define MONGOC_SCRAM_CLIENT_KEY "Client Key"

static bsoncxx::document::view GetView(const uint8_t* data, size_t length) {
    // 文档长度存储在前四个字节
    uint32_t doc_length = *reinterpret_cast<const uint32_t*>(data);
    bsoncxx::document::view view(data, doc_length);
    LOG(INFO) << bsoncxx::to_json(view);
    return view;
}

static std::string GetPayload(const uint8_t* data, size_t length) {
    bsoncxx::document::view view = GetView(data, length);
    auto v = view["payload"].get_binary();
    std::string ret((const char*)v.bytes, v.size);
    return ret;
}

bool IsDone(const uint8_t* data, size_t length) {
    bsoncxx::document::view view = GetView(data, length);
    return view["done"].get_bool().value;
}

int GetConversationId (const uint8_t* data, size_t length) {
    bsoncxx::document::view view = GetView(data, length);
    return view["conversationId"].get_int32().value;
}

int MongoAuthenticator::GenerateCredential(std::string* auth_str) const {
    //first step
    char r[256], s[256];
    int i;
    std::string encoded_nonce;
    std::string first_payload_str;
    int conv_id;
    std::string authmsg;
    std::string output_v_str;
    std::string salted_password_str;

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
    first_payload_str = GetPayload((const uint8_t*)response.message().c_str(), response.message().size());
    sscanf(first_payload_str.c_str(), "r=%[^,],s=%[^,],i=%d", r, s, &i);
    LOG(INFO) << "r: " << r << ", s: " << s << ", i: " << i;
    conv_id = GetConversationId((const uint8_t*)response.message().c_str(), response.message().size());

    //second step
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
    v = builder.view();
    request.set_message((char*)v.data(), v.length());
    response.Clear();
    cntl.Reset();
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return -1;
    }
    const std::string second_payload_str = GetPayload((const uint8_t*)response.message().c_str(), response.message().size());
    LOG(INFO) << "second_payload_str: " << second_payload_str;

    return 0;
}

}  // namespace policy
}  // namespace brpc
