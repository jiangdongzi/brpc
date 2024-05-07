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

#include "butil/base64.h"
#include "butil/iobuf.h"
#include "butil/string_printf.h"
#include "butil/sys_byteorder.h"
#include "brpc/redis_command.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/types.hpp>
#include "brpc/policy/mongo.pb.h"
#include <brpc/channel.h>
#include <bsoncxx/builder/stream/document.hpp>

namespace brpc {
namespace policy {

static // 生成客户端随机数的函数
void generate_client_nonce(char *nonce, size_t size) {
    const char *chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < size; i++) {
        int index = rand() % (strlen(chars));
        nonce[i] = chars[index];
    }
    nonce[size] = '\0';  // 字符串结束符
}


int MongoAuthenticator::GenerateCredential(std::string* auth_str) const {
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
    char fullnName[256];
    snprintf(fullnName, sizeof(fullnName), "%s.%s", "myDatabase", "$cmd");

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

    char fullCollectionName[256];
    snprintf(fullnName, sizeof(fullnName), "%s.%s", "myDatabase", "$cmd");
    int32_t fullCollectionNameLen = strlen(fullCollectionName) + 1;
    int32_t flags = 0; // No special options
    int32_t numberToSkip = 0;
    int32_t numberToReturn = 11; // Return all matching documents
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

    return 0;
}

}  // namespace policy
}  // namespace brpc
