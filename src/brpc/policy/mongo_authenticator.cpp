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
#include "brpc/controller.h"
#include "brpc/details/controller_private_accessor.h"
#include "brpc/mongo_head.h"
#include "bthread/id.h"
#include "butil/base64.h"
#include "butil/mongo_utils.h"
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

static std::string GetPayload(const std::string& data) {
    bsoncxx::document::view view = butil::mongo::GetViewFromRawBody(data);
    auto it = view.find("errmsg");
    if (it != view.end()) {
        LOG(ERROR) << "errmsg: " << it->get_string().value.to_string() << ", code" << view["code"].get_int32().value;
        return "";
    }
    auto v = view["payload"].get_binary();
    std::string ret((const char*)v.bytes, v.size);
    return ret;
}

bool IsDone(const std::string& data) {
    bsoncxx::document::view view = butil::mongo::GetViewFromRawBody(data);
    return view["done"].get_bool().value;
}

int GetConversationId (const std::string& data) {
    bsoncxx::document::view view = butil::mongo::GetViewFromRawBody(data);
    return view["conversationId"].get_int32().value;
}

static int SetControllerSendingSock(const SocketId id, Controller *cntl) {
    SocketUniquePtr tmp_sock;
    const int rc = Socket::Address(id, &tmp_sock);
    if (rc != 0) {
        LOG(ERROR) << "Fail to get address of socket=" << id << ": " << berror(rc);
        cntl->SetFailed(rc, "Fail to get address of socket");
        return rc;
    }
    cntl->SetSendingSock(tmp_sock.release());
    return 0;
}

int MongoAuthenticator::AuthSCRAMSHA1(Controller* raw_cntl) const{
    const SocketId id = raw_cntl->GetSendingSock()->id();
    //first step
    const std::string& user_name = _uri.username;
    const std::string& password = _uri.password;
    const std::string& database = _uri.database;
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
    std::string first_message = "n,,n=" + user_name + ",r=" + encoded_nonce;

    bsoncxx::builder::basic::document command;
    command.append(bsoncxx::builder::basic::kvp("saslStart", 1));
    command.append(bsoncxx::builder::basic::kvp("mechanism", "SCRAM-SHA-1"));
    AppendBinary(command, "payload", first_message);
    command.append(bsoncxx::builder::basic::kvp("autoAuthorize", 1));
    command.append(bsoncxx::builder::basic::kvp("$db", database));

    std::string sections = butil::mongo::BuildSections(command);
    butil::IOBuf buf;

    mongo_head_t header = {
        (int)(sizeof(mongo_head_t) + sizeof(uint32_t) + sections.size()),
        1,
        0,
        OP_MSG
    };
    buf.append(&header, sizeof(header));
    const int flags = 0;
    buf.append(&flags, sizeof(flags));
    buf.append(sections);
    brpc::Controller cntl;
    auto correlation_id = cntl.call_id();
    int rc = bthread_id_lock_and_reset_range(
                    correlation_id, NULL, 2);
    SetControllerSendingSock(id, &cntl);
    CallId cid = cntl.current_id();
    cntl.GetSendingSock()->set_correlation_id(cid.value);
    Socket::WriteOptions wopt;
    wopt.id_wait = cid;
    brpc::policy::MongoResponse response;
    cntl.SetResponse(&response);
    cntl.GetSendingSock()->Write(&buf, &wopt);
    bthread_id_unlock(cid);
    Join(cid);

    first_payload_str = GetPayload(response.sections());
    sscanf(first_payload_str.c_str(), "r=%[^,],s=%[^,],i=%d", r, s, &i);
    LOG(INFO) << "r: " << r << ", s: " << s << ", i: " << i;
    conv_id = GetConversationId(response.sections());

    //second step
    std::string tmp = user_name + ":mongo:" + password;
    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)tmp.c_str(), tmp.size(), result);

    char hexOutput[(MD5_DIGEST_LENGTH * 2) + 1];
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&hexOutput[i * 2], "%02x", result[i]);
    }
    hexOutput[MD5_DIGEST_LENGTH * 2] = '\0';  // 确保字符串以NULL结尾
    char *hashed_password = hexOutput;
    std::string out_str;
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
    builder.append(bsoncxx::builder::basic::kvp("$db", database));
    sections = butil::mongo::BuildSections(builder);
    response.Clear();

    header.message_length = (int)(sizeof(mongo_head_t) + sizeof(uint32_t) + sections.size());

    buf.clear();
    buf.append(&header, sizeof(header));
    buf.append(&flags, sizeof(flags));
    buf.append(sections);

    cntl.Reset();
    correlation_id = cntl.call_id();
    rc = bthread_id_lock_and_reset_range(
                    correlation_id, NULL, 2);
    SetControllerSendingSock(id, &cntl);
    cid = cntl.current_id();
    cntl.GetSendingSock()->set_correlation_id(cid.value);
    wopt.id_wait = cid;
    response.Clear();
    cntl.SetResponse(&response);
    cntl.GetSendingSock()->Write(&buf, &wopt);
    CHECK_EQ(0, bthread_id_unlock(cid));
    Join(cid);

    const std::string second_payload_str = GetPayload(response.sections());
    LOG(INFO) << "second_payload_str: " << second_payload_str;

    //verify server signature
    const std::string server_key_str = HMAC_SHA1(salted_password_str, MONGOC_SCRAM_SERVER_KEY);
    //authmsg hmac
    const std::string server_signature_str = HMAC_SHA1(server_key_str, authmsg);
    std::string encoded_server_signature_str;
    butil::Base64Encode(server_signature_str, &encoded_server_signature_str);
    const std::string out_v = second_payload_str.substr(2);
    if (out_v != encoded_server_signature_str) {
        LOG(ERROR) << "server signature verification failed";
        return -1;
    } else {
        LOG(INFO) << "server signature verification success";
    }

    //last step
    // 创建一个BSON文档构建器
    using namespace bsoncxx::builder::basic;
    builder.clear();
    // 添加saslContinue和conversationId字段
    builder.append(kvp("saslContinue", 1));
    builder.append(kvp("conversationId", conv_id));

    // 添加一个空的payload字段
    // 注意：根据你的需要，如果payload应该是空的二进制数据，你可以如下设置：
    builder.append(kvp("payload", bsoncxx::types::b_binary{bsoncxx::binary_sub_type::k_binary, 0, nullptr}));
    builder.append(bsoncxx::builder::basic::kvp("$db", database));

    sections = butil::mongo::BuildSections(builder);
    response.Clear();
    buf.clear();
    header.message_length = (int)(sizeof(mongo_head_t) + sizeof(uint32_t) + sections.size());
    buf.append(&header, sizeof(header));
    buf.append(&flags, sizeof(flags));
    buf.append(sections);
    cntl.Reset();
    cntl.SetResponse(&response);
    correlation_id = cntl.call_id();
    rc = bthread_id_lock_and_reset_range(
                    correlation_id, NULL, 2);
    SetControllerSendingSock(id, &cntl);
    cid = cntl.current_id();
    cntl.GetSendingSock()->set_correlation_id(cid.value);
    wopt.id_wait = cid;
    response.Clear();
    // accessor.get_sending_socket()->Write(&buf, &wopt);
    cntl.GetSendingSock()->Write(&buf, &wopt);
    bthread_id_unlock(cid);
    Join(cid);

    bool is_done = IsDone(response.sections());
    LOG(INFO) << "is_done: " << is_done;

    return 0;
}

}  // namespace policy
}  // namespace brpc
