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
char client_nonce[24];
char encoded_nonce[1024];
int encoded_nonce_len;
char first_payload[4096] = {0};
uint32_t first_payload_len = 0;
int conv_id;
uint8_t salted_password[32];
char authmsg[1024] = {0};
uint32_t auth_messagelen = 0;
uint32_t auth_max = 1024;
char output_v[4096] = {0};
int step = 0;

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
            std::cout << "payload: " << payload_str << std::endl;
            if (i == 0) {
                sscanf(payload_str.c_str(), "r=%[^,],s=%[^,],i=%d", r, s, &i);
                memcpy(first_payload, payload_str.c_str(), payload_str.size());
                first_payload_len = payload_str.size();
                conv_id = view["conversationId"].get_int32();
            }
            if (output_v[0] == 0 && step > 3) {
                memcpy(output_v, payload_str.c_str() + 2, payload_str.size() - 2);
                //打印output_v
                printf("output_v = %s\n", output_v);
            }
        }

        // 将 BSON 转换为 JSON 并输出
        std::cout << bsoncxx::to_json(view) << std::endl;
        
        // 移动偏移量到下一个文档的起始位置
        offset += doc_length;
    }
}

int base64_decode(const char *base64_input, char *outbuf, size_t outbuf_size) {
    BIO *b64, *bio;
    int total = 0, inlen;  // total 用来记录总共读取的字节数

    // 创建一个用于 Base64 解码的 BIO
    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // 设置不使用换行符

    // 创建一个内存 BIO，从 base64_input 读取数据
    bio = BIO_new_mem_buf((void*)base64_input, -1);  // -1 表示自动计算长度
    bio = BIO_push(b64, bio);

    // 循环读取数据进行解码
    while ((inlen = BIO_read(bio, outbuf + total, outbuf_size - total)) > 0) {
        total += inlen;
        if (total >= outbuf_size) break;  // 防止缓冲区溢出
    }

    // 确保输出是 null-terminated，防止溢出
    if (total < outbuf_size) {
        outbuf[total] = '\0';
    } else {
        outbuf[outbuf_size - 1] = '\0';
    }

    // 清理 BIO
    BIO_free_all(bio);

    // 返回实际写入的字节数，不包括最后的 null-terminator
    return total;
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

static bool
scram_buf_write (const char *src, int32_t src_len, uint8_t *outbuf, uint32_t outbufmax, uint32_t *outbuflen)
{
   if (src_len < 0) {
      src_len = (int32_t) strlen (src);
   }

   if (*outbuflen + src_len >= outbufmax) {
      return false;
   }

   memcpy (outbuf + *outbuflen, src, src_len);

   *outbuflen += src_len;

   return true;
}

/* Compute the SCRAM step Hi() as defined in RFC5802 */
static void
scram_salt_password (uint8_t *output,
                             const char *password,
                             uint32_t password_len,
                             const uint8_t *salt,
                             uint32_t salt_len,
                             uint32_t iterations)
{
   uint8_t intermediate_digest[32];
   uint8_t start_key[32];

   memcpy (start_key, salt, salt_len);

   start_key[salt_len] = 0;
   start_key[salt_len + 1] = 0;
   start_key[salt_len + 2] = 0;
   start_key[salt_len + 3] = 1;

//    mongoc_crypto_hmac (&scram->crypto, password, password_len, start_key, 20, output);
   HMAC (EVP_sha1 (), password, password_len, start_key, 20, output, NULL);

   memcpy (intermediate_digest, output, 20);

   /* intermediateDigest contains Ui and output contains the accumulated XOR:ed
    * result */
   for (uint32_t i = 2u; i <= iterations; i++) {
        const int hash_size = 20;

    //   mongoc_crypto_hmac (&scram->crypto, password, password_len, intermediate_digest, hash_size, intermediate_digest);
        HMAC (EVP_sha1 (), password, password_len, intermediate_digest, hash_size, intermediate_digest, NULL);

        for (int k = 0; k < hash_size; k++) {
            output[k] ^= intermediate_digest[k];
        }
   }
}

bool
crypto_openssl_sha1 (
                            const unsigned char *input,
                            const size_t input_len,
                            unsigned char *hash_out)
{
   EVP_MD_CTX *digest_ctxp = EVP_MD_CTX_new ();
   bool rval = false;


   if (1 != EVP_DigestInit_ex (digest_ctxp, EVP_sha1 (), NULL)) {
      goto cleanup;
   }

   if (1 != EVP_DigestUpdate (digest_ctxp, input, input_len)) {
      goto cleanup;
   }

   rval = (1 == EVP_DigestFinal_ex (digest_ctxp, hash_out, NULL));

cleanup:
   EVP_MD_CTX_free (digest_ctxp);

   return rval;
}

int base64_encode(const char* input, char* output, int input_length);

int GenerateCredential1(std::string* auth_str) {
    char tmp[] = "myUser:mongo:password123";
    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)tmp, strlen(tmp), result);

    char hexOutput[(MD5_DIGEST_LENGTH * 2) + 1];
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&hexOutput[i * 2], "%02x", result[i]);
    }
    hexOutput[MD5_DIGEST_LENGTH * 2] = '\0';  // 确保字符串以NULL结尾
    char *hashed_password = NULL;
    hashed_password = hexOutput;
    printf("MD5 digest: %s\n", hexOutput);
    uint8_t outbuf[4096] = {0};
    int outbufmax = 4096;
    uint32_t outbuflen = 0;
    const char* user_name = "myUser";
    scram_buf_write ("n,,n=", -1, outbuf, outbufmax, &outbuflen);
    scram_buf_write (user_name, strlen(user_name), outbuf, outbufmax, &outbuflen);
    scram_buf_write (",r=", -1, outbuf, outbufmax, &outbuflen);
    scram_buf_write (encoded_nonce, strlen(encoded_nonce), outbuf, outbufmax, &outbuflen);

    printf("outbuf = %s\n", outbuf);
    scram_buf_write (
          (char *) outbuf + 3, outbuflen - 3, (uint8_t*)authmsg, auth_max, &auth_messagelen);
    scram_buf_write (",", -1, (uint8_t*)authmsg, auth_max, &auth_messagelen);
    printf("authmsg = %s\n", authmsg);
    scram_buf_write (first_payload, first_payload_len, (uint8_t*)authmsg, auth_max, &auth_messagelen);
    scram_buf_write (",", -1, (uint8_t*)authmsg, auth_max, &auth_messagelen);
    outbuflen = 0;
    memset(outbuf, 0, outbufmax);
    scram_buf_write ("c=biws,r=", -1, outbuf, outbufmax, &outbuflen);
    scram_buf_write ((char *) r, strlen(r), outbuf, outbufmax, &outbuflen);
    printf("second outbuf = %s\n", outbuf);
    scram_buf_write ((const char*)outbuf, outbuflen, (uint8_t*)authmsg, auth_max, &auth_messagelen);
    printf("second authmsg = %s    len is: %d\n", authmsg, auth_messagelen);
    scram_buf_write (",p=", -1, outbuf, outbufmax, &outbuflen);
    char decoded_salt[1024];
    int decoded_salt_len = base64_decode(s, decoded_salt, sizeof(decoded_salt));
    // print_hex((const char *) decoded_salt);
    scram_salt_password (salted_password, hashed_password, strlen(hashed_password), (uint8_t *) decoded_salt, decoded_salt_len, i);

    //generate proof
   uint8_t stored_key[32];
   uint8_t client_signature[32];
   unsigned char client_proof[32];
   uint8_t client_key[32];

   int rr = 0;

      /* ClientKey := HMAC(saltedPassword, "Client Key") */
      //    HMAC (EVP_sha1 (), password, password_len, salt, salt_len, output, NULL);
    uint32_t key_len;
    HMAC (EVP_sha1 (),
                          salted_password,
                          20,
                          (uint8_t *) MONGOC_SCRAM_CLIENT_KEY,
                          (int) strlen (MONGOC_SCRAM_CLIENT_KEY),
                          client_key, &key_len);

    /* StoredKey := H(client_key) */
    crypto_openssl_sha1 (client_key, (size_t) 20, stored_key);

    /* ClientSignature := HMAC(StoredKey, AuthMessage) */
    HMAC (EVP_sha1 (),
                        stored_key,
                        20,
                        (uint8_t*)authmsg,
                        auth_messagelen,
                        client_signature, &key_len);

    /* ClientProof := ClientKey XOR ClientSignature */

    for (i = 0; i < 20; i++) {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }
    rr = base64_encode ((const char*)client_proof, (char *) outbuf + outbuflen, 20);
    if (-1 == rr) {
        return false;
    }

    outbuflen += rr;

    printf("outbuf = %s\n", outbuf);
    printf("outbuf len is: %d\n", outbuflen);
    printf ("rr = %d\n", rr);

        bsoncxx::builder::stream::document builder{};

        // Append the command fields
        builder << "saslContinue" << 1
                << "conversationId" << conv_id
                << "payload" << bsoncxx::types::b_binary{bsoncxx::binary_sub_type::k_binary, outbuflen, outbuf};
        auto v = builder.view();
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

int base64_encode(const char* input, char* output, int input_length) {
    BIO *b64, *bio;
    BUF_MEM *bufferPtr;

    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // Do not use newlines to flush buffer
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);

    BIO_write(bio, input, input_length);
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);

    memcpy(output, bufferPtr->data, bufferPtr->length);
    output[bufferPtr->length] = '\0'; // Null-terminate!

    // BIO_free_all(bio); // Also frees BUF_MEM bufferPtr
    return bufferPtr->length;
}

int GenerateCredential(std::string* auth_str) {
    char client_nonce[24];
    generate_client_nonce(client_nonce, 23);  // 生成一个长度为 23 的随机数
    encoded_nonce_len = base64_encode(client_nonce, encoded_nonce, sizeof(client_nonce));
    char first_message[128];
    snprintf(first_message, sizeof(first_message), "n,,n=myUser,r=%s", encoded_nonce);

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

int VerifyServerSign() {

    char encoded_server_signature[64];
    int32_t encoded_server_signature_len;
    uint8_t server_signature[32];
    uint8_t server_key[32];
    const size_t key_len = strlen (MONGOC_SCRAM_SERVER_KEY);
    uint32_t out_len;
    HMAC (EVP_sha1 (),
                          salted_password,
                          20,
                          (uint8_t *) MONGOC_SCRAM_SERVER_KEY,
                          (int) key_len,
                          server_key, &out_len);
    //authmsg hmac
    HMAC (EVP_sha1 (),
                        (const unsigned char*)server_key,
                        20,
                        (const unsigned char*)authmsg,
                        auth_messagelen,
                        server_signature, &out_len);
    //base64 endcode server_signature
    encoded_server_signature_len = base64_encode ((const char*)server_signature, encoded_server_signature, 20);
    printf ("encoded_server_signature = %s\n", encoded_server_signature);
    //compare encoded_server_signature and output_v, need care length
    if (strncmp (encoded_server_signature, output_v, encoded_server_signature_len) != 0) {
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
    GenerateCredential1(NULL);
    VerifyServerSign();

    LOG(INFO) << "memcache_client is going to quit";
    if (options.auth) {
        delete options.auth;
    }

    return 0;
}
