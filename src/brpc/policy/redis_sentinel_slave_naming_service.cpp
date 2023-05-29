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

#include "brpc/policy/redis_sentinel_slave_naming_service.h"
#include <brpc/policy/redis_authenticator.h>
#include "brpc/log.h"
#include "bthread/bthread.h"
#include "butil/strings/string_split.h"
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/redis.h>
#include <netdb.h>  // gethostbyname_r
#include <stdlib.h> // strtol
#include <string>   // std::string

namespace brpc {
namespace policy {

int RedisSentinelSlaveNamingService::GetServers(const char *service_and_token, std::vector<ServerNode> *servers) {
    servers->clear();

    std::vector<std::string> out;
    butil::SplitStringUsingSubstr(service_and_token, "\r\n", &out);

    if (out.size() != 3 || out[0].empty() || out[1].empty()) { //out[0] : url,  out[1] : master_name, out[2] : token
        LOG(ERROR) << "please check service_and_token";
        return -1;
    }

    const auto& service_name = out[0];
    const auto& master_name = out[1];
    const auto& token = out[2];

    brpc::Channel channel;
    
    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_REDIS;
    options.timeout_ms = 1000;
    options.max_retry = 3;
    if (!token.empty()) {
        brpc::policy::RedisAuthenticator* auth = new brpc::policy::RedisAuthenticator(token);
        options.auth = auth;
    }
    if (channel.Init(service_name.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    brpc::RedisRequest request;
    if (!request.AddCommand("SENTINEL SLAVES %s", master_name.c_str())) {
        LOG(ERROR) << "Fail to add command";
        return -1;
    }

    brpc::RedisResponse response;
    brpc::Controller cntl;
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access redis, " << cntl.ErrorText();
        return -1;
    }
    const auto& reply = response.reply(0);
    for (int i = 0; i < reply.size(); i++) {
        const auto& local_reply = reply[i];
        std::string ip, port, flags;
        for (int j = 0; j < local_reply.size(); j += 2) {
            if (local_reply[j].data() == "ip") {
                ip = local_reply[j + 1].c_str();
            } else if (local_reply[j].data() == "port") {
                port = local_reply[j + 1].c_str();
            } else if (local_reply[j].data() == "flags") {
                flags = local_reply[j + 1].c_str();
            }
        }

        if (flags != "slave") {
            continue;
        }

        const std::string str = ip + ":" + port;

        butil::EndPoint point;
        if (butil::str2endpoint(str.data(), &point) != 0 &&
            butil::hostname2endpoint(str.data(), &point) != 0) {
            LOG(ERROR) << "Invalid address=`" << ip.c_str() << ":" << port << '\'';
            continue;
        }
        servers->emplace_back(point);
    }
    return 0;
}

void RedisSentinelSlaveNamingService::Describe(std::ostream &os, const DescribeOptions &) const {
    os << "redis_sentinel_slave";
    return;
}

NamingService *RedisSentinelSlaveNamingService::New() const { return new RedisSentinelSlaveNamingService; }

void RedisSentinelSlaveNamingService::Destroy() { delete this; }

} // namespace policy
} // namespace brpc
