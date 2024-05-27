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

#include "brpc/policy/redis_sentinel_master_naming_service.h"
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
#include <regex>

namespace brpc {
namespace policy {


static std::string extractSlaveAddresses(const std::string& replicationInfo) {
    std::regex regex("slave\\d+:ip=(.*?),port=(.*?),");
    std::smatch matches;

    auto start = replicationInfo.cbegin();
    auto end = replicationInfo.cend();
    while (std::regex_search(start, end, matches, regex)) {
        std::string ip = matches[1].str();
        std::string port = matches[2].str();
        return ip + ":" + port;
    }

    return "";
}

int RedisSentinelMasterNamingService::GetServers(const char *service_and_token, std::vector<ServerNode> *servers) {
    servers->clear();

    std::vector<std::string> out;
    butil::SplitStringUsingSubstr(service_and_token, "\r\n", &out);

    if (out.size() != 2 || out[0].empty() || out[1].empty()) {
        LOG(ERROR) << "please check service_and_token";
        return -1;
    }

    const auto& service_name = out[0];
    const auto& token = out[1];

    butil::EndPoint point;
    str2endpoint(service_name.c_str(), &point);
    servers->emplace_back(point, "master");

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
    if (!request.AddCommand("INFO REPLICATION")) {
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
    for (size_t i = 0; i < reply.size(); i++) {
        const auto host = extractSlaveAddresses(reply[i].c_str());
        LOG(INFO) << "i: " << i << ", " <<   reply[i] << ", host: " << host;
        if (host.empty()) {
            continue;
        }
        butil::EndPoint point;
        str2endpoint(host.c_str(), &point);
        servers->emplace_back(point, "slave");
    }
    return 0;
}

void RedisSentinelMasterNamingService::Describe(std::ostream &os, const DescribeOptions &) const {
    os << "redis_sentinel_slave";
    return;
}

NamingService *RedisSentinelMasterNamingService::New() const { return new RedisSentinelMasterNamingService; }

void RedisSentinelMasterNamingService::Destroy() { delete this; }

} // namespace policy
} // namespace brpc
