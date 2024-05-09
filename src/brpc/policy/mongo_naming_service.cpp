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

#include "brpc/policy/mongo_naming_service.h"
#include <brpc/policy/redis_authenticator.h>
#include "brpc/log.h"
#include "brpc/policy/mongo.pb.h"
#include "brpc/policy/mongo_authenticator.h"
#include "bthread/bthread.h"
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/redis.h>
#include "butil/mongo_utils.h"
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <memory>
#include <netdb.h>  // gethostbyname_r
#include <queue>
#include <stdlib.h> // strtol
#include <string>   // std::string
#include <unordered_set>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

namespace brpc {
namespace policy {

MongoNamingService::MongoNamingService() = default;

static std::string GetIsMasterMsg (const std::string mongo_uri_str, const std::string& host) {
    brpc::Channel channel;
    const butil::MongoDBUri mongo_uri = butil::parse_mongo_uri(mongo_uri_str);
    
    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;

    options.protocol = brpc::PROTOCOL_MONGO;
    options.timeout_ms = 1000;
    options.max_retry = 3;
    if (mongo_uri.need_auth()) {
        MongoAuthenticator* auth = new MongoAuthenticator(mongo_uri_str);
        options.auth = auth;
    }
    std::unique_ptr<const Authenticator> auth_guard(options.auth);
    if (channel.Init(host.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return "";
    }
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;

    request.set_full_collection_name(mongo_uri.database + ".$cmd");
    request.set_number_to_return(1);
    bsoncxx::builder::basic::document document{};
    document.append(bsoncxx::builder::basic::kvp("isMaster", 1));
    auto v = document.view();
    request.set_message((char*)v.data(), v.length());
    request.mutable_header()->set_op_code(brpc::policy::DB_QUERY);
    channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access memcache, " << cntl.ErrorText();
        return "";
    }
    LOG(INFO) << "response_flags: " << response.response_flags();
    return response.message();
}

std::vector<std::string> ResolveHostsToIPPort(const std::vector<std::string>& hosts) {
    std::vector<std::string> results;
    struct addrinfo hints, *res, *p;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; // AF_INET or AF_INET6
    hints.ai_socktype = SOCK_STREAM;

    for (const auto& host_port : hosts) {
        size_t colonPos = host_port.find(':');
        if (colonPos == std::string::npos) {
            std::cerr << "Invalid host:port format" << std::endl;
            continue; // Skip this entry if format is incorrect
        }

        std::string host = host_port.substr(0, colonPos);
        std::string port = host_port.substr(colonPos + 1);

        if ((status = getaddrinfo(host.c_str(), port.c_str(), &hints, &res)) != 0) {
            std::cerr << "getaddrinfo error for " << host << " with port " << port << ": " << gai_strerror(status) << std::endl;
            continue; // Skip to the next host if resolution fails
        }

        for (p = res; p != NULL; p = p->ai_next) {
            void *addr;
            // Get the pointer to the address itself, different fields in IPv4 and IPv6:
            if (p->ai_family == AF_INET) { // IPv4
                struct sockaddr_in *ipv4 = (struct sockaddr_in *)p->ai_addr;
                addr = &(ipv4->sin_addr);
            } else { // IPv6
                struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)p->ai_addr;
                addr = &(ipv6->sin6_addr);
            }

            // Convert the IP to a string and store it in results:
            inet_ntop(p->ai_family, addr, ipstr, sizeof ipstr);
            results.push_back(std::string(ipstr) + ":" + port);
            break; // Only store the first resolved address
        }

        freeaddrinfo(res); // Free the linked list
    }

    return results;
}


int MongoNamingService::GetServers(const char *uri, std::vector<ServerNode> *servers) {
    servers->clear();
    butil::MongoDBUri mongo_uri = butil::parse_mongo_uri(uri);
    LOG(INFO) << "Mongo URI: " << uri;
    LOG(INFO) << mongo_uri.hosts[0];
    std::vector<std::string> hosts = ResolveHostsToIPPort(mongo_uri.hosts);
    std::unordered_set<std::string> visited_hosts;
    while (!hosts.empty()) {
        const std::string& host = hosts.back();
        hosts.pop_back();
        if (visited_hosts.find(host) != visited_hosts.end()) {
            continue;
        }
        visited_hosts.insert(host);
        std::string is_master_msg = GetIsMasterMsg(uri, host);
        if (is_master_msg.empty()) {
            continue;
        }
        const uint8_t* data = reinterpret_cast<const uint8_t*>(is_master_msg.c_str());
        uint32_t doc_length = *reinterpret_cast<const uint32_t*>(data);
        if (doc_length != is_master_msg.size()) {
            LOG(ERROR) << "Invalid BSON message length: " << doc_length;
            continue;
        }
        // 创建 BSON 视图
        bsoncxx::document::view view(data, doc_length);
        auto v = view["ismaster"];
        butil::EndPoint point;
        str2endpoint(host.c_str(), &point);
        if (v.type() == bsoncxx::type::k_bool && v.get_bool().value) {
            servers->emplace_back(point, "master");
        } else {
            servers->emplace_back(point, "slave");
        }
        auto it = view.find("hosts");
        if (it != view.end()) {
            auto hosts_arr = it->get_array().value;
            for (auto host : hosts_arr) {
                hosts.emplace_back(host.get_string().value.to_string());
            }
        }
    }
    return 0;
}

void MongoNamingService::Describe(std::ostream &os, const DescribeOptions &) const {
    os << "mongo_naming_service";
    return;
}

NamingService *MongoNamingService::New() const { return new MongoNamingService; }

void MongoNamingService::Destroy() { delete this; }

} // namespace policy
} // namespace brpc
