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
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;

    request.set_full_collection_name(host + ".$cmd");
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

int MongoNamingService::GetServers(const char *uri, std::vector<ServerNode> *servers) {
    servers->clear();
    butil::MongoDBUri mongo_uri = butil::parse_mongo_uri(uri);
    std::vector<std::string> hosts;
    hosts.swap(mongo_uri.hosts);
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
        // bsoncxx::document::view_or_value view = bsoncxx::from_json(is_master_msg);
        // bsoncxx::document::element ismaster = view["ismaster"];
        // if (ismaster.type() == bsoncxx::type::k_bool && ismaster.get_bool().value) {
        //     servers->emplace_back(host, "master");
        // }
        // bsoncxx::document::element hosts_element = view["hosts"];
        const uint8_t* data = reinterpret_cast<const uint8_t*>(is_master_msg.c_str());
        uint32_t doc_length = *reinterpret_cast<const uint32_t*>(data);
        if (doc_length != is_master_msg.size()) {
            LOG(ERROR) << "Invalid BSON message length: " << doc_length;
            continue;
        }
        // 创建 BSON 视图
        bsoncxx::document::view view(data, doc_length);
        auto v = view["ismaster"];
        if (v.type() == bsoncxx::type::k_bool && v.get_bool().value) {
            servers->emplace_back(host, "master");
        } else {
            servers->emplace_back(host, "slave");
        }
        auto it = view.find("hosts");
        if (it != view.end()) {
            auto hosts_arr = it->get_array().value;
            for (auto host : hosts_arr) {
                hosts.emplace_back(host.get_string().value.to_string());
            }
        }
    }


    // const auto& reply = response.reply(0);
    // for (int i = 0; i < reply.size(); i++) {
    //     const auto& slot_start = reply[i][0];
    //     const auto& slot_end = reply[i][1];
    //     const std::string tag = std::to_string(slot_start.integer()) + "-" + std::to_string(slot_end.integer());

    //     const auto& ip = reply[i][2][0];
    //     const auto& port = reply[i][2][1];
    //     butil::EndPoint point;
    //     if (butil::str2endpoint(ip.c_str(), port.integer(), &point) != 0 &&
    //         butil::hostname2endpoint(ip.c_str(), port.integer(), &point) != 0) {
    //         LOG(ERROR) << "Invalid address=`" << ip.c_str() << ":" << port.integer() << '\'';
    //         continue;
    //     }
    //     servers->emplace_back(point, tag);
    // }
    return 0;
}

void MongoNamingService::Describe(std::ostream &os, const DescribeOptions &) const {
    os << "redis_cluster";
    return;
}

NamingService *MongoNamingService::New() const { return new MongoNamingService; }

void MongoNamingService::Destroy() { delete this; }

} // namespace policy
} // namespace brpc
