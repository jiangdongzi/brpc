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

#pragma once

#include <vector>                                      // std::vector
#include "butil/containers/doubly_buffered_data.h"
#include "brpc/load_balancer.h"
#include "brpc/cluster_recover_policy.h"

namespace brpc {
namespace policy {

class MasterSlaveLoadBalancer : public LoadBalancer {
public:
    bool AddServer(const ServerId& id);
    bool RemoveServer(const ServerId& id);
    size_t AddServersInBatch(const std::vector<ServerId>& servers);
    size_t RemoveServersInBatch(const std::vector<ServerId>& servers);
    int SelectServer(const SelectIn& in, SelectOut* out);
    MasterSlaveLoadBalancer* New(const butil::StringPiece&) const;
    void Destroy();
    void Describe(std::ostream& os, const DescribeOptions&);

private:
    struct Servers {
        std::vector<ServerId> master_server_list;
        std::vector<ServerId> slave_server_list;
    };
    static bool Add(Servers& bg, const ServerId& id);
    static bool Remove(Servers& bg, const ServerId& id);
    static size_t BatchAdd(Servers& bg, const std::vector<ServerId>& servers);
    static size_t BatchRemove(Servers& bg, const std::vector<ServerId>& servers);
    static int SelectServerFromList (const std::vector<ServerId>& server_list, const SelectIn& in, SelectOut* out);

    butil::DoublyBufferedData<Servers> _db_servers;
};

}  // namespace policy
} // namespace brpc