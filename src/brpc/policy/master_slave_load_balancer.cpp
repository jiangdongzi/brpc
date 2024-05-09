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


#include "brpc/server_id.h"
#include "butil/macros.h"
#include "butil/fast_rand.h"
#include "brpc/socket.h"
#include "brpc/policy/master_slave_load_balancer.h"

namespace brpc {
namespace policy {

#define MASTER "master"
#define SLAVE "slave"

const uint32_t prime_offset[] = {
#include "bthread/offset_inl.list"
};

inline uint32_t GenRandomStride() {
    return prime_offset[butil::fast_rand_less_than(ARRAY_SIZE(prime_offset))];
}

static void AddSortAndUnique(std::vector<ServerId>& vec, const ServerId& id) {
    vec.push_back(id);
    std::sort(vec.begin(), vec.end());
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
}

bool MasterSlaveLoadBalancer::Add(Servers& bg, const ServerId& id) {
    if (id.tag != MASTER && id.tag != SLAVE) {
        LOG(INFO) << "Invalid tag: " << id.tag;
        return false;
    }
    if (id.tag == MASTER) {
        AddSortAndUnique(bg.master_server_list, id);
    } else {
        AddSortAndUnique(bg.slave_server_list, id);
    }
    return true;
}

bool MasterSlaveLoadBalancer::Remove(Servers& bg, const ServerId& id) {
        if (id.tag == MASTER) {
            bg.master_server_list.erase(
                std::remove(bg.master_server_list.begin(),
                            bg.master_server_list.end(), id),
                bg.master_server_list.end());
        } else {
            bg.slave_server_list.erase(
                std::remove(bg.slave_server_list.begin(),
                            bg.slave_server_list.end(), id),
                bg.slave_server_list.end());
        }
        return true;
}

size_t MasterSlaveLoadBalancer::BatchAdd(
    Servers& bg, const std::vector<ServerId>& servers) {
    size_t count = 0;
    for (size_t i = 0; i < servers.size(); ++i) {
        count += !!Add(bg, servers[i]);
    }
    return count;
}

size_t MasterSlaveLoadBalancer::BatchRemove(
    Servers& bg, const std::vector<ServerId>& servers) {
    size_t count = 0;
    for (size_t i = 0; i < servers.size(); ++i) {
        count += !!Remove(bg, servers[i]);
    }
    return count;
}

bool MasterSlaveLoadBalancer::AddServer(const ServerId& id) {
    return _db_servers.Modify(Add, id);
}

bool MasterSlaveLoadBalancer::RemoveServer(const ServerId& id) {
    return _db_servers.Modify(Remove, id);
}

size_t MasterSlaveLoadBalancer::AddServersInBatch(
    const std::vector<ServerId>& servers) {
    const size_t n = _db_servers.Modify(BatchAdd, servers);
    LOG_IF(ERROR, n != servers.size())
        << "Fail to AddServersInBatch, expected " << servers.size()
        << " actually " << n;
    return n;
}

size_t MasterSlaveLoadBalancer::RemoveServersInBatch(
    const std::vector<ServerId>& servers) {
    const size_t n = _db_servers.Modify(BatchRemove, servers);
    LOG_IF(ERROR, n != servers.size())
        << "Fail to RemoveServersInBatch, expected " << servers.size()
        << " actually " << n;
    return n;
}

int MasterSlaveLoadBalancer::SelectServer(const SelectIn& in, SelectOut* out) {
    butil::DoublyBufferedData<Servers>::ScopedPtr s;
    if (!in.has_request_code) {
        LOG(ERROR) << "Controller.set_request_code() is required";
        return EINVAL;
    }
    if (_db_servers.Read(&s) != 0) {
        return ENOMEM;
    }
    size_t n = s->master_server_list.size() + s->slave_server_list.size();
    if (n == 0) {
        return ENODATA;
    }
    uint32_t stride = 0;
    size_t offset = in.request_code % s->master_server_list.size();
    for (size_t i = 0; i < n; ++i) {
        const SocketId id = s->master_server_list[offset].id;
        if (((i + 1) == n  // always take last chance
             || !ExcludedServers::IsExcluded(in.excluded, id))
            && Socket::Address(id, out->ptr) == 0
            && (*out->ptr)->IsAvailable()) {
            // We found an available server
            return 0;
        }
        if (stride == 0) {
            stride = GenRandomStride();
        }
        // If `Address' failed, use `offset+stride' to retry so that
        // this failed server won't be visited again inside for
        offset = (offset + stride) % n;
    }
    return EHOSTDOWN;
}

MasterSlaveLoadBalancer* MasterSlaveLoadBalancer::New(
    const butil::StringPiece& params) const {
    MasterSlaveLoadBalancer* lb = new (std::nothrow) MasterSlaveLoadBalancer;
    return lb;
}

void MasterSlaveLoadBalancer::Destroy() {
    delete this;
}

void MasterSlaveLoadBalancer::Describe(
    std::ostream &os, const DescribeOptions& options) {
    if (!options.verbose) {
        os << "random";
        return;
    }
    os << "Randomized{";
    butil::DoublyBufferedData<Servers>::ScopedPtr s;
    if (_db_servers.Read(&s) != 0) {
        os << "fail to read _db_servers";
    } else {
        os << "master_server_list={";
        for (size_t i = 0; i < s->master_server_list.size(); ++i) {
            os << ' ' << s->master_server_list[i];
        }
        os << "}, slave_server_list={";
        for (size_t i = 0; i < s->slave_server_list.size(); ++i) {
            os << ' ' << s->slave_server_list[i];
        }
    }
    os << '}';
}

}  // namespace policy
} // namespace brpc