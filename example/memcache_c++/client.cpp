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

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/string_printf.h>
#include <butil/strings/string_split.h>
#include <brpc/channel.h>
#include <brpc/memcache.h>
#include <brpc/policy/couchbase_authenticator.h>


int main(int argc, char* argv[]) {
    std::string input = "hello|123|456||789|";
    std::vector<std::string> out;
    butil::SplitStringUsingSubstr(input, "|", &out);
    LOG(INFO) << "sz: " << out.size();
    for (const auto& ele : out) {
        LOG(INFO) << "aaa " << ele << " bbb";
    }
    return 0;
}
