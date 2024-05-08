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

#ifndef BRPC_POLICY_REDIS_AUTHENTICATOR_H
#define BRPC_POLICY_REDIS_AUTHENTICATOR_H

#include "brpc/authenticator.h"

namespace brpc {
namespace policy {

// Request to redis for authentication.
class MongoAuthenticator : public Authenticator {
public:
    MongoAuthenticator(){}

    int GenerateCredential(std::string* auth_str) const;

    int VerifyCredential(const std::string&, const butil::EndPoint&,
                         brpc::AuthContext*) const {
        return 0;
    }

private:
    char r[256], s[256];
    int i;
    std::string encoded_nonce;
    std::string first_payload_str;
    int conv_id;
    std::string authmsg;
    std::string output_v_str;
    std::string salted_password_str;
};

}  // namespace policy
}  // namespace brpc

#endif  // BRPC_POLICY_COUCHBASE_AUTHENTICATOR_H
