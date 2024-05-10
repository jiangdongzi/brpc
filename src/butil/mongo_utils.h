#include "brpc/channel.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <bsoncxx/types.hpp>
#include <bsoncxx/builder/basic/document.hpp>

#pragma once
namespace butil {
struct MongoDBUri {
    std::string username;
    std::string password;
    std::vector<std::string> hosts;
    std::string database;
    std::unordered_map<std::string, std::string> options;
    void swap(MongoDBUri& other) {
        username.swap(other.username);
        password.swap(other.password);
        hosts.swap(other.hosts);
        database.swap(other.database);
        options.swap(other.options);
    }
    bool need_auth() const {
        return !username.empty() && !password.empty();
    }
};

MongoDBUri parse_mongo_uri(const std::string& uri);
bool need_auth_mongo(const std::string& uri);

uint64_t GetRandomRequestCode (const uint64_t flag);
bool ReadSlavePreferred (const uint64_t request_code);
uint64_t GetRandomSlavePreferredRequestCode();

std::string SerializeBsonDocView(const bsoncxx::builder::basic::document& doc);
std::vector<bsoncxx::document::view> DeSerializeBsonDocView(const std::string& str);

namespace mongo {
class Client {
public:
    Client(const std::string& mongo_uri);

private:
    brpc::Channel* channel;
    static std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels;
    static thread_local std::unordered_map<std::string, brpc::Channel*> tls_channels;
    static brpc::Channel* GetChannel(const std::string& mongo_uri);
};
} // namespace mongo


} // namespace butil