#include <string>
#include <unordered_map>
#include <vector>
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
} // namespace butil