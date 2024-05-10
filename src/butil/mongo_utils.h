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
class Client;
class Database;

class Collection {
public:
    std::string name;
    Collection(const std::string& collection_name, Database* const db) {
        name = collection_name;
        database = db;
    }
    Database* database;
};

class Database {
public:
    std::string name;
    Database(const std::string& db_name, Client* c) {
        name = db_name;
        client = c;
    }
    Collection operator[](const std::string& collection_name) {
        return Collection(collection_name, this);
    }
    Client* client;
};

class Client {
public:
    Client(const std::string& mongo_uri);
    Database operator[](const std::string& database_name) {
        return Database(database_name, this);
    }

brpc::Channel* channel;
private:
    static std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels;
    static thread_local std::unordered_map<std::string, brpc::Channel*> tls_channels;
    static brpc::Channel* GetChannel(const std::string& mongo_uri);
};


} // namespace mongo


} // namespace butil