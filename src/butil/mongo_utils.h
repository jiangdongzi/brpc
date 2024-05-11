#include "brpc/channel.h"
#include <cstddef>
#include <cstdint>
#include <memory>
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
    bool read_slave_preferred() const {
        auto it = options.find("readPreference");
        return it != options.end() && it->second == "secondaryPreferred";
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
class Collection;

class Cursor {
public:
    Cursor(Collection* c);
    Collection* collection;
    uint64_t request_code;
    brpc::Channel* chan;
    std::string full_collection_name;

    // 内部迭代器类
    class Iterator {
    public:
        Iterator(Cursor* cursor, bsoncxx::document::view::iterator in_it) : cursor(cursor), it(in_it) {}

        Iterator& operator++();
        Iterator& operator++(int) {
            return operator++();
        }

        bool operator!=(const Iterator& other) const {
            return !(other == *this);
        }

        bool operator==(const Iterator& other) const {
            // return position != other.position || cursor != other.cursor;
            return (cursor == nullptr && other.cursor == nullptr) || (it == other.it && cursor == other.cursor);
        }

        bsoncxx::document::view operator*() {
            return it->get_document().view();
        }

    private:
        Cursor* cursor;
        bsoncxx::document::view::iterator it;
    };

    // 提供迭代器的开始和结束
    Iterator begin() {
        if (!initialized) {
            get_first_batch();
        }
        return Iterator(this, docs.begin());
    }

    Iterator end() {
        return Iterator(nullptr, docs.end());
    }

private:
    void get_first_batch();
    void get_next_batch();
    bsoncxx::document::view docs;
    std::string body;
    bool hasMore{}; // 标志是否还有更多数据可获取
    bool initialized{}; // 标志是否已经初始化
    int64_t cursor_id{};
};

class Collection {
public:
    std::string name;
    Collection(const std::string& collection_name, Database* const db);
    Cursor find(bsoncxx::document::view_or_value filter);
    // Database* database;
    std::unique_ptr<Database> database;
    bsoncxx::document::view_or_value filter;
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

bsoncxx::document::view GetViewFromRawBody(const std::string& body);

} // namespace mongo


} // namespace butil