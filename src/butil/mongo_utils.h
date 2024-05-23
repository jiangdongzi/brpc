#include "brpc/channel.h"
#include "brpc/policy/mongo.pb.h"
#include <cstdint>
#include <butil/optional.h>
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

std::string SerializeBsonDocView(const bsoncxx::builder::basic::document& doc);

namespace mongo {

namespace options {
struct update {
    update& bypass_document_validation(bool bypass_document_validation);
    const stdx::optional<bool>& bypass_document_validation() const;
    update& collation(bsoncxx::document::view_or_value collation);
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;
    update& let(bsoncxx::document::view_or_value let);
    const stdx::optional<bsoncxx::document::view_or_value> let() const;
    update& upsert(bool upsert);
    const stdx::optional<bool>& upsert() const;
    update& array_filters(bsoncxx::array::view_or_value array_filters);
    const stdx::optional<bsoncxx::array::view_or_value>& array_filters() const;

private:
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<bool> _upsert;
    stdx::optional<bsoncxx::array::view_or_value> _array_filters;
    stdx::optional<bsoncxx::document::view_or_value> _let;
};

struct insert {
    insert& bypass_document_validation(bool bypass_document_validation);
    const stdx::optional<bool>& bypass_document_validation() const;
    insert& ordered(bool ordered);
    const stdx::optional<bool>& ordered() const;

private:
    stdx::optional<bool> _ordered;
    stdx::optional<bool> _bypass_document_validation;
};

class find {
   public:
    find& allow_disk_use(bool allow_disk_use);
    const stdx::optional<bool>& allow_disk_use() const;
    find& allow_partial_results(bool allow_partial);
    const stdx::optional<bool>& allow_partial_results() const;
    find& batch_size(std::int32_t batch_size);
    const stdx::optional<std::int32_t>& batch_size() const;
    find& collation(bsoncxx::document::view_or_value collation);
    const stdx::optional<bsoncxx::document::view_or_value>& collation() const;
    find& let(bsoncxx::document::view_or_value let);
    const stdx::optional<bsoncxx::document::view_or_value> let() const;
    find& limit(std::int64_t limit);
    const stdx::optional<std::int64_t>& limit() const;
    find& max(bsoncxx::document::view_or_value max);
    const stdx::optional<bsoncxx::document::view_or_value>& max() const;
    find& max_await_time(std::chrono::milliseconds max_await_time);
    const stdx::optional<std::chrono::milliseconds>& max_await_time() const;
    find& max_time(std::chrono::milliseconds max_time);
    const stdx::optional<std::chrono::milliseconds>& max_time() const;
    find& min(bsoncxx::document::view_or_value min);
    const stdx::optional<bsoncxx::document::view_or_value>& min() const;
    find& no_cursor_timeout(bool no_cursor_timeout);
    const stdx::optional<bool>& no_cursor_timeout() const;
    find& projection(bsoncxx::document::view_or_value projection);
    const stdx::optional<bsoncxx::document::view_or_value>& projection() const;
    find& read_preference(bsoncxx::document::view_or_value rp);
    const stdx::optional<bsoncxx::document::view_or_value>& read_preference() const;
    find& return_key(bool return_key);
    const stdx::optional<bool>& return_key() const;
    find& show_record_id(bool show_record_id);
    const stdx::optional<bool>& show_record_id() const;
    find& skip(std::int64_t skip);
    const stdx::optional<std::int64_t>& skip() const;
    find& sort(bsoncxx::document::view_or_value ordering);
    const stdx::optional<bsoncxx::document::view_or_value>& sort() const;

   private:
    stdx::optional<bool> _allow_disk_use;
    stdx::optional<bool> _allow_partial_results;
    stdx::optional<std::int32_t> _batch_size;
    stdx::optional<bsoncxx::document::view_or_value> _collation;
    stdx::optional<bsoncxx::document::view_or_value> _let;
    stdx::optional<std::int64_t> _limit;
    stdx::optional<bsoncxx::document::view_or_value> _max;
    stdx::optional<std::chrono::milliseconds> _max_await_time;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<bsoncxx::document::view_or_value> _min;
    stdx::optional<bool> _no_cursor_timeout;
    stdx::optional<bsoncxx::document::view_or_value> _projection;
    stdx::optional<bsoncxx::document::view_or_value> _read_preference;
    stdx::optional<bool> _return_key;
    stdx::optional<bool> _show_record_id;
    stdx::optional<std::int64_t> _skip;
    stdx::optional<bsoncxx::document::view_or_value> _ordering;
};

enum class return_document {
    /// Return the version of the document before the modification takes place.
    k_before,
    /// Return the version of the document after the modification takes place.
    k_after,
};

class find_one_and_update {
  public:
    find_one_and_update& collation(bsoncxx::v_noabi::document::view_or_value collation);
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& collation() const;
    find_one_and_update& bypass_document_validation(bool bypass_document_validation);
    const stdx::optional<bool>& bypass_document_validation() const;
    find_one_and_update& let(bsoncxx::v_noabi::document::view_or_value let);
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value> let() const;
    find_one_and_update& max_time(std::chrono::milliseconds max_time);
    const stdx::optional<std::chrono::milliseconds>& max_time() const;
    find_one_and_update& projection(bsoncxx::v_noabi::document::view_or_value projection);
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& projection() const;
    find_one_and_update& return_document(return_document return_document);
    const stdx::optional<butil::mongo::options::return_document>& return_document() const;
    find_one_and_update& sort(bsoncxx::v_noabi::document::view_or_value ordering);
    const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& sort() const;
    find_one_and_update& upsert(bool upsert);
    const stdx::optional<bool>& upsert() const;
    find_one_and_update& array_filters(bsoncxx::v_noabi::array::view_or_value array_filters);
    const stdx::optional<bsoncxx::v_noabi::array::view_or_value>& array_filters() const;

   private:
    stdx::optional<bool> _bypass_document_validation;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _collation;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _let;
    stdx::optional<std::chrono::milliseconds> _max_time;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _projection;
    stdx::optional<butil::mongo::options::return_document> _return_document;
    stdx::optional<bsoncxx::v_noabi::document::view_or_value> _ordering;
    stdx::optional<bool> _upsert;
    stdx::optional<bsoncxx::v_noabi::array::view_or_value> _array_filters;
};

} // namespace options

class Client;
class Database;
class Collection;

enum class read_mode : std::uint8_t {
    ///
    /// Only read from a primary node.
    ///
    k_primary,

    ///
    /// Prefer to read from a primary node.
    ///
    k_primary_preferred,

    ///
    /// Only read from secondary nodes.
    ///
    k_secondary,

    ///
    /// Prefer to read from secondary nodes.
    ///
    k_secondary_preferred,

    ///
    /// Read from the node with the lowest latency irrespective of state.
    ///
    k_nearest
};

constexpr uint64_t primary_preferred = (1 << (int)read_mode::k_primary) | (1 << (int)read_mode::k_primary_preferred);
constexpr uint64_t secondary_preferred = (1 << (int)read_mode::k_secondary) | (1 << (int)read_mode::k_secondary_preferred);

inline uint32_t GetFlagFromRequestCode(uint64_t request_code) {
    return request_code >> 32;
}

class Cursor {
public:
    Cursor(Collection* c);
    Collection* collection;
    uint64_t request_code;
    brpc::Channel* chan;

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
            return (cursor == nullptr && other.cursor == nullptr) || (it == other.it && cursor == other.cursor);
        }

        bsoncxx::document::view operator*() {
            return it->get_document().view();
        }

        bsoncxx::document::view::iterator operator->() {
            return it;
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
        if (docs.begin() == docs.end()) {
            return Iterator(nullptr, docs.end());
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
    Cursor find(bsoncxx::document::view_or_value filter, const options::find& opts = options::find());
    stdx::optional<bsoncxx::document::value> find_one(bsoncxx::document::view_or_value filter, options::find opts = options::find());
    // Database* database;
    std::unique_ptr<Database> database;
    bsoncxx::document::view_or_value filter;
    bsoncxx::document::value insert_one(bsoncxx::document::view_or_value doc, const options::insert& opts = options::insert());
    void async_insert_one(bsoncxx::document::view_or_value doc, const options::insert& opts = options::insert());
    bsoncxx::document::value update_one(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,  const options::update& opts = options::update());
    void async_update_one(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,  const options::update& opts = options::update());
    bsoncxx::builder::basic::document find_opt_doc;
    stdx::optional<bsoncxx::document::value> find_one_and_update(bsoncxx::document::view_or_value query, bsoncxx::document::view_or_value update,  const options::find_one_and_update& opts = options::find_one_and_update());
private:
    brpc::policy::MongoRequest create_insert_requet(const bsoncxx::document::view_or_value doc, const options::insert& opts = options::insert());
    brpc::policy::MongoRequest create_update_requet(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,
        const options::update& opts = options::update());
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

enum class MongoServersMode {
    kSingle,
    kReplicaSet,
    kSharded,
};

class Client {
public:
    Client(const std::string& mongo_uri);
    Database operator[](const std::string& database_name) {
        return Database(database_name, this);
    }

brpc::Channel* channel;
MongoServersMode server_mode;
bool read_slave_preferred;
private:
    static std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels;
    static thread_local std::unordered_map<std::string, brpc::Channel*> tls_channels;
    static brpc::Channel* GetChannel(const std::string& mongo_uri);

    static std::unordered_map<std::string, MongoServersMode> server_modes;
    static thread_local std::unordered_map<std::string, MongoServersMode> tls_server_modes;
    static MongoServersMode GetMongoServersMode(const std::string& mongo_uri);
};

bsoncxx::document::view GetViewFromRawBody(const std::string& body);

void AddDoc2Request(const bsoncxx::builder::basic::document& doc, brpc::policy::MongoRequest* request);
std::string BuildSections (const bsoncxx::builder::basic::document& doc);

std::string RemoveMongoDBPrefix(const std::string& url);

} // namespace mongo


} // namespace butil