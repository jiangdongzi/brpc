#include "mongo_utils.h"
#include "brpc/policy/mongo.pb.h"
#include "butil/fast_rand.h"
#include "output/include/brpc/channel.h"
#include "output/include/butil/containers/flat_map.h"
#include "output/include/butil/logging.h"
#include <climits>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <butil/logging.h>
#include <bsoncxx/json.hpp>
#include <string>
#include <unordered_map>

namespace butil {

static void parse_options(const std::string& options_string, std::unordered_map<std::string, std::string>& options_map) {
    std::istringstream options_stream(options_string);
    std::string option;
    while (getline(options_stream, option, '&')) {
        size_t equals = option.find('=');
        if (equals != std::string::npos) {
            std::string key = option.substr(0, equals);
            std::string value = option.substr(equals + 1);
            options_map[key] = value;
        } else {
            options_map[option] = "";  // Handle cases where there is no value, just a key
        }
    }
}

MongoDBUri parse_mongo_uri(const std::string& uri) {
    MongoDBUri result;

    size_t at_sign = uri.rfind('@');
    size_t first_slash = uri.find('/');
    size_t question_mark = uri.find('?');

    // Extract username and password
    if (at_sign != std::string::npos) {
        std::string userinfo = uri.substr(0, at_sign);
        size_t colon = userinfo.find(':');
        if (colon != std::string::npos) {
            result.username = userinfo.substr(0, colon);
            result.password = userinfo.substr(colon + 1);
        } else {
            result.username = userinfo;
        }
    }

    // Extract hosts
    size_t host_start = at_sign != std::string::npos ? at_sign + 1 : 0;
    size_t host_end = first_slash != std::string::npos ? first_slash : (question_mark != std::string::npos ? question_mark : uri.length());
    std::string host_list = uri.substr(host_start, host_end - host_start);
    std::istringstream host_stream(host_list);
    std::string host;
    while (getline(host_stream, host, ',')) {
        result.hosts.push_back(host);
    }

    // Extract database
    if (first_slash != std::string::npos && (question_mark == std::string::npos || first_slash < question_mark)) {
        size_t db_start = first_slash + 1;
        size_t db_end = question_mark != std::string::npos ? question_mark : uri.length();
        result.database = uri.substr(db_start, db_end - db_start);
    }

    // Extract options
    if (question_mark != std::string::npos) {
        parse_options(uri.substr(question_mark + 1), result.options);
    }

    return result;
}

bool need_auth_mongo(const std::string& uri) {
    MongoDBUri parsed = parse_mongo_uri(uri);
    return parsed.need_auth();
}

uint64_t GetRandomRequestCode (const uint64_t flag) {
    auto random = butil::fast_rand_less_than(UINT_MAX);
    return (flag << 32) | random;
}
//定义flag enum
enum {
    MONGOC_READ_SLAVE_PREFERRED = (1 << 2)
};

static uint32_t ExtractFlag (const uint64_t request_code) {
    return request_code >> 32;
}

bool ReadSlavePreferred (const uint64_t request_code) {
    return ExtractFlag(request_code) & MONGOC_READ_SLAVE_PREFERRED;
}

uint64_t GetRandomSlavePreferredRequestCode() {
    return GetRandomRequestCode(MONGOC_READ_SLAVE_PREFERRED);
}

std::vector<bsoncxx::document::view> DeSerializeBsonDocView(const std::string& str)  {
    std::vector<bsoncxx::document::view> result;
    size_t offset = 0;
    const uint8_t* data = (const uint8_t*)str.c_str();
    const size_t length = str.length();
    while (offset < length) {
        // 假设文档长度存储在前四个字节
        uint32_t doc_length = *reinterpret_cast<const uint32_t*>(data + offset);

        // 创建 BSON 视图
        bsoncxx::document::view view(data + offset, doc_length);
        
        // 将 BSON 转换为 JSON 并输出
        result.emplace_back(view);
        
        // 移动偏移量到下一个文档的起始位置
        offset += doc_length;
    }
    return result;
}

namespace mongo {

bsoncxx::document::view GetViewFromRawBody(const std::string& body) {
    DCHECK(*body.c_str() == 0);
    uint32_t doc_length = *(int*)(body.c_str() + 1);
    DCHECK(doc_length == body.size() - 1);
    const uint8_t* data = (const uint8_t*)(body.c_str() + 1);
    return bsoncxx::document::view(data, doc_length);
}

brpc::Channel* Client::GetChannel(const std::string& mongo_uri) {
    auto it = tls_channels.find(mongo_uri);
    if (it != tls_channels.end()) {
        return it->second;
    }
    static std::mutex mtx;
    std::lock_guard<std::mutex> lock_gurad(mtx);
    if (channels.find(mongo_uri) != channels.end()) {
        tls_channels[mongo_uri] = channels[mongo_uri].get();
        return tls_channels[mongo_uri];
    }
    std::unique_ptr<brpc::Channel> channel_up(new brpc::Channel);
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_MONGO;
    std::string lb_with_ns_url = "ms:" + mongo_uri;
    if (channel_up->Init(mongo_uri.c_str(), lb_with_ns_url.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        throw std::runtime_error("Fail to initialize channel");
    }
    channels[mongo_uri].reset(channel_up.release());
    tls_channels[mongo_uri] = channels[mongo_uri].get();
    return tls_channels[mongo_uri];
}

Client::Client(const std::string& mongo_uri) {
    channel = GetChannel(mongo_uri);
}

Cursor::Cursor(Collection* c) {
    collection = c;
    request_code = butil::fast_rand_less_than(UINT_MAX);
    chan = collection->database->client->channel;
    full_collection_name = collection->database->name + "." + collection->name;
}

Cursor Collection::find(bsoncxx::document::view_or_value filter) {
    this->filter = filter;
    return Cursor(this);
}

void Cursor::get_first_batch() {
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;

    // sections.append((char*)collection->filter.view().data(), collection->filter.view().length());
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("find", collection->name));
    doc.append(bsoncxx::builder::basic::kvp("filter", collection->filter));
    doc.append(bsoncxx::builder::basic::kvp("$db", collection->database->name));

    AddDoc2Request(doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    cntl.set_request_code(request_code);
    chan->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return;
    }
    response.mutable_sections()->swap(body);
    bsoncxx::document::view view = GetViewFromRawBody(body);
    LOG(INFO) << "view: " << bsoncxx::to_json(view);
    bool ok = view["ok"].get_double() == 1.0;
    if (!ok) {
        LOG(ERROR) << "not ok: " << view["ok"].get_double();
        return;
    }
    auto cursor_info = view["cursor"];
    cursor_id = cursor_info["id"].get_int64();
    hasMore = cursor_id != 0;
    docs = cursor_info["firstBatch"].get_array().value;

    initialized = true;
}

void Cursor::get_next_batch() {
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    // sections.append((char*)collection->filter.view().data(), collection->filter.view().length());
    bsoncxx::builder::basic::document doc;
    doc.append(bsoncxx::builder::basic::kvp("getMore", cursor_id));
    doc.append(bsoncxx::builder::basic::kvp("collection", collection->name));
    doc.append(bsoncxx::builder::basic::kvp("$db", collection->database->name));
    AddDoc2Request(doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    cntl.set_request_code(request_code);
    chan->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return;
    }
    response.mutable_sections()->swap(body);
    bsoncxx::document::view view = GetViewFromRawBody(body);
    LOG(INFO) << "view: " << bsoncxx::to_json(view);
    bool ok = view["ok"].get_double() == 1.0;
    if (!ok) {
        hasMore = false;
        LOG(ERROR) << "not ok: " << view["ok"].get_double();
        return;
    }
    auto cursor_info = view["cursor"];
    cursor_id = cursor_info["id"].get_int64();
    hasMore = cursor_id != 0;
    docs = cursor_info["nextBatch"].get_array().value;
}

Collection::Collection (const std::string& collection_name, Database* const db) {
    name = collection_name;
    database = std::unique_ptr<Database>(new Database(*db));
}

Cursor::Iterator& Cursor::Iterator::operator++() {
    it++;
    if (it == cursor->docs.end() && cursor->hasMore) {
        cursor->get_next_batch();
        if (cursor->docs.begin() != cursor->docs.end()) {
            it = cursor->docs.begin();
        } else {
            cursor = nullptr;
            return *this;
        }
    } else if (!cursor->hasMore && it == cursor->docs.end()) {
        cursor = nullptr;
    }
    return *this;
}

std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> Client::channels;
thread_local std::unordered_map<std::string, brpc::Channel*> Client::tls_channels;

static std::string BuildSections (const bsoncxx::builder::basic::document& doc) {
    std::string sections;
    sections += '\0';
    sections.append((char*)doc.view().data(), doc.view().length());
    return sections;
}

void AddDoc2Request(const bsoncxx::builder::basic::document& doc, brpc::policy::MongoRequest* request) {
    std::string sections = BuildSections(doc);
    request->set_sections(std::move(sections));
}

} // namespace mongo
} // namespace butil