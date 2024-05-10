#include "mongo_utils.h"
#include "butil/fast_rand.h"
#include <climits>
#include <cstdint>
#include <sstream>
#include <butil/logging.h>
#include <bsoncxx/json.hpp>

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

std::string SerilizeBsonDocView(const bsoncxx::builder::basic::document& doc) {
    auto v = doc.view();
    return std::string((char*)v.data(), v.length());
}
std::vector<bsoncxx::document::view> DeSerilizeBsonDocView(const std::string& str)  {
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
        LOG(INFO) << bsoncxx::to_json(view);
        
        // 移动偏移量到下一个文档的起始位置
        offset += doc_length;
    }
    return result;
}

} // namespace butil
