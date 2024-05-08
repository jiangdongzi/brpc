#include "mongo_utils.h"
#include <sstream>

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

namespace butil {

MongoDBUri parse_mongo_uri(const std::string& uri) {
    MongoDBUri result;
    size_t protocol_end = uri.find("://");
    size_t at_sign = uri.rfind('@');
    size_t first_slash = uri.find('/', protocol_end + 3);
    size_t question_mark = uri.find('?');

    // Extract username and password
    if (protocol_end != std::string::npos && at_sign != std::string::npos) {
        std::string userinfo = uri.substr(protocol_end + 3, at_sign - protocol_end - 3);
        size_t colon = userinfo.find(':');
        if (colon != std::string::npos) {
            result.username = userinfo.substr(0, colon);
            result.password = userinfo.substr(colon + 1);
        } else {
            result.username = userinfo;
        }
    }

    // Extract hosts
    size_t host_start = at_sign != std::string::npos ? at_sign + 1 : protocol_end + 3;
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

} // namespace butil
