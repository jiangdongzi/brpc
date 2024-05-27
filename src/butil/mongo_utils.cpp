#include "mongo_utils.h"
#include "brpc/policy/mongo.pb.h"
#include "bsoncxx/builder/basic/document.hpp"
#include "butil/fast_rand.h"
#include "brpc/channel.h"
#include "butil/logging.h"
#include "brpc/policy/mongo_authenticator.h"
#include <climits>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <butil/logging.h>
#include <bsoncxx/json.hpp>
#include <string>
#include <unordered_map>
#include <bsoncxx/builder/basic/array.hpp>

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

MongoDBUri parse_mongo_uri(const std::string& input_uri) {
    const std::string uri = butil::mongo::RemoveMongoDBPrefix(input_uri);
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

namespace mongo {

namespace options {

find& find::allow_disk_use(bool allow_disk_use) {
    _allow_disk_use = allow_disk_use;
    return *this;
}

find& find::allow_partial_results(bool allow_partial) {
    _allow_partial_results = allow_partial;
    return *this;
}

find& find::batch_size(std::int32_t batch_size) {
    _batch_size = batch_size;
    return *this;
}

find& find::collation(bsoncxx::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

find& find::limit(std::int64_t limit) {
    _limit = limit;
    return *this;
}

find& find::let(bsoncxx::document::view_or_value let) {
    _let = let;
    return *this;
}


find& find::max(bsoncxx::document::view_or_value max) {
    _max = std::move(max);
    return *this;
}

find& find::max_await_time(std::chrono::milliseconds max_await_time) {
    _max_await_time = std::move(max_await_time);
    return *this;
}

find& find::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

find& find::min(bsoncxx::document::view_or_value min) {
    _min = std::move(min);
    return *this;
}

find& find::no_cursor_timeout(bool no_cursor_timeout) {
    _no_cursor_timeout = no_cursor_timeout;
    return *this;
}

find& find::projection(bsoncxx::document::view_or_value projection) {
    _projection = std::move(projection);
    return *this;
}

find& find::read_preference(bsoncxx::document::view_or_value rp) {
    _read_preference = std::move(rp);
    return *this;
}

find& find::return_key(bool return_key) {
    _return_key = return_key;
    return *this;
}

find& find::show_record_id(bool show_record_id) {
    _show_record_id = show_record_id;
    return *this;
}

find& find::skip(std::int64_t skip) {
    _skip = skip;
    return *this;
}

find& find::sort(bsoncxx::document::view_or_value ordering) {
    _ordering = std::move(ordering);
    return *this;
}

const stdx::optional<bool>& find::allow_disk_use() const {
    return _allow_disk_use;
}

const stdx::optional<bool>& find::allow_partial_results() const {
    return _allow_partial_results;
}

const stdx::optional<std::int32_t>& find::batch_size() const {
    return _batch_size;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::collation() const {
    return _collation;
}

const stdx::optional<std::int64_t>& find::limit() const {
    return _limit;
}

const stdx::optional<bsoncxx::document::view_or_value> find::let() const {
    return _let;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::max() const {
    return _max;
}

const stdx::optional<std::chrono::milliseconds>& find::max_await_time() const {
    return _max_await_time;
}

const stdx::optional<std::chrono::milliseconds>& find::max_time() const {
    return _max_time;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::min() const {
    return _min;
}

const stdx::optional<bool>& find::no_cursor_timeout() const {
    return _no_cursor_timeout;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::projection() const {
    return _projection;
}

const stdx::optional<bool>& find::return_key() const {
    return _return_key;
}

const stdx::optional<bool>& find::show_record_id() const {
    return _show_record_id;
}

const stdx::optional<std::int64_t>& find::skip() const {
    return _skip;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::sort() const {
    return _ordering;
}

const stdx::optional<bsoncxx::document::view_or_value>& find::read_preference() const {
    return _read_preference;
}

//update
update& update::bypass_document_validation(bool bypass_document_validation) {
    _bypass_document_validation = bypass_document_validation;
    return *this;
}

update& update::collation(bsoncxx::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

update& update::let(bsoncxx::document::view_or_value let) {
    _let = let;
    return *this;
}

update& update::upsert(bool upsert) {
    _upsert = upsert;
    return *this;
}

const stdx::optional<bool>& update::bypass_document_validation() const {
    return _bypass_document_validation;
}

const stdx::optional<bsoncxx::document::view_or_value>& update::collation() const {
    return _collation;
}

const stdx::optional<bsoncxx::document::view_or_value> update::let() const {
    return _let;
}

const stdx::optional<bool>& update::upsert() const {
    return _upsert;
}

update& update::array_filters(bsoncxx::array::view_or_value array_filters) {
    _array_filters = std::move(array_filters);
    return *this;
}

const stdx::optional<bsoncxx::array::view_or_value>& update::array_filters() const {
    return _array_filters;
}

insert& insert::bypass_document_validation(bool bypass_document_validation) {
    _bypass_document_validation = bypass_document_validation;
    return *this;
}

insert& insert::ordered(bool ordered) {
    _ordered = ordered;
    return *this;
}

const stdx::optional<bool>& insert::bypass_document_validation() const {
    return _bypass_document_validation;
}

const stdx::optional<bool>& insert::ordered() const {
    return _ordered;
}

find_one_and_update& find_one_and_update::bypass_document_validation(
    bool bypass_document_validation) {
    _bypass_document_validation = bypass_document_validation;
    return *this;
}

find_one_and_update& find_one_and_update::collation(
    bsoncxx::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

find_one_and_update& find_one_and_update::let(bsoncxx::document::view_or_value let) {
    _let = let;
    return *this;
}


find_one_and_update& find_one_and_update::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

find_one_and_update& find_one_and_update::projection(
    bsoncxx::document::view_or_value projection) {
    _projection = std::move(projection);
    return *this;
}

find_one_and_update& find_one_and_update::return_document(
    butil::mongo::options::return_document return_document) {
    _return_document = return_document;
    return *this;
}

find_one_and_update& find_one_and_update::sort(bsoncxx::document::view_or_value ordering) {
    _ordering = std::move(ordering);
    return *this;
}

find_one_and_update& find_one_and_update::upsert(bool upsert) {
    _upsert = upsert;
    return *this;
}

const stdx::optional<bool>& find_one_and_update::bypass_document_validation() const {
    return _bypass_document_validation;
}

const stdx::optional<bsoncxx::document::view_or_value>& find_one_and_update::collation()
    const {
    return _collation;
}


const stdx::optional<bsoncxx::document::view_or_value> find_one_and_update::let() const {
    return _let;
}

const stdx::optional<std::chrono::milliseconds>& find_one_and_update::max_time() const {
    return _max_time;
}

const stdx::optional<bsoncxx::document::view_or_value>& find_one_and_update::projection()
    const {
    return _projection;
}

const stdx::optional<return_document>& find_one_and_update::return_document() const {
    return _return_document;
}

const stdx::optional<bsoncxx::document::view_or_value>& find_one_and_update::sort() const {
    return _ordering;
}

const stdx::optional<bool>& find_one_and_update::upsert() const {
    return _upsert;
}

find_one_and_update& find_one_and_update::array_filters(
    bsoncxx::array::view_or_value array_filters) {
    _array_filters = std::move(array_filters);
    return *this;
}

const stdx::optional<bsoncxx::array::view_or_value>& find_one_and_update::array_filters()
    const {
    return _array_filters;
}



} //namespace options

static void build_find_options_document(const options::find& options, bsoncxx::builder::basic::document* doc) {

    using bsoncxx::builder::basic::kvp;
    if (const auto& adu = options.allow_disk_use()) {
        doc->append(kvp("allowDiskUse", *adu));
    }

    if (const auto& apr = options.allow_partial_results()) {
        doc->append(kvp("allowPartialResults", *apr));
    }

    if (const auto& batch_size = options.batch_size()) {
        doc->append(kvp("batchSize", *batch_size));
    }

    if (const auto& collation = options.collation()) {
        doc->append(kvp("collation", *collation));
    }

    if (const auto& let = options.let()) {
        doc->append(kvp("let", *let));
    }

    if (const auto& limit = options.limit()) {
        doc->append(kvp("limit", *limit));
    }

    if (const auto& max = options.max()) {
        doc->append(kvp("max", *max));
    }

    if (const auto& max_time = options.max_time()) {
        doc->append(
            kvp("maxTimeMS", bsoncxx::types::b_int64{max_time->count()}));
    }

    if (const auto& min = options.min()) {
        doc->append(kvp("min", *min));
    }

    if (const auto& nct = options.no_cursor_timeout()) {
        doc->append(kvp("noCursorTimeout", *nct));
    }

    if (const auto& projection = options.projection()) {
        doc->append(kvp("projection", bsoncxx::types::b_document{*projection}));
    }

    if (const auto& return_key = options.return_key()) {
        doc->append(kvp("returnKey", *return_key));
    }

    if (const auto& show_record_id = options.show_record_id()) {
        doc->append(kvp("showRecordId", *show_record_id));
    }

    if (const auto& skip = options.skip()) {
        doc->append(kvp("skip", *skip));
    }

    if (const auto& sort = options.sort()) {
        doc->append(kvp("sort", bsoncxx::types::b_document{*sort}));
    }
}

static void build_find_one_and_update_options_document(const options::find_one_and_update& options, bsoncxx::builder::basic::document* doc) {

    using bsoncxx::builder::basic::kvp;
        if (const auto& collation = options.collation()) {
        doc->append(kvp("collation", *collation));
    }

    if (const auto& af = options.array_filters()) {
        doc->append(kvp("arrayFilters", *af));
    }

    if (const auto& let = options.let()) {
        doc->append(kvp("let", *let));
    }
    
    if (const auto& st = options.sort()) {
        doc->append(kvp("sort", *st));
    }

    if (const auto& upsert = options.upsert()) {
        doc->append(kvp("upsert", *upsert));
    }

    if (const auto& max_time = options.max_time()) {
        doc->append(kvp("maxTimeMS", bsoncxx::types::b_int64{max_time->count()}));
    }

    if (const auto& rd = options.return_document()) {
        doc->append(kvp("new", rd.value() == butil::mongo::options::return_document::k_after));
    }

    if (const auto& bdv = options.bypass_document_validation()) {
        doc->append(kvp("bypassDocumentValidation", *bdv));
    }

    if (const auto& prj = options.projection()) {
        doc->append(kvp("fields", *prj));
    }
}

static void build_update_options_document(
    const options::update& options, bsoncxx::builder::basic::document* doc) {

    using bsoncxx::builder::basic::kvp;
    if (const auto& bdv = options.bypass_document_validation()) {
        doc->append(kvp("bypassDocumentValidation", *bdv));
    }
    if (const auto& collation = options.collation()) {
        doc->append(kvp("collation", *collation));
    }
    if (const auto& let = options.let()) {
        doc->append(kvp("let", *let));
    }
    if (const auto& upsert = options.upsert()) {
        doc->append(kvp("upsert", *upsert));
    }
    if (const auto& array_filters = options.array_filters()) {
        doc->append(kvp("arrayFilters", *array_filters));
    }
}

static void build_insert_options_document(const options::insert& options, bsoncxx::builder::basic::document* doc) {
    using bsoncxx::builder::basic::kvp;
    if (const auto& bdv = options.bypass_document_validation()) {
        doc->append(kvp("bypassDocumentValidation", *bdv));
    }
    if (const auto& ordered = options.ordered()) {
        doc->append(kvp("ordered", *ordered));
    }
}


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
    if (need_auth_mongo(mongo_uri)) {
        options.auth = new brpc::policy::MongoAuthenticator(mongo_uri);
    }
    if (channel_up->Init(mongo_uri.c_str(), lb_with_ns_url.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        throw std::runtime_error("Fail to initialize channel");
    }
    channels[mongo_uri].reset(channel_up.release());
    tls_channels[mongo_uri] = channels[mongo_uri].get();
    return tls_channels[mongo_uri];
}

MongoServersMode Client::GetMongoServersMode(const std::string& mongo_uri_str) {
    auto it = tls_server_modes.find(mongo_uri_str);
    if (it != tls_server_modes.end()) {
        return it->second;
    }
    static std::mutex mtx;
    std::lock_guard<std::mutex> lock_guard(mtx);
    if (server_modes.find(mongo_uri_str) != server_modes.end()) {
        tls_server_modes[mongo_uri_str] = server_modes[mongo_uri_str];
        return server_modes[mongo_uri_str];
    }
    auto* channel = GetChannel(mongo_uri_str);
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetRandomRequestCode(0));

    bsoncxx::builder::basic::document document{};
    document.append(bsoncxx::builder::basic::kvp("isMaster", 1));
    const butil::MongoDBUri mongo_uri = butil::parse_mongo_uri(mongo_uri_str);

    document.append(bsoncxx::builder::basic::kvp("$db", mongo_uri.database));

    butil::mongo::AddDoc2Request(document, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongodb, " << cntl.ErrorText();
        throw std::runtime_error("Fail to access mongodb");
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    LOG(INFO) << "view: " << bsoncxx::to_json(view);
    auto vit = view.find("setName");
    if (vit != view.end()) {
        tls_server_modes[mongo_uri_str] = MongoServersMode::kReplicaSet;
        server_modes[mongo_uri_str] = MongoServersMode::kReplicaSet;
        return MongoServersMode::kReplicaSet;
    }
    vit = view.find("msg");
    if (vit != view.end()) {
        if (vit->get_utf8().value.to_string() == "isdbgrid") {
            tls_server_modes[mongo_uri_str] = MongoServersMode::kSharded;
            server_modes[mongo_uri_str] = MongoServersMode::kSharded;
            return MongoServersMode::kSharded;
        } else {
            throw std::runtime_error("Unsupported server mode");
        }
    }
    return MongoServersMode::kSingle;
}

Client::Client(const std::string& mongo_uri) {
    channel = GetChannel(mongo_uri);
    server_mode = GetMongoServersMode(mongo_uri);
    MongoDBUri parsed = parse_mongo_uri(mongo_uri);
    read_slave_preferred = server_mode == MongoServersMode::kSharded && parsed.read_slave_preferred();
}

static uint64_t GetPrimaryPreferredRequestCode () {
    return GetRandomRequestCode(primary_preferred);
}

Cursor::Cursor(Collection* c) {
    collection = c;
    request_code = butil::fast_rand_less_than(UINT_MAX);
    chan = collection->database->client->channel;
}

Cursor Collection::find(bsoncxx::document::view_or_value filter, const options::find& opts) {
    this->filter = filter;
    find_opt_doc.append(bsoncxx::builder::basic::kvp("find", name));
    find_opt_doc.append(bsoncxx::builder::basic::kvp("filter", filter));
    find_opt_doc.append(bsoncxx::builder::basic::kvp("$db", database->name));
    if (database->client->read_slave_preferred) {
        find_opt_doc.append(
            bsoncxx::builder::basic::kvp("$readPreference",
                bsoncxx::builder::basic::make_document(
                    bsoncxx::builder::basic::kvp("mode", "secondaryPreferred")
                )
            )
        );
    }
    build_find_options_document(opts, &find_opt_doc);
    return Cursor(this);
}

void Cursor::get_first_batch() {
    brpc::policy::MongoRequest request;
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;

    AddDoc2Request(collection->find_opt_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    cntl.set_request_code(request_code);
    chan->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return;
    }
    response.mutable_sections()->swap(body);
    bsoncxx::document::view view = GetViewFromRawBody(body);
    bool ok = view["ok"].get_double() == 1.0;
    LOG(ERROR) << "ok view: " << bsoncxx::to_json(view);
    if (!ok) {
        LOG(ERROR) << "view: " << bsoncxx::to_json(view);
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
        LOG(ERROR) << "view: " << bsoncxx::to_json(view);
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

std::unordered_map<std::string, MongoServersMode> Client::server_modes;
thread_local std::unordered_map<std::string, MongoServersMode> Client::tls_server_modes;

std::string BuildSections (const bsoncxx::builder::basic::document& doc) {
    std::string sections;
    sections += '\0';
    sections.append((char*)doc.view().data(), doc.view().length());
    return sections;
}

void AddDoc2Request(const bsoncxx::builder::basic::document& doc, brpc::policy::MongoRequest* request) {
    std::string sections = BuildSections(doc);
    request->set_sections(std::move(sections));
}

std::string RemoveMongoDBPrefix(const std::string& url) {
    const std::string prefix = "mongodb://";
    // Check if the prefix exists at the beginning of the URL
    if (url.substr(0, prefix.size()) == prefix) {
        // If yes, return the substring that comes after the prefix
        return url.substr(prefix.size());
    }
    // If no prefix, return the original URL
    return url;
}

bsoncxx::document::value Collection::insert_one(bsoncxx::document::view_or_value doc, const options::insert& opts) {
    using namespace bsoncxx::builder::basic;

    brpc::policy::MongoRequest request = create_insert_requet(doc, opts);
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetPrimaryPreferredRequestCode());
    database->client->channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return make_document(kvp("n", "0"), kvp("ok", 0.0), kvp("err", cntl.ErrorText()));
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    return bsoncxx::document::value(view);
}

static void LOGMongoResponse(brpc::Controller* cntl, brpc::policy::MongoResponse* response) {
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<brpc::policy::MongoResponse> response_guard(response);
    if (cntl->Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl->ErrorText();
        return;
    }
    bsoncxx::document::view view = GetViewFromRawBody(response->sections());
    LOG(INFO) << "view: " << bsoncxx::to_json(view);
}

brpc::policy::MongoRequest Collection::create_insert_requet(const bsoncxx::document::view_or_value doc, const options::insert& opts) {
    using namespace bsoncxx::builder::basic;
    document insert_doc;
    insert_doc.append(kvp("insert", name));
    insert_doc.append(kvp("documents", make_array(doc)));
    insert_doc.append(kvp("$db", database->name));
    build_insert_options_document(opts, &insert_doc);
    brpc::policy::MongoRequest request;
    AddDoc2Request(insert_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    return request;
}

void Collection::async_insert_one(bsoncxx::document::view_or_value doc, const options::insert& opts) {
    using namespace bsoncxx::builder::basic;
    brpc::policy::MongoRequest request = create_insert_requet(doc, opts);
    brpc::policy::MongoResponse* response = new brpc::policy::MongoResponse();
    brpc::Controller* cntl = new brpc::Controller;
    cntl->set_request_code(GetPrimaryPreferredRequestCode());
    google::protobuf::Closure* done = brpc::NewCallback(
            &LOGMongoResponse, cntl, response);
    database->client->channel->CallMethod(NULL, cntl, &request, response, done);
}

brpc::policy::MongoRequest Collection::create_update_requet(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,
        const options::update& opts) {
    using namespace bsoncxx::builder::basic;
    document update_doc;
    update_doc.append(kvp("update", name));
    bsoncxx::builder::basic::document update_one_ele;
    update_one_ele.append(kvp("q", filter));
    update_one_ele.append(kvp("u", update));
    build_update_options_document(opts, &update_one_ele);
    update_doc.append(kvp("updates", make_array(update_one_ele)));
    update_doc.append(kvp("$db", database->name));
    brpc::policy::MongoRequest request;
    AddDoc2Request(update_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    return request;
}

bsoncxx::document::value Collection::update_one(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,  const options::update& opts) {
    using namespace bsoncxx::builder::basic;
    brpc::policy::MongoRequest request = create_update_requet(filter, update, opts);
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetPrimaryPreferredRequestCode());
    database->client->channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return make_document(kvp("n", "0"), kvp("ok", 0.0), kvp("err", cntl.ErrorText()));
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    return bsoncxx::document::value(view);
}

void Collection::async_update_one(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update,  const options::update& opts) {
    using namespace bsoncxx::builder::basic;
    brpc::policy::MongoRequest request = create_update_requet(filter, update, opts);
    brpc::policy::MongoResponse* response = new brpc::policy::MongoResponse();
    brpc::Controller* cntl = new brpc::Controller;
    cntl->set_request_code(GetPrimaryPreferredRequestCode());
    google::protobuf::Closure* done = brpc::NewCallback(
            &LOGMongoResponse, cntl, response);
    database->client->channel->CallMethod(NULL, cntl, &request, response, done);
}

stdx::optional<bsoncxx::document::value> Collection::find_one_and_update(bsoncxx::document::view_or_value query, bsoncxx::document::view_or_value update,  const options::find_one_and_update& opts) {
    using namespace bsoncxx::builder::basic;
    document fauo_doc;
    fauo_doc.append(kvp("findAndModify", name));
    fauo_doc.append(kvp("query", query));
    fauo_doc.append(kvp("update", update));
    build_find_one_and_update_options_document(opts, &fauo_doc);
    fauo_doc.append(kvp("$db", database->name));
    brpc::policy::MongoRequest request;
    AddDoc2Request(fauo_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);

    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetPrimaryPreferredRequestCode());
    database->client->channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return make_document(kvp("n", "0"), kvp("ok", 0.0), kvp("err", cntl.ErrorText()));
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    if (view["value"].type() == bsoncxx::type::k_null) {
        LOG(INFO) << "view: " << bsoncxx::to_json(view);
        return stdx::nullopt;
    }
    return bsoncxx::document::value(view["value"].get_document().view());
}

stdx::optional<bsoncxx::document::value> Collection::find_one(bsoncxx::document::view_or_value filter, options::find opts) {
    opts.limit(1);
    Cursor cursor = find(filter, opts);
    if (cursor.begin() == cursor.end()) {
        return stdx::nullopt;
    }
    stdx::optional<bsoncxx::document::value> ret;
    ret.emplace(cursor.begin()->get_document().value);
    return ret;
}

void Collection::BulkUpdate::append(bsoncxx::document::view_or_value filter, bsoncxx::document::view_or_value update, const options::update& opts) {
    using namespace bsoncxx::builder::basic;
    document update_one_ele;
    update_one_ele.append(kvp("q", filter));
    update_one_ele.append(kvp("u", update));
    build_update_options_document(opts, &update_one_ele);
    updates.append(update_one_ele);
}

bsoncxx::document::value Collection::BulkUpdate::execute() {
    using namespace bsoncxx::builder::basic;
    document bulk_update_doc;
    bulk_update_doc.append(kvp("update", col->name));
    bulk_update_doc.append(kvp("updates", updates));
    bulk_update_doc.append(kvp("$db", col->database->name));
    brpc::policy::MongoRequest request;
    AddDoc2Request(bulk_update_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetPrimaryPreferredRequestCode());
    col->database->client->channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return make_document(kvp("n", "0"), kvp("ok", 0.0), kvp("err", cntl.ErrorText()));
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    return bsoncxx::document::value(view);
}

void Collection::BulkInsert::append(bsoncxx::document::view_or_value doc) {
    docs.append(doc);
}

bsoncxx::document::value Collection::BulkInsert::execute() {
    using namespace bsoncxx::builder::basic;
    document bulk_insert_doc;
    bulk_insert_doc.append(kvp("insert", col->name));
    bulk_insert_doc.append(kvp("documents", docs));
    bulk_insert_doc.append(kvp("$db", col->database->name));
    brpc::policy::MongoRequest request;
    AddDoc2Request(bulk_insert_doc, &request);
    request.mutable_header()->set_op_code(brpc::policy::OP_MSG);
    brpc::policy::MongoResponse response;
    brpc::Controller cntl;
    cntl.set_request_code(GetPrimaryPreferredRequestCode());
    col->database->client->channel->CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to access mongo, " << cntl.ErrorText();
        return make_document(kvp("n", "0"), kvp("ok", 0.0), kvp("err", cntl.ErrorText()));
    }
    bsoncxx::document::view view = GetViewFromRawBody(response.sections());
    return bsoncxx::document::value(view);
}

} // namespace mongo
} // namespace butil