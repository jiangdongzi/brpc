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

#include <cstddef>
#include <google/protobuf/descriptor.h>         // MethodDescriptor
#include <google/protobuf/message.h>            // Message
#include <gflags/gflags.h>
#include "butil/time.h" 
#include "butil/iobuf.h"                         // butil::IOBuf
#include "brpc/controller.h"               // Controller
#include "brpc/socket.h"                   // Socket
#include "brpc/server.h"                   // Server
#include "brpc/span.h"
#include "brpc/mongo_head.h"
#include "brpc/details/server_private_accessor.h"
#include "brpc/details/controller_private_accessor.h"
#include "brpc/mongo_service_adaptor.h"
#include "brpc/policy/most_common_message.h"
#include "brpc/policy/nshead_protocol.h"
#include "brpc/policy/mongo.pb.h"
#include "brpc/details/usercode_backup_pool.h"
#include "bsoncxx/json.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/document/view.hpp"

extern "C" {
void bthread_assign_data(void* data);
}


namespace brpc {
namespace policy {

struct SendMongoResponse : public google::protobuf::Closure {
    SendMongoResponse(const Server *server) :
        status(NULL),
        received_us(0L),
        server(server) {}
    ~SendMongoResponse();
    void Run();

    MethodStatus* status;
    int64_t received_us;
    const Server *server;
    Controller cntl;
    MongoRequest req;
    MongoResponse res;
};

SendMongoResponse::~SendMongoResponse() {
    LogErrorTextAndDelete(false)(&cntl);
}

void SendMongoResponse::Run() {
    std::unique_ptr<SendMongoResponse> delete_self(this);
    ConcurrencyRemover concurrency_remover(status, &cntl, received_us);
    Socket* socket = ControllerPrivateAccessor(&cntl).get_sending_socket();

    if (cntl.IsCloseConnection()) {
        socket->SetFailed();
        return;
    }
    
    const MongoServiceAdaptor* adaptor =
            server->options().mongo_service_adaptor;
    butil::IOBuf res_buf;
    if (cntl.Failed()) {
        adaptor->SerializeError(res.header().response_to(), &res_buf);
    } else if (res.has_message()) {
        mongo_head_t header = {
            res.header().message_length(),
            res.header().request_id(),
            res.header().response_to(),
            res.header().op_code()
        };
        res_buf.append(static_cast<const void*>(&header), sizeof(mongo_head_t));
        int32_t response_flags = res.response_flags();
        int64_t cursor_id = res.cursor_id();
        int32_t starting_from = res.starting_from();
        int32_t number_returned = res.number_returned();
        res_buf.append(&response_flags, sizeof(response_flags));
        res_buf.append(&cursor_id, sizeof(cursor_id));
        res_buf.append(&starting_from, sizeof(starting_from));
        res_buf.append(&number_returned, sizeof(number_returned));
        res_buf.append(res.message());
    }

    if (!res_buf.empty()) {
        // Have the risk of unlimited pending responses, in which case, tell
        // users to set max_concurrency.
        Socket::WriteOptions wopt;
        wopt.ignore_eovercrowded = true;
        if (socket->Write(&res_buf, &wopt) != 0) {
            PLOG(WARNING) << "Fail to write into " << *socket;
            return;
        }
    }
}

ParseResult ParseMongoMessage(butil::IOBuf* source,
                              Socket* socket, bool /*read_eof*/, const void *arg) {
    const Server* server = static_cast<const Server*>(arg);
    const MongoServiceAdaptor* adaptor = server->options().mongo_service_adaptor;
    if (NULL == adaptor) {
        // The server does not enable mongo adaptor.
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }

    char buf[sizeof(mongo_head_t)];
    const char *p = (const char *)source->fetch(buf, sizeof(buf));
    if (NULL == p) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    mongo_head_t header = *(const mongo_head_t*)p;
    header.make_host_endian();
    if (!is_mongo_opcode(header.op_code)) {
        // The op_code plays the role of "magic number" here.
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }
    if (header.message_length < (int32_t)sizeof(mongo_head_t)) {
        // definitely not a valid mongo packet.
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }
    uint32_t body_len = static_cast<uint32_t>(header.message_length);
    if (body_len > FLAGS_max_body_size) {
        return MakeParseError(PARSE_ERROR_TOO_BIG_DATA);
    } else if (source->length() < body_len) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    // Mongo protocol is a protocol with state. Each connection has its own
    // mongo context. (e.g. last error occured on the connection, the cursor
    // created by the last Query). The context is stored in
    // socket::_input_message, and created at the first time when msg
    // comes over the socket.
    Destroyable *socket_context_msg = socket->parsing_context();
    if (NULL == socket_context_msg) {
        MongoContext *context = adaptor->CreateSocketContext();
        if (NULL == context) {
            return MakeParseError(PARSE_ERROR_NO_RESOURCE);
        }
        socket_context_msg = new MongoContextMessage(context);
        socket->reset_parsing_context(socket_context_msg);
    }
    policy::MostCommonMessage* msg = policy::MostCommonMessage::Get();
    source->cutn(&msg->meta, sizeof(buf));
    size_t act_body_len = source->cutn(&msg->payload, body_len - sizeof(buf));
    if (act_body_len != body_len - sizeof(buf)) {
        CHECK(false);     // Very unlikely, unless memory is corrupted.
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }
    return MakeMessage(msg);
}

// Defined in baidu_rpc_protocol.cpp
void EndRunningCallMethodInPool(
    ::google::protobuf::Service* service,
    const ::google::protobuf::MethodDescriptor* method,
    ::google::protobuf::RpcController* controller,
    const ::google::protobuf::Message* request,
    ::google::protobuf::Message* response,
    ::google::protobuf::Closure* done);

void ProcessMongoRequest(InputMessageBase* msg_base) {
    DestroyingPtr<MostCommonMessage> msg(static_cast<MostCommonMessage*>(msg_base));
    SocketUniquePtr socket_guard(msg->ReleaseSocket());
    Socket* socket = socket_guard.get();
    const Server* server = static_cast<const Server*>(msg_base->arg());
    ScopedNonServiceError non_service_error(server);

    char buf[sizeof(mongo_head_t)];
    const char *p = (const char *)msg->meta.fetch(buf, sizeof(buf));
    const mongo_head_t *header = (const mongo_head_t*)p;

    const google::protobuf::ServiceDescriptor* srv_des = MongoService::descriptor();
    if (1 != srv_des->method_count()) {
        LOG(WARNING) << "method count:" << srv_des->method_count()
                     << " of MongoService should be equal to 1!";
    }

    const Server::MethodProperty *mp =
            ServerPrivateAccessor(server)
            .FindMethodPropertyByFullName(srv_des->method(0)->full_name());

    MongoContextMessage *context_msg =
        dynamic_cast<MongoContextMessage*>(socket->parsing_context());
    if (NULL == context_msg) {
        LOG(WARNING) << "socket context wasn't set correctly";
        return;
    }

    SendMongoResponse* mongo_done = new SendMongoResponse(server);
    mongo_done->cntl.set_mongo_session_data(context_msg->context());

    ControllerPrivateAccessor accessor(&(mongo_done->cntl));
    accessor.set_server(server)
        .set_security_mode(server->options().security_mode())
        .set_peer_id(socket->id())
        .set_remote_side(socket->remote_side())
        .set_local_side(socket->local_side())
        .set_auth_context(socket->auth_context())
        .set_request_protocol(PROTOCOL_MONGO)
        .set_begin_time_us(msg->received_us())
        .move_in_server_receiving_sock(socket_guard);

    // Tag the bthread with this server's key for
    // thread_local_data().
    if (server->thread_local_options().thread_local_data_factory) {
        bthread_assign_data((void*)&server->thread_local_options());
    }
    do {
        if (!server->IsRunning()) {
            mongo_done->cntl.SetFailed(ELOGOFF, "Server is stopping");
            break;
        }

        if (!ServerPrivateAccessor(server).AddConcurrency(&(mongo_done->cntl))) {
            mongo_done->cntl.SetFailed(
                ELIMIT, "Reached server's max_concurrency=%d",
                server->options().max_concurrency);
            break;
        }
        if (FLAGS_usercode_in_pthread && TooManyUserCode()) {
            mongo_done->cntl.SetFailed(ELIMIT, "Too many user code to run when"
                                       " -usercode_in_pthread is on");
            break;
        }

        if (NULL == mp ||
            mp->service->GetDescriptor() == BadMethodService::descriptor()) {
            mongo_done->cntl.SetFailed(ENOMETHOD, "Fail to find default_method");
            break;
        }
        // Switch to service-specific error.
        non_service_error.release();
        MethodStatus* method_status = mp->status;
        mongo_done->status = method_status;
        if (method_status) {
            int rejected_cc = 0;
            if (!method_status->OnRequested(&rejected_cc)) {
                mongo_done->cntl.SetFailed(
                    ELIMIT, "Rejected by %s's ConcurrencyLimiter, concurrency=%d",
                    mp->method->full_name().c_str(), rejected_cc);
                break;
            }
        }
        
        if (!MongoOp_IsValid(header->op_code)) {
            mongo_done->cntl.SetFailed(EREQUEST, "Unknown op_code:%d", header->op_code);
            break;
        }
        
        mongo_done->cntl.set_log_id(header->request_id);
        const std::string &body_str = msg->payload.to_string();
        mongo_done->req.set_message(body_str.c_str(), body_str.size());
        mongo_done->req.mutable_header()->set_message_length(header->message_length);
        mongo_done->req.mutable_header()->set_request_id(header->request_id);
        mongo_done->req.mutable_header()->set_response_to(header->response_to);
        mongo_done->req.mutable_header()->set_op_code(
                static_cast<MongoOp>(header->op_code));
        mongo_done->res.mutable_header()->set_response_to(header->request_id);
        mongo_done->received_us = msg->received_us();

        google::protobuf::Service* svc = mp->service;
        const google::protobuf::MethodDescriptor* method = mp->method;
        accessor.set_method(method);
        
        if (!FLAGS_usercode_in_pthread) {
            return svc->CallMethod(
                method, &(mongo_done->cntl), &(mongo_done->req),
                &(mongo_done->res), mongo_done);
        }
        if (BeginRunningUserCode()) {
            return svc->CallMethod(
                method, &(mongo_done->cntl), &(mongo_done->req),
                &(mongo_done->res), mongo_done);
            return EndRunningUserCodeInPlace();
        } else {
            return EndRunningCallMethodInPool(
                svc, method, &(mongo_done->cntl), &(mongo_done->req),
                &(mongo_done->res), mongo_done);
        }
    } while (false);

    mongo_done->Run();
}

void PackRpcRequest(butil::IOBuf* req_buf,
                    SocketMessage**,
                    uint64_t correlation_id,
                    const google::protobuf::MethodDescriptor* /*method*/,
                    Controller* cntl,
                    const butil::IOBuf& request_body,
                    const Authenticator* auth) {

    ControllerPrivateAccessor accessor(cntl);
    accessor.get_sending_socket()->set_correlation_id(correlation_id);
    req_buf->append(request_body);
}

void SerializeMongoRequest(butil::IOBuf* buf,
                          Controller* cntl,
                          const google::protobuf::Message* pbreq) {
    const MongoRequest* req = static_cast<const MongoRequest*>(pbreq);
    mongo_head_t header = {
        (int)(sizeof(mongo_head_t) + req->fullcollectionname().size() + 2 * sizeof(int32_t) + req->message().size()),
        1,
        0,
        DB_QUERY
    };
    buf->append(&header, sizeof(header));
    buf->append(req->fullcollectionname());
    int number_to_skip = req->numbertoskip();
    int number_to_return = req->numbertoreturn();
    buf->append(&number_to_skip, sizeof(number_to_skip));
    buf->append(&number_to_return, sizeof(number_to_return));
    buf->append(req->message());
}

void ProcessMongoResponse(InputMessageBase* msg_base) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<MostCommonMessage> msg(static_cast<MostCommonMessage*>(msg_base));

    char buf[sizeof(mongo_head_t)];
    const char *p = (const char *)msg->meta.fetch(buf, sizeof(buf));
    const mongo_head_t *header = (const mongo_head_t*)p;

    const bthread_id_t cid = { static_cast<uint64_t>(header->response_to) };
    Controller* cntl = NULL;

    StreamId remote_stream_id = meta.has_stream_settings() ? meta.stream_settings().stream_id(): INVALID_STREAM_ID;

    const int rc = bthread_id_lock(cid, (void**)&cntl);
    if (rc != 0) {
        LOG_IF(ERROR, rc != EINVAL && rc != EPERM)
            << "Fail to lock correlation_id=" << cid << ": " << berror(rc);
        if (remote_stream_id != INVALID_STREAM_ID) {
            SendStreamRst(msg->socket(), meta.stream_settings().stream_id());
        }
        return;
    }
    
    ControllerPrivateAccessor accessor(cntl);
    if (remote_stream_id != INVALID_STREAM_ID) {
        accessor.set_remote_stream_settings(
                new StreamSettings(meta.stream_settings()));
    }

    if (!meta.user_fields().empty()) {
        for (const auto& it : meta.user_fields()) {
            (*cntl->response_user_fields())[it.first] = it.second;
        }
    }

    Span* span = accessor.span();
    if (span) {
        span->set_base_real_us(msg->base_real_us());
        span->set_received_us(msg->received_us());
        span->set_response_size(msg->meta.size() + msg->payload.size() + 12);
        span->set_start_parse_us(start_parse_us);
    }
    const RpcResponseMeta &response_meta = meta.response();
    const int saved_error = cntl->ErrorCode();
    do {
        if (response_meta.error_code() != 0) {
            // If error_code is unset, default is 0 = success.
            cntl->SetFailed(response_meta.error_code(), 
                                  "%s", response_meta.error_text().c_str());
            break;
        } 
        // Parse response message iff error code from meta is 0
        butil::IOBuf res_buf;
        const int res_size = msg->payload.length();
        butil::IOBuf* res_buf_ptr = &msg->payload;
        if (meta.has_attachment_size()) {
            if (meta.attachment_size() > res_size) {
                cntl->SetFailed(
                    ERESPONSE,
                    "attachment_size=%d is larger than response_size=%d",
                    meta.attachment_size(), res_size);
                break;
            }
            int body_without_attachment_size = res_size - meta.attachment_size();
            msg->payload.cutn(&res_buf, body_without_attachment_size);
            res_buf_ptr = &res_buf;
            cntl->response_attachment().swap(msg->payload);
        }

        const CompressType res_cmp_type = (CompressType)meta.compress_type();
        cntl->set_response_compress_type(res_cmp_type);
        if (cntl->response()) {
            if (!ParseFromCompressedData(
                    *res_buf_ptr, cntl->response(), res_cmp_type)) {
                cntl->SetFailed(
                    ERESPONSE, "Fail to parse response message, "
                    "CompressType=%s, response_size=%d", 
                    CompressTypeToCStr(res_cmp_type), res_size);
            }
        } // else silently ignore the response.        
    } while (0);
    // Unlocks correlation_id inside. Revert controller's
    // error code if it version check of `cid' fails
    msg.reset();  // optional, just release resource ASAP
    accessor.OnResponse(cid, saved_error);
}

}  // namespace policy
} // namespace brpc
