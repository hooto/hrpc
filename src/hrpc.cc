// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//

#include "hrpc.hh"
#include "hrpc_util.hh"

#include <unistd.h>
#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <sys/time.h>
#include <mutex>

#include "fmt/format.h"
#include "snappy/snappy.h"

#include "nanomsg/src/nn.h"
#include "nanomsg/src/reqrep.h"

#include "cjet/log.hh"

namespace hrpc {

int msgSocketSend(int fd, const std::string &msg) {

    int n = ::nn_send(fd, msg.c_str(), msg.size(), 0);
    if (n < 0) {
        return -1;
    }

    return n;
}

int msgSocketRecv(int fd, std::string &s) {

    char *buf = NULL;
    int n = ::nn_recv(fd, &buf, NN_MSG, 0);
    if (n > 0) {
        s.append(buf, n);
    }
    if (buf != NULL) {
        ::nn_freemsg(buf);
    }

    return n;
}

std::string msgRepEncode(RepMeta *meta_entry, const pb::Message *msg_entry) {

    if (meta_entry == NULL) {
        return "";
    }

    std::string buf({kMsgCodecVer100, 0x00});

    std::string msg_enc;
    if (msg_entry != NULL) {
        msg_entry->SerializeToString(&msg_enc);
        if (msg_enc.size() > 10) {
            std::string msg_cmp;
            int n = snappy::Compress(msg_enc.c_str(), msg_enc.size(), &msg_cmp);
            if (n > 0 && msg_cmp.size() < msg_enc.size()) {
                msg_enc = msg_cmp;
                meta_entry->set_msg_compress_type(kMsgCompressSnappy);
            }
        }
        meta_entry->set_msg_size(msg_enc.size());
    }

    std::string meta_enc;
    meta_entry->SerializeToString(&meta_enc);
    if (meta_enc.size() > 255) {
        return "";
    }

    buf[1] = uint8_t(meta_enc.size());
    buf.append(meta_enc);

    if (meta_entry->msg_size() > 0) {
        buf.append(msg_enc);
    }

    return buf;
}

std::string msgReqEncode(const ClientContext *ctx, ReqMeta *meta_entry,
                         const pb::Message *msg_entry) {

    if (meta_entry == NULL) {
        return "";
    }

    std::string buf({kMsgCodecVer100, 0x00});

    std::string msg_enc;
    if (msg_entry != NULL) {
        msg_entry->SerializeToString(&msg_enc);
        if (msg_enc.size() > 10) {
            std::string msg_cmp;
            int n = snappy::Compress(msg_enc.c_str(), msg_enc.size(), &msg_cmp);
            if (n > 0 && msg_cmp.size() < msg_enc.size()) {
                msg_enc = msg_cmp;
                meta_entry->set_msg_compress_type(kMsgCompressSnappy);
            }
        }
        meta_entry->set_msg_size(msg_enc.size());
    }

    //
    if (ctx != NULL) {
        if (ctx->auth_ != NULL) {
            meta_entry->set_auth_access_key(ctx->auth_->access_key_);
            meta_entry->set_auth_data(ctx->auth_->Sign(msg_enc));
        }
    }

    //
    std::string meta_enc;
    meta_entry->SerializeToString(&meta_enc);
    if (meta_enc.size() > 255) {
        return "";
    }

    buf[1] = uint8_t(meta_enc.size());
    buf.append(meta_enc);

    if (meta_entry->msg_size() > 0) {
        buf.append(msg_enc);
    }

    return buf;
};

int msgReqMetaDecode(const std::string &str, ReqMeta *meta_entry) {

    if (str.size() < 2) {
        return -1;
    }

    if (str[0] != kMsgCodecVer100) {
        return -1;
    }

    // meta
    int meta_size = int(str[1]);
    if (2 + meta_size > str.size()) {
        return -1;
    }
    meta_entry->ParseFromString(str.substr(2, meta_size));

    return 0;
};

int msgReqDataDecode(const std::string &str, const ReqMeta &meta_entry,
                     pb::Message *msg_entry) {
    if (str.size() < 4 || meta_entry.msg_size() < 1) {
        return -1;
    }

    if (str[0] != kMsgCodecVer100) {
        return -1;
    }

    // message
    int offset = 2 + int(str[1]);
    if (offset + meta_entry.msg_size() > str.size()) {
        return -1;
    }
    switch (meta_entry.msg_compress_type()) {

        case kMsgCompressNone:
            msg_entry->ParseFromString(
                str.substr(offset, meta_entry.msg_size()));
            break;

        case kMsgCompressSnappy: {
            std::string msg_unc;
            if (!snappy::Uncompress(
                     str.substr(offset, meta_entry.msg_size()).c_str(),
                     meta_entry.msg_size(), &msg_unc)) {
                return -1;
            }
            msg_entry->ParseFromString(msg_unc);
        } break;

        default:
            return -1;
    }

    return 0;
};

int msgRepDecode(const std::string &str, RepMeta *meta_entry,
                 pb::Message *msg_entry) {

    if (str.size() < 2) {
        return -1;
    }

    if (str[0] != kMsgCodecVer100) {
        return -1;
    }

    // meta
    int meta_size = int(str[1]);
    if (2 + meta_size > str.size()) {
        return -1;
    }
    meta_entry->ParseFromString(str.substr(2, meta_size));

    // message
    int offset = 2 + meta_size;
    if (offset + meta_entry->msg_size() > str.size()) {
        return -1;
    }
    switch (meta_entry->msg_compress_type()) {

        case kMsgCompressNone:
            msg_entry->ParseFromString(
                str.substr(offset, meta_entry->msg_size()));
            break;

        case kMsgCompressSnappy: {
            std::string msg_unc;
            if (!snappy::Uncompress(
                     str.substr(offset, meta_entry->msg_size()).c_str(),
                     meta_entry->msg_size(), &msg_unc)) {
                return -1;
            }
            msg_entry->ParseFromString(msg_unc);
        } break;

        default:
            return -1;
    }

    return 0;
};

void Server::RegisterService(pb::Service *service) {

    const pb::ServiceDescriptor *descriptor = service->GetDescriptor();

    for (int i = 0; i < descriptor->method_count(); i++) {

        const pb::MethodDescriptor *method = descriptor->method(i);

        std::string srv_name = std::string(method->full_name());

        int i1 = srv_name.find_first_of(".");
        int i2 = srv_name.find_last_of(".");

        std::string service_name = srv_name.substr(i1 + 1, i2 - i1 - 1);
        std::string method_name = srv_name.substr(i2 + 1);

        if (service_name.size() > 64 || method_name.size() > 64) {
            std::cerr << "RPC: Invalid Service/Method Name (" << service_name
                      << "/" << method_name << ")\n";
            continue;
        }

        std::string mapi = srv_name.substr(i1 + 1);

        ServiceMethodMap::const_iterator iter = map_.find(mapi);

        if (iter == map_.end()) {
            const pb::Message *req = &service->GetRequestPrototype(method);
            const pb::Message *rep = &service->GetResponsePrototype(method);
            map_[mapi] = new ServiceMethodEntry(service, req, rep, method);
        }
    }
}

ServiceMethodEntry *Server::mapMethodEntry(std::string name) {
    if (map_.size() > 0) {
        ServiceMethodMap::iterator it = map_.find(name);
        if (it != map_.end()) {
            return it->second;
        }
    }
    return NULL;
}

void Server::RegisterAuthMac(AuthMac *auth) {
    AuthMacMap::const_iterator iter = auth_map_.find(auth->access_key_);
    if (iter == auth_map_.end()) {
        auth_map_[auth->access_key_] = auth;
    }
}

AuthMac *Server::mapAuthMacEntry(std::string access_key) {
    if (auth_map_.size() > 0) {
        AuthMacMap::iterator it = auth_map_.find(access_key);
        if (it != auth_map_.end()) {
            return it->second;
        }
    }
    return NULL;
}

Server *NewServer(std::string ip, int port) {

    if (port < 1 || port > 65535) {
        throw std::invalid_argument(
            fmt::sprintf("invalid network port %d", port));
    }

    std::string addr = fmt::sprintf("tcp://%s:%d", ip, port);

    int sock = ::nn_socket(AF_SP, NN_REP);
    if (sock < 0) {
        throw std::invalid_argument("failed to create socket");
    }

    if (::nn_bind(sock, addr.c_str()) == -1) {
        ::nn_close(sock);
        std::cerr << "nn_bind " << addr << "\n";
        throw std::invalid_argument("failed to bind socket");
    }

    int opt = kMsgMaxSize;
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVMAXSIZE, &opt, sizeof(opt));

    opt = kMsgTimeOut;
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_SNDTIMEO, &opt, sizeof(opt));
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVTIMEO, &opt, sizeof(opt));

    Server *srv = new Server(addr, sock);

    return srv;
};

void *Server::Start(void *ptr) {

    Server *p = (Server *)ptr;
    // int stats_num = 0;

    for (;;) {

        // stats_num += 1;
        // if (stats_num % 1000 == 0) {
        //     std::cout << __FILE__ << ", " << __LINE__ << ", " << stats_num
        //               << "\n";
        // }

        p->handler();
    }

    return NULL;
}

void Server::handler() {

    std::string buf;
    int n = msgSocketRecv(sock_, buf);
    if (n < 1) {
        return;
    }

    ReqMeta req_meta_entry;
    if (msgReqMetaDecode(buf, &req_meta_entry) != 0) {
        return;
    }

    std::string mapi =
        req_meta_entry.service_name() + "." + req_meta_entry.method_name();

    std::string rep_data;
    RepMeta rep_meta_entry;

    ServiceMethodEntry *me = mapMethodEntry(mapi);

    if (me != NULL) {

        const pb::MethodDescriptor *method = me->met_;
        pb::Message *req = me->req_->New();
        pb::Message *rep = me->rep_->New();
        ServerContext *ctx = new ServerContext();

        bool authed = false;

        if (req_meta_entry.auth_data().size() > 0) {
            ctx->auth_data_ = req_meta_entry.auth_data();
        }

        if (auth_map_.size() > 0) {

            if (req_meta_entry.auth_access_key() != "" &&
                req_meta_entry.auth_data() != "") {

                AuthMac *am = mapAuthMacEntry(req_meta_entry.auth_access_key());
                if (am != NULL) {
                    if (am->Valid(ctx)) {
                        authed = true;
                    }
                }
            }
        } else {
            authed = true;
        }

        if (authed) {

            if (msgReqDataDecode(buf, req_meta_entry, req) == 0) {

                me->srv_->CallMethod(method, ctx, req, rep, NULL);

                if (rep->ByteSize() > 0) {
                    rep_data = msgRepEncode(&rep_meta_entry, rep);
                }
            } else {
                rep_meta_entry.set_error_code(kErrCodeBadArgument);
            }

        } else {
            rep_meta_entry.set_error_code(kErrCodeAccessDenied);
        }

        delete ctx;
        delete req;
        delete rep;
    } else {
        rep_meta_entry.set_error_code(kErrCodeBadArgument);
        rep_meta_entry.set_error_message("no service/method found");
    }

    if (rep_data.size() == 0) {
        rep_data = msgRepEncode(&rep_meta_entry, NULL);
    }

    msgSocketSend(sock_, rep_data);
}

void Channel::CallMethod(const pb::MethodDescriptor *method,
                         pb::RpcController *ctr, const pb::Message *req,
                         pb::Message *rep, pb::Closure *done) {

    int64_t tn = util::timenow_us();
    std::string srv_name = std::string(method->full_name());

    int i1 = srv_name.find_first_of(".");
    int i2 = srv_name.find_last_of(".");

    ReqMeta meta;
    meta.set_service_name(srv_name.substr(i1 + 1, i2 - i1 - 1));
    meta.set_method_name(srv_name.substr(i2 + 1));

    ClientContext *ctx = NULL;
    if (ctr != NULL) {
        ctx = static_cast<ClientContext *>(ctr);
    }

    std::string msg = msgReqEncode(ctx, &meta, req);
    if (msg.size() > 0) {

        // std::cout << " Channel Send " << msg.size() << "\n";
        for (int i = 0; i < 2; i++) {

            if (msgSocketSend(sock_, msg) >= 0) {

                std::string rep_msg;
                int n = msgSocketRecv(sock_, rep_msg);
                if (n > 0) {
                    RepMeta rep_meta;
                    msgRepDecode(rep_msg, &rep_meta, rep);
                    if (ctx != NULL && rep_meta.error_code() > 0) {
                        ctx->SetFailed(rep_meta.error_code(),
                                       rep_meta.error_message());
                    }
                    break;
                } else {
                    CJET_LOG("debug", "rpc/channel/recv err %s",
                             ::nn_strerror(errno));
                }
            } else {
                CJET_LOG("debug", "rpc/channel/send err %s",
                         ::nn_strerror(errno));
            }

            usleep(10000);
            if (reconnect() == -1) {
                break;
            }
        }
    }

    ChannelPool::Push(this);
};

void Channel::Timeout(int ms) {

    if (ms < 1000) {
        ms = 1000;
    } else if (ms > kMsgTimeOut) {
        ms = kMsgTimeOut;
    }
    tto_ = ms;

    if (sock_ >= 0) {
        ::nn_setsockopt(sock_, NN_SOL_SOCKET, NN_SNDTIMEO, &tto_, sizeof(tto_));
        ::nn_setsockopt(sock_, NN_SOL_SOCKET, NN_RCVTIMEO, &tto_, sizeof(tto_));
    }
}

int Channel::reconnect() {

    ::nn_shutdown(sock_, epid_);
    int epid = ::nn_connect(sock_, addr_.c_str());
    if (epid == -1) {
        CJET_LOG("warn", "rpc/channel/reconnect err %s", ::nn_strerror(errno));
        return -1;
    }

    epid_ = epid;

    CJET_LOG("debug", "rpc/channel/reconnect %d, %d, %s", sock_, epid_,
             addr_.c_str());

    return 0;
}

Channel *NewChannel(std::string addr) {

    int sock = ::nn_socket(AF_SP, NN_REQ);
    if (sock < 0) {
        return NULL;
    }

    int epid = ::nn_connect(sock, addr.c_str());
    if (epid == -1) {
        ::nn_close(sock);
        return NULL;
    }

    int _mms = kMsgMaxSize;
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVMAXSIZE, &_mms, sizeof(_mms));

    int _mbs = kMsgBufSize;
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_SNDBUF, &_mbs, sizeof(_mbs));
    ::nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVBUF, &_mbs, sizeof(_mbs));

    Channel *c = new Channel(addr, sock, epid);

    c->Timeout(kMsgTimeOut);

    return c;
};

std::mutex ChannelPool::mu_;
ChannelList ChannelPool::channels_;

Channel *ChannelPool::Pull(const std::string &addr) {

    Channel *ch = NULL;

    for (int retry = 0; retry < 10; retry++) {

        mu_.lock();

        for (int i = 0; i < 4; i++) {
            std::string k = fmt::sprintf("%s:%d", addr.c_str(), i);
            auto v = channels_.find(k);
            if (v != channels_.end()) {
                if (!v->second->active) {
                    ch = v->second;
                    break;
                }
            } else {
                ch = NewChannel(addr);
                if (ch != NULL) {
                    ch->id_ = k;
                    channels_[k] = ch;
                }
                break;
            }
        }

        if (ch != NULL) {
            ch->active = true;
        }

        mu_.unlock();

        if (ch != NULL) {
            break;
        }

        usleep(10000);
    }

    return ch;
};

void ChannelPool::Push(Channel *ch) {
    mu_.lock();
    // std::cout << "RPC PUSH " << ch->id_ << "\n";
    ch->active = false;
    mu_.unlock();
}

std::string AuthMac::Sign(const std::string &data) {
    std::string buf;
    buf.append(1, AuthMacVersionBasic);
    buf.append(secret_key_);
    return buf;
}

bool AuthMac::Valid(const ServerContext *ctx) {
    if (ctx->auth_data_.size() < 2) {
        return false;
    }
    uint8_t stype = ctx->auth_data_[0];
    switch (stype) {
        case AuthMacVersionBasic:
            if (ctx->auth_data_.substr(1) == secret_key_) {
                return true;
            }
            break;
    }
    return false;
}

AuthMac *NewAuthMac(std::string access_key, std::string secret_key) {
    return new AuthMac(access_key, secret_key);
};

} // namespace hrpc
