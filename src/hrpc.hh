// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//

#ifndef HRPC_HH
#define HRPC_HH

#include "hrpc.pb.h"
#include "pb.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "nanomsg/src/nn.h"
#include "nanomsg/src/reqrep.h"

namespace hrpc {

const std::string kVersion = "0.0.1";

typedef pb::Message Message;
typedef pb::Closure Closure;
typedef pb::RpcController RpcController;

const int kMsgMaxSize = 9 * 1024 * 1024;
const int kMsgTimeOut = 60 * 1000; // milliseconds
const int kMsgBufSize = 2 * 1024 * 1024;

const char kMsgCodecVer100 = 0x01;

const int kErrCodeBadArgument = 2;
const int kErrCodeClientError = 3;
const int kErrCodeServerError = 4;
const int kErrCodeAccessDenied = 5;
const int kErrCodeDataNotFound = 6;
const int kErrCodeDataConflict = 7;

const int kMsgCompressNone = 0;
const int kMsgCompressSnappy = 2;
const int kMsgCompressGzip = 3;

int msgSocketSend(int fd, const std::string &msg);
int msgSocketRecv(int fd, std::string &msg);

std::string msgRepEncode(RepMeta *meta_entry, const pb::Message *msg_entry);

std::string msgReqEncode(ReqMeta *meta_entry, const pb::Message *msg_entry);

int msgReqMetaDecode(const std::string &str, ReqMeta *meta_entry);

int msgReqDataDecode(const std::string &str, const ReqMeta &meta_entry,
                     pb::Message *msg_entry);

int msgRepDecode(const std::string &str, RepMeta *meta_entry,
                 pb::Message *msg_entry);

struct ServiceMethodEntry {
   public:
    pb::Service *srv_;
    const pb::Message *req_;
    const pb::Message *rep_;
    const pb::MethodDescriptor *met_;
    ServiceMethodEntry(pb::Service *service, const pb::Message *req,
                       const pb::Message *rep,
                       const pb::MethodDescriptor *method)
        : srv_(service), req_(req), rep_(rep), met_(method) {}
};

typedef std::unordered_map<std::string, ServiceMethodEntry *> ServiceMethodMap;

const uint8_t AuthMacVersionBasic = 1;

class ServerContext;

class AuthMac {
   private:
    std::string secret_key_;

   public:
    std::string access_key_;
    AuthMac(std::string access_key, std::string secret_key) {
        access_key_ = access_key;
        secret_key_ = secret_key;
    };
    ~AuthMac() {};
    std::string Sign(const std::string &data);
    bool Valid(const ServerContext *ctx);
};

extern AuthMac *NewAuthMac(std::string access_key, std::string secret_key);

typedef std::unordered_map<std::string, AuthMac *> AuthMacMap;

class ClientContext : public pb::RpcController {
   private:
    int error_code_ = 0;
    std::string error_message_;

   public:
    AuthMac *auth_ = NULL;
    ClientContext() {
        Reset();
    };
    ClientContext(AuthMac *auth) : auth_(auth) {};
    ~ClientContext() {};
    virtual void Reset() {
        auth_ = NULL;
        error_code_ = 0;
        error_message_ = "";
    };
    virtual bool Failed() const {
        return error_code_ > 0;
    };
    virtual std::string ErrorText() const {
        return error_message_;
    };
    virtual int ErrorCode() const {
        return error_code_;
    };
    virtual std::string ErrorMessage() const {
        return error_message_;
    };
    virtual void StartCancel() {
        return; // TODO
    };
    virtual void SetFailed(const int code, const std::string &reason) {
        error_code_ = code;
        error_message_ = reason;
    };
    virtual void SetFailed(const std::string &reason) {
        if (error_code_ < 1) {
            error_code_ = 1;
        }
        error_message_ = reason;
    };
    virtual bool IsCanceled() const {
        return true; // TODO
    };
    virtual void NotifyOnCancel(Closure *callback) {
        if (callback != NULL) {
            callback->Run();
        }
    };
    bool OK() const {
        return (error_code_ == 0);
    };
};

class ServerContext : public pb::RpcController {
   private:
    int error_code_;
    std::string error_message_;

   public:
    std::string auth_data_;
    ServerContext() {
        Reset();
    };
    ~ServerContext() {};
    virtual void Reset() {
        auth_data_ = "";
        error_code_ = 0;
        error_message_ = "";
    };
    virtual bool Failed() const {
        return error_code_ > 0;
    };
    virtual std::string ErrorText() const {
        return error_message_;
    };
    virtual void StartCancel() {
        return; // TODO
    };
    virtual void SetFailed(const int code, const std::string &reason) {
        error_code_ = code;
        error_message_ = reason;
    };
    virtual void SetFailed(const std::string &reason) {
        if (error_code_ < 1) {
            error_code_ = 1;
        }
        error_message_ = reason;
    };
    virtual bool IsCanceled() const {
        return true; // TODO
    };
    virtual void NotifyOnCancel(Closure *callback) {
        if (callback != NULL) {
            callback->Run();
        }
    };
};

class Server {
   private:
    std::string addr_;
    int sock_;
    ServiceMethodMap map_;
    ServiceMethodEntry *mapMethodEntry(std::string metname);
    AuthMacMap auth_map_;
    AuthMac *mapAuthMacEntry(std::string access_key);
    void handler();

   public:
    Server(std::string addr, int sock) {
        addr_ = addr;
        sock_ = sock;
    };
    ~Server() {
        if (sock_ >= 0) {
            ::nn_close(sock_);
        }
    };
    void RegisterService(pb::Service *service);
    void RegisterAuthMac(AuthMac *auth);
    static void *Start(void *ptr);
};

extern Server *NewServer(std::string ip, int port);

class Channel : public pb::RpcChannel {
   private:
    int sock_;
    int epid_;
    int tto_;
    int reconnect();

   public:
    std::string id_;
    std::string addr_;
    bool active;
    Channel(std::string addr, int sock, int epid) {
        addr_ = addr;
        sock_ = sock;
        epid_ = epid;
        active = false;
    };
    virtual ~Channel() {
        Close();
    };
    virtual void CallMethod(const pb::MethodDescriptor *method,
                            pb::RpcController *controller,
                            const pb::Message *req, pb::Message *rep,
                            pb::Closure *done);
    void Timeout(int ms);
    void Close() {
        if (sock_ >= 0) {
            if (epid_ >= 0) {
                ::nn_shutdown(sock_, epid_);
            }
            ::nn_close(sock_);
            sock_ = -1;
            epid_ = -1;
        }
    };
};

Channel *NewChannel(std::string addr);

typedef std::unordered_map<std::string, Channel *> ChannelList;

class ChannelPool {
   private:
    static std::mutex mu_;
    static ChannelList channels_;

   public:
    ChannelPool() {};
    ~ChannelPool() {};
    static Channel *Pull(const std::string &addr);
    static void Push(Channel *chan);
};

} // namespace hrpc

#endif
