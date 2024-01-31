#include <bvar/mico_bvar.h>
#include "brpc/metric_rpc.pb.h"

bthread_t bvar_stat_tid;
class PrometheusDumper : public bvar::Dumper {
public:
    bool dump(const std::string& name,
              const butil::StringPiece& description) {
        auto* new_metric = req.add_metric();
        new_metric->set_key(name);
        new_metric->set_value(description.as_string());
        return true;
    }
    brpc_metrics::MetricRequest req;
};

static brpc::Channel& GetPrometheusChannel (const std::string& pushgateway_server) {
  static brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = "h2:grpc";
  options.max_retry = 3;
  options.timeout_ms = 1000;
  const auto rc = channel.Init(pushgateway_server.c_str(), "", &options);
  if (rc != 0) {
    LOG(ERROR) << "GetPrometheusChannel: init channel failed, error_code: " << rc;
    std::abort();
  }
  return channel;
}

static void* dump_bvar(void* arg) {
  LOG(INFO) << "start dump mico bvar";
  PrometheusDumper d;
  std::unique_ptr<std::string> pushgateway_server_ptr(static_cast<std::string*>(arg));
  while (true) {
    int rv = bthread_usleep(1000 * 1000 * 10); //sleep 10s
    if (rv < 0) {
      return nullptr;
    }
    bvar::DumpOptions opts;
    bvar::MVariable::dump_exposed(&d, &opts);
    if (d.req.metric_size() == 0) continue;
    static brpc::Channel& channel = GetPrometheusChannel(*pushgateway_server_ptr);
    brpc_metrics::MetricRequest req;
    brpc_metrics::MetricResponse rsp;
    req.Swap(&d.req);
    brpc_metrics::MetricService_Stub stub(&channel);
    brpc::Controller cntl;
    stub.CollectMetrics(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "dump bvar failed, error_code: " << cntl.ErrorCode() << ", error_text: " << cntl.ErrorText();
    } else {
        LOG(INFO) << "dump bvar to prometheus, metric size: " << req.metric_size();
    }
  }

  return nullptr;
}

void start_stat_bvar(const std::string& pushgateway_server) {
    google::SetCommandLineOption("bvar_max_dump_multi_dimension_metric_number", "1000");
    google::SetCommandLineOption("bvar_dump_interval", "180");
    std::unique_ptr<std::string> pushgateway_server_ptr(new std::string(pushgateway_server));
    bthread_start_background(&bvar_stat_tid, nullptr, dump_bvar, pushgateway_server_ptr.release());
}

std::string brpc_get_host_name(){
    char name[256];
    gethostname(name, sizeof(name));
    std::string host_name(name);
    return host_name;
}

std::string brpc_get_app_name(){
    char path[1024] = {0};
    int r = readlink("/proc/self/exe", path, 1024);
    if (r == -1) {
        strcpy(path,  "/data/svr/default");
    }
    std::string exec_name = path;
    std::string app_name = exec_name.substr(exec_name.find_last_of('/') + 1);
    return app_name;
}