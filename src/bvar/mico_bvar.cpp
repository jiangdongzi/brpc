#include <bvar/mico_bvar.h>
#include "brpc/metric_rpc.pb.h"
#include "bvar/multi_dimension.h"
#include <brpc/channel.h>

class PrometheusDumper : public bvar::Dumper {
public:
    bool dump(const std::string& name,
              const butil::StringPiece& description) {
        if (description == "0") {
          return false;
        }
        auto* new_metric = req.add_metric();
        new_metric->set_key(name);
        new_metric->set_value(description.as_string());
        if (req.metric_size() >= 100) {
          reqs.emplace_back(std::move(req));
        }
        return true;
    }
    brpc_metrics::MetricRequest req;
    std::vector<brpc_metrics::MetricRequest> reqs;
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
    std::vector<brpc_metrics::MetricRequest> reqs;
    reqs.swap(d.reqs);
    if (d.req.metric_size() > 0) {
      reqs.emplace_back(std::move(d.req));
    }
    if (reqs.empty()) continue;
    static brpc::Channel& channel = GetPrometheusChannel(*pushgateway_server_ptr);
    brpc_metrics::MetricResponse rsp;
    brpc_metrics::MetricService_Stub stub(&channel);
    for (const auto& ele : reqs) {
      brpc::Controller cntl;
      stub.CollectMetrics(&cntl, &ele, &rsp, nullptr);
      if (cntl.Failed()) {
          LOG(ERROR) << "dump bvar failed, error_code: " << cntl.ErrorCode() << ", error_text: " << cntl.ErrorText();
      } else {
          LOG(INFO) << "dump bvar to prometheus, metric size: " << ele.metric_size();
      }
      rsp.Clear();
    }
  }

  return nullptr;
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

template <typename R>
struct RecorderMap {
  static thread_local butil::FlatMap<std::string, R*> tls_recorder;
  static butil::FlatMap<std::string, std::unique_ptr<bvar::MultiDimension<R>>> g_multiDimension;
  static void init() {
    g_multiDimension.init(512);
  }
};

template <typename R>
thread_local butil::FlatMap<std::string, R*> RecorderMap<R>::tls_recorder;

template <typename R>
butil::FlatMap<std::string, std::unique_ptr<bvar::MultiDimension<R>>> RecorderMap<R>::g_multiDimension;

static std::string app_name;
static std::string host_name;
static std::list<std::string> svr_identity;
static std::list<std::string> svr_identity_label_name {"app_name", "host_name"};

template <typename R>
R& get_recorder(const std::string& metric_name) {
  if (!RecorderMap<R>::tls_recorder.initialized()) {
    RecorderMap<R>::tls_recorder.init(512);
  }
  auto* valptr = RecorderMap<R>::tls_recorder.seek(metric_name);
  if (valptr != nullptr) {
      return *(*valptr);
  }
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  if (!RecorderMap<R>::g_multiDimension.initialized()) {
    RecorderMap<R>::init();
  }
  if (RecorderMap<R>::g_multiDimension.seek(metric_name) == nullptr) {
    RecorderMap<R>::g_multiDimension[metric_name].reset(new bvar::MultiDimension<R>(metric_name, svr_identity_label_name));
  }
  RecorderMap<R>::tls_recorder[metric_name] = RecorderMap<R>::g_multiDimension[metric_name]->get_stats(svr_identity);
  return *RecorderMap<R>::tls_recorder[metric_name];
}

bvar::LatencyRecorder& get_latency_recorder (const std::string& metric_name) {
  return get_recorder<bvar::LatencyRecorder>(metric_name);
}

bvar::CountRecorder& get_count_recorder (const std::string& metric_name) {
  return get_recorder<bvar::CountRecorder>(metric_name);
}

//just a bvar, not recorder, just use recorder code, maybe we can change the misleading name(recorder)
static bvar::Status<int>& get_status_bvar(const std::string& metric_name) {
  return get_recorder<bvar::Status<int>>(metric_name);
}

void SetStatusBvarValue(const std::string& metric_name, const int value) {
  get_status_bvar(metric_name).set_value(value);
}

static void start_stat_bvar_internal(const std::string& pushgateway_server) {
    google::SetCommandLineOption("bvar_max_dump_multi_dimension_metric_number", "10000");
    google::SetCommandLineOption("bvar_dump_interval", "180");
    std::unique_ptr<std::string> pushgateway_server_ptr(new std::string(pushgateway_server));

    RecorderMap<bvar::LatencyRecorder>::init();
    RecorderMap<bvar::CountRecorder>::init();
    app_name = brpc_get_app_name();
    host_name = brpc_get_host_name();
    svr_identity = {app_name, host_name};

    bthread_t bvar_stat_tid;
    bthread_start_background(&bvar_stat_tid, nullptr, dump_bvar, pushgateway_server_ptr.release());
}

void start_stat_bvar(const std::string& pushgateway_server) {
  static std::once_flag flag;
  std::call_once(flag, start_stat_bvar_internal, pushgateway_server);
}
