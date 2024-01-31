#ifndef  MICO_BVAR_H
#define  MICO_BVAR_H
#include "bvar/bvar.h"
#include <brpc/channel.h>
#include <bthread/bthread.h>

void start_stat_bvar(const std::string& pushgateway_server);
std::string brpc_get_host_name();
std::string brpc_get_app_name();

#define INIT_LATENCY_RECORDER(metric_name, label_name) \
    static thread_local butil::FlatMap<std::string, bvar::LatencyRecorder*> tls_##metric_name##_latency_recorder; \
    static butil::FlatMap<std::string, bvar::LatencyRecorder*> metric_name##_latency_recorder; \
    static std::mutex metric_name##latency_recorder_mt; \
    static bvar::MultiDimension<bvar::LatencyRecorder> metric_name##latency_recorder_bvar(#metric_name, {label_name, "app_name", "host_name"}); \
    class metric_name##_latency_recorder##InitHelper { \
    public:    \
        metric_name##_latency_recorder##InitHelper() {    \
            metric_name##_latency_recorder.init(512); \
            tls_##metric_name##_latency_recorder.init(512); \
        } \
    };  \
    metric_name##_latency_recorder##InitHelper metric_name##_latency_recorder##initHelper; \
    bvar::LatencyRecorder& get##metric_name##latency_recorder(const std::string& label_value) { \
        auto* valptr = tls_##metric_name##_latency_recorder.seek(label_value); \
        if (valptr != nullptr) { \
            return *(*valptr); \
        }  \
        std::lock_guard<std::mutex> lock(metric_name##latency_recorder_mt); \
        valptr = metric_name##_latency_recorder.seek(label_value); \
        if (valptr == nullptr) { \
            metric_name##_latency_recorder[label_value] = metric_name##latency_recorder_bvar.get_stats({label_value, brpc_get_app_name(), brpc_get_host_name()}); \
        } \
        return *metric_name##_latency_recorder[label_value]; \
    } \
    std::function<bvar::LatencyRecorder&(const std::string&)> get##metric_name##latency_recorder_closure = [](const std::string& label_value) -> bvar::LatencyRecorder& { \
        return get##metric_name##latency_recorder(label_value); \
    }


#define GET_LATENCY_RECORDER(metric_name, label_value) \
    [&]() -> bvar::LatencyRecorder& { \
        extern std::function<bvar::LatencyRecorder&(const std::string&)> get##metric_name##latency_recorder_closure; \
        return get##metric_name##latency_recorder_closure(label_value); \
    }()

class LatencyRecorderGuard {
    bvar::LatencyRecorder* _recorder;
    butil::Timer _timer;
    public:
    LatencyRecorderGuard(bvar::LatencyRecorder* recorder) : _recorder(recorder) {
        _timer.start();
    }
    ~LatencyRecorderGuard() {
        _timer.stop();
        (*_recorder) << _timer.u_elapsed();
    }
};

#define SCOPED_LATENCY_RECORDER(metric_name, label_value) \
    LatencyRecorderGuard BAIDU_CONCAT(scoped_latecy_recorder_dummy_at_line_, __LINE__)(&GET_LATENCY_RECORDER(metric_name, label_value))


#define INIT_COUNT_RECORDER(metric_name, label_name) \
    static thread_local butil::FlatMap<std::string, bvar::CountRecorder*> tls_##metric_name##_count_recorder; \
    static butil::FlatMap<std::string, bvar::CountRecorder*> metric_name##_count_recorder; \
    static std::mutex metric_name##count_recorder_mt; \
    static bvar::MultiDimension<bvar::CountRecorder> metric_name##count_recorder_bvar(#metric_name, {label_name, "app_name", "host_name"}); \
    class metric_name##_count_recorder##InitHelper { \
    public:    \
        metric_name##_count_recorder##InitHelper() {    \
            metric_name##_count_recorder.init(512); \
            tls_##metric_name##_count_recorder.init(512); \
        } \
    };  \
    metric_name##_count_recorder##InitHelper metric_name##_count_recorder##initHelper; \
    bvar::CountRecorder& get##metric_name##count_recorder(const std::string& label_value) { \
        auto* valptr = tls_##metric_name##_count_recorder.seek(label_value); \
        if (valptr != nullptr) { \
            return *(*valptr); \
        }  \
        std::lock_guard<std::mutex> lock(metric_name##count_recorder_mt); \
        valptr = metric_name##_count_recorder.seek(label_value); \
        if (valptr == nullptr) { \
            metric_name##_count_recorder[label_value] = metric_name##count_recorder_bvar.get_stats({label_value, brpc_get_app_name(), brpc_get_host_name()}); \
        } \
        return *metric_name##_count_recorder[label_value]; \
    } \
    std::function<bvar::CountRecorder&(const std::string&)> get##metric_name##count_recorder_closure = [](const std::string& label_value) -> bvar::CountRecorder& { \
        return get##metric_name##count_recorder(label_value); \
    }


#define GET_COUNT_RECORDER(metric_name, label_value) \
    [&]() -> bvar::CountRecorder& { \
        extern std::function<bvar::CountRecorder&(const std::string&)> get##metric_name##count_recorder_closure; \
        return get##metric_name##count_recorder_closure(label_value); \
    }()


#endif //MICO_BVAR_H