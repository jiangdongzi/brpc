#ifndef  MICO_BVAR_H
#define  MICO_BVAR_H
#include "bvar/bvar.h"
#include <bthread/bthread.h>

void start_stat_bvar(const std::string& pushgateway_server);
bvar::LatencyRecorder& get_latency_recorder(const std::string& metric_name);
bvar::CountRecorder& get_count_recorder (const std::string& metric_name);
bvar::WindowEx<bvar::IntRecorder, 20>& get_win_mean_recorder (const std::string& metric_name);

class LatencyRecorderGuard {
    std::string _metric_name;
    butil::Timer _timer;
    public:
    LatencyRecorderGuard(const std::string& metric_name) : _metric_name(metric_name) {
        _timer.start();
    }
    ~LatencyRecorderGuard() {
        _timer.stop();
        get_latency_recorder(_metric_name) << _timer.u_elapsed();
    }
};

#define SCOPED_LATENCY_RECORDER(metric_name) \
    LatencyRecorderGuard BAIDU_CONCAT(scoped_latecy_recorder_dummy_at_line_, __LINE__)(metric_name)

inline void SetCountRecorder(const std::string& metric_name, const int64_t count = 1) {
    get_count_recorder(metric_name) << count;
}

inline void SetWinMeanRecorder(const std::string& metric_name, const int64_t count = 1) {
    get_win_mean_recorder(metric_name) << count;
}

void SetStatusBvarValue(const std::string& metric_name, const int value);

bvar::Adder<int>& get_adder_bvar(const std::string& metric_name);
inline void IncrAdderBvar(const std::string& metric_name, const int value = 1) {
    get_adder_bvar(metric_name) << value;
}

#endif //MICO_BVAR_H