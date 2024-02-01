#ifndef  MICO_BVAR_H
#define  MICO_BVAR_H
#include "bvar/bvar.h"
#include <bthread/bthread.h>

void start_stat_bvar(const std::string& pushgateway_server);
bvar::LatencyRecorder& get_latency_recorder(const std::string& metric_name);

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

#define SCOPED_LATENCY_RECORDER(metric_name) \
    LatencyRecorderGuard BAIDU_CONCAT(scoped_latecy_recorder_dummy_at_line_, __LINE__)(&get_latency_recorder(metric_name))

#endif //MICO_BVAR_H