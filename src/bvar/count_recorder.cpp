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

// Date: 2024/01/25 11:57:43

#include <gflags/gflags.h>
#include "butil/unique_ptr.h"
#include "bvar/count_recorder.h"

namespace bvar {

// Reloading following gflags does not change names of the corresponding bvars.
// Avoid reloading in practice.
DEFINE_int32(bvar_count_p1, 80, "First count percentile");
DEFINE_int32(bvar_count_p2, 90, "Second count percentile");
DEFINE_int32(bvar_count_p3, 99, "Third count percentile");

static bool valid_percentile(const char*, int32_t v) {
    return v > 0 && v < 100;
}

namespace detail {

typedef PercentileSamples<1022> CombinedPercentileSamples;

// Return random int value with expectation = `dval'
static int64_t double_to_random_int(double dval) {
    int64_t ival = static_cast<int64_t>(dval);
    if (dval > ival + butil::fast_rand_double()) {
        ival += 1;
    }
    return ival;
}

static int64_t get_window_recorder_qps(void* arg) {
    detail::Sample<Stat> s;
    static_cast<RecorderWindow*>(arg)->get_span(&s);
    // Use floating point to avoid overflow.
    if (s.time_us <= 0) {
        return 0;
    }
    return double_to_random_int(s.data.num * 1000000.0 / s.time_us);
}

static int64_t get_recorder_total(void* arg) {
    return static_cast<IntRecorder*>(arg)->get_value().num;
}

// Caller is responsible for deleting the return value.
static CombinedPercentileSamples* combine(PercentileWindow* w) {
    CombinedPercentileSamples* cb = new CombinedPercentileSamples;
    std::vector<GlobalPercentileSamples> buckets;
    w->get_samples(&buckets);
    cb->combine_of(buckets.begin(), buckets.end());
    return cb;
}

template <int64_t numerator, int64_t denominator>
static int64_t get_percetile(void* arg) {
    return ((CountRecorder*)arg)->count_percentile(
            (double)numerator / double(denominator));
}

static int64_t get_p1(void* arg) {
    CountRecorder* lr = static_cast<CountRecorder*>(arg);
    return lr->count_percentile(FLAGS_bvar_count_p1 / 100.0);
}
static int64_t get_p2(void* arg) {
    CountRecorder* lr = static_cast<CountRecorder*>(arg);
    return lr->count_percentile(FLAGS_bvar_count_p2 / 100.0);
}
static int64_t get_p3(void* arg) {
    CountRecorder* lr = static_cast<CountRecorder*>(arg);
    return lr->count_percentile(FLAGS_bvar_count_p3 / 100.0);
}

CountRecorderBase::CountRecorderBase(time_t window_size)
    : _max_count(0)
    , _count_window(&_count, window_size)
    , _max_count_window(&_max_count, window_size)
    , _total(get_recorder_total, &_count)
    , _qps(get_window_recorder_qps, &_count_window)
    , _count_percentile_window(&_count_percentile, window_size)
    , _count_p1(get_p1, this)
    , _count_p2(get_p2, this)
    , _count_p3(get_p3, this)
    , _count_999(get_percetile<999, 1000>, this)
    , _count_9999(get_percetile<9999, 10000>, this)
{}

}  // namespace detail


int64_t CountRecorder::qps(time_t window_size) const {
    detail::Sample<Stat> s;
    _count_window.get_span(window_size, &s);
    // Use floating point to avoid overflow.
    if (s.time_us <= 0) {
        return 0;
    }
    return detail::double_to_random_int(s.data.num * 1000000.0 / s.time_us);
}

int CountRecorder::expose(const butil::StringPiece& prefix1,
                            const butil::StringPiece& prefix2) {
    if (prefix2.empty()) {
        LOG(ERROR) << "Parameter[prefix2] is empty";
        return -1;
    }
    butil::StringPiece prefix = prefix2;
    std::string tmp;
    if (!prefix1.empty()) {
        tmp.reserve(prefix1.size() + prefix.size() + 1);
        tmp.append(prefix1.data(), prefix1.size());
        tmp.push_back('_'); // prefix1 ending with _ is good.
        tmp.append(prefix.data(), prefix.size());
        prefix = tmp;
    }

    // set debug names for printing helpful error log.
    _count.set_debug_name(prefix);
    _count_percentile.set_debug_name(prefix);

    if (_count_window.expose(prefix) != 0) {
        return -1;
    }
    if (_max_count_window.expose_as(prefix, "max") != 0) {
        return -1;
    }

    if (_total.expose_as(prefix, "total") != 0) {
        return -1;
    }

    if (_qps.expose_as(prefix, "qps") != 0) {
        return -1;
    }
    char namebuf[32];
    snprintf(namebuf, sizeof(namebuf), "%d", (int)FLAGS_bvar_count_p1);
    if (_count_p1.expose_as(prefix, namebuf, DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "%d", (int)FLAGS_bvar_count_p2);
    if (_count_p2.expose_as(prefix, namebuf, DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "%u", (int)FLAGS_bvar_count_p3);
    if (_count_p3.expose_as(prefix, namebuf, DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    if (_count_999.expose_as(prefix, "999", DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    if (_count_9999.expose_as(prefix, "9999") != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "%d%%,%d%%,%d%%,99.9%%",
             (int)FLAGS_bvar_count_p1, (int)FLAGS_bvar_count_p2,
             (int)FLAGS_bvar_count_p3);
    return 0;
}

int64_t CountRecorder::count_percentile(double ratio) const {
    std::unique_ptr<detail::CombinedPercentileSamples> cb(
        combine((detail::PercentileWindow*)&_count_percentile_window));
    return cb->get_number(ratio);
}

void CountRecorder::hide() {
    _count_window.hide();
    _max_count_window.hide();
    _qps.hide();
    _count_p1.hide();
    _count_p2.hide();
    _count_p3.hide();
    _count_999.hide();
    _count_9999.hide();
}

CountRecorder& CountRecorder::operator<<(int64_t count) {
    _count << count;
    _max_count << count;
    _count_percentile << count;
    return *this;
}

std::ostream& operator<<(std::ostream& os, const CountRecorder& rec) {
    return os << "{count=" << rec.count()
              << " max" << rec.window_size() << '=' << rec.max_count()
              << " qps=" << rec.qps() << '}';
}

}  // namespace bvar
