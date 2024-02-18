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

#ifndef  BVAR_COUNT_RECORDER_H
#define  BVAR_COUNT_RECORDER_H

#include "bvar/recorder.h"
#include "bvar/reducer.h"
#include "bvar/passive_status.h"
#include "bvar/detail/percentile.h"

namespace bvar {
namespace detail {

class Percentile;
typedef Window<IntRecorder, SERIES_IN_SECOND> RecorderWindow;
typedef Window<Maxer<int64_t>, SERIES_IN_SECOND> MaxWindow;
typedef Window<Percentile, SERIES_IN_SECOND> PercentileWindow;

// NOTE: Always use int64_t in the interfaces no matter what the impl. is.
/*
class CDF : public Variable {
public:
    explicit CDF(PercentileWindow* w);
    ~CDF();
    void describe(std::ostream& os, bool quote_string) const override;
    int describe_series(std::ostream& os, const SeriesOptions& options) const override;
private:
    PercentileWindow* _w; 
};

*/

// For mimic constructor inheritance.
class CountRecorderBase {
public:
    explicit CountRecorderBase(time_t window_size);
    time_t window_size() const { return _count_window.window_size(); }
protected:
    IntRecorder _count;
    Maxer<int64_t> _max_count;
    Percentile _count_percentile;

    RecorderWindow _count_window;
    MaxWindow _max_count_window;
    PassiveStatus<int64_t> _total;
    PassiveStatus<int64_t> _qps;
    PercentileWindow _count_percentile_window;
    PassiveStatus<int64_t> _count_p1;
    PassiveStatus<int64_t> _count_p2;
    PassiveStatus<int64_t> _count_p3;
    PassiveStatus<int64_t> _count_999;  // 99.9%
    PassiveStatus<int64_t> _count_9999; // 99.99%
};
} // namespace detail

// Specialized structure to record count.
// It's not a Variable, but it contains multiple bvar inside.
class CountRecorder : public detail::CountRecorderBase {
    typedef detail::CountRecorderBase Base;
public:
    CountRecorder() : Base(-1) {}
    explicit CountRecorder(time_t window_size) : Base(window_size) {}
    explicit CountRecorder(const butil::StringPiece& prefix) : Base(-1) {
        expose(prefix);
    }
    CountRecorder(const butil::StringPiece& prefix,
                    time_t window_size) : Base(window_size) {
        expose(prefix);
    }
    CountRecorder(const butil::StringPiece& prefix1,
                    const butil::StringPiece& prefix2) : Base(-1) {
        expose(prefix1, prefix2);
    }
    CountRecorder(const butil::StringPiece& prefix1,
                    const butil::StringPiece& prefix2,
                    time_t window_size) : Base(window_size) {
        expose(prefix1, prefix2);
    }

    ~CountRecorder() { hide(); }

    // Record the count.
    CountRecorder& operator<<(int64_t count);
        
    int expose(const butil::StringPiece& prefix) {
        return expose(butil::StringPiece(), prefix);
    }
    int expose(const butil::StringPiece& prefix1,
               const butil::StringPiece& prefix2);
    
    // Hide all internal variables, called in dtor as well.
    void hide();

    // Get the average count in recent |window_size| seconds
    // If |window_size| is absent, use the window_size to ctor.
    int64_t count(time_t window_size) const
    { return _count_window.get_value(window_size).get_average_int(); }
    int64_t count() const
    { return _count_window.get_value().get_average_int(); }

    // Get p1/p2/p3/99.9-ile counts in recent window_size-to-ctor seconds.
    Vector<int64_t, 4> count_percentiles() const;

    // Get the max count in recent window_size-to-ctor seconds.
    int64_t max_count() const { return _max_count_window.get_value(); }

    // Get qps in recent |window_size| seconds. The `q' means counts
    // recorded by operator<<().
    // If |window_size| is absent, use the window_size to ctor.
    int64_t qps(time_t window_size) const;
    int64_t qps() const { return _qps.get_value(); }

    // Get |ratio|-ile count in recent |window_size| seconds
    // E.g. 0.99 means 99%-ile
    int64_t count_percentile(double ratio) const;
};

std::ostream& operator<<(std::ostream& os, const CountRecorder&);

}  // namespace bvar

#endif  //BVAR_COUNT_RECORDER_H
