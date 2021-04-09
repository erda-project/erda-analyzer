// Copyright (c) 2021 Terminus, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloud.erda.analyzer.common.schemas.requests;

import cloud.erda.analyzer.common.utils.NumberParser;

public class PerformanceTiming {
    public long firstPaintTime = 0;
    public long connectEnd = 0;
    public long connectStart = 0;
    public long domComplete = 0;
    public long domContentLoadedEventEnd = 0;
    public long domContentLoadedEventStart = 0;
    public long domInteractive = 0;
    public long domLoading = 0;
    public long domainLookupEnd = 0;
    public long domainLookupStart = 0;
    public long fetchStart = 0;
    public long loadEventEnd = 0;
    public long loadEventStart = 0;
    public long navigationStart = 0;
    public long redirectEnd = 0;
    public long redirectStart = 0;
    public long requestStart = 0;
    public long responseEnd = 0;
    public long responseStart = 0;
    public long secureConnectionStart = 0;
    public long unloadEventEnd = 0;
    public long unloadEventStart = 0;

    public static PerformanceTiming parse(String nt) {
        PerformanceTiming result = new PerformanceTiming();
        if(nt==null || "".equals(nt)) return result;
        String[] times = nt.split("\\,");
        long[] ts = new long[22];
        for(int i = 0; i < 22 && i < times.length; i++) {
            ts[i] = NumberParser.parseLong(times[i],0, 36);
        }
        result.firstPaintTime = ts[0];
        if(result.firstPaintTime < 0) result.firstPaintTime = 0;
        result.connectEnd = ts[1];
        result.connectStart = ts[2];
        result.domComplete = ts[3];
        result.domContentLoadedEventEnd = ts[4];
        result.domContentLoadedEventStart = ts[5];
        result.domInteractive = ts[6];
        result.domLoading = ts[7];
        result.domainLookupEnd = ts[8];
        result.domainLookupStart = ts[9];
        result.fetchStart = ts[10];
        result.loadEventEnd = ts[11];
        result.loadEventStart = ts[12];
        result.navigationStart = ts[13];
        result.redirectEnd = ts[14];
        result.redirectStart = ts[15];
        result.requestStart = ts[16];
        result.responseEnd = ts[17];
        result.responseStart = ts[18];
        result.secureConnectionStart = ts[19];
        result.unloadEventEnd = ts[20];
        result.unloadEventStart = ts[21];
        return result;
    }
}
