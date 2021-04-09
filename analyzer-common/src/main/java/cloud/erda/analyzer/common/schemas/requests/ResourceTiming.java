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

import com.google.gson.JsonSyntaxException;
import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.common.utils.NumberParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceTiming {

    private final static Logger logger = LoggerFactory.getLogger(ResourceTiming.class);

    private static Map<String,String> InitiatorType = new HashMap();

    static {
        InitiatorType.put("0", "other");
        InitiatorType.put("1", "img");
        InitiatorType.put("2", "link");
        InitiatorType.put("3", "script");
        InitiatorType.put("4", "css");
        InitiatorType.put("5", "xmlhttprequest");
        InitiatorType.put("6", "iframe");
        InitiatorType.put("7", "image");
    }

    public String name;
    public int initiatorType;
    public long startTime;
    public long responseEnd;
    public long responseStart;
    public long requestStart;
    public long connectEnd;
    public long secureConnectionStart;
    public long connectStart;
    public long domainLookupEnd;
    public long domainLookupStart;
    public long redirectEnd;
    public long redirectStart;

    public static List<ResourceTiming> parseToList(String rt) throws JsonSyntaxException {
        List<ResourceTiming> list = new ArrayList<ResourceTiming>();
        if(rt == null || "".equals(rt)) return list;
        Map<String,Object> map = GsonUtil.toObject(rt, Map.class);
        Map<String,String> resMap = new HashMap();
        decodeResource(resMap, map, "");
        for(Map.Entry<String,String> entry : resMap.entrySet()) {
            String val = entry.getValue();
            if(val == null || "".equals(val) || val.length() < 2) {
                continue;
            }
            String typeKey = val.substring(0, 1);
            if(InitiatorType.get(typeKey) == null) {
                continue;
            }
            String timing = val.substring(1);
            long[] times = resTimingDecode(timing);
            ResourceTiming item = new ResourceTiming();
            item.name = entry.getKey();
            item.initiatorType = NumberParser.parseInt(typeKey,0);
            item.startTime = times[0];
            item.responseEnd = times[1];
            item.responseStart = times[2];
            item.requestStart = times[3];
            item.connectEnd = times[4];
            item.secureConnectionStart = times[5];
            item.connectStart = times[6];
            item.domainLookupEnd = times[7];
            item.domainLookupStart = times[8];
            item.redirectEnd = times[9];
            item.redirectStart = times[10];
            list.add(item);
        }
        return list;
    }

    private static void decodeResource(Map<String,String> output, Map<String,Object> input, String preKey) {
        for(Map.Entry<String,Object> entry: input.entrySet()) {
            if(Map.class.isAssignableFrom(entry.getValue().getClass())) {
                decodeResource(output, (Map<String,Object>)entry.getValue(),preKey+entry.getKey());
            } else {
                output.put(preKey+entry.getKey(), entry.getValue().toString());
            }
        }
    }

    private static long[] resTimingDecode(String timing) {
        long[] times = new long[11];
        String[] parts = timing.split("\\,");
        times[0] = NumberParser.parseLong(parts[0],0, 36);
        for(int i = 1; i < 11 && i < parts.length; i++) {
            times[i] = times[0] + NumberParser.parseLong(parts[i],0, 36);
        }
        return times;
    }

    public static long resourceTiming(List<ResourceTiming> resTiming) {
        if(resTiming == null || resTiming.size() <= 0) return 0;
        long firstStart = Long.MAX_VALUE;
        long lastEnd = Long.MIN_VALUE;
        for(ResourceTiming res : resTiming) {
            if(res.startTime < firstStart) {
                firstStart = res.startTime;
            }
            if(res.responseEnd > lastEnd) {
                lastEnd = res.responseEnd;
            }
        }
        long rlt = lastEnd - firstStart;
        if(rlt < 0) {
            logger.error(String.format("Resource loading time must be positive, instead got: {}, restTiming: {}", rlt, resTiming));
            return 0;
        }
        return  rlt;
    }

    public static long resourceDnsCount(List<ResourceTiming> resTiming) {
        if(resTiming == null || resTiming.size() <= 0) return 0;
        long count = 0;
        for(ResourceTiming res : resTiming) {
            if((res.domainLookupEnd - res.domainLookupStart) > 0) {
                count++;
            }
        }
        return count;
    }

}