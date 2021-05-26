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

package cloud.erda.analyzer.common.schemas;

import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.requests.*;
import cloud.erda.analyzer.common.utils.NumberParser;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import cloud.erda.analyzer.common.schemas.requests.*;
import lombok.val;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qiniu.ip17mon.LocationInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class RequestEventSchema implements DeserializationSchema<MetricEvent> {

    private final static Logger logger = LoggerFactory.getLogger(RequestEventSchema.class);
    private final static Gson gson = new Gson();
    private final static long MILLISECOND = 1;
    private final static long NANOSECOND = 1000 * 1000;

    private static class IngoreException extends IOException {
    }

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length <= 0) {
            logger.error("event is empty");
            return null;
        }
        String message = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = message.split("\\,", 5);
        if (parts.length != 5) {
            logger.error("invalid event", message);
            return null;
        }
        try {
            val metric = new MetricEvent();
            val data = FormParams.parse(parts[4]);
            if (data == null) return null;
            String name = data.get("t");
            metric.setTimestamp(Long.parseLong(data.get("date")) * NANOSECOND);
            // set common tags
            metric.addTag("tk", parts[0]);
            metric.addTag("cid", parts[1]);
            metric.addTag("uid", parts[2]);
            metric.addTag("vid", data.get("vid"));
            metric.addTag("ip", parts[3]);
            metric.addTag("host", data.get("dh"));
            metric.addTag("doc_path", PathUtils.getRoute(data.get("dp")));
            switch (name) {
                case "req":
                case "ajax":
                case "request":
                    return toRequestMetricEvent(data, metric);
                case "timing":
                    return toTimingMetricEvent(data, metric);
                case "error":
                    return toErrorMetricEvent(data, metric);
                case "device":
                    return toDeviceMetricEvent(data, metric);
                case "browser":
                    return toBrowserMetricEvent(data, metric);
                case "document":
                    return toDocumentMetricEvent(data, metric);
                case "script":
                    return toScriptMetricEvent(data, metric);
                case "event":
                    return toEventMetricEvent(data, metric);
                default:
                    logger.error(String.format("unknown t=%s in data", name));
                    return null;
            }
        } catch (IngoreException e) {
            return null;
        } catch (IOException e) {
            logger.error("Deserialize browser insight event fail. \n " + e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("Deserialize browser insight event fail. \n " + e.getMessage());
            return null;
        }
    }

    private MetricEvent toRequestMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_req");
        Map<String, Object> fields = metric.getFields();
        appendMobileInfoIfNeed(data, metric);
        fields.put("tt", NumberParser.parseLong(data.get("tt"), 0) * MILLISECOND);
        fields.put("req", NumberParser.parseDouble(data.get("req"), 0));
        fields.put("res", NumberParser.parseDouble(data.get("res"), 0));
        int status = NumberParser.parseInt(data.get("st"), 0);
        fields.put("errors", status >= 400 ? 1 : 0);
        fields.put("status", status);

        String url = data.get("url");
        metric.addTag("url", url);
        metric.addTag("req_path", PathUtils.getRoute(url));
        metric.addTag("status_code", data.get("st"));
        metric.addTag("method", data.get("me"));
        return metric;
    }

    private MetricEvent toTimingMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_timing");
        Map<String, Object> fields = metric.getFields();
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (isMobile) {
            long nt = NumberParser.parseLong(data.get("nt"), 0);
            fields.put("plt", nt * MILLISECOND);
        } else {
            UserAgent ua = UserAgent.parse(data.get("ua"));
            metric.addTag("browser", ua.browser);
            metric.addTag("browser_version", ua.browserVersion);
            metric.addTag("os", ua.os);
            metric.addTag("osv", ua.osVersion);
            metric.addTag("device", ua.device);

            String timingStr = data.get("pt");
            long plt, act, dns, tcp, srt, net;
            if (timingStr == null || timingStr.length() == 0) {
                NavigationTiming nt = NavigationTiming.parse(data.get("nt"));
                plt = putPositiveValue(fields, "plt", (nt.loadTime + nt.readyStart) * MILLISECOND); // ? loadTime
                putPositiveValue(fields, "rrt", nt.redirectTime * MILLISECOND);
                putPositiveValue(fields, "put", nt.unloadEventTime * MILLISECOND);
                act = putPositiveValue(fields, "act", nt.appCacheTime * MILLISECOND);
                dns = putPositiveValue(fields, "dns", nt.lookupDomainTime * MILLISECOND);
                tcp = putPositiveValue(fields, "tcp", nt.connectTime * MILLISECOND);
                putPositiveValue(fields, "rqt", (nt.requestTime - nt.responseTime) * MILLISECOND); // ? requestTime
                putPositiveValue(fields, "rpt", nt.responseTime * MILLISECOND);
                srt = putPositiveValue(fields, "srt", nt.requestTime * MILLISECOND); // ? requestTime + responseTime
                putPositiveValue(fields, "dit", nt.initDomTreeTime * MILLISECOND);
                putPositiveValue(fields, "drt", nt.domReadyTime * MILLISECOND);
                putPositiveValue(fields, "clt", nt.loadEventTime * MILLISECOND);
                putPositiveValue(fields, "set", nt.scriptExecuteTime * MILLISECOND);
                putPositiveValue(fields, "wst", (
                        nt.redirectTime + nt.appCacheTime + nt.lookupDomainTime + nt.connectTime + nt.requestTime - nt.responseTime
                        // ? redirectTime + appCacheTime + lookupDomainTime + connectTime
                ) * MILLISECOND);
                putPositiveValue(fields, "fst", (
                        (nt.loadTime + nt.readyStart) - (nt.initDomTreeTime + nt.domReadyTime + nt.loadEventTime)
                        // ? loadTime - (initDomTreeTime + domReadyTime + loadEventTime)
                ) * MILLISECOND);
                putPositiveValue(fields, "pct", (
                        (nt.loadTime + nt.readyStart) - (nt.domReadyTime + nt.loadEventTime)
                        // ? loadTime - (domReadyTime + loadEventTime)
                ) * MILLISECOND);
                putPositiveValue(fields, "rct", (
                        (nt.loadTime + nt.readyStart) - nt.loadEventTime
                        // ? loadTime - loadEventTime
                ) * MILLISECOND);
            } else {
                PerformanceTiming pt = PerformanceTiming.parse(timingStr);
                plt = putPositiveValue(fields, "plt", pt.loadEventEnd - pt.navigationStart);
                putPositiveValue(fields, "rrt", pt.redirectEnd - pt.redirectStart);
                putPositiveValue(fields, "put", pt.unloadEventEnd - pt.unloadEventStart);
                act = putPositiveValue(fields, "act", pt.domainLookupStart - pt.fetchStart);
                dns = putPositiveValue(fields, "dns", pt.domainLookupEnd - pt.domainLookupStart);
                tcp = putPositiveValue(fields, "tcp", pt.connectEnd - pt.connectStart);
                putPositiveValue(fields, "rqt", pt.responseStart - pt.requestStart);
                putPositiveValue(fields, "rpt", pt.responseEnd - pt.responseStart);
                srt = putPositiveValue(fields, "srt", pt.responseEnd - pt.requestStart);
                putPositiveValue(fields, "dit", pt.domInteractive - pt.responseEnd);
                putPositiveValue(fields, "drt", pt.domComplete - pt.domInteractive);
                putPositiveValue(fields, "clt", pt.loadEventEnd - pt.loadEventStart);
                putPositiveValue(fields, "set", pt.domContentLoadedEventEnd - pt.domContentLoadedEventStart);
                putPositiveValue(fields, "wst", pt.responseStart - pt.navigationStart);
                putPositiveValue(fields, "fst", pt.firstPaintTime);
                putPositiveValue(fields, "pct",
                        (pt.loadEventEnd - pt.fetchStart) + (pt.fetchStart - pt.navigationStart) -
                                ((pt.domComplete - pt.domInteractive) + (pt.loadEventEnd - pt.loadEventStart)));
                putPositiveValue(fields, "rct",
                        (pt.loadEventEnd - pt.fetchStart) + (pt.fetchStart - pt.navigationStart) -
                                (pt.loadEventEnd - pt.loadEventStart));
            }
            net = act + tcp + dns;
            putPositiveValue(fields, "net", net);
            putPositiveValue(fields, "prt", plt - srt - net);

            List<ResourceTiming> rtList = null;
            try {
                rtList = ResourceTiming.parseToList(data.get("rt"));
            } catch (JsonSyntaxException e) {
                logger.error("invalid timing.rt", data.get("rt"));
                return null;
            }
            putPositiveValue(fields, "rlt", ResourceTiming.resourceTiming(rtList) * MILLISECOND);
            putPositiveValue(fields, "rdc", ResourceTiming.resourceDnsCount(rtList));

            if (plt > 8000 * MILLISECOND) {
                metric.addTag("slow", "true");
            }
        }

        metric.addTag("country", "中国");
        metric.addTag("province", "局域网");
        metric.addTag("city", "局域网");
        // metric.addTag("isp", "");
        String ip = metric.getTags().get("ip");
        if (ip != null && ip.length() > 0 && !ip.contains(":")) {
            LocationInfo info = IPDB.find(ip);
            metric.addTag("country", info.country);
            metric.addTag("province", info.state);
            metric.addTag("city", info.city);
            // metric.addTag("isp", info.isp);
        }
        return metric;
    }

    private MetricEvent toErrorMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_error");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (!isMobile) {
//            metric.addTag("source", data.get("ers"));
//            metric.addTag("line_no", data.get(""));
//            metric.addTag("column_no", data.get("erc"));
            UserAgent ua = UserAgent.parse(data.get("ua"));
            metric.addTag("browser", ua.browser);
//            metric.addTag("browser_version", ua.browserVersion);
//            metric.addTag("os", ua.os);
//            metric.addTag("osv", ua.osVersion);
//            metric.addTag("device", ua.device);
        }
//        metric.addTag("vid", data.get("vid"));
        metric.addTag("error", data.get("erm"));
//        metric.addTag("stack_trace", data.get("sta"));
        return metric;
    }

    private MetricEvent toDeviceMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_device");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (isMobile) {
            metric.addTag("channel", data.get("ch"));
            if ("1".equals(data.get("jb"))) {
                metric.addTag("jb", "true");
            } else {
                metric.addTag("jb", "false");
            }
            metric.addTag("cpu", data.get("cpu"));
            metric.addTag("sdk", data.get("sdk"));
            metric.addTag("sd", data.get("sd"));
            metric.addTag("mem", data.get("men"));
            metric.addTag("rom", data.get("rom"));
        }
        metric.addTag("sr", data.get("sr"));
        // metric.addTag("vid", data.get("vid"));
        return metric;
    }

    private MetricEvent toBrowserMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_browser");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (isMobile) {
            metric.addTag("sr", data.get("sr"));
        } else {
            UserAgent ua = UserAgent.parse(data.get("ua"));
            metric.addTag("browser", ua.browser);
            metric.addTag("ce", data.get("ce"));
            metric.addTag("vp", data.get("vp"));
            metric.addTag("ul", data.get("ul"));
            metric.addTag("sr", data.get("sr"));
            metric.addTag("sd", data.get("sd"));
            metric.addTag("fl", data.get("fl"));
        }
        return metric;
    }

    private MetricEvent toDocumentMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_document");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (!isMobile) {
            metric.addTag("ds", data.get("ds"));
            metric.addTag("dr", data.get("dr"));
            metric.addTag("de", data.get("de"));
            metric.addTag("dk", data.get("dk"));
            metric.addTag("dl", data.get("dl"));
        }
        metric.addTag("dt", data.get("dt"));
        metric.addTag("tp", data.get("tp"));
        return metric;
    }

    private MetricEvent toScriptMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_script");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        if (!isMobile) {
            UserAgent ua = UserAgent.parse(data.get("ua"));
            metric.addTag("browser", ua.browser);
            // setTag(tags, "browser_version", ua.browserVersion);
            metric.addTag("os", ua.os);
            // setTag(tags, "osv", ua.osVersion);
            metric.addTag("device", ua.device);
        }
        String msg = data.get("msg");
        if (msg != null) {
            String[] lines = msg.split("\\n");
            metric.addTag("error", lines[0]);
        }
        metric.addTag("source", data.get("source"));
        metric.addTag("line_no", data.get("lineno"));
        metric.addTag("column_no", data.get("colno"));
        metric.addTag("message", data.get("msg"));
        return metric;
    }

    private MetricEvent toEventMetricEvent(Map<String, String> data, MetricEvent metric) throws Exception {
        metric.setName("ta_event");
        metric.getFields().put("count", 1);
        boolean isMobile = appendMobileInfoIfNeed(data, metric);
        metric.getFields().put("x", Long.parseLong(data.get("x")));
        metric.getFields().put("y", Long.parseLong(data.get("y")));
        metric.addTag("x", data.get("x"));
        metric.addTag("y", data.get("y"));
        metric.addTag("xp", data.get("xp"));
        if (isMobile) {
            metric.addTag("en", data.get("en"));
            metric.addTag("ei", data.get("ei"));
        }
        return metric;
    }

    private long putPositiveValue(Map<String, Object> fields, String key, long value) {
        if (value < 0) {
            throw new RuntimeException(key + " must be positive number " + value);
        }
        fields.put(key, value);
        return value;
    }

    private boolean appendMobileInfoIfNeed(Map<String, String> data, MetricEvent metric) throws IngoreException {
        String ua = data.get("ua");
        if (isMobile(ua)) {
            // throw  new IngoreException();
            metric.setName(metric.getName() + "_mobile");
            metric.addTag("type", "mobile");
            appendMobileInfo(data, metric);
            return true;
        } else {
            metric.addTag("type", "browser");
            return false;
        }
    }

    public boolean isMobile(String ua) {
        if (ua == null) return false;
        ua = ua.toLowerCase();
        if ("ios".equals(ua) || "android".equals(ua)) {
            return true;
        }
        return false;
    }

    private void appendMobileInfo(Map<String, String> data, MetricEvent metric) {
        metric.addTag("ns", data.remove("ns"));
        metric.addTag("av", data.remove("av"));
        metric.addTag("br", data.remove("br"));
        metric.addTag("gps", data.remove("gps"));
        metric.addTag("osv", data.remove("osv"));
        String val = data.get("osn");
        if (val == null || "".equals(val)) {
            metric.addTag("os", UserAgent.OTHER);
        } else {
            metric.addTag("os", val);
        }
        val = data.get("md");
        if (val == null || "".equals(val)) {
            metric.addTag("device", UserAgent.OTHER);
        } else {
            metric.addTag("device", val);
        }
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
