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

package cloud.erda.analyzer.common.functions;

import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.val;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author liuhaoyang
 * @date 2020/9/25 17:23
 */
public class MetricEventCorrectFunction implements FlatMapFunction<MetricEvent, MetricEvent> {

//    @Override
//    public boolean filter(MetricEvent metricEvent) throws Exception {
//        return metricEvent != null;
//    }

    @Override
    public void flatMap(MetricEvent metricEvent, Collector<MetricEvent> collector) throws Exception {
        if (metricEvent != null) {
            long currentTimestamp = System.currentTimeMillis() * 1000 * 1000;
            if (metricEvent.getTimestamp() > currentTimestamp) {
                metricEvent.setTimestamp(currentTimestamp);
            }
            String displayUrl = metricEvent.getTags().get("display_url");
            String recordUrl = metricEvent.getTags().get("record_url");
            String orgName = metricEvent.getTags().get("org_name");
            if (orgName == null) {
                orgName = getOrgName(displayUrl,recordUrl);
            }
            if (orgName != null) {
                if (displayUrl != null) {
                    displayUrl = ModifyUrl(orgName, displayUrl);
                    metricEvent.getTags().put("display_url", displayUrl);
                }
                if (recordUrl != null) {
                    recordUrl = ModifyUrl(orgName, recordUrl);
                    metricEvent.getTags().put("record_url", recordUrl);
                }
            }
            collector.collect(metricEvent);
        }
    }

    public String ModifyUrl(String orgName, String url) throws MalformedURLException {
        URL u = new URL(url);
        String protocol = u.getProtocol();
        String host = u.getHost();
        StringBuffer stringBuffer = new StringBuffer(url);
        String head = protocol + "://" + host + "/";
        String subString = url.substring(head.length() - 1, url.length() - head.length() - 1);
        val elements = subString.split("/");
        if (!elements[0].equals(orgName)) {
            stringBuffer.insert(head.length(), orgName + "/");
            return stringBuffer.toString();
        }
        return url;
    }

    public String getOrgName(String displayUrl,String recordUrl) throws MalformedURLException {
        if (displayUrl == null && recordUrl == null) {
            return null;
        }
        String url = displayUrl == null ? recordUrl : displayUrl;
        URL u = new URL(url);
        String host = u.getHost();
        String pattern = "(.*)-org.*";
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(host);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
