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

package cloud.erda.analyzer.alert.utils;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


// Repair the old display_url and record_url
// Due to the change of access rules, the organization
// name needs to be added after the domain name.

@Slf4j
public class RepairErrorUrlUtils {
    private static String DataCenter = "dataCenter", WorkBench = "workBench", MicroService = "microService";

    private static String pattern = "(.*)-org.*";
    private static Pattern p = Pattern.compile(pattern);

    //部署中心（详情）
    //	routeFormatRuntime      = "/workBench/projects/%s/apps/%s/deploy/runtimes/%s/overview"(原)
    private static String routeFormatRuntime = "dop/projects/%s/deploy/list/%s/{{application_id}}/runtime/{{runtime_id}}";

    //记录
    //  RecordPathFormat    = "/microService/%s/%s/%s/alarm-management/%s/alarm-record"(原）
    private static String recordPathFormat = "msp/%s/%s/%s/alarm-management/%s/list/events/{{family_id}}";


    public static MetricEvent modifyMetricEvent(MetricEvent metricEvent) throws MalformedURLException {
        String displayUrl = metricEvent.getTags().get(AlertConstants.DISPLAY_URL);
        String recordUrl = metricEvent.getTags().get(AlertConstants.RECORD_URL);
        String url = StringUtil.isEmpty(displayUrl) ? recordUrl : displayUrl;
        if (matchOldUrl(url).find()) {
            String orgName = getOrgName(url);
            if (displayUrl != null) {
                displayUrl = modifyUrl(orgName, displayUrl);
                metricEvent.getTags().put(AlertConstants.DISPLAY_URL, displayUrl);
            }
            if (recordUrl != null) {
                recordUrl = modifyUrl(orgName, recordUrl);
                metricEvent.getTags().put(AlertConstants.RECORD_URL, recordUrl);
            }
        }
        return metricEvent;
    }

    public static String[] getElements(String url, MetricEvent metricEvent) throws MalformedURLException {
        String head = getHead(url, metricEvent);
        String subString = url.substring(head.length());
        String[] elements = subString.split("/");
        return elements;
    }

    public static String getHead(String url, MetricEvent metricEvent) throws MalformedURLException {
        URL u = new URL(url);
        String protocol = u.getProtocol();
        String host = u.getHost();
        String head = protocol + "://" + host + "/" + metricEvent.getTags().get("org_name") + "/";
        return head;
    }

    public static void replaceMetricEvent(MetricEvent metricEvent) throws MalformedURLException {
        String displayUrl = metricEvent.getTags().get(AlertConstants.DISPLAY_URL);
        String recordUrl = metricEvent.getTags().get(AlertConstants.RECORD_URL);
        String head = "";
        if (StringUtil.isNotEmpty(displayUrl)) {
            head = getHead(displayUrl, metricEvent);
            String[] elements = getElements(displayUrl, metricEvent);
            if (elements[0].equals(WorkBench)) {
                String rDisplayUrl = String.format(routeFormatRuntime, elements[2], metricEvent.getTags().get("workspace"));
                metricEvent.getTags().put(AlertConstants.DISPLAY_URL, head + rDisplayUrl);
            }
        }
        if (StringUtil.isNotEmpty(recordUrl)) {
            head = getHead(recordUrl, metricEvent);
            String[] elements = getElements(recordUrl, metricEvent);
            if (elements[0].equals(MicroService)) {
                String rRecordUrl = String.format(recordPathFormat, elements[1], elements[2], elements[3], elements[5]);
                metricEvent.getTags().put(AlertConstants.RECORD_URL, head + rRecordUrl);
            }
        }
    }


    public static String modifyUrl(String orgName, String url) throws MalformedURLException {
        URL u = new URL(url);
        String protocol = u.getProtocol();
        String host = u.getHost();
        StringBuffer stringBuffer = new StringBuffer(url);
        String head = protocol + "://" + host + "/";
        String subString = url.substring(head.length());
        String[] elements = subString.split("/");
        if (!elements[0].equals(DataCenter) && !elements[0].equals(WorkBench) && !elements[0].equals(MicroService)) {
            return url;
        }
        if (!elements[0].equals(orgName)) {
            log.info("the old url is: " + url);
            stringBuffer.insert(head.length(), orgName + "/");
            return stringBuffer.toString();
        }
        return url;
    }

    public static Matcher matchOldUrl(String url) throws MalformedURLException {
        URL u = new URL(url);
        String host = u.getHost();
        Matcher matcher = p.matcher(host);
        return matcher;
    }

    public static String getOrgName(String url) throws MalformedURLException {
        Matcher matcher = matchOldUrl(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
