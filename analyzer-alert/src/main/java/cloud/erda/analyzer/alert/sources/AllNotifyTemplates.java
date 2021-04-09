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

package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.NotifyTemplate;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.utils.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class AllNotifyTemplates {
    public ArrayList<NotifyTemplate> GetSysTemplateList(Properties props) throws IOException {
        ArrayList<NotifyTemplate> templateArr = new ArrayList<>();
        String monitorAddr = (String) props.getOrDefault(Constants.MONITOR_ADDR, "monitor.default.svc.cluster.local:7096");
        String uri = "/api/notify/all-templates";
        String templateUrl = "http://" + monitorAddr + uri;
//        String templateUrl = "http://localhost:7096/api/notify/all-templates";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpGet = new HttpGet(templateUrl);
            CloseableHttpResponse closeableHttpResponse = httpclient.execute(httpGet);
            try {
                if (closeableHttpResponse.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
                    String str = EntityUtils.toString(closeableHttpResponse.getEntity());
                    Map<String, Object> templateMap = GsonUtil.toMap(str, String.class, Object.class);
                    Object templateInfo = templateMap.get("data");
                    templateArr = GsonUtil.toArrayList(GsonUtil.toJson(templateInfo),NotifyTemplate.class);
                }
            } finally {
                closeableHttpResponse.close();
            }
        } catch (Exception e) {
            log.error("get all templates is failed err:", e);
            return templateArr;
        } finally {
            httpclient.close();
        }
        return templateArr;
    }
}
