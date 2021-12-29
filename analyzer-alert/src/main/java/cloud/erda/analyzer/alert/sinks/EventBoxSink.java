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

package cloud.erda.analyzer.alert.sinks;

import cloud.erda.analyzer.alert.models.eventbox.EventBoxRequest;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.common.utils.http.ContentTypes;
import cloud.erda.analyzer.common.utils.http.HttpMethods;
import cloud.erda.analyzer.common.utils.http.HttpRequestDTO;
import cloud.erda.analyzer.common.utils.http.HttpAsyncRequestService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: liuhaoyang
 * @create: 2020-01-07 10:37
 **/
@Slf4j
public class EventBoxSink extends RichSinkFunction<EventBoxRequest> {

    private String eventBoxCreateUrl;
    private HttpAsyncRequestService httpAsyncRequestService = new HttpAsyncRequestService();

    public EventBoxSink(Properties props) {
        String eventBoxAddr = (String) props.getOrDefault(Constants.EVENTBOX_ADDR, "eventbox.default.svc.cluster.local:9528");
        String eventBoxMessagePath = (String) props.getOrDefault(Constants.EVENTBOX_MESSAGE, "/api/dice/eventbox/message/create");
        eventBoxCreateUrl = "http://" + eventBoxAddr + eventBoxMessagePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        httpAsyncRequestService.init();
    }

    @Override
    public void invoke(EventBoxRequest value, Context context) throws Exception {
        if (value == null) {
            return;
        }
        String body = JsonMapperUtils.toStrings(value);
        HttpRequestDTO httpRequestDTO = new HttpRequestDTO();
        httpRequestDTO.setBody(body);
        httpRequestDTO.setContentType(ContentTypes.APPLICATION_JSON);
        httpRequestDTO.setMethod(HttpMethods.POST);
        httpRequestDTO.setUrl(eventBoxCreateUrl);
        Map<String, String> headers = new HashMap<>();
        headers.put("internal-client", "analyzer-alert");
        headers.put("User-ID", "1");
        httpRequestDTO.setHeaders(headers);
        httpAsyncRequestService.create(httpRequestDTO).onCompleted(r -> {
            HttpEntity httpEntity = r.getEntity();
            log.info("Send message to eventbox.\nrequest {} \nresponse {}", body, EntityUtils.toString(httpEntity));
            EntityUtils.consume(httpEntity);
        }).onFailed(ex -> {
            log.error("Send message to eventbox fail. \ndata === " + body, ex);
        }).request();
    }
}
