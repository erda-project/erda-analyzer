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

import cloud.erda.analyzer.alert.models.Ticket;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.common.utils.http.ContentTypes;
import cloud.erda.analyzer.common.utils.http.HttpAsyncRequestService;
import cloud.erda.analyzer.common.utils.http.HttpMethods;
import cloud.erda.analyzer.common.utils.http.HttpRequestDTO;
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
 * @create: 2020-01-07 17:13
 **/
@Slf4j
public class TicketSink extends RichSinkFunction<Ticket> {

    private String ticketCreateURL;
    private String ticketCloseURL;

    private HttpAsyncRequestService httpAsyncRequestService = new HttpAsyncRequestService();

    public TicketSink(Properties props) {
        String cmdbAddr = (String) props.getOrDefault(Constants.CMDB_ADDR, "cmdb.marathon.l4lb.thisdcos.directory:9093");
        ticketCreateURL = "http://" + cmdbAddr + "/api/tickets";
        ticketCloseURL = "http://" + cmdbAddr + "/api/tickets/actions/close-by-key?key=";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        httpAsyncRequestService.init();
    }

    @Override
    public void invoke(Ticket value, Context context) throws Exception {
        String body = GsonUtil.toJson(value);
        HttpRequestDTO httpRequestDTO = new HttpRequestDTO();
        String trigger = value.getLabel().getOrDefault(AlertConstants.TRIGGER, AlertTrigger.alert.name());
        if (AlertTrigger.alert.equals(AlertTrigger.valueOf(trigger))) {
            httpRequestDTO.setBody(body);
            httpRequestDTO.setContentType(ContentTypes.APPLICATION_JSON);
            httpRequestDTO.setMethod(HttpMethods.POST);
            httpRequestDTO.setUrl(ticketCreateURL);
        } else {
            httpRequestDTO.setUrl(ticketCloseURL + value.getKey());
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("internal-client", "analyzer-alert");
        headers.put("User-ID", "1");
        httpRequestDTO.setHeaders(headers);

        httpAsyncRequestService.create(httpRequestDTO).onCompleted(r -> {
            HttpEntity httpEntity = r.getEntity();
            log.info("Send message to ticket.\nrequest {} \nresponse {}", body, EntityUtils.toString(httpEntity));
            EntityUtils.consume(httpEntity);
        }).onFailed(ex -> {
            log.error("Send message to ticket fail. \ndata === " + body, ex);
        }).request();
    }
}
