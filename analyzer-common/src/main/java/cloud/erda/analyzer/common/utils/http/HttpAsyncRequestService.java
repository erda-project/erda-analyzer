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

package cloud.erda.analyzer.common.utils.http;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.nio.reactor.IOReactorException;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-11-04 11:30
 **/
@Slf4j
public class HttpAsyncRequestService implements Serializable {

    private HttpAsyncClient httpAsyncClient;

    public void init() throws IOReactorException {
        httpAsyncClient = HttpClientFactory.getCloseableHttpClientStatus();
    }

    public HttpRequestBuilder create(HttpRequestDTO httpRequestDTO) throws Exception {
        HttpRequestBase httpRequest = getHttpRequest(httpRequestDTO.getMethod());
        httpRequest.setURI(new URI(httpRequestDTO.getUrl()));
        Map<String, String> headers = httpRequestDTO.getHeaders();
        if (headers == null) {
            headers = new HashMap<>();
        }
        if (httpRequestDTO.getContentType() != null) {
            headers.put("Content-Type", httpRequestDTO.getContentType());
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            httpRequest.setHeader(header.getKey(), header.getValue());
        }
        if (httpRequestDTO.getBody() != null && httpRequest instanceof HttpEntityEnclosingRequest) {
            String contentType = headers.get("Content-Type");
            HttpEntity httpEntity = new StringEntity(httpRequestDTO.getBody(), contentType == null ? ContentType.TEXT_PLAIN : ContentType.getByMimeType(contentType));
            ((HttpEntityEnclosingRequest) httpRequest).setEntity(httpEntity);
        }
        HttpRequestConfigDTO httpRequestConfigDTO = httpRequestDTO.getHttpRequestConfig();
        if (httpRequestConfigDTO != null) {
            if (httpRequestConfigDTO.getRequestId() != null) {
                httpRequest.setHeader("terminus-request-id", httpRequestConfigDTO.getRequestId());
                httpRequest.setHeader("terminus-request-sampled", httpRequestConfigDTO.getTraceSampled() == null ? String.valueOf(true) : String.valueOf(httpRequestConfigDTO.getTraceSampled()));
            }
            RequestConfig.Builder builder = RequestConfig.custom();
            if (!StringUtils.isEmpty(httpRequestConfigDTO.getCookieSpec())) {
                builder.setCookieSpec(httpRequestConfigDTO.getCookieSpec());
            }
            RequestConfig requestConfig = builder.setConnectTimeout(httpRequestConfigDTO.getConnectionTimeout()).setConnectionRequestTimeout(httpRequestConfigDTO.getRequestTimeout()).build();
            httpRequest.setConfig(requestConfig);
        }
        return new HttpRequestBuilder(httpAsyncClient, httpRequest);
    }

    private HttpRequestBase getHttpRequest(String method) {
        if (method == null || method.length() == 0) {
            method = HttpMethods.GET;
        }
        switch (method.toUpperCase()) {
            case HttpMethods.GET:
                return new HttpGet();
            case HttpMethods.POST:
                return new HttpPost();
            case HttpMethods.PUT:
                return new HttpPut();
            case HttpMethods.DELETE:
                return new HttpDelete();
            case HttpMethods.PATCH:
                return new HttpPatch();
            case HttpMethods.HEAD:
                return new HttpHead();
            case HttpMethods.OPTIONS:
                return new HttpOptions();
            case HttpMethods.TRACE:
                return new HttpTrace();
            default:
                throw new IllegalArgumentException(String.format("Unsupported HTTP method : %s", method));
        }
    }
}
