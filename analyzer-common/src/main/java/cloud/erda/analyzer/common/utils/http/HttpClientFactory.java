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

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.IOReactorException;

/**
 * @author: liuhaoyang
 * @create: 2020-01-14 13:31
 **/
public class HttpClientFactory {

    private static int httpClientMaxConnection = 256;

    private static int httpClientMaxPerRoute = 32;

    private static int httpClientConnectTimeout = 15000;

    private static int httpClientSocketTimeout = 15000;

    private static int httpClientConnectionRequestTimeout = 30000;

    private static PoolingNHttpClientConnectionManager getNHttpClientConnectionManagerStatus() throws IOReactorException {
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(Runtime.getRuntime().availableProcessors())
                .setSoKeepAlive(true).build();
        PoolingNHttpClientConnectionManager httpClientConnectionManager = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor(ioReactorConfig));
        httpClientConnectionManager.setMaxTotal(httpClientMaxConnection);
        httpClientConnectionManager.setDefaultMaxPerRoute(httpClientMaxPerRoute);
        return httpClientConnectionManager;
    }

    public static CloseableHttpAsyncClient getCloseableHttpClientStatus() throws IOReactorException {
        HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create();
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(httpClientConnectTimeout)
                .setSocketTimeout(httpClientSocketTimeout)
                .setConnectionRequestTimeout(httpClientConnectionRequestTimeout)
                .build();
        builder.setConnectionManager(getNHttpClientConnectionManagerStatus());
        builder.setDefaultRequestConfig(requestConfig);
        CloseableHttpAsyncClient client = builder.build();
        client.start();
        return client;
    }
}
