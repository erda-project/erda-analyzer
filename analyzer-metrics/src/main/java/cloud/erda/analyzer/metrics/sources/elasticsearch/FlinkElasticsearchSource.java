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

package cloud.erda.analyzer.metrics.sources.elasticsearch;

import com.google.gson.Gson;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * { "_index": "spot-machine_summary-full_cluster", "_id": "{{cluster_name}}/{{host_ip}}/{{terminus_version}}"
 * "_source": { "name": "machine_summary", "timestamp": 1587957030000000000, "@timestamp": 1587957030000, "tags": {
 * "name": "machine_summary", "timestamp": 1587957030000000000, "@timestamp": 1587957030000, "tags": { "cluster_name":
 * "terminus-dev", "host_ip": "10.168.0.100", "terminus_index_id": "terminus-dev/10.168.0.100/2", "terminus_version":
 * "2" }, "fields": { "labels": [ "org-terminus", "offline" ] } } } }
 */

/**
 * @author randomnil
 */
@Slf4j
public class FlinkElasticsearchSource extends AbstractRichFunction implements SourceFunction<MetricEvent> {

    private final static long DEFAULT_INTERVAL = 60000;

    private ParameterTool parameterTool;
    private HttpHost[] httpHosts;
    private String index;
    private String query;
    private final long interval;
    private RestHighLevelClient client;

    private final static Gson GSON = new Gson();

    private volatile boolean isRunning = true;

    public FlinkElasticsearchSource(long interval, String index, String query, ParameterTool parameterTool) {
        String esUrl = parameterTool.get(Constants.ES_URL);
        String[] esUrls = esUrl.split(",");
        this.httpHosts = new HttpHost[esUrls.length];
        for (int i = 0; i < esUrls.length; i++) {
            URL url;
            try {
                url = new URL(esUrls[i]);
            } catch (MalformedURLException e) {
                String msg = String.format("url %s is not correct", esUrl);
                log.error(msg, e);
                throw new RuntimeException(msg);
            }

            HttpHost httpHost = new HttpHost(url.getHost(), url.getPort());
            this.httpHosts[i] = httpHost;
        }

        if (interval <= 0) {
            interval = DEFAULT_INTERVAL;
        }
        this.interval = interval;
        this.index = index;
        this.query = query;
        this.parameterTool = parameterTool;
    }

    @Override
    public void run(SourceContext<MetricEvent> ctx) throws Exception {
        val searchRequest = this.buildSearchRequest();

        while (isRunning) {
            if (this.client == null) {
                this.initESClient();
            }
            if (this.client == null) {
                TimeUnit.MILLISECONDS.sleep(this.interval);
                continue;
            }

            this.searchRequest(ctx, searchRequest);
            TimeUnit.MILLISECONDS.sleep(this.interval);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
        if (this.client != null) {
            try {
                this.client.close();
            } catch (IOException e) {
                log.error("Close es client error", e);
            }
        }
    }

    private SearchRequest buildSearchRequest() {
        val queryBuilder = QueryBuilders.wrapperQuery(this.query);
        val searchBuilder = new SearchSourceBuilder().query(queryBuilder);
        return new SearchRequest()
            .indicesOptions(IndicesOptions.fromOptions(
                true,
                true,
                SearchRequest.DEFAULT_INDICES_OPTIONS.expandWildcardsOpen(),
                SearchRequest.DEFAULT_INDICES_OPTIONS.expandWildcardsClosed(),
                SearchRequest.DEFAULT_INDICES_OPTIONS
            ))
            .indices(this.index)
            .source(searchBuilder);
    }

    private void initESClient() {
        val clientBuilder = RestClient.builder(httpHosts);
        RestClientFactory restClientFactory = restClientBuilder -> {
            restClientBuilder
                .setMaxRetryTimeoutMillis(parameterTool.getInt(Constants.ES_MAXRETRY_TIMEOUT, 5 * 60 * 1000))
                .setRequestConfigCallback(builder -> {
                    builder.setConnectTimeout(parameterTool.getInt(Constants.ES_REQUEST_CONNECT_TIMEOUT, 5000));
                    builder.setSocketTimeout(parameterTool.getInt(Constants.ES_REQUEST_SOCKET_TIMEOUT, 4000));
                    builder.setConnectionRequestTimeout(
                        parameterTool.getInt(Constants.ES_REQUEST_CONN_REQUEST_TIMEOUT, 1000));
                    return builder;
                });
            if (parameterTool.getBoolean(Constants.ES_SECURITY_ENABLE, false)) {
                val credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(
                        parameterTool.get(Constants.ES_SECURITY_USERNAME),
                        parameterTool.get(Constants.ES_SECURITY_PASSWORD)));
                restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider));
            }
        };
        restClientFactory.configureRestClientBuilder(clientBuilder);

        val rhlClient = new RestHighLevelClient(clientBuilder);
        try {
            if (!rhlClient.ping()) {
                log.error("There are no reachable Elasticsearch nodes!");
            } else {
                this.client = rhlClient;
            }
        } catch (Exception e) {
            log.error("es client ping error.", e);
        }
    }

    private void searchRequest(SourceContext<MetricEvent> ctx, SearchRequest searchRequest) {
        SearchResponse searchResponse;
        try {
            searchResponse = this.client.search(searchRequest);
        } catch (Exception e) {
            log.error("es client search {} error.", searchRequest.toString(), e);
            return;
        }

        if (searchResponse == null || searchResponse.getHits() == null) {
            return;
        }
        for (val hit : searchResponse.getHits().getHits()) {
            val metric = GSON.fromJson(hit.getSourceAsString(), MetricEvent.class);
            ctx.collect(metric);
        }
    }
}
