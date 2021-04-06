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

package cloud.erda.analyzer.common.sinks;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * @author liuhaoyang
 */
public class TerminusElasticsearch6ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

    private static final long serialVersionUID = -5222683870097809633L;

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6ApiCallBridge.class);

    /**
     * User-provided HTTP Host.
     */
    private final List<HttpHost> httpHosts;

    /**
     * The factory to configure the rest client.
     */
    private final RestClientFactory restClientFactory;

    public TerminusElasticsearch6ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
        Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());
        this.httpHosts = httpHosts;
        this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
    }

    @Override
    public RestHighLevelClient createClient(Map<String, String> clientConfig) {
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        restClientFactory.configureRestClientBuilder(builder);

        RestHighLevelClient rhlClient = new RestHighLevelClient(builder);

        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHosts);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHosts.toString());
        }

        return rhlClient;
    }

    @Override
    public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client,
        BulkProcessor.Listener listener) {
        return BulkProcessor.builder(client::bulkAsync, listener);
    }

    @Override
    public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
        if (!bulkItemResponse.isFailed()) {
            return null;
        } else {
            return bulkItemResponse.getFailure().getCause();
        }
    }

    @Override
    public void configureBulkProcessorBackoff(
        BulkProcessor.Builder builder,
        @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

        BackoffPolicy backoffPolicy;
        if (flushBackoffPolicy != null) {
            switch (flushBackoffPolicy.getBackoffType()) {
                case CONSTANT:
                    backoffPolicy = BackoffPolicy.constantBackoff(
                        new TimeValue(flushBackoffPolicy.getDelayMillis()),
                        flushBackoffPolicy.getMaxRetryCount());
                    break;
                case EXPONENTIAL:
                default:
                    backoffPolicy = BackoffPolicy.exponentialBackoff(
                        new TimeValue(flushBackoffPolicy.getDelayMillis()),
                        flushBackoffPolicy.getMaxRetryCount());
            }
        } else {
            backoffPolicy = BackoffPolicy.noBackoff();
        }

        builder.setBackoffPolicy(backoffPolicy);
    }

    @Override
    public void verifyClientConnection(RestHighLevelClient restHighLevelClient) throws IOException {
        if (!restHighLevelClient.ping()) {
            throw new RuntimeException("There are no reachable Elasticsearch nodes!");
        }
    }

    @Override
    public RequestIndexer createBulkProcessorIndexer(
        BulkProcessor bulkProcessor,
        boolean flushOnCheckpoint,
        AtomicLong numPendingRequestsRef) {
        return new Elasticsearch6BulkProcessorIndexer(
            bulkProcessor,
            flushOnCheckpoint,
            numPendingRequestsRef);
    }

    public static class Elasticsearch6BulkProcessorIndexer implements RequestIndexer {
        private final BulkProcessor bulkProcessor;
        private final boolean flushOnCheckpoint;
        private final AtomicLong numPendingRequestsRef;

        Elasticsearch6BulkProcessorIndexer(
            BulkProcessor bulkProcessor,
            boolean flushOnCheckpoint,
            AtomicLong numPendingRequestsRef) {
            this.bulkProcessor = checkNotNull(bulkProcessor);
            this.flushOnCheckpoint = flushOnCheckpoint;
            this.numPendingRequestsRef = checkNotNull(numPendingRequestsRef);
        }

        @Override
        public void add(DeleteRequest... deleteRequests) {
            for (DeleteRequest deleteRequest : deleteRequests) {
                if (flushOnCheckpoint) {
                    numPendingRequestsRef.getAndIncrement();
                }
                this.bulkProcessor.add(deleteRequest);
            }
        }

        @Override
        public void add(IndexRequest... indexRequests) {
            for (IndexRequest indexRequest : indexRequests) {
                if (flushOnCheckpoint) {
                    numPendingRequestsRef.getAndIncrement();
                }
                this.bulkProcessor.add(indexRequest);
            }
        }

        @Override
        public void add(UpdateRequest... updateRequests) {
            for (UpdateRequest updateRequest : updateRequests) {
                if (flushOnCheckpoint) {
                    numPendingRequestsRef.getAndIncrement();
                }
                this.bulkProcessor.add(updateRequest);
            }
        }
    }
}
