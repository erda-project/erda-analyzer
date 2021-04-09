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

import cloud.erda.analyzer.common.functions.TerminusElasticsearchIndexFactory;
import cloud.erda.analyzer.common.functions.TerminusElasticsearchSinkFunction;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuhaoyang
 */
@PublicEvolving
public class TerminusElasticsearchSink<T> extends ElasticsearchSinkBase<T, RestHighLevelClient> {

    private static final long serialVersionUID = 1L;

    private TerminusElasticsearchSink(
            Map<String, String> bulkRequestsConfig,
            List<HttpHost> httpHosts,
            TerminusElasticsearchSinkFunction<T> elasticsearchSinkFunction,
            TerminusElasticsearchIndexFactory<T> elasticsearchIndexFactory,
            ActionRequestFailureHandler failureHandler,
            RestClientFactory restClientFactory,
            ParameterTool parameterTool) {

        super(new TerminusElasticsearch6ApiCallBridge(httpHosts, restClientFactory), bulkRequestsConfig, (ElasticsearchSinkFunction<T>) (element, ctx, indexer) -> elasticsearchSinkFunction.process(elasticsearchIndexFactory.create(element), element, ctx, indexer), failureHandler);
    }

    /**
     * A builder for creating an {@link org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink}.
     *
     * @param <T> Type of the elements handled by the sink this builder creates.
     */
    @PublicEvolving
    public static class Builder<T> {

        private final ParameterTool parameterTool;
        private final List<HttpHost> httpHosts;
        private final TerminusElasticsearchSinkFunction<T> elasticsearchSinkFunction;
        private final TerminusElasticsearchIndexFactory<T> elasticsearchIndexFactory;

        private Map<String, String> bulkRequestsConfig = new HashMap<>();
        private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
        private RestClientFactory restClientFactory = restClientBuilder -> {
        };

        /**
         * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
         *
         * @param httpHosts                 The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
         * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming
         *                                  element.
         */
        public Builder(ParameterTool parameterTool, List<HttpHost> httpHosts, TerminusElasticsearchSinkFunction<T> elasticsearchSinkFunction, TerminusElasticsearchIndexFactory<T> elasticsearchIndexFactory) {
            this.parameterTool = parameterTool;
            this.httpHosts = Preconditions.checkNotNull(httpHosts);
            this.elasticsearchSinkFunction = Preconditions.checkNotNull(elasticsearchSinkFunction);
            this.elasticsearchIndexFactory = elasticsearchIndexFactory;
        }

        /**
         * Sets the maximum number of actions to buffer for each bulk request.
         *
         * @param numMaxActions the maxinum number of actions to buffer per bulk request.
         */
        public void setBulkFlushMaxActions(int numMaxActions) {
            Preconditions.checkArgument(
                    numMaxActions > 0,
                    "Max number of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
        }

        /**
         * Sets the maximum size of buffered actions, in mb, per bulk request.
         *
         * @param maxSizeMb the maximum size of buffered actions, in mb.
         */
        public void setBulkFlushMaxSizeMb(int maxSizeMb) {
            Preconditions.checkArgument(
                    maxSizeMb > 0,
                    "Max size of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
        }

        /**
         * Sets the bulk flush interval, in milliseconds.
         *
         * @param intervalMillis the bulk flush interval, in milliseconds.
         */
        public void setBulkFlushInterval(long intervalMillis) {
            Preconditions.checkArgument(
                    intervalMillis >= 0,
                    "Interval (in milliseconds) between each flush must be larger than or equal to 0.");

            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
        }

        /**
         * Sets whether or not to enable bulk flush backoff behaviour.
         *
         * @param enabled whether or not to enable backoffs.
         */
        public void setBulkFlushBackoff(boolean enabled) {
            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
        }

        /**
         * Sets the type of back of to use when flushing bulk requests.
         *
         * @param flushBackoffType the backoff type to use.
         */
        public void setBulkFlushBackoffType(FlushBackoffType flushBackoffType) {
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
                    Preconditions.checkNotNull(flushBackoffType).toString());
        }

        /**
         * Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
         *
         * @param maxRetries the maximum number of retries for a backoff attempt when flushing bulk requests
         */
        public void setBulkFlushBackoffRetries(int maxRetries) {
            Preconditions.checkArgument(
                    maxRetries > 0,
                    "Max number of backoff attempts must be larger than 0.");

            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
        }

        /**
         * Sets the amount of delay between each backoff attempt when flushing bulk requests, in milliseconds.
         *
         * @param delayMillis the amount of delay between each backoff attempt when flushing bulk requests, in
         *                    milliseconds.
         */
        public void setBulkFlushBackoffDelay(long delayMillis) {
            Preconditions.checkArgument(
                    delayMillis >= 0,
                    "Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
            this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
        }

        /**
         * Sets a failure handler for action requests.
         *
         * @param failureHandler This is used to handle failed {@link ActionRequest}.
         */
        public void setFailureHandler(ActionRequestFailureHandler failureHandler) {
            this.failureHandler = Preconditions.checkNotNull(failureHandler);
        }

        /**
         * Sets a REST client factory for custom client configuration.
         *
         * @param restClientFactory the factory that configures the rest client.
         */
        public void setRestClientFactory(RestClientFactory restClientFactory) {
            this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
        }

        /**
         * Creates the Elasticsearch sink.
         *
         * @return the created Elasticsearch sink.
         */
        public TerminusElasticsearchSink<T> build() {
            return new TerminusElasticsearchSink<>(bulkRequestsConfig, httpHosts, elasticsearchSinkFunction, elasticsearchIndexFactory, failureHandler, restClientFactory, parameterTool);
        }
    }
}