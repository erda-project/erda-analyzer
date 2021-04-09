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

package cloud.erda.analyzer.common.utils;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.TerminusElasticsearchIndexFactory;
import cloud.erda.analyzer.common.functions.TerminusElasticsearchSinkFunction;
import cloud.erda.analyzer.common.sinks.TerminusElasticsearchSink;
import lombok.val;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;

public class ElasticSearchSinkUtil {

    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchSinkUtil.class);

    public static <T> void addSink(ParameterTool parameterTool, List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> output, TerminusElasticsearchSinkFunction<T> func, TerminusElasticsearchIndexFactory<T> indexFactory) {
        TerminusElasticsearchSink.Builder<T> esSinkBuilder = new TerminusElasticsearchSink.Builder<>(parameterTool, hosts, func, indexFactory);
        esSinkBuilder.setRestClientFactory((RestClientFactory) restClientBuilder -> {
            restClientBuilder.setMaxRetryTimeoutMillis(parameterTool.getInt("es.maxretry.timeout", 5 * 60 * 1000))
                    .setRequestConfigCallback(builder -> {
                        builder.setConnectTimeout(parameterTool.getInt("es.request.connect.timeout", 5000));
                        builder.setSocketTimeout(parameterTool.getInt("es.request.socket.timeout", 4000));
                        builder.setConnectionRequestTimeout(parameterTool.getInt("es.request.connectionrequest.timeout", 1000));
                        return builder;
                    });
            if (parameterTool.getBoolean(Constants.ES_SECURITY_ENABLE, false)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(parameterTool.get(Constants.ES_SECURITY_USERNAME), parameterTool.get(Constants.ES_SECURITY_PASSWORD)));
                restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
        });
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        output.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    public static class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {
        private static final long serialVersionUID = -7423562912824511906L;

        public RetryRejectedExecutionFailureHandler() {
        }

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(new ActionRequest[]{action});
            } else {
                if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                    // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启flink job
                    return;
                } else {
                    val exp = ExceptionUtils.findThrowable(failure, IOException.class);
                    if (exp.isPresent()) {
                        IOException ioExp = exp.get();
                        if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                            // request retries exceeded max retry timeout
                            // 经过多次不同的节点重试，还是写入失败的，则忽略这个错误，丢失数据。
                            logger.error(ioExp.getMessage());
                            return;
                        }
                    }
                }
                logger.error(failure.getMessage() + action.toString());
            }
        }
    }
}
