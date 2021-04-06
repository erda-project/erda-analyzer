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
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.nio.client.HttpAsyncClient;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author: liuhaoyang
 * @create: 2019-11-04 14:31
 **/
public class HttpRequestBuilder {

    private final static HttpRequestCompletedCallback defaultHttpRequestCompletedCallback = new DefaultHttpRequestCompletedCallback();
    private final static HttpRequestFailedCallback defaultHttpRequestFailedCallback = new DefaultHttpRequestFailedCallback();

    private HttpAsyncClient httpAsyncClient;
    private HttpRequestBase httpRequest;
    private HttpRequestCompletedCallback completedCallback = defaultHttpRequestCompletedCallback;
    private HttpRequestFailedCallback failedCallback = defaultHttpRequestFailedCallback;

    public HttpRequestBuilder(HttpAsyncClient httpAsyncClient, HttpRequestBase httpRequest) {
        this.httpAsyncClient = httpAsyncClient;
        this.httpRequest = httpRequest;
    }

    public HttpRequestBuilder onCompleted(HttpRequestCompletedCallback completedCallback) {
        this.completedCallback = completedCallback;
        return this;
    }

    public HttpRequestBuilder onFailed(HttpRequestFailedCallback failedCallback) {
        this.failedCallback = failedCallback;
        return this;
    }

    public Future<HttpResponse> request() {
        return httpAsyncClient.execute(httpRequest, new DefaultHttpRequestCallback(completedCallback, failedCallback));
    }

    public static class DefaultHttpRequestCallback implements FutureCallback<HttpResponse> {

        private HttpRequestCompletedCallback completedCallback;
        private HttpRequestFailedCallback failedCallback;

        public DefaultHttpRequestCallback(HttpRequestCompletedCallback completedCallback, HttpRequestFailedCallback failedCallback) {
            this.completedCallback = completedCallback;
            this.failedCallback = failedCallback;
        }

        @Override
        public void completed(HttpResponse result) {
            if (completedCallback != null) {
                try {
                    this.completedCallback.completed(result);
                } catch (IOException e) {
                    this.failedCallback.failed(e);
                }
            }
        }

        @Override
        public void failed(Exception ex) {
            if (failedCallback != null) {
                this.failedCallback.failed(ex);
            }
        }

        @Override
        public void cancelled() {
        }
    }

    public static class DefaultHttpRequestCompletedCallback implements HttpRequestCompletedCallback {

        @Override
        public void completed(HttpResponse result) {
        }
    }

    @Slf4j
    public static class DefaultHttpRequestFailedCallback implements HttpRequestFailedCallback {

        @Override
        public void failed(Exception ex) {
            log.error("Request invoke failed.", ex);
        }
    }
}