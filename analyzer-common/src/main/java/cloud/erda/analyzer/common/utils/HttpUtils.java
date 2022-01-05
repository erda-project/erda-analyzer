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

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.UUID;

/**
 * todo 3.11重构
 */
@Slf4j
public class HttpUtils {
    public static final CloseableHttpClient httpClient = HttpClients.createDefault();

    /**
     * 通过GET方式发起http请求
     */
    public static String doGet(String url) {
        HttpGet get = new HttpGet(url);
        get.setHeader("Internal-Client", "analyzer-alert");
        get.setHeader("content-type", "application/json");
        get.setHeader("User-ID", "1");
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(get);
//            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            HttpEntity entity = httpResponse.getEntity();
            if (null != entity) {
                return EntityUtils.toString(httpResponse.getEntity());
            }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpResponse != null)
                    httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 发送 POST 请求（HTTP），JSON形式
     *
     * @param url        调用的地址
     * @param jsonParams 调用的参数
     * @return
     * @throws Exception
     */
    public static CloseableHttpResponse doPostResponse(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
            httpPost.setHeader("Internal-Client", "analyzer-alert");
            httpPost.setHeader("User-ID", "1");
            response = httpClient.execute(httpPost);
        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return response;
    }

    public static String doPostString(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        String httpStr;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
            httpPost.setHeader("RequestID", UUID.randomUUID().toString());
            httpPost.setHeader("Internal-Client", "analyzer-alert");
//            httpPost.setHeader("User-ID", "1");
            response = httpClient.execute(httpPost);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpStr;
    }

    public static String doPutString(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPut httpPut = new HttpPut(url);

        String httpStr;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPut.setEntity(entity);
            httpPut.setHeader("content-type", "application/json");
            httpPut.setHeader("internal-client", "analyzer-alert");
            httpPut.setHeader("User-ID", "1");
            response = httpClient.execute(httpPut);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpStr;
    }

    public static String doPut(String url) throws Exception {
        CloseableHttpResponse response = null;
        HttpPut httpPut = new HttpPut(url);

        String httpStr;
        try {
            httpPut.setHeader("content-type", "application/json");
            httpPut.setHeader("internal-client", "analyzer-alert");
            httpPut.setHeader("User-ID", "1");
            response = httpClient.execute(httpPut);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");
        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpStr;
    }

}