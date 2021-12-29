package cloud.erda.analyzer.runtime.httpconnect;

import cloud.erda.analyzer.common.utils.GsonUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConnectManager {
    private static CloseableHttpClient httpClient = null;
    static {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(1500);
        connectionManager.setDefaultMaxPerRoute(150);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(10000)
                .setConnectionRequestTimeout(10000)
                .setSocketTimeout(10000)
                .build();
        httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .disableAutomaticRetries()
                .build();
    }
    public static <T> T doHttpGet(String uri, Map<String,String> getParams,Class<T> clazz) {
        HttpGet httpGet = null;
        CloseableHttpResponse response = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(uri);
            if (getParams != null && !getParams.isEmpty()) {
                List<NameValuePair> list = new ArrayList<>();
                for (Map.Entry<String,String> param : getParams.entrySet()) {
                    list.add(new BasicNameValuePair(param.getKey(),param.getValue()));
                }
                uriBuilder.setParameters(list);
            }
            httpGet = new HttpGet(uriBuilder.build());
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpURLConnection.HTTP_OK) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String str = EntityUtils.toString(entity);
                    Map<String,Object> dataMap = GsonUtil.toMap(str,String.class,Object.class);
                    String data = JSON.toJSONString(dataMap.get("data"));
                    return JSONObject.parseObject(data,clazz);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                if (httpGet != null) {
                    httpGet.releaseConnection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
