package cloud.erda.analyzer.alert.utils;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpClientPool {
    private static final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();

    static {
        poolingHttpClientConnectionManager.setMaxTotal(200);
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(20);
    }

    public static CloseableHttpClient getConnection() {
        RequestConfig config = RequestConfig.custom().build();
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(config)
                .build();
        return httpClient;
    }
}
