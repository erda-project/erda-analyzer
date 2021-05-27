package cloud.erda.analyzer.alert.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpClientPool {
    private static PoolingHttpClientConnectionManager pool;
    private static RequestConfig requestConfig;

    static {
        SSLContextBuilder builder = new SSLContextBuilder();
        try {
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        }
        SSLConnectionSocketFactory sslsf = null;
        try {
            sslsf = new SSLConnectionSocketFactory(builder.build());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslsf).build();
        pool = new PoolingHttpClientConnectionManager(
                socketFactoryRegistry);
        pool.setMaxTotal(200);
        pool.setDefaultMaxPerRoute(20);
        int socketTimeout = 1000;
        int connectTimeout = 10000;
        int connectionRequestTimeout = 10000;
        requestConfig = RequestConfig.custom().setConnectionRequestTimeout(
                connectionRequestTimeout).setSocketTimeout(socketTimeout).setConnectTimeout(
                connectTimeout).build();
    }

    public static CloseableHttpClient getHttpClient() {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(pool)
                .setDefaultRequestConfig(requestConfig)
                .evictIdleConnections(2, TimeUnit.SECONDS)
                .setRetryHandler((exception,executionCount,context) -> {
                    if (executionCount > 3) {
                        log.warn("Maximum tries reached for client http pool");
                        return false;
                    }
                    if (exception instanceof NoHttpResponseException) {
                        log.warn("NoHttpResponseException on " + executionCount + " call");
                        return true;
                    }
                    if (exception instanceof SSLHandshakeException) {
                        return false;
                    }
                    if (exception instanceof InterruptedIOException) {
                        return false;
                    }
                    if (exception instanceof UnknownHostException) {
                        return false;
                    }
                    if (exception instanceof ConnectTimeoutException) {
                        return false;
                    }
                    return false;
                })
                .build();
        return httpClient;

    }
}
