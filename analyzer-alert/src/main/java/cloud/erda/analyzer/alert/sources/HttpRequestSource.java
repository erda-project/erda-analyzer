package cloud.erda.analyzer.alert.sources;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;

@Slf4j
public abstract class HttpRequestSource<T> implements SourceFunction<T> {
    public CloseableHttpClient httpClient;
    public String requestAddr;
    public final long httpInterval = 60000;
    public CloseableHttpResponse closeableHttpResponse;

    public HttpRequestSource(String requestAddr) {
        this.requestAddr = requestAddr;
    }

    public abstract ArrayList<T> HttpRequestGetData() throws IOException;

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        while (true) {
            ArrayList<T> list = HttpRequestGetData();
            for (T t : list) {
                sourceContext.collect(t);
            }
            Thread.sleep(this.httpInterval);
        }
    }

    @Override
    public void cancel() {
        if (this.httpClient != null) {
            try {
                this.httpClient.close();
            } catch (Exception e) {
                log.error("close httpclient is error");
            }
        }
    }
}
