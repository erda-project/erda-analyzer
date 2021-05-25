package cloud.erda.analyzer.metrics.sources;

import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.runtime.models.DiceOrg;
import lombok.extern.slf4j.Slf4j;
import cloud.erda.analyzer.common.source.*;
import lombok.val;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class AllDiceOrg extends HttpRequestSource<DiceOrg> {

    public AllDiceOrg(String requestAddr) {
        super(requestAddr);
    }

    @Override
    public ArrayList<DiceOrg> HttpRequestGetData() throws IOException {
        ArrayList<DiceOrg> orgs = new ArrayList<>();
        String uri = "/api/orgs";

        String orgUrl = "http://" + this.requestAddr + uri;
        this.httpClient = HttpClients.createDefault();
        try {
            HttpGet httpGet = new HttpGet(orgUrl);
            httpGet.addHeader("Internal-Client", "bundle");
            httpGet.addHeader("pageNo", String.valueOf(1));
            httpGet.addHeader("pageSize", String.valueOf(1000));
            this.closeableHttpResponse = this.httpClient.execute(httpGet);
            if (this.closeableHttpResponse.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
                String responseContent = EntityUtils.toString(this.closeableHttpResponse.getEntity());
                Map<String, Object> orgMap = GsonUtil.toMap(responseContent, String.class, Object.class);
                Object orgInfo = orgMap.get("data");
                Map<String, Object> orgListMap = GsonUtil.toMap(GsonUtil.toJson(orgInfo), String.class, Object.class);
                Object listInfo = orgListMap.get("list");
                orgs = GsonUtil.toArrayList(GsonUtil.toJson(listInfo), DiceOrg.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.closeableHttpResponse.close();
        }
        return orgs;
    }
}

