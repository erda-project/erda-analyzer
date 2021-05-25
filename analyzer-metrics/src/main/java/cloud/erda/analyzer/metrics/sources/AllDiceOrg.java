package cloud.erda.analyzer.metrics.sources;

import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.runtime.models.DiceOrg;
import lombok.extern.slf4j.Slf4j;
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
public class AllDiceOrg implements SourceFunction<DiceOrg> {
    private String cmdbAddr;
    private CloseableHttpClient httpClient;
    private long httpInterval = 60000;
    private CloseableHttpResponse closeableHttpResponse;

    public AllDiceOrg(String cmdbAddr) {
        this.cmdbAddr = cmdbAddr;
    }

    public ArrayList<DiceOrg> GetAllDiceOrg() throws IOException {
        ArrayList<DiceOrg> orgs = new ArrayList<>();
        String uri = "/api/orgs";
        String orgUrl = "http://" + cmdbAddr + uri;
        this.httpClient = HttpClients.createDefault();
        try {
            HttpGet httpGet = new HttpGet(orgUrl);
            httpGet.addHeader("Internal-Client","bundle");
            httpGet.addHeader("pageNo", String.valueOf(1));
            httpGet.addHeader("pageSize", String.valueOf(1000));
            closeableHttpResponse = httpClient.execute(httpGet);
            if (closeableHttpResponse.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
                String responseContent = EntityUtils.toString(closeableHttpResponse.getEntity());
                Map<String,Object> orgMap = GsonUtil.toMap(responseContent,String.class,Object.class);
                Object orgInfo = orgMap.get("data");
                Map<String,Object> orgListMap = GsonUtil.toMap(GsonUtil.toJson(orgInfo),String.class,Object.class);
                Object listInfo = orgListMap.get("list");
                orgs = GsonUtil.toArrayList(GsonUtil.toJson(listInfo),DiceOrg.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            closeableHttpResponse.close();
        }
        return orgs;
    }

    @Override
    public void run(SourceContext<DiceOrg> sourceContext) throws Exception {
        while (true) {
            val orgList = GetAllDiceOrg();
            for (DiceOrg org : orgList) {
                sourceContext.collect(org);
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
