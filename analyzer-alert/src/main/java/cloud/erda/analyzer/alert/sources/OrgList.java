package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.DiceOrg;
import cloud.erda.analyzer.alert.utils.HttpClientPool;
import cloud.erda.analyzer.common.utils.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class OrgList extends HttpRequestSource<DiceOrg> {

    public OrgList(String requestAddr) {
        super(requestAddr);
    }

    @Override
    public ArrayList<DiceOrg> HttpRequestGetData() throws IOException {
        ArrayList<DiceOrg> orgs = new ArrayList<>();
        String uri = "/api/orgs";
        String orgUrl = "http://" + this.requestAddr + uri;
        this.httpClient = HttpClientPool.getHttpClient();
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
            if (this.closeableHttpResponse != null) {
                this.closeableHttpResponse.close();
            }
        }
        return orgs;
    }
}

