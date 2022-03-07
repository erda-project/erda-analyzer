package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.Org;
import cloud.erda.analyzer.alert.models.OrgData;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.sources.HttpSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Map;

@Slf4j
public class OrgLocaleReader implements SourceFunction<Org> {
    private String monitorAddr;
    private long httpInterval = 60000;

    public OrgLocaleReader(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<Org> GetOrgLocale() throws Exception {
        String uri = "/api/alert/org-locale";
        ArrayList<Org> orgs = new ArrayList<>();
        OrgData orgData = HttpSource.doHttpGet(uri, this.monitorAddr, 0, 0, OrgData.class);
        log.isInfoEnabled();
        if (orgData != null) {
            if (!orgData.isSuccess()) {
                log.info("get org locale is failed err is {}", orgData.getErr().toString());
                return orgs;
            }
            for (Map.Entry<String, String> entry : orgData.getData().entrySet()) {
                Org org = new Org();
                org.setName(entry.getKey());
                org.setLocale(entry.getValue());
                org.setProcessingTime(System.currentTimeMillis());
                log.info("Read Org data:{}", JsonMapperUtils.toStrings(org));
                orgs.add(org);
            }
        }
        return orgs;
    }

    @Override
    public void run(SourceContext<Org> sourceContext) throws Exception {
        while (true) {
            ArrayList<Org> orgs = GetOrgLocale();
            for (Org org : orgs) {
                sourceContext.collect(org);
            }
            Thread.sleep(this.httpInterval);
        }
    }

    @Override
    public void cancel() {

    }
}
