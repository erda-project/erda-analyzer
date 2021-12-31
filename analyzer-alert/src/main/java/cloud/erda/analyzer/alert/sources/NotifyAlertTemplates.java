package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.AlertNotifyTemplate;
import cloud.erda.analyzer.alert.models.AlertNotifyTemplateData;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.sources.HttpSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class NotifyAlertTemplates implements SourceFunction<AlertNotifyTemplate> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public NotifyAlertTemplates(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<AlertNotifyTemplate> GetEnabledTemplates() throws Exception {
        String uri = "/api/alert/templates";
        String templateUrl = "http://" + monitorAddr + uri;
        ArrayList<AlertNotifyTemplate> notifyTemplateList = new ArrayList<>();
        while (true) {
            String url = String.format(templateUrl, this.pageNo, this.pageSize);
            AlertNotifyTemplateData alertNotifyTemplateData = HttpSource.doHttpGet(url, AlertNotifyTemplateData.class);
            for (AlertNotifyTemplate alertNotifyTemplate : alertNotifyTemplateData.getList()) {
                checkNotNull(alertNotifyTemplate.getTitle(), "Title cannot be null");
                checkNotNull(alertNotifyTemplate.getTemplate(), "Template cannot be null");
                alertNotifyTemplate.setProcessingTime(System.currentTimeMillis());
                if (alertNotifyTemplate.getAlertType().contains("customize")) {
                    alertNotifyTemplate.setVariable(false);
                } else {
                    alertNotifyTemplate.setVariable(true);
                }
                alertNotifyTemplate.setId(alertNotifyTemplate.getAlertIndex() + "_" + alertNotifyTemplate.getTarget());
                log.info("Read notify template {} data: {}", alertNotifyTemplate.getAlertIndex(), JsonMapperUtils.toStrings(alertNotifyTemplate));
                notifyTemplateList.add(alertNotifyTemplate);
            }
            if (this.pageNo * this.pageSize >= alertNotifyTemplateData.getTotal()) {
                break;
            }
            this.pageNo++;
        }
        return notifyTemplateList;
    }

    @Override
    public void run(SourceContext<AlertNotifyTemplate> sourceContext) throws Exception {
        while (true) {
            ArrayList<AlertNotifyTemplate> alertNotifyTemplates = GetEnabledTemplates();
            for (AlertNotifyTemplate alertNotifyTemplate : alertNotifyTemplates) {
                sourceContext.collect(alertNotifyTemplate);
            }
            Thread.sleep(this.httpInterval);
        }
    }

    @Override
    public void cancel() {

    }
}
