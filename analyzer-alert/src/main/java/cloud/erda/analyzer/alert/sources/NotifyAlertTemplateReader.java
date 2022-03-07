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
public class NotifyAlertTemplateReader implements SourceFunction<AlertNotifyTemplate> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public NotifyAlertTemplateReader(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<AlertNotifyTemplate> GetEnabledTemplates() throws Exception {
        String uri = "/api/alert/templates?pageNo=%d&pageSize=%d";
        ArrayList<AlertNotifyTemplate> notifyTemplateList = new ArrayList<>();
        log.isInfoEnabled();
        while (true) {
            AlertNotifyTemplateData alertNotifyTemplateData = HttpSource.doHttpGet(uri, this.monitorAddr, this.pageNo, this.pageSize, AlertNotifyTemplateData.class);
            if (alertNotifyTemplateData != null) {
                if (!alertNotifyTemplateData.isSuccess()) {
                    log.error("get expression is failed err is {}", alertNotifyTemplateData.getErr().toString());
                    this.pageNo++;
                    continue;
                }
                for (AlertNotifyTemplate alertNotifyTemplate : alertNotifyTemplateData.getData().getList()) {
                    checkNotNull(alertNotifyTemplate.getTitle(), "Title cannot be null");
                    checkNotNull(alertNotifyTemplate.getTemplate(), "Template cannot be null");
                    alertNotifyTemplate.setProcessingTime(System.currentTimeMillis());
                    if (alertNotifyTemplate.getAlertType().contains("customize")) {
                        alertNotifyTemplate.setVariable(false);
                    } else {
                        alertNotifyTemplate.setVariable(true);
                    }
                    String[] targetArr = alertNotifyTemplate.getTarget().split(",");
                    if (targetArr.length > 1) {
                        for (String target : targetArr) {
                            AlertNotifyTemplate template = alertNotifyTemplate.copy();
                            template.setTarget(target);
                            template.setId(template.getAlertIndex() + "_" + target);
//                            log.info("Read notify template {} data: {}", template.getAlertIndex(), JsonMapperUtils.toStrings(template));
                            notifyTemplateList.add(template);
                        }
                    } else {
                        alertNotifyTemplate.setId(alertNotifyTemplate.getAlertIndex() + "_" + alertNotifyTemplate.getTarget());
//                        log.info("Read notify template {} data: {}", alertNotifyTemplate.getAlertIndex(), JsonMapperUtils.toStrings(alertNotifyTemplate));
                        notifyTemplateList.add(alertNotifyTemplate);
                    }
                }

                if (this.pageNo * this.pageSize >= alertNotifyTemplateData.getData().getTotal()) {
                    this.pageNo = 1;
                    break;
                }
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
