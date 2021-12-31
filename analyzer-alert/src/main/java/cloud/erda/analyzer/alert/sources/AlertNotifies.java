package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.alert.models.AlertNotify;
import cloud.erda.analyzer.alert.models.AlertNotifyData;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.runtime.sources.HttpSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AlertNotifies implements SourceFunction<AlertNotify> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;
    Map<String, String> params = new HashMap<>();

    public AlertNotifies(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<AlertNotify> GetAllNotifies() throws Exception {
        String uri = "/api/alert/notifies?pageNo=%d&pageSize=%d";
        String alertNotifyUrl = "http://" + monitorAddr + uri;
        ArrayList<AlertNotify> notifies = new ArrayList<>();
        while (true) {
            String url = String.format(alertNotifyUrl, this.pageNo, this.pageSize);
            AlertNotifyData alertNotifyData = HttpSource.doHttpGet(url, AlertNotifyData.class);
            for (AlertNotify alertNotify : alertNotifyData.getList()) {
                if (AlertConstants.ALERT_NOTIFY_TYPE_NOTIFY_GROUP.equals(alertNotify.getNotifyTarget().getType())) {
                    alertNotify.getNotifyTarget().setGroupTypes((alertNotify.getNotifyTarget().getGroupType().split(",")));
                }
                if (StringUtil.isNotEmpty(alertNotify.getNotifyTarget().getLevel())) {
                    String[] levelStr = alertNotify.getNotifyTarget().getLevel().split(",");
                    AlertLevel[] levels = new AlertLevel[levelStr.length];
                    for (int i = 0; i < levelStr.length; i++) {
                        levels[i] = AlertLevel.of(levelStr[i]);
                    }
                    alertNotify.getNotifyTarget().setLevels(levels);
                } else {
                    alertNotify.getNotifyTarget().setLevels(new AlertLevel[0]);
                }
                alertNotify.setProcessingTime(System.currentTimeMillis());
                log.info("Read alert notify {} data: {}", alertNotify.getId(), alertNotify);
                notifies.add(alertNotify);
            }
            if (this.pageNo * this.pageSize >= alertNotifyData.getTotal()) {
                break;
            }
            this.pageNo++;
        }
        return notifies;
    }

    @Override
    public void run(SourceContext<AlertNotify> sourceContext) throws Exception {
        while (true) {
            ArrayList<AlertNotify> notifies = GetAllNotifies();
            for (AlertNotify notify : notifies) {
                sourceContext.collect(notify);
            }
            Thread.sleep(this.httpInterval);
        }
    }

    @Override
    public void cancel() {

    }
}
