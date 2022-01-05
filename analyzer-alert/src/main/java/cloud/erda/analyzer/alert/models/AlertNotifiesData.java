package cloud.erda.analyzer.alert.models;

import cloud.erda.analyzer.runtime.models.ErrorMessage;
import lombok.Data;


@Data
public class AlertNotifiesData {
    private boolean success;
    private ErrorMessage err;
    private AlertNotifyData data;
}
