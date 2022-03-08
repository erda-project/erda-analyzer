package cloud.erda.analyzer.alert.models;

import cloud.erda.analyzer.runtime.models.ErrorMessage;
import lombok.Data;


@Data
public class AlertNotifyTemplateData {
    private boolean success;
    private ErrorMessage err;
    private AlertNotifyTemplates data;
}
