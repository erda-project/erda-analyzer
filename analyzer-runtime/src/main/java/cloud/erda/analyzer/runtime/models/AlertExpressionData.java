package cloud.erda.analyzer.runtime.models;

import lombok.Data;

@Data
public class AlertExpressionData {
    private boolean success;
    private ErrorMessage err;
    private AlertExpression data;
}
