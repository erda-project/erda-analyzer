package cloud.erda.analyzer.runtime.models;

import lombok.Data;

import java.util.ArrayList;

@Data
public class AlertExpression {
    private int total;
    private ArrayList<ExpressionMetadata> list;
}
