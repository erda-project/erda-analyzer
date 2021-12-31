package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.runtime.models.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class AlertExpressions implements SourceFunction<ExpressionMetadata> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public AlertExpressions(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<ExpressionMetadata> GetAlertEnabledExpressions() throws Exception {
        String uri = "/api/alert/expressions?pageNo=%d&pageSize=%d";
        String expressionUrl = "http://" + monitorAddr + uri;
        ArrayList<ExpressionMetadata> expressionMetadataList = new ArrayList<>();
        while (true) {
            String url = String.format(expressionUrl, this.pageNo, this.pageSize);
            AlertExpression alertExpression = HttpSource.doHttpGet(url, AlertExpression.class);
            for (ExpressionMetadata expressionMetadata : alertExpression.getList()) {
                expressionMetadata.getAttributes().put("window", expressionMetadata.getExpression().getWindow().toString());
                expressionMetadata.setProcessingTime(System.currentTimeMillis());
                expressionMetadata.setId(String.format("alert_%s", expressionMetadata.getId()));
                checkNotNull(expressionMetadata.getAttributes().get(AlertConstants.ALERT_INDEX), "Attribute alert_index cannot be null");
                checkNotNull(expressionMetadata.getAttributes().get(AlertConstants.ALERT_TYPE), "Attribute alert_type cannot be null");
                expressionMetadata.checkExpression(expressionMetadata);
                log.info("Read alert metadata {}  expression: {}  attributes: {}", expressionMetadata.getId(), expressionMetadata.getExpression(), expressionMetadata.getAttributes());
                expressionMetadataList.add(expressionMetadata);
            }
            if (this.pageNo * this.pageSize >= alertExpression.getTotal()) {
                break;
            }
            this.pageNo++;
        }
        return expressionMetadataList;
    }

    @Override
    public void run(SourceContext<ExpressionMetadata> sourceContext) throws Exception {
        while (true) {
            ArrayList<ExpressionMetadata> expressionMetadatas = GetAlertEnabledExpressions();
            for (ExpressionMetadata expressionMetadata : expressionMetadatas) {
                sourceContext.collect(expressionMetadata);
            }
            Thread.sleep(this.httpInterval);
        }
    }

    @Override
    public void cancel() {

    }
}
