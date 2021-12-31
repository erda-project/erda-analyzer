package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.runtime.models.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

@Slf4j
public class MetricExpressions implements SourceFunction<ExpressionMetadata> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public MetricExpressions(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<ExpressionMetadata> GetAllMetricEnabledExpressions() throws Exception {
        String uri = "/api/metric/expressions?pageNo=%d&pageSize=%d";
        String expressionUrl = "http://" + monitorAddr + uri;
        ArrayList<ExpressionMetadata> expressionMetadataList = new ArrayList<>();
        while (true) {
            String url = String.format(expressionUrl, this.pageNo, this.pageSize);
            AlertExpression metricExpression = HttpSource.doHttpGet(url, AlertExpression.class);
            for (ExpressionMetadata expressionMetadata : metricExpression.getList()) {
                expressionMetadata.getAttributes().put("window", expressionMetadata.getExpression().getWindow().toString());
                expressionMetadata.setProcessingTime(System.currentTimeMillis());
                expressionMetadata.setId(String.format("metric_%s", expressionMetadata.getId()));
                expressionMetadata.checkExpression(expressionMetadata);
                log.info("Read metric metadata {}  expression: {}  attributes: {}", expressionMetadata.getExpression().getMetric(), expressionMetadata.getExpression(), expressionMetadata.getAttributes());
                expressionMetadataList.add(expressionMetadata);
            }
            if (this.pageNo * this.pageSize >= metricExpression.getTotal()) {
                break;
            }
            this.pageNo++;
        }
        return expressionMetadataList;
    }

    @Override
    public void run(SourceContext<ExpressionMetadata> sourceContext) throws Exception {
        while (true) {
            ArrayList<ExpressionMetadata> expressionMetadatas = GetAllMetricEnabledExpressions();
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
