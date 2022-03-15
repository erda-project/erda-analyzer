package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.runtime.models.*;
import cloud.erda.analyzer.runtime.utils.CheckUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

@Slf4j
public class MetricExpressionReader implements SourceFunction<ExpressionMetadata> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public MetricExpressionReader(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<ExpressionMetadata> GetAllMetricEnabledExpressions() throws Exception {
        String uri = "/api/metric/expressions?pageNo=%d&pageSize=%d";
        ArrayList<ExpressionMetadata> expressionMetadataList = new ArrayList<>();
        while (true) {
            AlertExpressionData metricExpressionData = HttpSource.doHttpGet(uri, this.monitorAddr, this.pageNo, this.pageSize, AlertExpressionData.class);
            if (metricExpressionData != null) {
                if (!metricExpressionData.isSuccess()) {
                    log.error("get expression is failed err is {}", metricExpressionData.getErr().toString());
                    this.pageNo++;
                    continue;
                }
                for (ExpressionMetadata expressionMetadata : metricExpressionData.getData().getList()) {
                    expressionMetadata.getAttributes().put("window", expressionMetadata.getExpression().getWindow().toString());
                    expressionMetadata.setProcessingTime(System.currentTimeMillis());
                    expressionMetadata.setId(String.format("metric_%s", expressionMetadata.getId()));
                    CheckUtils.checkExpression(expressionMetadata);
                    log.info("Read metric metadata {}  expression: {}  attributes: {}", expressionMetadata.getExpression().getMetric(), expressionMetadata.getExpression(), expressionMetadata.getAttributes());
                    expressionMetadataList.add(expressionMetadata);
                }
                if (this.pageNo * this.pageSize >= metricExpressionData.getData().getTotal()) {
                    this.pageNo = 1;
                    break;
                }
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
