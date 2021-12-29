package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.ExpressionConstants;
import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.common.utils.HttpUtils;
import cloud.erda.analyzer.runtime.expression.filters.FilterOperatorDefine;
import cloud.erda.analyzer.runtime.models.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class AllAlertExpressions implements SourceFunction<ExpressionMetadata> {
    private String monitorAddr;
    private long httpInterval = 60000;
    private int pageSize = 100;
    private int pageNo = 1;

    public AllAlertExpressions(String monitorAddr) {
        this.monitorAddr = monitorAddr;
    }

    public ArrayList<ExpressionMetadata> GetAlertEnabledExpressions() throws Exception {
        String uri = "/api/alert/expressions?pageNo=%d&pageSize=%d";
        String expressionUrl = "http://" + monitorAddr + uri;
        ArrayList<ExpressionMetadata> expressionMetadataList = new ArrayList<>();
        while (true) {
            String url = String.format(expressionUrl,this.pageNo,this.pageSize);
            String dataStr = HttpUtils.doGet(url);
            Map<String,Object> dataMap = GsonUtil.toMap(dataStr,String.class,Object.class);
            String data = JSON.toJSONString(dataMap.get("data"));
            AlertExpression alertExpression = JSONObject.parseObject(data,AlertExpression.class);
            for (ExpressionMetadata expressionMetadata : alertExpression.getList()) {
                expressionMetadata.getAttributes().put("window", expressionMetadata.getExpression().getWindow().toString());
                expressionMetadata.setProcessingTime(System.currentTimeMillis());
                expressionMetadata.setId(String.format("alert_%s", expressionMetadata.getId()));
                checkNotNull(expressionMetadata.getAttributes().get(AlertConstants.ALERT_INDEX), "Attribute alert_index cannot be null");
                checkNotNull(expressionMetadata.getAttributes().get(AlertConstants.ALERT_TYPE), "Attribute alert_type cannot be null");
                checkExpression(expressionMetadata);
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

    private void checkExpression(ExpressionMetadata metadata) {

        Expression expression = metadata.getExpression();

        if (expression.getMetrics() == null) {
            expression.setMetrics(new ArrayList<>());
        }

        if (expression.getFilters() == null) {
            expression.setFilters(new ArrayList<>());
        }

        if (expression.getGroup() == null) {
            expression.setGroup(new ArrayList<>());
        }

        if (expression.getFunctions() == null) {
            expression.setFunctions(new ArrayList<>());
        }

        if (expression.getSelect() == null) {
            expression.setSelect(new HashMap<>());
        }

        if (expression.getCondition() == null) {
            expression.setCondition(FunctionCondition.and);
        }

        if (expression.getWindowBehavior() == null) {
            expression.setWindowBehavior(WindowBehavior.none);
        }

        for (ExpressionFunction function : expression.getFunctions()) {
            function.setCondition(expression.getCondition());
            if (function.getTrigger() == null) {
                function.setTrigger(ExpressionFunctionTrigger.applied);
            }
        }

        if (ExpressionConstants.EXPRESSION_VERSION_1_0.equals(metadata.getVersion())) {
            List<String> outputs = expression.getOutputs();
            if (outputs == null) {
                outputs = new ArrayList<>();
            }
            if (!outputs.contains(ExpressionConstants.OUTPUT_ALERT)) {
                outputs.add(ExpressionConstants.OUTPUT_ALERT);
            }
            expression.setOutputs(outputs);
        }

        if (ExpressionConstants.EXPRESSION_VERSION_1_0.equals(metadata.getVersion()) || ExpressionConstants.EXPRESSION_VERSION_2_0.equals(metadata.getVersion())) {
            Map<String, Object> filter = expression.getFilter();
            if (filter != null) {
                for (Map.Entry<String, Object> entry : filter.entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof String) {
                        ExpressionFilter expressionFilter = new ExpressionFilter();
                        expressionFilter.setTag(entry.getKey());
                        expressionFilter.setValue(value);
                        expressionFilter.setOperator(FilterOperatorDefine.Equal);
                        expression.getFilters().add(expressionFilter);
                    }
                }
            }
        }

        if (expression.getMetric() != null) {
            if (!expression.getMetrics().contains(expression.getMetric())) {
                expression.getMetrics().add(expression.getMetric());
            }
        }

        if (expression.getAlias() == null) {
            if (expression.getMetrics().size() == 1) {
                expression.setAlias(expression.getMetrics().get(0));
            } else {
                expression.setAlias(String.join("_", expression.getMetrics()));
            }
        }
        checkNotNull(expression.getOutputs(), "Expression outputs cannot be null");
        checkNotNull(expression.getAlias(), "Expression alias cannot be null");
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
