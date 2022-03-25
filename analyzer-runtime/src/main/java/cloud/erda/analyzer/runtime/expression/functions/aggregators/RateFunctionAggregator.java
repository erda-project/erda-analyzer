package cloud.erda.analyzer.runtime.expression.functions.aggregators;

import cloud.erda.analyzer.common.utils.ConvertUtils;

import java.util.ArrayList;
import java.util.List;

// Same semantic as https://prometheus.io/docs/prometheus/latest/querying/functions/#rate
public class RateFunctionAggregator implements FunctionAggregator {

    private final long interval;
    private double resultValue;
    private double lastValue;

    public RateFunctionAggregator(long window) {
        lastValue = 0d;
        resultValue = 0d;
        interval = window * 60;
    }

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.RATE;
    }

    @Override
    public Object value() {
        return resultValue / interval;
    }

    @Override
    // TODO. This is assumed event come in order
    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d != null) {
            if (d < lastValue) {
                // in case counter reset
                resultValue += d;
            } else {
                resultValue += d - lastValue;
            }
            lastValue = d;
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof RateFunctionAggregator) {
            this.resultValue += ((RateFunctionAggregator) other).resultValue;
        }
    }
}
