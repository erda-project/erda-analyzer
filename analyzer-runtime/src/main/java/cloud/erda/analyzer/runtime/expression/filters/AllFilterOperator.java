package cloud.erda.analyzer.runtime.expression.filters;

import cloud.erda.analyzer.runtime.models.ExpressionFilter;

public class AllFilterOperator implements FilterOperator{
    @Override
    public String operator() {
        return FilterOperatorDefine.All;
    }

    @Override
    public boolean invoke(ExpressionFilter filter, Object value) {
        if (filter.getValue() == null || value == null) {
            return false;
        }
        String allValue = filter.getValue().toString();
        String[] valueArr = allValue.split(",");
        if (valueArr == null || valueArr.length == 0) {
            return false;
        }
        for (String item : valueArr) {
            if (!value.equals(item)) {
                return false;
            }
        }
        return true;
    }

    public static final FilterOperator INSTANCE = new AllFilterOperator();
}
