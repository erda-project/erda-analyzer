package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.common.utils.HttpUtils;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;

import java.io.IOException;

public class HttpSource {
    public static <T> T doHttpGet(String uri, String monitorAddr, int pageNo, int pageSize, Class<T> clazz) throws IOException {
        String expressionUrl = "http://" + monitorAddr + uri;
        String url = String.format(expressionUrl, pageNo, pageSize);
        String dataStr = HttpUtils.doGet(url);
        if (dataStr == null) {
            return null;
        }
        T result = JsonMapperUtils.toObject(dataStr,clazz);
        return result;
    }
}
