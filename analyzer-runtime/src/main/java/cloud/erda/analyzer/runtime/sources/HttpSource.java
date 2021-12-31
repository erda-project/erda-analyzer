package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.common.utils.HttpUtils;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;

import java.io.IOException;
import java.util.Map;

public class HttpSource {
    public static <T> T doHttpGet(String url, Class<T> clazz) throws IOException {
        String dataStr = HttpUtils.doGet(url);
        Map<String,Object> dataMap = JsonMapperUtils.toHashMap(dataStr,String.class,Object.class);
        String data = JsonMapperUtils.toStrings(dataMap.get("data"));
        T result = JsonMapperUtils.toObject(data,clazz);
        return result;
    }
}
