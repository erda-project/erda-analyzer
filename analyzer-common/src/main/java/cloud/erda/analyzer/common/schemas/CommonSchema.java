package cloud.erda.analyzer.common.schemas;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

public class CommonSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {

    private final static Logger logger = LoggerFactory.getLogger(CommonSchema.class);
    private static final Gson gson = new Gson();

    private final TypeInformation<T> type;

    public CommonSchema(Class<T> type) {
        Preconditions.checkNotNull(type, "type");
        this.type = TypeInformation.of(type);
    }


    @Override
    public T deserialize(byte[] bytes) throws IOException {
        String input = new String(bytes);
        try {
            return gson.fromJson(input, this.type.getTypeClass());
        } catch (Throwable throwable) {
            logger.error("Deserialize record fail. \nSource : {} \n", input, throwable);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T record) {
        return false;
    }

    @Override
    public byte[] serialize(T record) {
        return gson.toJson(record).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(this.type.getTypeClass());
    }
}
