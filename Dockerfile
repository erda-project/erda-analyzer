FROM registry.erda.cloud/erda-x/openjdk-maven:8_11-3.9 AS builder

WORKDIR /app

COPY <<EOF /root/.m2/settings.xml
<?xml version=\"1.0\"?>
<settings>
    <mirrors>
        <mirror>
            <id>alimaven</id>
            <name>aliyun maven</name>
            <url>https://maven.aliyun.com/repository/central</url>
            <mirrorOf>central</mirrorOf>
        </mirror>
    </mirrors>
</settings>
EOF
COPY . .

ARG APP
RUN mvn clean package -pl ${APP} -am -B -DskipTests

FROM registry.erda.cloud/erda-addons/flink:1.12.7-erda

RUN sed -i 's/X9\.62 \w\+, //g' /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/java.security

WORKDIR $FLINK_HOME

ARG APP
COPY --from=builder /app/dist/${APP}.jar $FLINK_JOB_ARTIFACTS_DIR/${APP}.jar
