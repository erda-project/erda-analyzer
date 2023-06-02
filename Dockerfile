FROM registry.erda.cloud/erda-addons/flink:1.12.7-erda

RUN sed -i 's/X9\.62 \w\+, //g' /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/java.security

WORKDIR $FLINK_HOME

ARG APP
COPY dist/${APP}.jar $FLINK_JOB_ARTIFACTS_DIR/${APP}.jar
