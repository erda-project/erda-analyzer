FROM registry.erda.cloud/erda-addons/flink:1.12.7-erda

WORKDIR $FLINK_HOME

ARG APP
COPY dist/${APP}.jar $FLINK_JOB_ARTIFACTS_DIR/${APP}.jar
