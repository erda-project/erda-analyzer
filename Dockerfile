FROM registry.erda.cloud/erda/erda-flink-1.12.4:20210707-090efe2

RUN echo "Asia/Shanghai" | tee /etc/timezone

WORKDIR $FLINK_HOME

ARG APP
COPY dist/${APP}.jar $FLINK_JOB_ARTIFACTS_DIR/${APP}.jar
