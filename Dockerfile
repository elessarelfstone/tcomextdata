FROM python:alpine

ENV TEMP_DIR /data/temp
ENV DATA_DIR /data
ENV ETL_CONFIG_DIR /etl/config
ENV ETL_TASKS_DIR /etl/tasks
ENV PATH="/etl:${PATH}"

RUN mkdir /etl && mkdir ${DATA_DIR}

WORKDIR /etl

COPY requirements.txt /etl/requirements.txt

RUN apk add bash p7zip && pip install poetry && pip install -r requirements.txt \
        && mkdir -p config tasks

ADD ./config ${ETL_CONFIG_DIR}
ADD tcomextdata/tasks ${ETL_TASKS_DIR}
ADD ./task_run.sh task_run

VOLUME ["${DATA_DIR}", "${ETL_CONFIG_DIR}", "${ETL_TASKS_DIR}"]


#ENTRYPOINT ["task_run"]