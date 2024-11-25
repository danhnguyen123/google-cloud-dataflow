FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
RUN mkdir -p ${WORKDIR}/jars
WORKDIR ${WORKDIR}

ARG TEMPLATE_NAME=src
COPY . ${WORKDIR}/

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/${TEMPLATE_NAME}/main.py
# ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=${WORKDIR}/${TEMPLATE_NAME}/requirements.txt
# ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

ENV PYTHONPATH=${PYTHONPATH}:${WORKDIR}/${TEMPLATE_NAME}

RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    # Install dependencies from requirements file in the launch environment.
    # && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # When FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE  option is used,
    # then during Template launch Beam downloads dependencies
    # into a local requirements cache folder and stages the cache to workers.
    # To speed up Flex Template launch, pre-download the requirements cache
    # when creating the Template.
    # && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE
    && pip install -r ${WORKDIR}/${TEMPLATE_NAME}/requirements.txt

# RUN python setup.py install

ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]