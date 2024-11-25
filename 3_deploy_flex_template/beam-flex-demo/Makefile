GCP_PROJECT=danh-nguyen-418615
GCP_REGION=us-west1
TEMPLATE_NAME=beam-flex-demo
TEMPLATE_TAG=0.5.0
PROJECT_NUMBER=$$(gcloud projects list --filter=${GCP_PROJECT} --format="value(PROJECT_NUMBER)")
GCS_PATH=gs://${GCP_PROJECT}-dataflow-${PROJECT_NUMBER}
TEMPLATE_PATH=${GCS_PATH}/templates/${TEMPLATE_NAME}
REPOSITORY_NAME=playground
TEMPLATE_IMAGE=${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/${REPOSITORY_NAME}/${TEMPLATE_NAME}:${TEMPLATE_TAG}
FLEX_TEMPLATE_BASE_IMAGE=gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

JOB_NAME=beam-flex-demo-$$(date +%Y%m%dt%H%M%S)
OUTPUT=${GCS_PATH}/out/${JOB_NAME}/output
INPUT_PATH=gs://${GCP_PROJECT}/events.json
OUTPUT_PATH=gs://${GCP_PROJECT}-coldline/template_output/
BQ_TABLE=${GCP_PROJECT}:logs.logs_filtered

WORKDIR=/opt/dataflow
FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/$$(echo ${TEMPLATE_NAME}|tr '-' '_')/main.py
FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@echo ${MAKEFILE_LIST}
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init: ## Build Bucket for Demo and the Artifact Registry -- Run this One time Only!
	@gcloud config set project ${GCP_PROJECT}
	@echo "Enabling Private Google Access in Region ${GCP_REGION} ......"
	@gcloud compute networks subnets update default \
	--region=${GCP_REGION} \
	--enable-private-ip-google-access
	@echo "Enabling Dataflow Service...." && gcloud services enable dataflow --project ${GCP_PROJECT}
	@echo "Enabling Artifact Registry..." && gcloud services enable artifactregistry.googleapis.com --project ${GCP_PROJECT}
	@echo "Building Bucket to Store template...." && gsutil mb -c standard -l ${GCP_REGION} -p ${GCP_PROJECT} ${GCS_PATH}
	@gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev

# @echo "Building Artifact Repo to Store Docker Image of Code...." && gcloud artifacts repositories create ${TEMPLATE_NAME} \
# --repository-format=docker \
# --location=${GCP_REGION} \
# --async

build: ## Build Flex Template Container and Upload Container to GCS Bucket
	gcloud config set project ${GCP_PROJECT}
	# Build DataFlow Container and Upload to Artifact Registry
	gcloud config set builds/use_kaniko True
	gcloud config set builds/kaniko_cache_ttl 480
	gcloud builds submit --tag ${TEMPLATE_IMAGE} .
	# Build Data Flex Template and Upload to GCS

template:
	gcloud dataflow flex-template build ${GCS_PATH}/templates/${TEMPLATE_NAME}/${TEMPLATE_TAG}.json \
    --image ${TEMPLATE_IMAGE} \
    --sdk-language PYTHON \
    --metadata-file "metadata.json" \

run: ## Run the Dataflow Container
	gcloud config set project ${GCP_PROJECT}
	gcloud dataflow flex-template run ${JOB_NAME} \
    --template-file-gcs-location ${GCS_PATH}/templates/${TEMPLATE_NAME}/${TEMPLATE_TAG}.json \
    --region ${GCP_REGION} \
    --staging-location ${GCS_PATH}/staging \
	--temp-location ${GCS_PATH}/temp \
	--network=default \
	--disable-public-ips \
	--additional-experiments=enable_prime \
    --parameters inputPath=${INPUT_PATH},outputPath=${OUTPUT_PATH},tableName=${BQ_TABLE}


test-template: ## Test the Integrity of the Flex Container
	@gcloud config set project ${GCP_PROJECT}
	@gcloud auth configure-docker  ${GCP_REGION}-docker.pkg.dev
	@docker pull ${TEMPLATE_IMAGE}
	@echo "Checking if ENV Var FLEX_TEMPLATE_PYTHON_PY_FILE is Available" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'env|grep -q "FLEX_TEMPLATE_PYTHON_PY_FILE" && echo ✓'
	@echo "Checking if ENV Var FLEX_TEMPLATE_PYTHON_SETUP_FILE is Available" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'env|grep -q "FLEX_TEMPLATE_PYTHON_PY_FILE" && echo ✓'
	@echo "Checking if Driver Python File (main.py) Found on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c "/usr/bin/test -f ${FLEX_TEMPLATE_PYTHON_PY_FILE} && echo ✓"
	@echo "Checking if setup.py File Found on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'test -f ${FLEX_TEMPLATE_PYTHON_SETUP_FILE} && echo ✓'
	@echo "Checking if Package Installed on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'python -c "import beam_flex_demo" && echo ✓'
	@echo "Checking if UDFs Installed on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'python -c "from beam_flex_demo.utils.common_functions import split_element" && echo ✓'
	@echo "Running Pipeline Locally..." && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c "python ${FLEX_TEMPLATE_PYTHON_PY_FILE} --runner DirectRunner --output output.txt && cat output.txt*"