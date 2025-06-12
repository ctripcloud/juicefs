#!/bin/bash
app_id="${app_id:-100061316}"
HERALD_TOKEN=$(cat /opt/settings/"${app_id}"/herald/HERALD_TOKEN)
request_body=$(
    cat <<EOF
{
    "appid": "${app_id}",
    "token": "${KMS_TOKEN}",
    "type": "JSON"
}
EOF
)

# if $PASS_ENV is fat
if [ "$PAAS_ENV" == "UAT" ]; then
    echo "deploying juicefs gateway in uat"
    request_uri="https://kms-server.infosec.uat.tripqate.com/query-pwd"
    response=$(curl -X POST -H "herald-token: ${HERALD_TOKEN}" -H "Content-Type: application/json" -k -d "${request_body}" ${request_uri})
    echo "response: ${response}"
    pwdValue=$(echo "${response}" | jq -r '.result.pwdValue')
    pwdAccount=$(echo "${response}" | jq -r '.result.pwdAccount')
    # set env
    export MINIO_ROOT_USER=${pwdAccount}
    export MINIO_ROOT_PASSWORD=${pwdValue}
elif [ "$PAAS_ENV" == "PROD" ]; then
    echo "deploying juicefs gateway in prod"
    request_uri="https://kms-server.infosec.ctripcorp.com/query-pwd"
    response=$(curl -X POST -H "herald-token: ${HERALD_TOKEN}" -H "Content-Type: application/json" -k -d "${request_body}" ${request_uri})
    echo "response: ${response}"
    pwdValue=$(echo "${response}" | jq -r '.result.pwdValue')
    pwdAccount=$(echo "${response}" | jq -r '.result.pwdAccount')
    # set env
    export MINIO_ROOT_USER=${pwdAccount}
    export MINIO_ROOT_PASSWORD=${pwdValue}
else
    echo "PASS_ENV is not set"
    request_uri="https://kms-server.fat70.tripqate.com/query-pwd"
    response=$(curl -X POST -H "Content-Type: application/json" -k -d "${request_body}" ${request_uri})
    echo "response: ${response}"
    pwdValue=$(echo "${response}" | jq -r '.result.pwdValue')
    pwdAccount=$(echo "${response}" | jq -r '.result.pwdAccount')
    # set env
    export MINIO_ROOT_USER=${pwdAccount}
    export MINIO_ROOT_PASSWORD=${pwdValue}
fi

juicefs gateway --cache-size=0 --buffer-size=2048M --max-readahead=512M --log=/var/log/juicefs.log --access-log=/var/log/juicefs-access.log "${META_URL}" 0.0.0.0:9000
