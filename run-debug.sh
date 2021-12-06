#!/bin/bash
#export DDT_URL=http://internmatch.genny.life:8280
export GENNY_SHOW_VALUES=TRUE
# export GENNY_KEYCLOAK_URL=https://keycloak-office.gada.io
export GENNY_KEYCLOAK_URL=https://keycloak.gada.io
export GENNY_API_URL=http://internmatch.genny.life:8280
export GENNY_REALM=internmatch

# echo $GENNY_CLIENT_ID
# echo $GENNY_CLIENT_SECRET
# echo $GENNY_SERVICE_USERNAME
echo $GENNY_SERVICE_PASSWORD

# echo $GENNY_MYSQL_USERNAME
# echo $GENNY_MYSQL_PASSWORD
# echo $GENNY_MYSQL_URL
# echo $GENNY_MYSQL_PORT
# echo $GENNY_MYSQL_DB

./mvnw  quarkus:dev -Ddebug=5558

