#!/bin/bash

KAFKA_BROKER_KEYSTORE_PASSWORD_FILE="/dev/shm/kafka_broker_keystore_pass"
KAFKA_BROKER_KEYSTORE_PASSWORD=""
KAFKA_BROKER_TRUSTSTORE_PASSWORD_FILE="/dev/shm/kafka_broker_truststore_pass"
KAFKA_BROKER_TRUSTSTORE_PASSWORD=""
KAFKA_CLIENT_KEYSTORE_PASSWORD_FILE="/dev/shm/kafka_client_pass"
KAFKA_CLIENT_KEYSTORE_PASSWORD=""
KAFKA_CLIENT_TRUSTSTORE_PASSWORD_FILE="/dev/shm/kafka_client_truststore_pass"
KAFKA_CLIENT_TRUSTSTORE_PASSWORD=""
# KAFKA_KEY_PASSWORD_FILE="/dev/shm/kafka_key_pass"
KAFKA_KEY_PASSWORD=""
ANSWERS_CA=""
ANSWERS_BROKER=""
ANSWERS_CLIENT=""

if [ -f "$KAFKA_BROKER_KEYSTORE_PASSWORD_FILE" ]; then
    KAFKA_BROKER_KEYSTORE_PASSWORD=$(cat "$KAFKA_BROKER_KEYSTORE_PASSWORD_FILE")
else
    KAFKA_BROKER_KEYSTORE_PASSWORD=$(openssl rand -base64 32)
fi

if [ -f "$KAFKA_CLIENT_KEYSTORE_PASSWORD_FILE" ]; then
    KAFKA_CLIENT_KEYSTORE_PASSWORD=$(cat "$KAFKA_CLIENT_KEYSTORE_PASSWORD_FILE")
else
    KAFKA_CLIENT_KEYSTORE_PASSWORD=$(openssl rand -base64 32)
fi

if [ -f "$KAFKA_BROKER_TRUSTSTORE_PASSWORD_FILE" ]; then
    KAFKA_BROKER_TRUSTSTORE_PASSWORD=$(cat "$KAFKA_BROKER_TRUSTSTORE_PASSWORD_FILE")
else
    KAFKA_BROKER_TRUSTSTORE_PASSWORD=$(openssl rand -base64 32)
fi

if [ -f "$KAFKA_CLIENT_TRUSTSTORE_PASSWORD_FILE" ]; then
    KAFKA_CLIENT_TRUSTSTORE_PASSWORD=$(cat "$KAFKA_CLIENT_TRUSTSTORE_PASSWORD_FILE")
else
    KAFKA_CLIENT_TRUSTSTORE_PASSWORD=$(openssl rand -base64 32)
fi

# if [ -f "$KAFKA_KEY_PASSWORD_FILE" ]; then
#     KAFKA_KEY_PASSWORD=$(cat "$KAFKA_KEY_PASSWORD_FILE")
# else
#     KAFKA_KEY_PASSWORD=$(openssl rand -base64 32)
# fi

KAFKA_BROKER_KEYSTORE_PASSWORD="password"
KAFKA_BROKER_TRUSTSTORE_PASSWORD="password"

echo "$KAFKA_BROKER_KEYSTORE_PASSWORD"
echo "$KAFKA_CLIENT_KEYSTORE_PASSWORD"
echo "$KAFKA_BROKER_TRUSTSTORE_PASSWORD"
echo "$KAFKA_CLIENT_TRUSTSTORE_PASSWORD"
echo "$KAFKA_KEY_PASSWORD"

ANSWERS_CA=$(cat <<EOF
US
FL
Miami
FIU
CIARA
Kafka CA Testing
.
.
EOF
)

ANSWERS_BROKER=$(cat <<EOF
US
FL
Miami
FIU
CIARA
172.27.0.2
.
.
.
EOF
)

ANSWERS_CLIENT=$(cat <<EOF
US
FL
Miami
FIU
CIARA
172.27.0.1
.
.
.
EOF
)

cd ..

if [ -d "./secrets" ]; then
    rm -rf secrets/
fi

mkdir ./secrets
cd ./secrets

openssl genpkey -algorithm RSA -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 <<< "$ANSWERS_CA"

openssl genpkey -algorithm RSA -out broker.key
openssl req -new -key broker.key -out broker.csr <<< "$ANSWERS_BROKER"

openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out broker.crt -days 365
openssl pkcs12 -export -in broker.crt -inkey broker.key -out broker.p12 -name broker -passout pass:password
openssl pkcs12 -in broker.p12 -out broker-key.pem -nocerts -nodes -passin pass:password
openssl pkcs12 -in broker.p12 -out broker-cert.pem -clcerts -nokeys -passin pass:password

openssl genpkey -algorithm RSA -out client.key
openssl req -new -key client.key -out client.csr <<< "$ANSWERS_CLIENT"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
openssl pkcs12 -export -in client.crt -inkey client.key -out client.p12 -name client -passout pass:password
openssl pkcs12 -in client.p12 -out client-key.pem -nocerts -nodes -passin pass:password
openssl pkcs12 -in client.p12 -out client-cert.pem -clcerts -nokeys -passin pass:password

keytool -import -alias client -file client.crt -keystore broker.truststore.p12 -storetype pkcs12 -storepass password -noprompt
keytool -import -alias ca -file ca.crt -keystore broker.truststore.p12 -storetype pkcs12 -storepass password -noprompt

keytool -import -alias broker -file broker.crt -keystore client.truststore.p12 -storetype pkcs12 -storepass password -noprompt
keytool -import -alias ca -file ca.crt -keystore client.truststore.p12 -storetype pkcs12 -storepass password -noprompt

# printf "%s" "$KAFKA_BROKER_KEYSTORE_PASSWORD" > kafka_broker_keystore_creds
# printf "%s" "$KAFKA_BROKER_TRUSTSTORE_PASSWORD" > kafka_broker_truststore_creds
# printf "%s" "$KAFKA_KEY_PASSWORD" > kafka_broker_key_creds

# echo -n "$KAFKA_BROKER_KEYSTORE_PASSWORD" > kafka_broker_keystore_creds
# echo -n "$KAFKA_BROKER_TRUSTSTORE_PASSWORD" > kafka_broker_truststore_creds
# echo -n "$KAFKA_KEY_PASSWORD" > kafka_broker_key_creds

echo password > kafka_broker_keystore_creds
echo password > kafka_broker_truststore_creds
echo password > 

# cat << EOF > kafka_broker_keystore_creds
# $KAFKA_BROKER_KEYSTORE_PASSWORD
# EOF

# cat << EOF > kafka_broker_truststore_creds
# $KAFKA_BROKER_TRUSTSTORE_PASSWORD
# EOF

# cat << EOF > kafka_broker_key_creds
# $KAFKA_KEY_PASSWORD
# EOF

openssl x509 -in ca.crt -out ca.pem -outform PEM

cd ..

sudo chgrp 1000 secrets/ -R
chmod g=u secrets -R