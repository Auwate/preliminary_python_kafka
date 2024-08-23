# CIARA Kafka Testing w/ Python & `kafka-python-ng`

# SSL

The following subheaders are the steps you need to take to set up SSL encryption for data in-transit for the Kafka node. This is current **not optional**, as SSL is hardcoded into the `docker-compose.yml` file.

This assumes you are at the root, being `/preliminary_python_kafka`

## 1: Setting up `secrets/`

```
mkdir secrets/
cd secrets/
```

## 2: Creating input files

```
cat >answers-ca <<EOF
US
FL
Miami
FIU
CIARA
Kafka CA Testing
.
EOF
```
```
cat >answers-broker <<EOF
US
FL
Miami
FIU
CIARA
172.27.0.2
.


EOF
```
```
cat >answers-client <<EOF
US
FL
Miami
FIU
CIARA
172.27.0.1
.


EOF
```
## 3: Creating CA

```
openssl genpkey -algorithm RSA -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 < answers-ca
```

## 4: Creating the broker's key, CSR, signed certificate, and keystore

```
openssl genpkey -algorithm RSA -out broker.key
openssl req -new -key broker.key -out broker.csr < answers-broker
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out broker.crt -days 365
openssl pkcs12 -export -in broker.crt -inkey broker.key -out broker.p12 -name broker -passout pass:password
openssl pkcs12 -in broker.p12 -out broker-key.pem -nocerts -nodes -passin pass:password
openssl pkcs12 -in broker.p12 -out broker-cert.pem -clcerts -nokeys -passin pass:password
```

## 5: Creating the client's key, CSR, signed certificate, and keystore

```
openssl genpkey -algorithm RSA -out client.key
openssl req -new -key client.key -out client.csr < answers-client
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
openssl pkcs12 -export -in client.crt -inkey client.key -out client.p12 -name client -passout pass:password
openssl pkcs12 -in client.p12 -out client-key.pem -nocerts -nodes -passin pass:password
openssl pkcs12 -in client.p12 -out client-cert.pem -clcerts -nokeys -passin pass:password
```

## 6: Create broker truststore

```
keytool -import -alias client -file client.crt -keystore broker.truststore.p12 -storetype pkcs12 -storepass password -noprompt
keytool -import -alias ca -file ca.crt -keystore broker.truststore.p12 -storetype pkcs12 -storepass password -noprompt
```

## 7: Create client truststore

```
keytool -import -alias broker -file broker.crt -keystore client.truststore.p12 -storetype pkcs12 -storepass password -noprompt
keytool -import -alias ca -file ca.crt -keystore client.truststore.p12 -storetype pkcs12 -storepass password -noprompt
```

## 8: Create password files

```
echo password > kafka_broker_creds
echo password > kafka_broker_key_creds
```

## 9: Create PEM files (required for `kafka-python-ng`)

```
openssl x509 -in ca.crt -out ca.pem -outform PEM
```

## 10: Change permissions for `/secrets`

```
cd ..

sudo chgrp 1000 secrets/ -R
chmod g=u secrets -R
```

## 11: Run Kafka

```
docker-compose up -d
```
