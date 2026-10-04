#!/usr/bin/env bash
# Starts a single-node Kafka that is encrypted (TLS), authenticated (SASL/SCRAM)
# and authorised (ACLs), and sets up every service's principal with
# scripts/kafka-acls.sh. It is how to try the production security model on a
# laptop; it is NOT a production deployment (one node, a throwaway CA, the
# controller listener unencrypted on localhost).
#
#   KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh
#
# It needs a Kafka download (not Docker), Java, openssl and keytool. Everything
# it makes goes under $DIR (default .dev/kafka-secure), including the CA, the
# admin password and each service's password file. It prints the environment
# to give a service.
set -euo pipefail

KAFKA_HOME=${KAFKA_HOME:?set KAFKA_HOME to an unpacked Kafka 3.8+ distribution}
DIR=$(cd "$(dirname "${DIR:-.dev/kafka-secure}")" 2>/dev/null && pwd)/$(basename "${DIR:-.dev/kafka-secure}")
PORT=${PORT:-9094}
CONTROLLER_PORT=${CONTROLLER_PORT:-19094}
REPO=$(cd "$(dirname "$0")/../.." && pwd)

mkdir -p "$DIR/secrets"
cd "$DIR"

# The admin password is generated once and kept.
[ -s admin.password ] || (umask 077; openssl rand -base64 24 | tr -d '\n=+/' > admin.password)
ADMIN_PW=$(cat admin.password)
STORE_PW=$(cat admin.password)

# A certificate for the broker, signed by a throwaway CA. Clients trust the CA
# (KAFKA_CA_FILE=$DIR/ca.pem), and the name must match what they connect to.
if [ ! -s broker.p12 ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 30 -subj "/CN=grab-mini-local-ca" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -keyout broker.key -out broker.csr -subj "/CN=localhost" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.ext
  openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out broker.pem -days 30 -extfile san.ext 2>/dev/null
  openssl pkcs12 -export -in broker.pem -inkey broker.key -certfile ca.pem -name broker -out broker.p12 -passout "pass:$STORE_PW"
  keytool -importcert -alias ca -file ca.pem -keystore truststore.p12 -storetype PKCS12 -storepass "$STORE_PW" -noprompt >/dev/null
  chmod 600 ./*.key ./*.p12
fi

cat > jaas.conf <<JAAS
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin" password="$ADMIN_PW" user_admin="$ADMIN_PW";
};
JAAS

cat > broker.properties <<PROPS
process.roles=broker,controller
node.id=2
controller.quorum.voters=2@localhost:$CONTROLLER_PORT
listeners=EXTERNAL://localhost:$PORT,CONTROLLER://localhost:$CONTROLLER_PORT
advertised.listeners=EXTERNAL://localhost:$PORT
controller.listener.names=CONTROLLER
inter.broker.listener.name=EXTERNAL
listener.security.protocol.map=EXTERNAL:SASL_SSL,CONTROLLER:SASL_PLAINTEXT
sasl.enabled.mechanisms=SCRAM-SHA-512,PLAIN
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.mechanism.controller.protocol=PLAIN
ssl.keystore.type=PKCS12
ssl.keystore.location=$DIR/broker.p12
ssl.keystore.password=$STORE_PW
ssl.key.password=$STORE_PW
ssl.truststore.type=PKCS12
ssl.truststore.location=$DIR/truststore.p12
ssl.truststore.password=$STORE_PW
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="$ADMIN_PW";
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin
allow.everyone.if.no.acl.found=false
log.dirs=$DIR/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
num.partitions=3
PROPS

cat > admin.properties <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="$ADMIN_PW";
ssl.truststore.type=PKCS12
ssl.truststore.location=$DIR/truststore.p12
ssl.truststore.password=$STORE_PW
PROPS
chmod 600 admin.properties jaas.conf broker.properties

if [ ! -d data ]; then
  ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid | tail -1)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$ID" -c broker.properties \
    --add-scram "SCRAM-SHA-512=[name=admin,password=$ADMIN_PW]" >/dev/null
fi

KAFKA_OPTS="-Djava.security.auth.login.config=$DIR/jaas.conf" KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx512m}" \
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" broker.properties > broker.log 2>&1 &
echo $! > broker.pid

echo -n "waiting for the broker"
for _ in $(seq 1 60); do
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:$PORT" --command-config admin.properties >/dev/null 2>&1; then
    echo " ... up (pid $(cat broker.pid))"; break
  fi
  echo -n "."; sleep 1
done

for topic in order.created order.matched order.no_driver_found trip.status.changed trip.rated driver.location.updated; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "localhost:$PORT" --command-config admin.properties \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null 2>&1
done

BOOTSTRAP="localhost:$PORT" KAFKA_BIN="$KAFKA_HOME/bin" ADMIN_CONFIG="$DIR/admin.properties" SECRETS_DIR="$DIR/secrets" \
  "$REPO/scripts/kafka-acls.sh"

cat <<MSG

Kafka is up with TLS, SASL/SCRAM and ACLs on localhost:$PORT.
Stop it with:  kill \$(cat $DIR/broker.pid)

Give each service (here, trip-service) its own principal:

  KAFKA_BROKERS=localhost:$PORT
  KAFKA_SECURITY_PROTOCOL=SASL_SSL
  KAFKA_USERNAME=trip-service
  KAFKA_PASSWORD_FILE=$DIR/secrets/trip-service.password
  KAFKA_CA_FILE=$DIR/ca.pem

The admin (super user) credentials are in $DIR/admin.password and admin.properties.
MSG
