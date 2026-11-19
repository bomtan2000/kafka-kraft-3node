#!/bin/bash
set -euo pipefail

# ============================================
# Kafka KRaft 3-node installer (combined broker + controller on every node)
# Reads ip_list.txt (1 IP / line, in order: node 1, 2, 3)
#
#   node_id   listener INTERNAL   CONTROLLER   EXTERNAL
#   1         <ip>:9092           <ip>:9093    0.0.0.0:9094
#   2         <ip>:9092           <ip>:9093    0.0.0.0:9095
#   3         <ip>:9092           <ip>:9093    0.0.0.0:9096
#
# CLUSTER_ID: generated once on node 1, reused on every re-run (read back from
# node 1's meta.properties), so a re-run or a rebuilt node never gets a new ID.
# Run as root on an admin host with SSH key access to the 3 nodes, with the
# JDK and Kafka tarballs in the current directory.
# ============================================

if [ "$EUID" -ne 0 ]; then
   echo "Please run as root"
   exit 1
fi

IP_LIST_FILE="./ip_list.txt"
SSH_USER="root"
#SSH_KEY="/root/.ssh/id_rsa"
SSH_KEY="/root/.ssh/id_ed25519"
# Update these two when bumping JDK/Kafka versions; everything else derives from them.
JDK_TGZ="jdk-17.0.19_linux-x64_bin.tar.gz"
KAFKA_TGZ="kafka_2.13-4.3.1.tgz"

BASE_DIR="/APP"
KAFKA_HOME="${BASE_DIR}/kafka"
JAVA_HOME="/usr/local/${JDK_TGZ%_linux-x64_bin.tar.gz}"

# Indexed arrays (not declare -A): iterate in node order 1, 2, 3
NODE_IPS=()
EXT_PORTS=()

echo "[INFO] Loading ip_list.txt..."

NODE_ID=0
# "|| [[ -n ... ]]": also read a last line without a trailing newline (otherwise node 3 is
# silently dropped and the quorum is built with 2 voters)
while read -r NODE_IP || [[ -n "$NODE_IP" ]]; do
    NODE_IP="${NODE_IP//$'\r'/}"
    [[ -z "$NODE_IP" ]] && continue
    NODE_ID=$((NODE_ID+1))
    NODE_IPS[NODE_ID]=$NODE_IP
    EXT_PORTS[NODE_ID]=$((9093 + NODE_ID))
done < "$IP_LIST_FILE"

if [ "${#NODE_IPS[@]}" -lt 1 ]; then
    echo "[ERROR] No IPs found in $IP_LIST_FILE"
    exit 1
fi

# ============================================
# Build Raft voter list
# ============================================
generate_voters() {
    local s=""
    for id in "${!NODE_IPS[@]}"; do
        s+="${id}@${NODE_IPS[$id]}:9093,"
    done
    echo "${s%,}"
}

# ============================================
# Copy tgz files to remote node
# ============================================
copy_installer() {
    local ip=$1
    echo "[INFO] Copying installers to $ip..."
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
        "$JDK_TGZ" "$KAFKA_TGZ" \
        "$SSH_USER@$ip:/root/"
}
###
## Disable firewall 
disable_security() {
    cat <<'EOF'
#!/bin/bash
set -e

echo "[1] SELinux permissive now..."
if command -v setenforce >/dev/null 2>&1; then
    setenforce 0 || true
fi

echo "[2] SELinux disabled in /etc/selinux/config..."
if [ -f /etc/selinux/config ]; then
    sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
else
    echo "SELINUX=disabled" > /etc/selinux/config
fi

echo "[3] Stop firewalld..."
systemctl stop firewalld.service || true

echo "[4] Disable firewalld..."
systemctl disable firewalld.service || true

echo "[OK] SELinux and firewalld disabled."
EOF
}
###
# ============================================
# Check status on remote node
# ============================================
check_status() {
    local ip=$1
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$ip" "
        echo '--- Kafka status on $ip ---'
        systemctl status kafka | sed -n '1,12p' || echo 'kafka service not running'
        echo '--- Last 10 logs ---'
        tail -n 10 ${KAFKA_HOME}/logs/server.log 2>/dev/null || echo 'no server.log yet'
    "
}

# ============================================
# GENERATE CLUSTER_ID ON NODE 1
# ============================================

FIRST_NODE_ID=1
FIRST_NODE_IP=${NODE_IPS[$FIRST_NODE_ID]}

echo "[INFO] Creating CLUSTER_ID on $FIRST_NODE_IP"
copy_installer "$FIRST_NODE_IP"

# Re-run safe: if node 1 is already formatted, reuse its cluster.id. A fresh random-uuid on
# every run would format any empty/rebuilt node with a different ID -> it never joins.
CLUSTER_ID=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$FIRST_NODE_IP" "
    set -e
    if [ -f ${KAFKA_HOME}/kafka-logs/meta.properties ]; then
        sed -n 's/^cluster.id=//p' ${KAFKA_HOME}/kafka-logs/meta.properties
        exit 0
    fi
    mkdir -p /usr/local
    # Install Java 17 (for CLUSTER_ID generation)
    tar -zxf /root/${JDK_TGZ} -C /usr/local/
    export JAVA_HOME=${JAVA_HOME}
    export PATH=\$JAVA_HOME/bin:\$PATH

    mkdir -p ${KAFKA_HOME}
    tar -zxf /root/${KAFKA_TGZ} -C ${KAFKA_HOME} --strip 1

    ${KAFKA_HOME}/bin/kafka-storage.sh random-uuid
")

CLUSTER_ID=$(echo "$CLUSTER_ID" | tr -d '[:space:]')

if [ -z "$CLUSTER_ID" ]; then
    echo "[ERROR] Failed to generate CLUSTER_ID on $FIRST_NODE_IP (JDK/Kafka extraction likely failed there)"
    exit 1
fi

echo "[INFO] CLUSTER_ID = $CLUSTER_ID"

VOTERS=$(generate_voters)
echo "[INFO] VOTERS = $VOTERS"

# ============================================
# NODE CONFIG SCRIPT TEMPLATE
# ============================================

generate_node_script() {
    local NODE_ID=$1
    local NODE_IP=${NODE_IPS[$NODE_ID]}
    local EXT_PORT=${EXT_PORTS[$NODE_ID]}

cat <<EOF
#!/bin/bash
set -e

# Define directory variables
BASE_DIR="${BASE_DIR}"
KAFKA_HOME="${KAFKA_HOME}"
KAFKA_LOGS="\${KAFKA_HOME}/kafka-logs"
JAVA_HOME="${JAVA_HOME}"

echo "[INFO] Setting up Kafka node ${NODE_ID} (\${BASE_DIR}, \${KAFKA_HOME})"

# Service account: no password, no login shell, not in wheel (no sudo)
if ! id kafka &>/dev/null; then
  useradd -r -m -d /home/kafka -s /sbin/nologin kafka
fi

# Create necessary directories
mkdir -p "\${KAFKA_HOME}"
mkdir -p "\${KAFKA_LOGS}"
mkdir -p /usr/local

# Install Java 17
if [ ! -d "\${JAVA_HOME}" ]; then
  tar -zxf /root/${JDK_TGZ} -C /usr/local/
fi

# Export Java globally (for login shells)
if ! grep -q "export JAVA_HOME=${JAVA_HOME}" /etc/profile 2>/dev/null; then
  echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile
  echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile
fi

# Export Java for this script
export JAVA_HOME=${JAVA_HOME}
export PATH=\$JAVA_HOME/bin:\$PATH

java -version || { echo "[ERROR] Java 17 not working"; exit 1; }

# Install Kafka
if [ ! -f "\${KAFKA_HOME}/bin/kafka-server-start.sh" ]; then
  cd "\${KAFKA_HOME}"
  tar -xzf /root/${KAFKA_TGZ} --strip 1
fi

# Configure server.properties
cat > "\${KAFKA_HOME}/config/server.properties" <<EOC
process.roles=broker,controller
node.id=${NODE_ID}

listeners=INTERNAL://${NODE_IP}:9092,CONTROLLER://${NODE_IP}:9093,EXTERNAL://0.0.0.0:${EXT_PORT}
advertised.listeners=INTERNAL://${NODE_IP}:9092,EXTERNAL://${NODE_IP}:${EXT_PORT}

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
controller.quorum.voters=${VOTERS}

log.dirs=${KAFKA_HOME}/kafka-logs
default.replication.factor=3
min.insync.replicas=2
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.retention.hours=72
log.segment.bytes=1073741824
delete.topic.enable=true
num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOC

# Format storage (idempotent-ish)
if [ ! -d "\${KAFKA_LOGS}" ] || [ -z "\$(ls -A "\${KAFKA_LOGS}" 2>/dev/null)" ]; then
  echo "[INFO] Formatting storage for node ${NODE_ID}..."
  "\${KAFKA_HOME}/bin/kafka-storage.sh" format -t ${CLUSTER_ID} -c "\${KAFKA_HOME}/config/server.properties"
else
  echo "[INFO] Kafka log dir not empty, skip format"
fi

# Systemd service
cat > /etc/systemd/system/kafka.service <<EOC
[Unit]
Description=Kafka KRaft Node
After=network.target

[Service]
User=kafka
Group=kafka
WorkingDirectory=${KAFKA_HOME}
Environment="JAVA_HOME=${JAVA_HOME}"
Environment="PATH=${JAVA_HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="KAFKA_HEAP_OPTS=-Xms8G -Xmx8G"
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOC

# Set correct permissions
chown -R kafka:kafka "\${KAFKA_HOME}"
chmod -R 755 "\${KAFKA_HOME}/bin"
chmod -R 755 "\${KAFKA_HOME}/config"
mkdir -p "\${KAFKA_LOGS}"
chmod -R 775 "\${KAFKA_LOGS}"

systemctl daemon-reload
systemctl enable kafka
systemctl restart kafka

echo "[INFO] Node ${NODE_ID} setup done."
EOF
}

# ============================================
# INSTALL EACH NODE
# ============================================

for NODE_ID in "${!NODE_IPS[@]}"; do
    NODE_IP=${NODE_IPS[$NODE_ID]}
    echo "===== Installing Kafka KRaft on $NODE_IP (node_id=$NODE_ID) ====="
    # 1) Copy Java + Kafka
    copy_installer "$NODE_IP"
    # 2) Disable SELinux + firewalld before installing
    disable_security | ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$NODE_IP" 'bash -s' || {
        echo "[ERROR] disable_security failed on $NODE_IP"
        exit 1
    }
    # 3) Install Kafka
    generate_node_script "$NODE_ID" | \
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$NODE_IP" 'bash -s'
    # 4) Check the service
    check_status "$NODE_IP"
done

echo "=== Kafka KRaft cluster ready ==="
echo "Cluster ID: $CLUSTER_ID"