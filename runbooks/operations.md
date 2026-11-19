# Operations

All commands run on a Kafka node as root. `K=/APP/kafka/bin`, bootstrap `192.168.64.11:9092`.

```bash
K=/APP/kafka/bin
BS=192.168.64.11:9092,192.168.64.12:9092,192.168.64.13:9092
CTRL=192.168.64.11:9093
```

## Health

```bash
$K/kafka-metadata-quorum.sh --bootstrap-controller $CTRL describe --status
$K/kafka-metadata-quorum.sh --bootstrap-controller $CTRL describe --replication --human-readable
$K/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions   # must be empty
$K/kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions        # must be empty
systemctl status kafka
tail -f /APP/kafka/logs/server.log
```

`describe --status`: one `LeaderId`, `CurrentVoters` = 1, 2, 3, `HighWatermark` moving.
`--replication`: every voter `Lag 0`, `LastCaughtUpTimestamp` recent.

## Produce / consume test

```bash
$K/kafka-topics.sh --bootstrap-server $BS --create --topic smoke --partitions 3 --replication-factor 3
seq 1 1000 | $K/kafka-console-producer.sh --bootstrap-server $BS --topic smoke --producer-property acks=all
$K/kafka-console-consumer.sh --bootstrap-server $BS --topic smoke --from-beginning --max-messages 1000 | wc -l   # 1000
$K/kafka-topics.sh --bootstrap-server $BS --delete --topic smoke
```

## Failover test

```bash
# on node 2
systemctl stop kafka
# elsewhere: quorum still has a leader, producers with acks=all still succeed (2 of 3 in sync)
$K/kafka-metadata-quorum.sh --bootstrap-controller $CTRL describe --status
$K/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions   # node 2's replicas
# on node 2
systemctl start kafka
# wait until --under-replicated-partitions is empty again
```

Stopping **two** nodes stops the quorum (1 of 3 voters) and every partition with `min.insync.replicas=2`.

## Rolling restart (config change, upgrade)

One node at a time, and only continue when the cluster is fully in sync again:

```bash
for ip in 192.168.64.11 192.168.64.12 192.168.64.13; do
  ssh root@$ip 'systemctl restart kafka'
  until [ -z "$($K/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions)" ]; do sleep 10; done
  echo "$ip back in sync"
done
```

Restart the **active controller** (the `LeaderId` from `describe --status`) last: one leader
election instead of several.

## Rebuild one node (disk lost, data corrupt)

The other two keep the quorum and every partition. The rebuilt node re-replicates from them.

Do it by hand. Re-running `install_kraft.sh` would also restart the two healthy nodes one
after another without waiting for them to catch up.

```bash
# on a healthy node: the cluster ID every node must share
CID=$(sed -n 's/^cluster.id=//p' /APP/kafka/kafka-logs/meta.properties); echo "$CID"

# on the broken node (config, JDK and systemd unit are still in place)
systemctl stop kafka
rm -rf /APP/kafka/kafka-logs/*
sudo -u kafka /APP/kafka/bin/kafka-storage.sh format -t "$CID" -c /APP/kafka/config/server.properties
systemctl start kafka

# watch it catch up
$K/kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
$K/kafka-metadata-quorum.sh --bootstrap-controller $CTRL describe --replication
```

A brand-new VM with the same IP: run the installer during a maintenance window. It reuses
node 1's `CLUSTER_ID` and formats only the empty node. **Except when the new VM is node 1**: it
has no `meta.properties`, so the installer would generate a new ID. Rebuild node 1 by hand as
above (install JDK/Kafka, copy `server.properties` and `kafka.service` from the old config, format
with the ID from node 2).

## Kernel tuning (all nodes)

```bash
cat > /etc/sysctl.d/99-kafka.conf <<'EOF'
# larger accept/SYN queues for many client connections
net.core.somaxconn = 8192
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.tcp_syncookies = 1
EOF
sysctl --system
```
