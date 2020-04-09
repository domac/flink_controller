ip=127.0.0.1
kafka=${ip}:9092
zk=${ip}:2181

nohup /data/svr/flink/bin/flink run /data/svr/projects/flink/host-security-1.0-SNAPSHOT.jar --key "$1" --zk "${zk}" --kafka "${kafka}" > /dev/null 2>&1  &
