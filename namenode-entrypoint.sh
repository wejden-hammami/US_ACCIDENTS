#!/bin/bash
set -e

mkdir -p /opt/hadoop/etc/hadoop
mkdir -p /tmp/hadoop-hadoop/dfs/name

# Write core-site.xml directly
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:8020</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hadoop.groups</name>
    <value>*</value>
  </property>
</configuration>
EOF

# Write hdfs-site.xml directly
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hadoop-hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hadoop-hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>namenode:8020</value>
  </property>
</configuration>
EOF

chown -R hadoop:hadoop /tmp/hadoop-hadoop
chown -R hadoop:hadoop /opt/hadoop/etc/hadoop

if [ ! -f /tmp/hadoop-hadoop/dfs/name/current/VERSION ]; then
  su -s /bin/bash hadoop -c "hdfs namenode -format -force -nonInteractive"
fi
exec su -s /bin/bash hadoop -c "hdfs namenode"