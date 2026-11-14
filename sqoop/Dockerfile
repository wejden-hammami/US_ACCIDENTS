FROM dvoros/sqoop:hive2-sqoop1.4.7

USER root

RUN curl -L https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
    -o /usr/local/sqoop/lib/mysql-connector-java.jar

RUN find /usr/local/apache-hive-2.3.3-bin/lib/ -name "derby*.jar" -exec mv {} {}.bak \; && \
    find /usr/local/db-derby-10.14.2.0-bin/lib/ -name "derby*.jar" -exec mv {} {}.bak \; && \
    find /usr/local/sqoop/lib/ -name "derby*.jar" -exec mv {} {}.bak \;

RUN mkdir -p /usr/local/hadoop-2.9.0/etc/hadoop

COPY core-site.xml /usr/local/hadoop-2.9.0/etc/hadoop/core-site.xml
COPY mapred-site.xml /usr/local/hadoop-2.9.0/etc/hadoop/mapred-site.xml
COPY yarn-site.xml /usr/local/hadoop-2.9.0/etc/hadoop/yarn-site.xml