# Creates pseudo distributed hadoop

FROM debian AS HADOOP
MAINTAINER Clinton Yeboah
USER root

RUN apt-get update \
 && apt-get install -y locales \
 && dpkg-reconfigure -f noninteractive locales \
 && locale-gen C.UTF-8 \
 && /usr/sbin/update-locale LANG=C.UTF-8 \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
 && locale-gen \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Users with other locales should set this in their derivative image
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get update \
 && apt-get install -y curl unzip tar sudo openssh-server openssh-client rsync apt-utils wget gnupg \
 software-properties-common build-essential python-dev python3 python3-setuptools python3-pip  \
 build-essential python3-dev python3-wheel python3-cffi libcairo2 libpango-1.0-0 \
 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info

# passwordless ssh
RUN yes 'y' | ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key \
&& yes 'y' | ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key \
&& yes 'y' | ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
&& cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# Python
RUN ln -sfn /usr/bin/python3 /usr/bin/python \
 && python -m pip install pygal cairosvg \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# JAVA
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add - && \
    add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt-get update -y \
 && apt-get install -y adoptopenjdk-8-hotspot \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/adoptopenjdk-8-hotspot-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# Hadoop
ENV HADOOP_VERSION 3.0.0
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_YARN_HOME $HADOOP_HOME
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
RUN curl -# -L --retry 3 "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" | tar -xz -C /usr/local/ && \
    cd /usr/local && ln -s ./hadoop-${HADOOP_VERSION} hadoop && \
    rm -rf $HADOOP_HOME/share/doc && chown -R root:root $HADOOP_HOME

RUN sed -i '/.*export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    sed -i '/.*export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# RUN cat $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir $HADOOP_HOME/input \
&& cp $HADOOP_HOME/etc/hadoop/*.xml $HADOOP_HOME/input

# pseudo distributed
ENV HDFS_NAMENODE_USER root
ENV HDFS_DATANODE_USER root
ENV HDFS_SECONDARYNAMENODE_USER root
ENV YARN_RESOURCEMANAGER_USER root
ENV YARN_NODEMANAGER_USER root
ENV LD_LIBRARY_PATH $HADOOP_HOME/lib/native

# SPARK
ENV SPARK_VERSION 2.4.5
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-without-hadoop
ENV SPARK_HOME /usr/local/spark
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ \
 && mv /usr/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME

# Entry point
ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh \
&& chmod 700 /etc/bootstrap.sh
ENV BOOTSTRAP /etc/bootstrap.sh

# Copy artefacts
ADD target/scala-2.11/final-project.jar /app/target/final-project.jar
COPY graph-visual.py /app/graph-visual.py

CMD ["/etc/bootstrap.sh", "-d"]
