.PHONY: \
	build \
	clean \
	hadoop_base \
	hadoop_namenode \
	hadoop_datanode \
	hadoop_yarn_resourcemanager \
	hadoop_yarn_nodemanager \
	spark \
	zookeeper \
	hbase_base \
	hbase_master \
	hbase_regionserver \
	postgres \
	livy \
	bda_devel

build: \
	hadoop_base \
	hadoop_namenode \
	hadoop_datanode \
	hadoop_yarn_resourcemanager \
	hadoop_yarn_nodemanager \
	spark \
	zookeeper \
	hbase_base \
	hbase_master \
	hbase_regionserver \
	postgres \
	livy \
	bda_devel

include .env
export

hadoop_base:
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag selis/hadoop-base-$(HADOOP_VERSION) \
		./hadoop/base

hadoop_namenode: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag selis/hadoop-namenode-$(HADOOP_VERSION) \
		./hadoop/namenode

hadoop_datanode: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag selis/hadoop-datanode-$(HADOOP_VERSION) \
		./hadoop/datanode

hadoop_yarn_resourcemanager: hadoop_base 
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag selis/hadoop-yarn-resourcemanager-$(HADOOP_VERSION) \
		./hadoop/resourcemanager

hadoop_yarn_nodemanager: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--tag selis/hadoop-yarn-nodemanager-$(HADOOP_VERSION) \
		./hadoop/nodemanager

spark: hadoop_yarn_nodemanager
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--tag selis/spark-$(SPARK_VERSION) \
		./spark

zookeeper: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
		--tag selis/zookeeper-$(ZOOKEEPER_VERSION) \
		./zookeeper

hbase_base: hadoop_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag selis/hbase-base-$(HBASE_VERSION) \
		./hbase/base

hbase_master: hbase_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag selis/hbase-master-$(HBASE_VERSION) \
		./hbase/master

hbase_regionserver: hbase_base
	docker build \
		--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
		--build-arg HBASE_VERSION=$(HBASE_VERSION) \
		--tag selis/hbase-regionserver-$(HBASE_VERSION) \
		./hbase/regionserver

postgres:
	docker build \
		--tag selis/postgres-11 \
		./postgres

livy: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg LIVY_VERSION=$(LIVY_VERSION) \
		--tag selis/livy-$(LIVY_VERSION) \
		./livy

bda: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--build-arg BDA_VERSION=$(BDA_VERSION) \
		--build-arg BDA_PREFIX=$(BDA_PREFIX) \
		--file ./bda/Dockerfile \
		--tag selis/bda-$(BDA_VERSION) \
		./bda

bda_devel: spark
	docker build \
		--build-arg SPARK_VERSION=$(SPARK_VERSION) \
		--file ./bda/Dockerfile.devel \
		--tag selis/bda-devel \
		./bda

clean:
	docker rmi \
		selis/hadoop-base-$(HADOOP_VERSION) \
		selis/hadoop-namenode-$(HADOOP_VERSION) \
		selis/hadoop-datanode-$(HADOOP_VERSION) \
		selis/hadoop-yarn-resourcemanager-$(HADOOP_VERSION) \
		selis/hadoop-yarn-nodemanager-$(HADOOP_VERSION) \
		selis/spark-$(SPARK_VERSION) \
		selis/zookeeper-$(ZOOKEEPER_VERSION) \
		selis/hbase-base-$(HBASE_VERSION) \
		selis/hbase-master-$(HBASE_VERSION) \
		selis/hbase-regionserver-$(HBASE_VERSION) \
		selis/postgres-11 \
		selis/livy-$(LIVY_VERSION) \
		selis/bda-devel
