#!/bin/bash

# Folder to store JARs
JAR_DIR="jars"
mkdir -p $JAR_DIR

# Compile Broker
echo "Building Broker JAR..."
mvn clean package -DskipTests -f pom.xml -Pbroker
cp target/my-pub-sub-system-1.0-SNAPSHOT.jar $JAR_DIR/broker.jar

# Compile Publisher
echo "Building Publisher JAR..."
mvn clean package -DskipTests -f pom.xml -Ppublisher
cp target/my-pub-sub-system-1.0-SNAPSHOT.jar $JAR_DIR/publisher.jar

# Compile Subscriber
echo "Building Subscriber JAR..."
mvn clean package -DskipTests -f pom.xml -Psubscriber
cp target/my-pub-sub-system-1.0-SNAPSHOT.jar $JAR_DIR/subscriber.jar

echo "All JARs built and stored in '$JAR_DIR' directory."
