#! /usr/bin/env bash

mkdir tmp
cp -r src tmp
cp pom.xml tmp

docker run --rm -v $(pwd)/tmp:/root/smt -w /root/smt maven:3-jdk-17 mvn clean package -DskipTests

