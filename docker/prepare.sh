#!/bin/bash

NAR_VERSION=0.2.2-SNAPSHOT

cd ..
mvn clean install -DskipTests
cd docker
echo "Copying nars to NiFi volume"
cp ../nifi-ignite-extensions/nifi-ignite-nar/target/nifi-ignite-nar-${NAR_VERSION}.nar ./nifi/user/nars
cp ../nifi-ignite-extensions/nifi-ignite-services-api-nar/target/nifi-ignite-services-api-nar-${NAR_VERSION}.nar ./nifi/user/nars
cp ../nifi-ignite-extensions/nifi-ignite-services-nar/target/nifi-ignite-services-nar-${NAR_VERSION}.nar ./nifi/user/nars
