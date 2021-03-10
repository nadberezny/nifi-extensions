cd ..
mvn install -DskipTests
cd docker
echo "Copying nars to NiFi volume"
cp ../nifi-ignite-extensions/nifi-ignite-services-api-nar/target/nifi-ignite-services-api-nar-0.1.nar ./nifi/user/nars
cp ../nifi-ignite-extensions/nifi-ignite-services-nar/target/nifi-ignite-services-nar-0.1.nar ./nifi/user/nars
