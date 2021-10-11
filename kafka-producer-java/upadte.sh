version=$1;
#docker_login;
docker build . -t acreastus.azurecr.io/kafka-producer-java-blocking-test:$version;
docker push acreastus.azurecr.io/kafka-producer-java-blocking-test:$version;