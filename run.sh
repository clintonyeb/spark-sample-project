#!/bin/bash
name=spark-app

# start services
docker-compose up -d

# stop application
docker stop $name && docker rm $name

# Build application and deploy to docker
docker build -t $name .

# Run application in docker
docker run --network=final-project_default --name $name --rm -it --link master $name /etc/bootstrap.sh -bash

# copy output to local
#rm -rf ./output
#docker cp $name:/output ./output

#  Shutdown services
docker-compose down