#!/bin/bash
set -e
mvn package -Dmaven.test.skip=true
docker-compose build
