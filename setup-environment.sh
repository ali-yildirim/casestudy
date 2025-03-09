
mkdir casestudy

cd casestudy

touch main.py

mkdir database

cd database

docker run --name mysql-docker -e MYSQL_ROOT_PASSWORD=secret -p 3307:3306 -d mysql:latest

