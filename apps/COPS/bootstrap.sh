#!/usr/bin/env bash

#SET SSH BY PASSWORD
sed -i 's/ChallengeResponseAuthentication no/ChallengeResponseAuthentication yes/g' /etc/ssh/sshd_config
sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config
service ssh restart


apt update
apt full-upgrade -y
apt install openjdk-8-jdk -y
apt install net-tools -y


#SERVERNAME=$HOSTNAME
#SERVERNAME=${SERVERNAME//-/_}


#cd /vagrant/cops/cops-server
#mvn exec:java -Dexec.args=""