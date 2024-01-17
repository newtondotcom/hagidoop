#!/bin/bash

chemin="/home/raugerea2/Téléchargements/Hagidoop"

# This script is used copy the project source, compile on one of the machine 
# Then it starts the HDFS server and the RMI server on all the machines of the main.cfg file
listepc=$(sed "1q;d" src/config/main.cfg)
listehdfs=$(sed "2q;d" src/config/main.cfg)
listeprmi=$(sed "3q;d" src/config/main.cfg)
IFS=',' read -ra tabpc <<< "$listepc"
IFS=',' read -ra tabhdfs <<< "$listehdfs"
IFS=',' read -ra tabrmi <<< "$listeprmi"

index=0
# Check if the bin directory exists
#if ! ssh raugerea2@${tabpc[$index]} 'test -d /home/raugerea2/Téléchargements/Hagidoop/src/'; then
    ssh raugerea2@${tabpc[$index]} rm -rf ${chemin}/src/
    ssh raugerea2@${tabpc[$index]} rm -rf ${chemin}/bin/
    ssh raugerea2@${tabpc[$index]} mkdir -p ${chemin}/bin/
    scp -r src/ raugerea2@${tabpc[$index]}:${chemin}/
    scp -r data/ raugerea2@${tabpc[$index]}:${chemin}/
    scp -r scripts/ raugerea2@${tabpc[$index]}:${chemin}/
    ssh raugerea2@${tabpc[$index]} javac -d ${chemin}/bin ${chemin}/src/**/*.java
#fi

for index in ${!tabpc[*]}; do 
  ssh raugerea2@${tabpc[$index]} 'mkdir -p /tmp/data'

  echo "Démarrage du serveur HDFS sur ${tabpc[$index]}"
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin hdfs.HdfsServer ${tabhdfs[$index]} &

  echo "Démarrage du serveur RMI sur ${tabpc[$index]}"
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin daemon.WorkerImpl ${tabrmi[$index]} &
done