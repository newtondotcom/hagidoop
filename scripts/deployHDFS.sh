#!/bin/bash

listepc=$(sed "2q;d" src/config/main_n7.cfg)
IFS=',' read -ra tabpc <<< "$listepc"
listeph=$(sed "4q;d" src/config/main_n7.cfg)
IFS=',' read -ra tabph <<< "$listeph"
chemin="/home/raugerea2/Téléchargements/Hagidoop"

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

  echo " java -cp ${chemin}/bin hdfs.HdfsServer ${tabph[$index]} &"
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin hdfs.HdfsServer ${tabph[$index]} &
done
