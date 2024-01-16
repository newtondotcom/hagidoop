#!/bin/bash

listepc=$(sed "2q;d" src/config/main_n7.cfg)
IFS=',' read -ra tabpc <<< "$listepc"
listeph=$(sed "6q;d" src/config/main_n7.cfg)
IFS=',' read -ra tabph <<< "$listeph"
chemin="/home/raugerea2/Téléchargements/Hagidoop"

for index in ${!tabpc[*]}; do
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin daemon.WorkerImpl ${tabph[$index]} &
done
