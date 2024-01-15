#!/bin/bash

###############################################################
###################### CONFIGURATION ##########################
###############################################################

# Récupérer la 2e ligne du fichier de config (noms des machines)
listepc=$(sed "2q;d" src/config/main_n7.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabpc <<< "$listepc"

# Récupérer la 4e ligne du fichier de config (ports Hdfs)
listeph=$(sed "4q;d" src/config/main_n7.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabph <<< "$listeph"

###############################################################
# Compiler les fichiers du projet
#javac -d bin src/**/*.java

# Chemin d'accès vers le projet Hidoop
chemin="/home/raugerea2/Téléchargements/Hagidoop"

index=0
# Check if the bin directory exists
#if ! ssh raugerea2@${tabpc[$index]} 'test -d /home/raugerea2/Téléchargements/Hagidoop/src/'; then
    ssh raugerea2@${tabpc[$index]} rm -rf ${chemin}/src/
    ssh raugerea2@${tabpc[$index]} rm -rf ${chemin}/bin/
    ssh raugerea2@${tabpc[$index]} mkdir -p ${chemin}/bin/
    scp -r src/ raugerea2@${tabpc[$index]}:${chemin}/
    scp -r filesample.txt raugerea2@${tabpc[$index]}:${chemin}/
    scp -r scripts/ raugerea2@${tabpc[$index]}:${chemin}/
    ssh raugerea2@${tabpc[$index]} javac -d ${chemin}/bin ${chemin}/src/**/*.java
#fi

for index in ${!tabpc[*]}; do 
  # Creer le dossier data if it doesn't exist
  ssh raugerea2@${tabpc[$index]} 'mkdir -p /tmp/data'

  echo " java -cp ${chemin}/bin daemon.WorkerImpl ${tabph[$index]} &"
  # Lancer les démons Hdfs
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin hdfs.HdfsServer ${tabph[$index]} &
done


# Lancement du client
#sleep 0.5

#java -cp bin hdfs.HdfsClient write line data.txt $1
#java -cp bin hdfs.HdfsClient delete filesample.txt
#java -cp bin hdfs.HdfsClient read data/filesample.txt data/filesample-red.txt
