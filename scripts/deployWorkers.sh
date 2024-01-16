#!/bin/bash

# Chemin d'accès vers le projet Hidoop
chemin="/home/raugerea2/Téléchargements/Hagidoop"

# Récupérer la 2e ligne du fichier de config (noms des machines)
listepc=$(sed "2q;d" src/config/main_n7.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabpc <<< "$listepc"

# Récupérer la 6e ligne du fichier de config (ports Hagidoop)
listeph=$(sed "6q;d" src/config/main_n7.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabph <<< "$listeph"

###############################################################
# Compiler les fichiers du projet
javac -d bin src/**/*.java

# Lancer les démons sur les machines distantes
for index in ${!tabpc[*]}; do
  ssh raugerea2@${tabpc[$index]} java -cp ${chemin}/bin daemon.WorkerImpl ${tabph[$index]} &
done

#sleep 3
#java -cp bin daemon.JobLauncher data.txt line $1
