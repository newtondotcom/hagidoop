#!/bin/bash

###############################################################
###################### CONFIGURATION ##########################
###############################################################

# Chemin d'accès vers le projet Hidoop
chemin="Téléchargements/Hidoopgit/"

# Récupérer la 2e ligne du fichier de config (noms des machines)
listepc=$(sed "2q;d" src/config/config_hidoop.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabpc <<< "$listepc"

# Récupérer la 6e ligne du fichier de config (ports Hidoop)
listeph=$(sed "6q;d" src/config/config_hidoop.cfg)

# Découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabph <<< "$listeph"

###############################################################
# Compiler les fichiers du projet
javac -d bin src/**/*.java

# Lancer les démons sur les machines distantes
for index in ${!tabpc[*]}; do
  ssh ${tabpc[$index]} java -cp ${chemin}/bin ordo.DaemonImpl ${tabph[$index]} &
done

sleep 3

java -cp bin ordo.HidoopClient data.txt line $1
