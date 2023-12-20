#!/bin/bash

# récupérer la 2e ligne du fichier de config (noms des machines)
listepc=$(sed "2q;d" src/config/config_hidoop.cfg)

# découper la chaîne obtenue sur les délimiteurs ","
# et la stocker dans un tableau
IFS=',' read -ra tabpc <<< "$listepc"

# Arrêter les démons hidoop et hdfs sur les machines distantes
for pc in "${tabpc[@]}"
do
	ssh raugerea2@$pc "kill \$(jps | grep DaemonImpl | awk '{print \$1}')"
	ssh raugerea2@$pc "kill \$(jps | grep HdfsServer | awk '{print \$1}')"
done
