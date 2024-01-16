#!/bin/bash

listepc=$(sed "1q;d" src/config/main.cfg)
IFS=',' read -ra tabpc <<< "$listepc"

for pc in "${tabpc[@]}" 
do
	echo "Suppression des fragments sur $pc"
	ssh raugerea2@$pc "rm -rf /tmp/data"
	echo "Arrêt du démon Hagidoop sur $pc"
	ssh raugerea2@$pc "kill \$(jps | grep WorkerImpl | awk '{print \$1}')"
	echo "Arrêt du démon HdfsServer sur $pc"
	ssh raugerea2@$pc "kill \$(jps | grep HdfsServer | awk '{print \$1}')"
done
