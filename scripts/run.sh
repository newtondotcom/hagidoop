sh deployHDFS.sh
sh deployWorkers.sh
java HdfsClient write file.txt
java MyMapReduce file.txt