package impl;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Arrays;

import application.MyMapReduce;
import daemon.*;
import hdfs.HdfsClient;
import interfaces.*;

public class HagiDoopClient {
  // chemin pour le fichier de configuration
  public static String path = "src/config/main.cfg";

  private static String[] recupURL(int nbMachines){
		int[] ports = new int[nbMachines];
		String[] noms = new String[nbMachines];
		String[] urls = new String[nbMachines];

    // On récupère le ports et le noms avec la classe utils
    noms = config.Utils.recupnom(path, nbMachines);
    ports = config.Utils.recupport(path, nbMachines);

    // On créer les urls pour récupérer les instance de worker
    // On vérifie que l'on a assez d'infos
			if (noms.length != 0 && ports.length == noms.length) {
				for (int i=0 ; i < nbMachines ; i++) {
					urls[i] = "//" + noms[i] + ":" + ports[i] + "/Worker";
				}
			} else {
				System.err.println("Problème avec la création des urls dans HagidoopClient");
			}
    return urls;
  }

  private static Worker[] recupWorker() {
    int nbMachines = config.Utils.recupnbmachines(path);
    Worker[] listWorker = new Worker[nbMachines];
    String[] urlWorker = recupURL(nbMachines);
    // récupérer les références des objets Worker sur les machines
    try {
      for (int i = 0 ; i < nbMachines ; i++) {
        listWorker[i]=(Worker) Naming.lookup(urlWorker[i]);
        System.out.println("Worker " + i + " récupéré");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return listWorker;
  }

  public static void main(String[] args) throws RemoteException{
    try{
      // Formats de fichiers utilisables
      String[] formats = {"line","kv"};

      // informations de format de fichier
      int format;

      // On vérifie qu'il y a assez d'argument pour appeler le client
      if (args.length < 3) {
          System.err.println("Erreur lancement HagidoopClient, pass assez d'argument"); 
          System.exit(1);
      } else {
        if (!Arrays.asList(formats).contains(args[1])) {
          System.err.println("Erreur lancement HagidoopClient, format de fichier non reconnu");
          System.exit(1);
        }
      }

      // On récupère le nom du fichier où l'on applique le traitement
      String hdfsFilename = args[0];

      // On créer le nom du fichier avant le reduce
      String[] nomExt = hdfsFilename.split("\\.");
      String localFSDestFname = "data/" + nomExt[0] + "-res" + "." + nomExt[1];
      System.out.println(localFSDestFname);

      // fichier résultat du reduce : ajout du suffixe "-red"
      // Nom du fichier traité après application du reduce
      String reduceDestFname = "data/" + nomExt[0] + "-red" + "." + nomExt[1];
      System.out.println(reduceDestFname);

      // Récupérer le format de fichier indiqué en argument
        if (args[1].equals("line")) {
          format = 0;
        } else {
          format = 1;
        }

      // On récupère le nombre de machine 
      int nbMachines = config.Utils.recupnbmachines(path);
      System.out.println("Nombre de machines : " + nbMachines);
      // On récupère les instance des worker sur les machines 
      Worker[] listWorker = recupWorker();
      System.out.println("Récupération des workers terminée");
      // On créer le callback
      Callback cb = new ImplCallback();

      // On créer Le jobLauncher, celui qui va lancer le reduce sur chacune des machines
      JobLauncher jobLauncher = new JobLauncher(nbMachines, listWorker, cb);

      // On créer le MapReduce que l'on donnera au Worker
      MyMapReduce mr = new MyMapReduce();

      // On lance le start Job
      System.out.println("StartJob");
      jobLauncher.startJob(mr, format, hdfsFilename);
      System.out.println("EndJob");

      // On attend que tous les workers aient fini leur traitement
      System.out.println("Début attente Machine");
      while (cb.getTachesFinies() != nbMachines) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      System.out.println("Fin attente Machine");

      
      // récupérer le fichier traité via HDFS
			HdfsClient.HdfsRead(localFSDestFname);
			System.out.println("Lecture terminée");

      ImplNetworkRW reader = new ImplNetworkRW(7000, localFSDestFname);
      reader.openClient();
      FileReaderWriter writer = new ImplFileRW(0, "Resultat.txt", "w", 1);

      // On fait le reduce
      System.out.println("Début du reduce");
			mr.reduce(reader, writer);
			System.out.println("Fin du reduce");
      reader.closeClient();
			writer.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
