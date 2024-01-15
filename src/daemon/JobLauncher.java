package daemon;

import static interfaces.FileReaderWriter.FMT_TXT;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.LinkedList;

import application.MyMapReduce;
import interfaces.*;
import impl.*;
import config.*;

public class JobLauncher extends UnicastRemoteObject {
	// emplacement et port du service

	// Callback pour suivre l'avancer des workers
	static Callback cb;

	// Nombre de worker équivalent au nombre de machine qui travaille
	static int nbWorker;
	// Liste des workers
	static Worker[] listeWorker;

	// chemin pour le fichier de configuration
  public static String pathConfig = "src/config/main.cfg";
	// Chemin d'accès vers les fragments
	final static String path = "/tmp/data/";

	// Nombre de tâche finie parmi les workers lancés
	static int nbTacheFinie;
	static int nbMachines = Utils.recupnbmachines(pathConfig);


	// private static PersistentStorage node = new PersistentStorage();

	public JobLauncher(int _nbWorker, Worker[] _listWorkers, Callback _cb) throws RemoteException{
		nbWorker = _nbWorker;
		listeWorker = _listWorkers;
		cb = _cb;
	}

	public static <List> void startJob (MapReduce mr, int format, String fname) throws RemoteException{
		String[] inter = fname.split("\\.");
    String nom = inter[0];
		String extension = inter[1];

    int nbfragments = 2;//node.getNbFragments(fname);

		try{
			// On créer les reader et les writer pour chaque fragment
			FileReaderWriter writerFinal = new ImplFileRW(0, "Final.txt", "w", 1);
            LinkedList<KV> dynamicList = new LinkedList<KV>();
            for (int i = 0 ; i < nbfragments; i++) {
					// On donne le nom au fichier HDFS
					String fSrcName = path + nom + "_" + i + "." + extension;
					// On créer le reader et le writer que l'on donne au worker
					System.out.println(fSrcName);
					ImplFileRW reader = new ImplFileRW(0, fSrcName, "r", FMT_TXT);
					NetworkReaderWriter writer = new ImplNetworkRW(7001+i, "localhost");
					System.out.println("1");
					System.out.println("Lancement du runMap : " + (7001+i));
					writer.openServer();
					listeWorker[i%nbWorker].runMap(mr, reader, writer, cb);
					System.out.println("Fin du runMap" + (7001+i));
					NetworkReaderWriter r = writer.accept();
					r.openClient();
                    while (r.read() != null) {
                        dynamicList.add(r.read());
                    }
            }
            ImplFileRW file = new ImplFileRW(0, "tmp/data/Final.txt", "w", 1);
            for (KV kv : dynamicList) {
                file.write(kv);
            }
            mr.reduce(file, writerFinal);
            System.out.println("Fin du reduce");


		} catch(Exception e){
			e.printStackTrace();
		}
	}

  private static String[] recupURL(int nbMachines){
		int[] ports = new int[nbMachines];
		String[] noms = new String[nbMachines];
		String[] urls = new String[nbMachines];

    // On récupère le ports et le noms avec la classe utils
    noms = config.Utils.recupnom(pathConfig, nbMachines);
    ports = config.Utils.recupport(pathConfig, nbMachines);

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
    int nbMachines = config.Utils.recupnbmachines(pathConfig);
    Worker[] listWorker = new Worker[nbMachines];
    String[] urlWorker = recupURL(nbMachines);
    // récupérer les références des objets Worker sur les machines
    try {
      for (int i = 0 ; i < nbMachines ; i++) {
				System.out.println(urlWorker[i]);
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
      // Informations de format de fichier
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
      int nbMachines = config.Utils.recupnbmachines(pathConfig);
      System.out.println("Nombre de machines : " + nbMachines);
      // On récupère les instance des worker sur les machines 
      Worker[] listWorker = recupWorker();
			System.out.println(listWorker[0].ToString());
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


    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
