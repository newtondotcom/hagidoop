package daemon;

import static interfaces.FileReaderWriter.FMT_KV;
import static interfaces.FileReaderWriter.FMT_TXT;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

import application.MyMapReduce;
import interfaces.*;
import impl.*;
import hdfs.HdfsClient;
import config.*;

class MyThread extends Thread {
	String nom;
	int i;
	String extension;	
	MapReduce mr;
	Worker[] listeWorker;
	Callback cb;
	int nbWorker;

	public MyThread(String _nom, int _i, String _extension, MapReduce _mr, Worker[] _listeWorker, Callback _cb, int _nbWorker) {
		nom = _nom;
		i = _i;
		extension = _extension;
		mr = _mr;
		listeWorker = _listeWorker;
		cb = _cb;
		nbWorker = _nbWorker;
	}
	public void run() {
		try{
		System.out.println("MyThread running");
		// On donne le nom au fichier HDFS
		String fSrcName = JobLauncher.path + nom + "_" + i + "." + extension;
		// On créer le reader et le writer que l'on donne au worker
		System.out.println(fSrcName);
		ImplFileRW reader = new ImplFileRW(0, fSrcName, "r", FMT_TXT);
		String fKVName = JobLauncher.pathKV + nom + "_" + i + ".kv";
		FileReaderWriter writerFinal = new ImplFileRW(0, fKVName, "w", FMT_KV);
		NetworkReaderWriter writer = new ImplNetworkRW(7001+i, "heidi");
		System.out.println("1");
		System.out.println("Lancement du runMap : " + (7001+i));
		writer.openServer();
		listeWorker[i%nbWorker].runMap(mr, reader, writer);
		System.out.println("Fin du runMap" + (7001+i));
		NetworkReaderWriter r = writer.accept();
		r.openClient();
		mr.reduce(r, writerFinal);
		System.out.println("Fin du reduce");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

public class JobLauncher extends UnicastRemoteObject {
	// emplacement et port du service

	// Callback pour suivre l'avancer des workers
	static Callback cb;

	// Nombre de worker équivalent au nombre de machine qui travaille
	static int nbWorker;
	// Liste des workers
	static Worker[] listeWorker;

	// chemin pour le fichier de configuration
  	public static String pathConfig = Project.config;

	// Chemin d'accès vers les fragments
	final static String path = "/tmp/data/";
	final static String pathKV = "temp/";

	// Nombre de tâche finie parmi les workers lancés
	static int nbTacheFinie;
	static int nbMachines = Utils.recupnbmachines(pathConfig);


	// private static PersistentStorage node = new PersistentStorage();

	public JobLauncher(int _nbWorker, Worker[] _listWorkers, Callback _cb) throws RemoteException{
		nbWorker = _nbWorker;
		listeWorker = _listWorkers;
		cb = _cb;
	}

	public static void startJob (MapReduce mr, int format, String fname) throws RemoteException{
		String[] inter = fname.split("\\.");
    	String nom = inter[0];
		String extension = inter[1];

    	int nbfragments = config.Utils.recupnbmachines(pathConfig);

		try{
			// On créer les reader et les writer pour chaque fragment
			MyThread[] threadsList = new MyThread[nbfragments];
			for (int i = 0 ; i < nbfragments; i++) {
				threadsList[i] = new MyThread(nom, i, extension, mr, listeWorker, cb, nbWorker);
				threadsList[i].start();
			}
			for (int i = 0; i < nbfragments; i++) {
				threadsList[i].join();
				System.out.println("Thread " + i + " terminé");
		}
			FileReduce(nbfragments, nom);
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
    ports = config.Utils.recuprmi(pathConfig, nbMachines);

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

	private static void FileReduce(int nbfragments, String filename) {
		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		Locale locale = new Locale("fr", "FR");
		ImplFileRW writer = new ImplFileRW(0, "Final.txt", "w", FMT_TXT);
		for(int i = 0; i < nbfragments; i++) {
			ImplFileRW reader = new ImplFileRW(0, pathKV + filename + "_" + i + ".kv", "r", FMT_KV);
			KV kv;
			System.out.println("FileReduce");
			while ((kv = reader.readkv()) != null) {
				kv.k = kv.k.toLowerCase(locale);
				if (hm.containsKey(kv.k)) {hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));}
				else hm.put(kv.k, Integer.parseInt(kv.v));
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
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
