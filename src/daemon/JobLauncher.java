package daemon;

import static interfaces.FileReaderWriter.FMT_KV;
import static interfaces.FileReaderWriter.FMT_TXT;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

import impl.MapReduce;
import hdfs.PersistentStorage;
import interfaces.*;
import impl.*;
import config.*;

class MyThread extends Thread {
	String nom;
	int i;
	String extension;	
	interfaces.MapReduce mr;
	Worker[] listeWorker;
	int nbWorker;
	String mainMachineName = "succube";

	public MyThread(String _nom, int _i, String _extension, interfaces.MapReduce _mr, int _nbWorker, Worker[] _listeWorker) {
		nom = _nom;
		i = _i;
		extension = _extension;
		mr = _mr;
		listeWorker = _listeWorker;
		nbWorker = _nbWorker;
	}
	public void run() {
		try{
		System.out.println("MyThread running");
		// On donne le nom du fichier HDFS que l'on récupère
		String fSrcName = JobLauncher.path + nom + "_" + i + "." + extension;
		// On créer le reader du fichier HDFS
		ImplFileRW reader = new ImplFileRW(0, fSrcName, FMT_TXT);
		reader.open("r");
		// On créer le writer pour le fichier temporaire pour le map
		NetworkReaderWriter writer = new ImplNetworkRW(7001+i, mainMachineName);
		// On créer le writer pour le fichier temporaire pour le reduce
		String fKVName = JobLauncher.pathKV + nom + "_" + i + ".kv";
		FileReaderWriter writerFinal = new ImplFileRW(0, fKVName, FMT_KV);
		writerFinal.open("w");
		// On ouvre le serveur pour laisser le worker se connecter
		writer.openServer();
		// On lance le map sur le worker
		listeWorker[i%nbWorker].runMap(mr, reader, writer);
		// On accpet la connexion du Worker
		NetworkReaderWriter r = writer.accept();
		// On ouvre le client sur le writer que l'on vietn de récupérer afin de devenir le client
		r.openClient();
		// On peut lancer le reduce
		mr.reduce(r, writerFinal);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

public class JobLauncher extends UnicastRemoteObject {
	// Liste des workers
	static Worker[] listeWorker;

	// chemin pour le fichier de configuration
  public static String pathConfig = Project.config;

	// Chemin d'accès vers les fragments
	final static String path = "/tmp/data/";
	final static String pathKV = "temp/";

	// Format du fichiers que l'on lit en entrée
	static int format;

	// Nombre de machines
	static int nbMachines = Utils.recupnbmachines(pathConfig);
	// Nombre de fragment créer par HDFS
	static int nbfragments;

	public JobLauncher(Worker[] _listWorkers) throws RemoteException{
		listeWorker = _listWorkers;
	}

	public static void startJob (interfaces.MapReduce mr, int format, String fname) throws RemoteException{
		String[] inter = fname.split("\\.");
		String nom = inter[0];
		String extension = inter[1];



		PersistentStorage storage = new PersistentStorage();
		nbfragments = storage.getNbFragments(fname);

		try{
			// On créer les reader et les writer pour chaque fragment
			MyThread[] threadsList = new MyThread[nbfragments];
			for (int i = 0 ; i < nbfragments; i++) {
				// On lance les différents Thread sur toutes les machines
				threadsList[i] = new MyThread(nom, i, extension, mr, nbMachines, listeWorker);
				threadsList[i].start();
			}
			for (int i = 0; i < nbfragments; i++) {
				// On récupère les différents Thread sur toutes les machines
				threadsList[i].join();
				System.out.println("Thread " + i + " terminé");
		}
			FileReduce(nbfragments, nom);
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void FileReduce(int nbfragments, String filename) {
		// On créer une hashmap pour stocker les mots et leur nombre d'occurence
		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		// On créer le locale pour le français afin de mettre tous les mots en minuscule
		Locale locale = new Locale("fr", "FR");
		// On créer le writer pour le fichier final
		ImplFileRW writer = new ImplFileRW(0, "Final.txt", FMT_TXT);
		writer.open("w");
		for(int i = 0; i < nbfragments; i++) {
			// On créer le reader pour lire chaque fichier temporaire
			ImplFileRW reader = new ImplFileRW(0, pathKV + filename + "_" + i + ".kv", FMT_KV);
			reader.open("r");
			KV kv;
			// On écrit sur le fichier final en vérifiant si un KV existe déja
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
      // On vérifie qu'il y a assez d'argument pour appeler le client
      if (args.length < 2) {
          System.err.println("Erreur lancement HagidoopClient, pass assez d'argument"); 
          System.exit(1);
      } 

      // On récupère le nom du fichier où l'on applique le traitement
      String nomFichier = args[0];

      // Récupérer le format de fichier indiqué en argument
        if (args[1].equals("line")) {
          format = 0;
        } else if (args[1].equals("txt")) {
					format = 1;
				} else{
          System.err.println("Format de fichier non reconnu");
        }

      // On récupère les instance des worker sur les machines 
      Worker[] listWorker = config.Utils.recupWorker(Project.config);
      System.out.println("Récupération des workers terminée");

      // On créer Le jobLauncher, celui qui va lancer le reduce sur chacune des machines
      JobLauncher jobLauncher = new JobLauncher(listWorker);

      // On créer le MapReduce que l'on donnera au Worker
      MapReduce mr = new MapReduce();

      // On lance le start Job
      System.out.println("StartJob");
      JobLauncher.startJob(mr, format, nomFichier);
      System.out.println("EndJob");
	  System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
