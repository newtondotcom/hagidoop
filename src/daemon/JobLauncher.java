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
	String mainMachineName = "farman";

	public MyThread(String _nom, int _i, String _extension, interfaces.MapReduce _mr, Worker[] _listeWorker, int _nbWorker) {
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
		ImplFileRW reader = new ImplFileRW(0, fSrcName, "r", FMT_TXT);
		// On créer le writer pour le fichier temporaire pour le map
		NetworkReaderWriter writer = new ImplNetworkRW(7001+i, mainMachineName);
		// On créer le writer pour le fichier temporaire pour le reduce
		String fKVName = JobLauncher.pathKV + nom + "_" + i + ".kv";
		FileReaderWriter writerFinal = new ImplFileRW(0, fKVName, "w", FMT_KV);
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
	// Nombre de worker équivalent au nombre de machine qui travaille
	static int nbWorker;
	// Liste des workers
	static Worker[] listeWorker;

	// chemin pour le fichier de configuration
  public static String pathConfig = Project.config;

	// Chemin d'accès vers les fragments
	final static String path = "/tmp/data/";
	final static String pathKV = "temp/";

	// Nombre de machines
	static int nbMachines = Utils.recupnbmachines(pathConfig);
	// Nombre de fragment créer par HDFS
	static int nbfragments;

	public JobLauncher(int _nbWorker, Worker[] _listWorkers) throws RemoteException{
		nbWorker = _nbWorker;
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
				threadsList[i] = new MyThread(nom, i, extension, mr, listeWorker, nbWorker);
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
		ImplFileRW writer = new ImplFileRW(0, "Final.txt", "w", FMT_TXT);
		for(int i = 0; i < nbfragments; i++) {
			// On créer le reader pour lire chaque fichier temporaire
			ImplFileRW reader = new ImplFileRW(0, pathKV + filename + "_" + i + ".kv", "r", FMT_KV);
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
      // Formats de fichiers utilisables
      String[] formats = {"line","kv"};
      // Informations de format de fichier
      int format;

      // On vérifie qu'il y a assez d'argument pour appeler le client
      if (args.length < 2) {
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

      // Récupérer le format de fichier indiqué en argument
        if (args[1].equals("line")) {
          format = 0;
        } else {
          format = 1;
        }

      // On récupère les instance des worker sur les machines 
      Worker[] listWorker = config.Utils.recupWorker(Project.config);
      System.out.println("Récupération des workers terminée");
      // On créer le callback

      // On créer Le jobLauncher, celui qui va lancer le reduce sur chacune des machines
      JobLauncher jobLauncher = new JobLauncher(nbMachines, listWorker);

      // On créer le MapReduce que l'on donnera au Worker
      MapReduce mr = new MapReduce();

      // On lance le start Job
      System.out.println("StartJob");
      jobLauncher.startJob(mr, format, hdfsFilename);
      System.out.println("EndJob");
	  System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
