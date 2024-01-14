package daemon;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import interfaces.*;
import impl.*;
import hdfs.PersistentStorage;
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
	static int nbMachines = Utils.recupnb(pathConfig);


	private static PersistentStorage node;

	public JobLauncher(int _nbWorker, Worker[] _listWorkers, Callback _cb) throws RemoteException{
		nbWorker = _nbWorker;
		listeWorker = _listWorkers;
		cb = _cb;
	}

	public static void startJob (MapReduce mr, int format, String fname) throws RemoteException{
		String[] inter = fname.split("\\.");
    String nom = inter[0];
		String extension = inter[1];

    int nbfragments = node.getNbFragments(fname);

		try{
			if (nbfragments == 1) {
				// On donne le nom au fichier HDFS
				String fSrcName = path + nom + "_" + 0 + "." + extension;
				String fDestname = fSrcName + "-res";
				
				// On créer le reader et le writer
				FileReaderWriter reader = new ImplFileRW(1, fSrcName, "r", format);
				NetworkReaderWriter writer = new ImplNetworkRW(7000, fDestname);
				
				listeWorker[0].runMap(mr, reader, writer, cb);

				NetworkReaderWriter serveur = new ImplNetworkRW(7000, fDestname);
					serveur.accept();
			} 
			else {
				for (int i = 0 ; i < nbfragments; i++) {
					// On donne le nom au fichier HDFS
					String fSrcName = path + nom + "_" + i + "." + extension;
					String fDestname = fSrcName + "-res";

					// On créer le reader et le writer
					FileReaderWriter reader = new ImplFileRW(0, fSrcName, "r", format);
					NetworkReaderWriter writer = new ImplNetworkRW(7000+i, fDestname);
					
					
					listeWorker[i%nbWorker].runMap(mr, reader, writer, cb);

					NetworkReaderWriter serveur = new ImplNetworkRW(7000+i, fDestname);
					serveur.accept();
				}
			}
			while (cb.getTachesFinies() != nbMachines) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
			String fDest = path + nom + "_"  + "." + extension + "-res";
			NetworkReaderWriter client = new ImplNetworkRW(7500, fDest );
			client.openServer();
			client.accept();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}
