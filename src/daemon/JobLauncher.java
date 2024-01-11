package daemon;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import interfaces.*;
import impl.*;
import config.*;

public class JobLauncher extends UnicastRemoteObject {
	// emplacement et port du service
	static int[] port;

	static Callback cb;

	// Nombre de worker équivalent au nombre de machine qui travaille
	static int nbWorker;
	// Liste des workers
	static Worker[] listeWorker;

	// Nombre de tâche finie parmi les workers lancés
	static int nbTacheFinie;

	// Chemin d'accès vers les fragments
	String path = "/data/";

	public JobLauncher(int _nbWorker) throws RemoteException{
		this.nbWorker = _nbWorker;
		this.port = new int[_nbWorker];
		this.listeWorker = new Worker[_nbWorker];
	}

	public static void startJob (MapReduce mr, int format, String fname) throws RemoteException{
		try{
			if (nbWorker == 1) {
				// On donne le nom au fichier HDFS
				String fSrcName = "";

				// On donne le nom au fichier du résultat 
				String fDestName = fSrcName + "_resultat";
				
				// On créer le reader et le writer
				FileReaderWriter reader = new ImplFileRW(1, fSrcName, "r", format);
				NetworkReaderWriter writer = new ImplNetworkRW(fDestName);
				
				listeWorker[0].runMap(mr, reader, writer, cb);
			} 
			else {
				for (int i = 0 ; i < nbWorker; i++) {
					// On donne le nom au fichier HDFS
					String fSrcName = "";

					// On donne le nom au fichier du résultat 
					String fDestName = fSrcName + "_resultat";

					// On créer le reader et le writer
					FileReaderWriter reader = new ImplFileRW(1, fSrcName, "r", format);
					NetworkReaderWriter writer = new ImplNetworkRW(fDestName);
					
					// compteur : si on atteint la fin de la liste de démons,
					// retourner au début de celle-ci ; ainsi, on parcourt
					// bien les fragments conformément à HDFS
					
					listeWorker[i%nbWorker].runMap(mr, reader, writer, cb);
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		Properties properties = config.Project.loadProperties(config.Project.nameNode);
		String serverAddress = properties.getProperty("server.address");
    String port = properties.getProperty("server.port");
	}
}
