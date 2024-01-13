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

	static Callback cb;

	// Nombre de worker équivalent au nombre de machine qui travaille
	static int nbWorker;
	// Liste des workers
	static Worker[] listeWorker;

	// Nombre de tâche finie parmi les workers lancés
	static int nbTacheFinie;

	// Chemin d'accès vers les fragments
	String path = "/tmp/data/";

	public JobLauncher(int _nbWorker) throws RemoteException{
		this.nbWorker = _nbWorker;
		this.listeWorker = new Worker[_nbWorker];
	}

	public static void startJob (MapReduce mr, int format, String fname) throws RemoteException{
		try{
			if (nbWorker == 1) {
				// On donne le nom au fichier HDFS
				String fSrcName = fname;
				
				// On créer le reader et le writer
				FileReaderWriter reader = new ImplFileRW(1, fSrcName, "r", format);
				NetworkReaderWriter writer = new ImplNetworkRW(1, "");
				
				listeWorker[0].runMap(mr, reader, writer, cb);
			} 
			else {
				for (int i = 0 ; i < nbWorker; i++) {
					// On donne le nom au fichier HDFS
					String fSrcName = fname;

					// On créer le reader et le writer
					FileReaderWriter reader = new ImplFileRW(1, fSrcName, "r", format);
					NetworkReaderWriter writer = new ImplNetworkRW(1, "");
					
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
}
