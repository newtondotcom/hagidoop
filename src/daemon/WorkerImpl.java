package daemon;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import interfaces.Callback;
import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject  implements Worker{

  // Registre du service daemon
  static Registry registre;
	//
	static String host = "localhost";

	/* 
	 * Constructeur
	*/
  public WorkerImpl() throws RemoteException{
  }


  public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer, Callback cb) throws RemoteException{

		try{
			// On ouvre la connexion du reader et du writer
			reader.open("r");
			writer.openClient();
			
			// Lancer la fonction map sur le fragment de fichier
			m.map(reader, writer);

			// Utiliser Callback pour prévenir que le traitement est terminé
			
			
			// On ferme le reader et le writer
			reader.close();
			writer.closeClient();
		} catch (Exception e){
			e.printStackTrace();
		}
  }



	

	public static void main (String args[]) {
		
		// vérifier le bon usage du daemon
		try {
			if (args.length < 1) {
				System.out.println("DaemonImpl port non donnée");
				System.exit(1);
			}
			
			int port = Integer.parseInt(args[0]);
			host = "localhost";
			
			// Création du serveur de noms sur le port indiqué
			try {
				registre = LocateRegistry.createRegistry(port);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			String url = "//" + host + ":" + port + "/Worker";
			
			// Inscription auprès du registre
			Naming.bind(url, new WorkerImpl());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
