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

	public String ToString() throws RemoteException{
		return "WorkerImpl : " + host + registre.toString();
	}

  public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer, Callback cb) throws RemoteException{

		try{
			// On ouvre la connexion du reader et du writer
			System.out.println("Ouverture du reader" + reader.getFname());
			reader.open("r");
			System.out.println("Ouverture du client writer");
			writer.openClient();
			System.out.println("Lancement du worker");
			
			// Lancer la fonction map sur le fragment de fichier
			System.out.println("Lancement de la fonction map");
			m.map(reader, writer);
			System.out.println("Fin de la fonction map");

			// Utiliser Callback pour prévenir que le traitement est terminé
			cb.tacheFinie();
			
			// On ferme le reader et le writer
			reader.close();
			//writer.closeClient();
			System.out.println("Fermeture du worker");
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
			System.out.println(host + ":" + port);
			
			// Création du serveur de noms sur le port indiqué
			System.out.println("Création du registre sur le port " + port);
			try {
				registre = LocateRegistry.createRegistry(port);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Création de l'URL du service
			System.out.println("Création de l'URL du service");
			String url = "//" + host + ":" + port + "/Worker";
			
			// Inscription auprès du registre
			Naming.bind(url, new WorkerImpl());
			System.out.println("Inscription de l'objet avec l'url : " + url);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
