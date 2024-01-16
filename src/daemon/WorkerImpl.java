package daemon;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject  implements Worker{

  // Registre du service worker
  static Registry registre;
	// Host de la machine worker
	static String host;


  public WorkerImpl() throws RemoteException{
  }

  public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException{

		try{
			// On ouvre le fichier
			reader.open("r");
			// On ouvre le client 
			writer.openClient();
			
			// Lancer la fonction map sur le fragment de fichier
			m.map(reader, writer);

			// On ferme le fichier
			reader.close();

			// On ecrit un KV EOF pour définir la fin du fichier
			writer.write(new KV("EOF","0"));
			System.out.println("Fermeture du worker");
		} catch (Exception e){
			e.printStackTrace();
		}
  }

	public static void main (String args[]) {
		
		// On vérifie que le port est donnée
		try {
			if (args.length < 1) {
				System.err.println("Le port n'est pas donnée");
				System.exit(1);
			}
			
			// On récupère le port avec l'argument
			int port = Integer.parseInt(args[0]);
			host = "localhost";
			
			// Création du registry sur le bon port
			try {
				registre = LocateRegistry.createRegistry(port);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Création de l'URL du service
			String url = "//" + host + ":" + port + "/Worker";
			
			// Inscription auprès du registre et envoie du worker
			Naming.bind(url, new WorkerImpl());
			System.out.println("Bind du Worker sur l'url : " + url);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
