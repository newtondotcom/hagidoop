package daemon;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.Format;

import interfaces.Callback;
import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject  implements Worker{

  // Registre du service daemon
  static Registry registre;
  // Port du service
	static int port;
	//
	static String host;

	/* 
	 * Constructeur
	*/
  public WorkerImpl() throws RemoteException{
  }

	private static void fctusage() {
		System.out.println("Utilisation : java DaemonImpl port");
	}

  public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer, Callback cb) throws RemoteException{

		try{
			// On ouvre la connexion du reader et du writer
			reader.open("R");
			writer.openServer();
			
			// Lancer la fonction map sur le fragment de fichier
			m.map(reader, writer);

			// Utiliser Callback pour prévenir que le traitement est terminé
			
			// On ferme le reader et le writer
			reader.close();
			writer.closeServer();
		} catch (Exception e){
			e.printStackTrace();
		}
  }



	

	public static void main (String args[]) {
		
		// vérifier le bon usage du daemon
		try {
			if (args.length < 1) {
				fctusage();
				System.exit(1);
			}
			
			port = Integer.parseInt(args[0]);
			host = "localhost";
			
			// Création du serveur de noms sur le port indiqué
			try {
				registre = LocateRegistry.createRegistry(port);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			String url = "//" + host + ":" + port + "/Daemon";
			
			// Inscription auprès du registre
			Naming.bind(url, new WorkerImpl());
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}
}
