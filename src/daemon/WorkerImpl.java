package daemon;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.text.Format;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl implements Worker extends UnicastRemoteObject{

  // Registre du service daemon
  private Registry registre;
  // Port du service
	private int port;

  public WorkerImpl() throws RemoteException{
  }
  public void runMap (Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException{

		// Lancer la fonction map sur le fragment de fichier
		reader.open("R");
		writer.openServer();
		
		m.map(reader, writer);

		// Utiliser Callback pour prévenir que le traitement est terminé
		cb.tacheFinie();
		
		reader.close();
		writer.close();
  }
}
