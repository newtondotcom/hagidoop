package daemon;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import interfaces.MapReduce;

public class JobLauncher extends UnicastRemoteObject {
	// emplacement et port du service
	private int nbMachine;
	
	private int[] port;


	public static Daemon listeDaemon[];

	public JobLauncher(int nbMachine) throws RemoteException{
		this.nbMachine = nbMachine;
		this.port = new int[nbMachine];
	}
	public static void startJob (MapReduce mr, int format, String fname) {
			
	}
}
