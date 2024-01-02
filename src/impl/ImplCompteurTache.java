package impl;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import interfaces.CompteurTache;


public class ImplCompteurTache extends UnicastRemoteObject implements CompteurTache {
	
  // Nombre de machine ayant terminer leur tâche
	private int nbMachinesFinies;
	
  /* Constructor */
	public ImplCompteurTache(int nbFragments) throws RemoteException {
		this.nbMachinesFinies = 0;
	}
	
	public int getTachesFinies() {
		return this.nbMachinesFinies;
	}

	// compter le nombre de tâches finies
	public void tacheFinie() {
		this.nbMachinesFinies++;
	}
}