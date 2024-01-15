package impl;

import interfaces.Callback;

public class ImplCallback implements Callback {

	int nbTacheFinie;
	
	public ImplCallback(){
	}
	
	public void tacheFinie(){
		System.out.println("Tache finie" + nbTacheFinie);
		nbTacheFinie++;
	}

	public int getTachesFinies(){
		System.out.println("Tache" + nbTacheFinie);
		return this.nbTacheFinie;
	}
}