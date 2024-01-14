package impl;

import interfaces.Callback;

public class ImplCallback implements Callback {

	int nbTacheFinie;
	
	public ImplCallback(){
	}
	
	public void tacheFinie(){
		nbTacheFinie++;
	}

	public int getTachesFinies(){
		return this.nbTacheFinie;
	}
}