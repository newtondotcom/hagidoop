package impl;

import interfaces.Callback;


public class ImplCallback implements Callback {

		private String adresseMain; // Adresse pour contacter le main et prevenir que l'on a fini
    private int id; // Id du Callback, le worker saura sur quel fragment il travaille

  /* Constructor */
	public ImplCallback(int _id, String _adresseMain) {
		this.id = _id;
		this.adresseMain = _adresseMain;
	}
	
	public String getAdresseRetour(){
		return this.adresseMain;
	}

	public int getID(){
		return this.id;
	}
}