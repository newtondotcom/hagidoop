package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;

import daemon.Worker;

public class Utils {

    // récupérer la taille du fragment indiqué dans le fichier de configuration
    public static int recuptaille(String path) {
        File file = new File(path);
        int cpt = 0;
        BufferedReader br;
        int Taillefr = 0;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 3) {
                        Taillefr = Integer.parseInt(st);
                    }
                    cpt++;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Taillefr;

    }

    // récupérer noms des machines indiqués dans le fichier de configuration
    public static String[] recupnom(String path, Integer nbServers) {
        File file = new File(path);
        int cpt = 0;
        int nbMachines = nbServers;
        String[] noms = new String[nbMachines];
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                // si la ligne n'est pas un commentaire
                if (!st.startsWith("#")) {
                    // noms des machines
                    if (cpt == 0) {
                        noms = st.split(",");
                    }
                    cpt++;
                }
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return noms;

    }

    // récupérer les ports indiqués dans le fichier de configuration
    public static int[] recupport(String path, Integer nbServers) {
        File file = new File(path);
        int cpt = 0;
        int nbMachines = nbServers;
        String[] ports = new String[nbMachines];
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 1) {
                        ports = st.split(",");
                    }
                    cpt++;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int[] intports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            intports[i] = Integer.parseInt(ports[i]);
        }
        return intports;

    }

    public static int[] recuprmi(String path, Integer nbServers) {
        File file = new File(path);
        int cpt = 0;
        int nbMachines = nbServers;
        String[] ports = new String[nbMachines];
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 2) {
                        ports = st.split(",");
                    }
                    cpt++;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int[] intports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            intports[i] = Integer.parseInt(ports[i]);
        }
        return intports;

    }

    public static int recupnbmachines(String path) {
        File file = new File(path);
        int cpt = 0;
        int nbMachines = 0;
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 0) {
                        nbMachines = st.split(",").length;
                    }
                    cpt++;
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return nbMachines;
    }

    public static String[] recupURLWorker(String _path, int _nbMachines){
		int[] ports = new int[_nbMachines];
		String[] noms = new String[_nbMachines];
		String[] urls = new String[_nbMachines];

    // On récupère le ports et le noms avec la classe utils
    noms = recupnom(_path, _nbMachines);
    ports = recuprmi(_path, _nbMachines);

    // On créer les urls pour récupérer les instance de worker
    // On vérifie que l'on a assez d'infos
			if (noms.length != 0 && ports.length == noms.length) {
				for (int i=0 ; i < _nbMachines ; i++) {
					urls[i] = "//" + noms[i] + ":" + ports[i] + "/Worker";
				}
			} else {
				System.err.println("Problème avec la création des urls dans HagidoopClient");
			}
    return urls;
  }

  public static Worker[] recupWorker(String _path) {
    int nbMachines = config.Utils.recupnbmachines(_path);
    Worker[] listWorker = new Worker[nbMachines];
    String[] urlWorker = recupURLWorker(_path, nbMachines);
    // récupérer les références des objets Worker sur les machines
    try {
      for (int i = 0 ; i < nbMachines ; i++) {
				System.out.println(urlWorker[i]);
        listWorker[i]=(Worker) Naming.lookup(urlWorker[i]);
        System.out.println("Worker " + i + " récupéré");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return listWorker;
  }

}
