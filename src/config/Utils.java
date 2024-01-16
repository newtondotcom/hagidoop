package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;

import daemon.Worker;
import impl.ImplFileRW;
import interfaces.FileReaderWriter;

public class Utils {

    public static int recuptaille(String path) {
        int cpt = 0;
        int Taillefr = 0;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        String st;
        while ((st = reader.readtxt()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 3) {
                        Taillefr = Integer.parseInt(st);
                    }
                    cpt++;
                }
        }
        reader.close();
        return Taillefr;

    }

    public static String[] recupnom(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        String[] noms = new String[nbMachines];
        String st;
        while ((st = reader.readtxt()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 0) {
                        noms = st.split(",");
                    }
                    cpt++;
                }
        }
        reader.close();

        return noms;

    }

    public static int[] recupport(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        String[] ports = new String[nbMachines];
        String st;
        while ((st = reader.readtxt()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 1) {
                        ports = st.split(",");
                    }
                    cpt++;
                }
        }
        reader.close();

        int[] intports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            intports[i] = Integer.parseInt(ports[i]);
        }
        return intports;

    }

    public static int[] recuprmi(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        String[] ports = new String[nbMachines];
        BufferedReader br;
        String st;
        while ((st = reader.readtxt()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 2) {
                        ports = st.split(",");
                    }
                    cpt++;
                }
        }
        reader.close();

        int[] intports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            intports[i] = Integer.parseInt(ports[i]);
        }
        return intports;

    }

    public static int recupnbmachines(String path) {
        int cpt = 0;
        int nbMachines = 0;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        String st;
        while ((st = reader.readtxt()) != null) {
                if (!st.startsWith("#")) {
                    if (cpt == 0) {
                        nbMachines = st.split(",").length;
                    }
                    cpt++;
                }
        }
        reader.close();
        return nbMachines;
    }

    public static String[] recupURLWorker(String _path, int _nbMachines){
		int[] ports = new int[_nbMachines];
		String[] noms = new String[_nbMachines];
		String[] urls = new String[_nbMachines];

        noms = recupnom(_path, _nbMachines);
        ports = recuprmi(_path, _nbMachines);

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
