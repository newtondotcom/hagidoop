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

    public static int recupTaille(String path) {
        int cpt = 0;
        int taille = 0;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        reader.open("r");
        String st;
        while ((st = reader.readtxt()) != null) {
            if (cpt == 3) {
                taille = Integer.parseInt(st);
            }
            cpt++;
        }
        reader.close();
        return taille;

    }

    public static String[] recupNom(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        reader.open("r");
        String[] noms = new String[nbMachines];
        String st;
        while ((st = reader.readtxt()) != null) {
            if (cpt == 0) {
                noms = st.split(",");
            }
            cpt++;
        }
        reader.close();

        return noms;

    }

    public static int[] recupPorts(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        reader.open("r");
        String[] ports = new String[nbMachines];
        String st;
        while ((st = reader.readtxt()) != null) {
            if (cpt == 1) {
                ports = st.split(",");
            }
            cpt++;
        }
        reader.close();

        int[] tabports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            tabports[i] = Integer.parseInt(ports[i]);
        }
        return tabports;

    }

    public static int[] recupRMI(String path, Integer nbServers) {
        int cpt = 0;
        int nbMachines = nbServers;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        reader.open("r");
        String[] ports = new String[nbMachines];
        BufferedReader br;
        String st;
        while ((st = reader.readtxt()) != null) {
            if (cpt == 2) {
                ports = st.split(",");
            }
            cpt++;
        }
        reader.close();

        int[] tabports = new int [nbMachines];
        for (int i=0; i<nbMachines; i++){
            tabports[i] = Integer.parseInt(ports[i]);
        }
        return tabports;

    }

    public static int recupNbMachines(String path) {
        int cpt = 0;
        int nbMachines = 0;
        ImplFileRW reader = new ImplFileRW(0, path, FileReaderWriter.FMT_TXT);
        reader.open("r");
        String st;
        while ((st = reader.readtxt()) != null) {
            if (cpt == 0) {
                nbMachines = st.split(",").length;
            }
            cpt++;
        }
        reader.close();
        return nbMachines;
    }

    public static String[] recupURLWorker(String _path, int _nbMachines){
		int[] ports = new int[_nbMachines];
		String[] noms = new String[_nbMachines];
		String[] urls = new String[_nbMachines];

        noms = recupNom(_path, _nbMachines);
        ports = recupRMI(_path, _nbMachines);

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
    int nbMachines = config.Utils.recupNbMachines(_path);
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
