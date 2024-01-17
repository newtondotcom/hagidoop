package hdfs;

import config.Project;
import impl.ImplFileRW;
import interfaces.KV;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static config.Utils.*;
import static interfaces.FileReaderWriter.FMT_KV;
import static interfaces.FileReaderWriter.FMT_TXT;

public class HdfsClient {

    private static int[] numPorts;
    private static String[] nomMachines;
    private static int nbServers;
    public static String path = Project.config;
    private static Integer taille_fragment = recupTaille(path);
    private static final KV cst = new KV("hi","hello");
    private static final String SOURCE_INPUT = "src/io/in/";
    private static final String SOURCE_OUTPUT = "src/io/out/";
    private static PersistentStorage storage;

    public static void main(String[] args) {
        try {
            // On vérifie  le nombnre d'arguments
            if (args.length<2) {
                System.err.println("Pas assez d'arguments");
                return;
            }
            storage = new PersistentStorage();
            nbServers = recupNbMachines(path);
            numPorts = recupPorts(path,nbServers);
            nomMachines = recupNom(path,nbServers);
            int fmt = 0;
            if (args[0].equals("read")){
                try {
                    String extension = args[2].split("\\.")[1];
                    if (!args[1].equals(extension)) {
                        System.err.println("Les arguments de sont pas les bons");
                        return;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
                HdfsRead(args[2]);
            } else if (args[0].equals("delete")){
                HdfsDelete(args[1]);
            } else if (args[0].equals("write")){
                try {
                    fmt = args[1].equals("kv") ? FMT_KV : FMT_TXT;
                    String extension = args[2].split("\\.")[1];
                    if (!args[1].equals(extension)) {
                        System.err.println("Les arguments de sont pas bon");
                        return;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
              HdfsWrite(fmt,args[2]);
          }	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
    public static void HdfsDelete(String fname) {
        try{
        	int j ;
            int nbfragments = storage.getNbFragments(fname);
            for (int i = 0; i < nbfragments; i++) {
            	j = i % nbServers;
                Socket sock = new Socket (nomMachines[j], numPorts[j]);
                String[] inter = fname.split("\\.");
                String nom = inter[0];
                String extension = inter[1];
                ObjectOutputStream objectOS = new ObjectOutputStream(sock.getOutputStream());
                objectOS.writeObject(Project.CommandPrefix + "#" + nom + "_" + Integer.toString(i) + "." + extension);
                objectOS.close();
                sock.close();
            }
        	storage.removeFragment(fname); 
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	public static void HdfsWrite(int fmt, String fname) {
         try {
            ImplFileRW fichierLocal = new ImplFileRW((long) 0, SOURCE_INPUT+fname, fmt);
            fichierLocal.open("r");
            int i = 0;

            // Boucle pour envoyer les fragments aux différents workers, avec possibilité de plusieurs fragments par worker
            while (true) {
                int index = 0;
                KV buffer = cst;
                String buffertxt = "";
                StringBuilder fragment = new StringBuilder();
                // Parcours du fichier pour créer le fragment
                while (index < taille_fragment) {
                    if (fmt == FMT_KV) {
                        buffer = fichierLocal.read();
                        if (buffer == null) {
                            break;
                        }
                    fragment.append(buffer.k).append(KV.SEPARATOR).append(buffer.v).append("\n");
                    index = (int)(fichierLocal.getIndex() - i * taille_fragment);
                    } else if (fmt == FMT_TXT) {
                        buffertxt = fichierLocal.readtxt();
                        if (buffertxt == null) {
                            break;
                        }
                        fragment.append(buffertxt).append("\n");
                        index = (int)(fichierLocal.getIndex() - i * taille_fragment);
                    }
                }
                // Si le fragment est vide, on arrête la boucle
                if (fragment.length() == 0) {
                    // Break the loop if the fragment is empty
                    break;
                }

                // Modulo pour qu'un serveur ait plusieurs fragments sur lui et donc pluieurs workers
                int t = i % nbServers;
                System.out.println("Fragment " + i + " sent to " + nomMachines[t] + " on port " + numPorts[t]);
                Socket socket = new Socket(nomMachines[t], numPorts[t]);
                String[] inter = fname.split("\\.");
                String nom = inter[0];
                String extension = (fmt == FMT_KV) ? "kv" : "txt";


                if (fragment.toString().equals("")) {
                    System.out.println("Fragment " + i + " is empty");
                }

                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                String obj = Project.CommandPrefix + "WRITE" + "#" + nom + "_" + i + "." + extension + "#" + fragment.toString();
                objectOS.writeObject(obj);
                objectOS.close();
                socket.close();

                i++;
            }
            storage.addFragment(fname, i);
            fichierLocal.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void HdfsRead(String fname) {
        String[] inter = fname.split("\\.");
        String nom = inter[0];
        String extension = inter[1];
        int nbfragments = storage.getNbFragments(fname);
        if (nbfragments == 0) {
            System.out.println("Le fichier n'existe pas");
            nbfragments = 1;
        }
        int format = 0;
        if (extension.equals("kv")) {
            format = FMT_KV;
        }
        ImplFileRW fileLocal = new ImplFileRW((long) 0, SOURCE_OUTPUT+fname,format);
        fileLocal.open("w");
        try {
            int j;
            System.out.println("Le fichier " + fname + " contient " + String.valueOf(nbfragments)+" fragment(s)");
            for (int i = 0; i < nbfragments; i++) {
                j = i % nbfragments;
                Socket socket = new Socket (nomMachines[j], numPorts[j]);
                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                objectOS.writeObject(Project.CommandPrefix + "#" + nom +"_"+ Integer.toString(i) + "." + extension);
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String fragment = (String) objectIS.readObject();
                fileLocal.write(fragment);
                objectIS.close();
                objectOS.close();
                socket.close();
            }
            fileLocal.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
