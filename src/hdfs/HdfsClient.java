package hdfs;

import interfaces.FileReaderWriter.*;
import impl.ImplFileRW.*;

import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.text.Format;
import interfaces.KV;

import static config.Utils.*;
import static interfaces.FileReaderWriter.FMT_KV;
import static interfaces.FileReaderWriter.FMT_TXT;

public class HdfsClient {

    private static int numPorts[];
    private static String nomMachines[];
    private static int nbServers;
    public static String path = "src/config/config_hidoop.cfg";
    private static Integer taille_fragment = recuptaille(path);
    private static KV cst = new KV("hi","hello");
    private static String SOURCE = System.getProperty("user.home")+"/nosave/hidoop_data/";

    private static Storage node;

    public static void main(String[] args) {
        try {
            if (args.length<3) {fctusage(); return;}
            node = new Storage();
            nbServers = Integer.parseInt(args[3]);
            numPorts = recupport(path,nbServers);
            nomMachines = recupnom(path,nbServers);
            switch (args[0]) {
              case "read": HdfsRead(args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                int fmt;
                if (args.length<3) {fctusage(); return;}
                if (args[1].equals("line")) {
                    fmt = FMT_TXT;
                } else if(args[1].equals("kv")) {
                    fmt = FMT_KV;
                } else {fctusage(); return;}
                HdfsWrite(fmt,args[2]);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
    public static void HdfsDelete(String fname) {
        try{
        	int j ;
        	
            // supprimer les fichiers générés des serveurs


        	int nbfragments = node.getNbFragments(fname);	// nbs de fragments du fichier
        	
            for (int i = 0; i < nbfragments; i++) {
            	j = i % nbServers;
                Socket sock = new Socket (nomMachines[j], numPorts[j]);
                String[] inter = fname.split("\\.");
                String nom = inter[0];
                String extension = inter[1];
                ObjectOutputStream objectOS = new ObjectOutputStream(sock.getOutputStream());
                //System.out.println("i"+Integer.toString(i)+"::::j::::"+Integer.toString(j));
                objectOS.writeObject(":DELETE" + "#" + nom + "_" + Integer.toString(i) + "." + extension);
                objectOS.close();
                sock.close();
            }
        	
        	//supprimer les info de fname du fichier node.
        	
            
        	node.removeFragment(fname);					// suppression de l'occurence du fichier dans la node
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	public static void HdfsWrite(int fmt, String fname) {
         try {
            // Calculer la taille du fichier
        	File file = new File(SOURCE+fname);
        	long taille = file.length();
           // System.out.println(String.valueOf(taille));
        	// vérifier que la taille du bloc est un diviseur de la taille totale sinon ajouter 1.

        	int nbfragments = (int) (taille/taille_fragment);
            if (taille%taille_fragment != 0) { nbfragments ++;}
            System.out.println(String.valueOf(nbfragments));

        	// Ajouter le nombre de fragments dans le fichier node.
        	node.addFragment(fname, nbfragments);

            Format fichier = null;

            // Fichier texte ou KV ?
            if (fmt == FMT_TXT) {
                fichier = new Formats.LineFormat(SOURCE + fname);
            }
            else if (fmt == FMT_KV) {
                fichier = new Formats.KVFormat(fname);
            }

            if (fichier != null) {

                if (fmt == FMT_TXT){
                    ((Formats.LineFormat) fichier).open("R");
                } else {
                    ((Formats.KVFormat) fichier).open("R");
                }

                for (int i = 0; i < nbfragments; i++) {
                    int index = 0;
                    KV buffer = cst;
                    StringBuilder fragment = new StringBuilder();

                    // Process the fragment content
                    while (index < taille_fragment) {
                        if (fmt == FMT_TXT) {
                            buffer = ((Formats.LineFormat) fichier).read();
                        } else {
                            buffer = ((Formats.KVFormat) fichier).read();
                        }
                        if (buffer == null) {
                            break;
                        }
                        fragment.append(buffer.v).append("\n");
                        if (fmt == FMT_TXT) {
                            index = (int)((Formats.LineFormat) fichier).getIndex() - i * taille_fragment;
                        } else {
                            index = (int)((Formats.KVFormat) fichier).getIndex() - i * taille_fragment;
                        }
                    }

                    int t = i % nbServers;
                    Socket socket = new Socket(nomMachines[t], numPorts[t]);

                    String[] inter = fname.split("\\.");
                    String nom = inter[0];
                    String extension = inter[1];

                    ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                    objectOS.writeObject(":WRITE" + "#" + nom + "_" + i + "." + extension + "#" + fragment.toString());
                    objectOS.close();
                    socket.close();

                    System.out.println("fragment machine " + i);
                }

                if (fmt == FMT_TXT) {
                    ((Formats.LineFormat) fichier).close();
                } else {
                    ((Formats.KVFormat) fichier).close();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

	public static void HdfsRead(String fname) {
        //String fname, String fname, int nb

        String[] inter = fname.split("\\.");
        String nom = inter[0];
        String extension = inter[1];
        int nb = node.getNbFragments(fname);
        File file = new File(fname); // le format de ce dernier soit data/filesample-red.txt
        try {
        	int j;
		System.out.println("hello this is the file ~/Téléchargements/Hidoopgit/"+fname);
            FileWriter fWrite = new FileWriter(file);
            int nbfragments = node.getNbFragments(fname);
            for (int i = 0; i < nbfragments; i++) {
                //System.out.println(Integer.toString(i));
                j = i % nb;
                //System.out.println(Integer.toString(j));
                Socket socket = new Socket (nomMachines[j], numPorts[j]);
                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                objectOS.writeObject(":READ" + "#" + nom +"_"+ Integer.toString(i) + "-res" + "." + extension);
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String fragment = (String) objectIS.readObject();
                fWrite.write(fragment,0,fragment.length());
                objectIS.close();
                objectOS.close();
                socket.close();
            }
            fWrite.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	private static void fctusage() {
		System.out.println("Usage: java HdfsClient read <file|file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
	}
}
