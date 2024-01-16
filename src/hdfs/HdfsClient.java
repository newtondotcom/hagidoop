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
    private static Integer taille_fragment = recuptaille(path);
    private static final KV cst = new KV("hi","hello");
    private static final String SOURCE_INPUT = "src/io/in/";
    private static final String SOURCE_OUTPUT = "src/io/out/";
    private static PersistentStorage node;

    public static void main(String[] args) {
        try {
            if (args.length<2) {fctusage(); return;}
            node = new PersistentStorage();
            node.ListFragments();
            nbServers = recupnbmachines(path);
            numPorts = recupport(path,nbServers);
            nomMachines = recupnom(path,nbServers);
            int fmt = 0;
            switch (args[0]) {
              case "read":
                  try {
                      String extension = args[2].split("\\.")[1];
                      if (!args[1].equals(extension)) {fctusage(); return;}
                  } catch (Exception e) {
                      fctusage();
                      return;
                  }
                  HdfsRead(args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write":
                  try {
                      fmt = args[1].equals("kv") ? FMT_KV : FMT_TXT;
                      String extension = args[2].split("\\.")[1];
                      if (!args[1].equals(extension)) {fctusage(); return;}
                  } catch (Exception e) {
                      fctusage();
                      return;
                  }
                HdfsWrite(fmt,args[2]);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
    public static void HdfsDelete(String fname) {
        try{
        	int j ;
            int nbfragments = node.getNbFragments(fname);
            if (nbfragments == 0) {
                System.out.println("Le fichier n'existe pas");
                nbfragments = 1;
            }
        	
            for (int i = 0; i < nbfragments; i++) {
            	j = i % nbServers;
                Socket sock = new Socket (nomMachines[j], numPorts[j]);
                String[] inter = fname.split("\\.");
                String nom = inter[0];
                String extension = inter[1];
                ObjectOutputStream objectOS = new ObjectOutputStream(sock.getOutputStream());
                objectOS.writeObject(":DELETE" + "#" + nom + "_" + Integer.toString(i) + "." + extension);
                objectOS.close();
                sock.close();
            }
        	
        	//supprimer les info de fname du fichier node.
        	node.removeFragment(fname);

            System.out.println("OPERATION DELETE FINISHED on file "+fname);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	public static void HdfsWrite(int fmt, String fname) {
         try {
            ImplFileRW fichierLocal = new ImplFileRW((long) 0, SOURCE_INPUT+fname, fmt);
            fichierLocal.open("r");
             int i = 0;
             while (true) {
                 int index = 0;
                 KV buffer = cst;
                 String buffertxt = "";
                 StringBuilder fragment = new StringBuilder();

                 // Process the fragment content
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

                 if (fragment.length() == 0) {
                     // Break the loop if the fragment is empty
                     break;
                 }

                 int t = i % nbServers;
                 System.out.println("Fragment " + i + " sent to " + nomMachines[t] + " on port " + numPorts[t]);
                 Socket socket = new Socket(nomMachines[t], numPorts[t]);
                 String[] inter = fname.split("\\.");
                 String nom = inter[0];
                 String extension = (fmt == FMT_KV) ? "kv" : "txt";

                 ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                 if (fragment.toString().equals("")) {
                     System.out.println("Fragment " + i + " is empty");
                 }
                 String obj = ":WRITE" + "#" + nom + "_" + i + "." + extension + "#" + fragment.toString();
                 objectOS.writeObject(obj);
                 objectOS.close();
                 socket.close();

                 i++;
             }
             node.addFragment(fname, i);
             fichierLocal.close();

                System.out.println("OPERATION WRITE FINISHED on file "+fname);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void HdfsRead(String fname) {
        String[] inter = fname.split("\\.");
        String nom = inter[0];
        String extension = inter[1];
        int nbfragments = node.getNbFragments(fname);
        if (nbfragments == 0) {
            System.out.println("Le fichier n'existe pas");
            nbfragments = 1;
        }
        int format ;
        if (extension.equals("txt")) {
            format = FMT_TXT;
        } else {
            format = FMT_KV;
        }
        ImplFileRW fileLocal = new ImplFileRW((long) 0, SOURCE_OUTPUT+fname,format);
        fileLocal.open("w");
        try {
            int j;
            System.out.println("hello this is the file "+fname);
            System.out.println(String.valueOf(nbfragments)+" fragment(s)");
            for (int i = 0; i < nbfragments; i++) {
                j = i % nbfragments;
                Socket socket = new Socket (nomMachines[j], numPorts[j]);
                ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                objectOS.writeObject(":READ" + "#" + nom +"_"+ Integer.toString(i) + "." + extension);
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String fragment = (String) objectIS.readObject();
                fileLocal.write(fragment);
                objectIS.close();
                objectOS.close();
                socket.close();
            }
            fileLocal.close();

            System.out.println("OPERATION READ FINISHED on file "+fname);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	private static void fctusage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
        System.out.println("Note : you must write a kv file into a kv format and a txt file into a txt format");
	}
}
