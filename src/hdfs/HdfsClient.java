package hdfs;

import impl.ImplFileRW;
import interfaces.KV;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static config.Utils.*;
import static interfaces.FileReaderWriter.FMT_KV;
import static interfaces.FileReaderWriter.FMT_TXT;

public class HdfsClient {

    private static int numPorts[];
    private static String nomMachines[];
    private static int nbServers;
    public static String path = "src/config/config_hidoop.cfg";
    private static final Integer taille_fragment = recuptaille(path);
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
        	node.removeFragment(fname);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

	public static void HdfsWrite(int fmt, String fname) {
         try {
             ImplFileRW fichierLocal = new ImplFileRW((long) 0, SOURCE+fname, "r", fmt);
        	long taille = fichierLocal.getFileLength();

        	int nbfragments = (int) (taille/taille_fragment);
            if (taille%taille_fragment != 0) { nbfragments ++;}
            System.out.println(String.valueOf(nbfragments));

        	// Ajouter le nombre de fragments dans le fichier node.
        	node.addFragment(fname, nbfragments);


                for (int i = 0; i < nbfragments; i++) {

                    int index = 0;
                    KV buffer = cst;
                    StringBuilder fragment = new StringBuilder();

                    // Process the fragment content
                    while (index < taille_fragment) {
                        buffer = fichierLocal.read();
                        if (buffer == null) {
                            break;
                        }
                        fragment.append(buffer.v).append("\n");
                        index = (int)(fichierLocal.getIndex() - i * taille_fragment);
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
             fichierLocal.close();
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
        int format ;
        if (extension.equals("txt")) {
            format = FMT_TXT;
        } else {
            format = FMT_KV;
        }
        ImplFileRW fileLocal = new ImplFileRW((long) 0, System.getProperty("user.home")+"/Téléchargements/Hagidoop/dl/"+fname, "w",format);
        try {
            int j;
            System.out.println("hello this is the file ~/Téléchargements/Hidoopgit/"+fname);
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

	private static void fctusage() {
		System.out.println("Usage: java HdfsClient read <file|file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
	}
}
