package hdfs;

import interfaces.FileReaderWriter.*;
import impl.ImplFileRW.*;

public class HdfsClient {

    private static int numPorts[];
    private static String nomMachines[];
    private static int nbServers;
    private static long taille_fragment = recuptaille();
    private static KV cst = new KV("hi","hello");
    private static String SOURCE = System.getProperty("user.home")+"/nosave/hidoop_data/";

    public static void main(String[] args) {
        try {
            if (args.length<3) {fctusage(); return;}
	    nbServers = Integer.parseInt(args[3]);
	    numPorts = recupport();
	    nomMachines = recupnom();
            switch (args[0]) {
              case "read": HdfsRead(args[1],args[2],4); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {fctusage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {fctusage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
    public static void HdfsDelete(String fname) {
        try{
        	int j ;
        	
            // supprimer les fichiers générés des serveurs
        	
        	Namenode node = new Namenode();
        	int nbfragments = node.getNbFragments(hdfsFname);	// nb de fragments du fichier
        	
            for (int i = 0; i < nbfragments; i++) {
            	j = i % nbServers;
                Socket sock = new Socket (nomMachines[j], numPorts[j]);
                String[] inter = hdfsFname.split("\\.");
                String nom = inter[0];
                String extension = inter[1];
                ObjectOutputStream objectOS = new ObjectOutputStream(sock.getOutputStream());
                //System.out.println("i"+Integer.toString(i)+"::::j::::"+Integer.toString(j));
                objectOS.writeObject(":DELETE" + "#" + nom + "_" + Integer.toString(i) + "." + extension);
                objectOS.close();
                sock.close();
            }
        	
        	//supprimer les info de hdfsFname du fichier node.
        	
            
        	node.Enlever(hdfsFname);					// suppression de l'occurence du fichier dans la node
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	public static void HdfsWrite(int fmt, String fname) {
         try {
            
            // créer les constantes du problème.
            
            String fragment = "";
            int index ;
            KV buffer = new KV();
            
            // Calculer la taille du fichier
        	File file = new File(SOURCE+localFSSourceFname);
        	long taille = file.length();
           // System.out.println(String.valueOf(taille));
        	// vérifier que la taille du bloc est un diviseur de la taille totale sinon ajouter 1.
        	
        	int nbfragments = (int) (taille/taille_fragment);
            if (taille%taille_fragment != 0) { nbfragments ++;}
            System.out.println(String.valueOf(nbfragments));
            
        	// Ajouter le nombre de fragments dans le fichier node.
        	Namenode node = new Namenode();
        	node.Ajouter(localFSSourceFname, nbfragments);
            
            Format fichier = null; 

            // Fichier texte ou KV ?
            if (fmt == FMT_TXT) {
                fichier = new LineFormat(SOURCE + localFSSourceFname);
            } 
            else if (fmt == FMT_KV) {
                fichier = new KVFormat(localFSSourceFname);
            }

            if (fichier != null) {
                fichier.open(Format.OpenMode.R); 

                for (int i = 0; i < nbfragments; i++) {
                    int index = 0;
                    KV buffer = cst;
                    String fragment = "";

                    // Process the fragment content
                    while (index < taille_fragment) {
                        buffer = fichier.read();
                        if (buffer == null) {
                            break;
                        }
                        fragment += buffer.v + "\n";
                        index = (int) (fichier.getIndex() - i * taille_fragment);
                    }

                    int t = i % nbServers;
                    Socket socket = new Socket(nomMachines[t], numPorts[t]);

                    String[] inter = localFSSourceFname.split("\\.");
                    String nom = inter[0];
                    String extension = inter[1];

                    ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                    objectOS.writeObject(":WRITE" + "#" + nom + "_" + i + "." + extension + "#" + fragment);
                    objectOS.close();
                    socket.close();

                    if (i % 1 == 0) {
                        System.out.println("fragment machine " + i);
                    }
                }

                fichier.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

	public static void HdfsRead(String fname) {    
        String[] inter = hdfsFname.split("\\.");
        String nom = inter[0];
        String extension = inter[1];
        File file = new File(localFSDestFname); // le format de ce dernier soit data/filesample-red.txt
        try {
        	int j;
		System.out.println("hello this is the file ~/Téléchargements/Hidoopgit/"+localFSDestFname);
            FileWriter fWrite = new FileWriter(file);
            Namenode node = new Namenode();
            int nbfragments = node.getNbFragments(hdfsFname);
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
