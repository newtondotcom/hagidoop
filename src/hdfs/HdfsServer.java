package hdfs;

import java.io.*;
import java .net.*;

public class HdfsServer {

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {

        // Format de la commande côté client :
		// java HdfsClient <read|write> <txt|kv> <file>

        try {
            int port = Integer.parseInt(args[0]);

            // Ouverture Socket Serveur

            ServerSocket sSocket = new ServerSocket(port);
            
            boolean c = true;
            while (c) {
                Socket socket = sSocket.accept();
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String msg = (String) objectIS.readObject();
                String[] req = msg.split("#");
                String commande = req[0];

                // Switch sur la commande
                switch (commande) {
                    case ":DELETE" :
                        
                        File file = new File("/tmp/data/"+req[1]);
                        file.delete();
                        break;
                    
                    case ":WRITE" :
                        
                        //System.out.println("début write");
                        //System.out.println(""+req[1]);
                        //System.out.println(req[1]);
                        //File wFile = new File(System.getProperty("user.home")+"/Téléchargements/Hidoop-master/"+req[1]);
                        File wFile = new File("/tmp/data/");
                        wFile.mkdir();
                        
                        wFile = new File("/tmp/data/"+req[1]);
                        //System.out.println("création fichier");
                        /*try{
                        if (wFile.createNewFile()) {
                            System.out.println("fichier créé");
                        } else {
                            System.out.println("fichier  existe déjà");
                        }
                        } catch(IOException e) {
                            e.printStackTrace();
                        }*/
                        FileWriter fWriter = new FileWriter(wFile);
                        BufferedWriter writr = new BufferedWriter(fWriter);
                        writr.write(req[2], 0, req[2].length());
                        //fWriter.write(req[2], 0, req[2].length());
                        writr.close();
                        fWriter.close();

                        break;
                    
                    case ":READ" :
                        File rFile = new File("/tmp/data/"+ req[1]);
                        BufferedReader bufReader = new BufferedReader(new FileReader(rFile));
                        String fragment = "";
                        String d = bufReader.readLine();
                        while (d != null) {
                            fragment = fragment + d + "\n";
                            d = bufReader.readLine();
                        }
                        ObjectOutputStream objectOS = new ObjectOutputStream(socket.getOutputStream());
                        objectOS.writeObject(fragment);
                        bufReader.close();
                        objectOS.close();
                        break;
                }
                objectIS.close();
                socket.close();
            }
            sSocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
