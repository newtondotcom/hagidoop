package hdfs;

import java.io.*;
import java .net.*;

public class HdfsServer {

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {

        // Format de la commande côté client :
		// java HdfsClient write <txt|kv> <file> ou java HdfsClient <read | delete> <file>

        try {
            // Récupération du port
            int port = Integer.parseInt(args[0]);

            // Ouverture du socket côté serveur
            ServerSocket waitingsocket = new ServerSocket(port);

            // Boucle infinie pour accepter les connexions des clients
            Boolean running = true; // variable pour lancer le script
            while (running) {
                Socket socket = waitingsocket.accept();
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());
                String msg = (String) objectIS.readObject();
                String[] req = msg.split("#");
                String commande = req[0];

                // Switch sur la commande
                switch (commande) {
                    
                    case ":WRITE" :
                        File folder = new File("/tmp/data/");
                        Boolean exists = folder.mkdir();
                        File wrtfile = new File("/tmp/data/"+req[1]);
                        FileWriter fwrt = new FileWriter(wrtfile);
                        BufferedWriter bffwrt = new BufferedWriter(fwrt);
                        bffwrt.write(req[2], 0, req[2].length());
                        bffwrt.close();
                        fwrt.close();
                        break;
                    
                    case ":READ" :
                        File rdfile = new File("/tmp/data/"+ req[1]);
                        BufferedReader bffrd = new BufferedReader(new FileReader(rdfile));
                        String fragment = "";
                        String d = bffrd.readLine();
                        while (d != null) { // tant qu'il y a des lignes à lire
                            fragment = fragment + d + "\n";
                            d = bffrd.readLine();
                        }
                        ObjectOutputStream objectos = new ObjectOutputStream(socket.getOutputStream());
                        objectos.writeObject(fragment);
                        bffrd.close();
                        objectos.close();
                        break;

                    case ":DELETE" :
                        File file = new File("/tmp/data/"+req[1]);
                        Boolean deleted = file.delete();
                        break;
                }
                objectIS.close();
                socket.close();
            }
            waitingsocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
