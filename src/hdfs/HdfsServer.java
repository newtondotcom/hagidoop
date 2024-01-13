package hdfs;

import impl.ImplFileRW;
import interfaces.FileReaderWriter;
import interfaces.KV;

import java.io.*;
import java .net.*;
import java.util.Arrays;

public class HdfsServer {

	public static void main(String[] args) {

        // Format de la commande côté client :
		// java HdfsClient write <txt|kv> <file> ou java HdfsClient <read | delete> <file>

        try {
            // Récupération du port
            int port = Integer.parseInt(args[0]);

            // Ouverture du socket côté serveur
            ServerSocket waitingsocket = new ServerSocket(port);

            // Boucle infinie pour accepter les connexions des clients
            Boolean running = true; // variable pour lancer le script sinon erreur Java
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
                        ImplFileRW fileRW = new ImplFileRW(0, "/tmp/data/"+req[1], "w", FileReaderWriter.FMT_KV);
                        fileRW.write(req[2]);
                        break;
                    
                    case ":READ" :
                        ImplFileRW fileRW2 = new ImplFileRW(0, "/tmp/data/"+req[1], "r", FileReaderWriter.FMT_KV);
                        KV d = fileRW2.read();
                        StringBuilder fragment = new StringBuilder();
                        while (d != null) {
                            fragment.append(d).append("\n");
                            d = fileRW2.read();
                        }
                        ObjectOutputStream objectos = new ObjectOutputStream(socket.getOutputStream());
                        objectos.writeObject(fragment);
                        fileRW2.close();
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
