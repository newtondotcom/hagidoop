package hdfs;

import impl.ImplFileRW;
import interfaces.FileReaderWriter;
import interfaces.KV;

import java.io.*;
import java .net.*;
import java.util.Arrays;
import java.util.Objects;

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
                String extension = req[1].split("\\.")[1];
                int type = 0;
                if (extension.equals("kv")) type = FileReaderWriter.FMT_KV;

                // Switch sur la commande
                switch (commande) {
                    
                    case ":WRITE" :
                        File folder = new File("/tmp/data/");
                        Boolean exists = folder.mkdir();
                        ImplFileRW fileRW = new ImplFileRW(0, "/tmp/data/"+req[1], type);
                        fileRW.open("w");
                        String content;
                        if (req.length > 2){
                            content = req[2];
                        } else {
                            content = "";
                        }
                        fileRW.write(content.trim());
                        fileRW.close();
                        System.out.println("OPERATION WRITE FINISHED on file "+req[1]);
                        break;
                    
                    case ":READ" :
                        ImplFileRW fileRW2 = new ImplFileRW(0, "/tmp/data/"+req[1], type);
                        fileRW2.open("r");
                        StringBuilder fragment = new StringBuilder();
                        String d = "";
                        while (true) {
                            d = String.valueOf(fileRW2.readtxt());
                            if (!Objects.equals(d, "null") && d != null) {
                                fragment.append(d).append("\n");
                                System.out.println(d);
                            } else {
                                break;
                            }
                        }
                        ObjectOutputStream objectos = new ObjectOutputStream(socket.getOutputStream());
                        objectos.writeObject(fragment.toString());
                        fileRW2.close();
                        objectos.close();

                        System.out.println("OPERATION READ FINISHED on file "+req[1]);
                        break;

                    case ":DELETE" :
                        File file = new File("/tmp/data/"+req[1]);
                        boolean deleted = file.delete();
                        System.out.println("OPERATION DELETE FINISHED on file "+req[1]+" : "+deleted);
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
