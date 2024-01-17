package hdfs;

import config.Project;
import impl.ImplFileRW;
import interfaces.FileReaderWriter;
import interfaces.KV;

import java.io.*;
import java .net.*;
import java.util.Arrays;
import java.util.Objects;

public class HdfsServer {

	public static void main(String[] args) {
        try {
            // Port avec ligne de commande
            int port = Integer.parseInt(args[0]);

            // Ouverture du socket côté serveur
            ServerSocket waitingsocket = new ServerSocket(port);

            // Boucle infinie pour accepter les connexions des clients V
            Boolean running = true; // variable pour lancer le script sinon erreur Java
            while (running) {
                // Attente d'une connexion d'un client
                Socket socket = waitingsocket.accept();
                ObjectInputStream objectIS = new ObjectInputStream(socket.getInputStream());

                // Récupération du message envoyé par le client
                String msg = (String) objectIS.readObject();
                String[] req = msg.split("#");
                String commande = req[0];
                String extension = req[1].split("\\.")[1];
                int type = 0;
                if (extension.equals("kv")) {
                type = FileReaderWriter.FMT_KV;
                }

                // Commandes
                if(commande.equals(Project.CommandPrefix +"WRITE")){
                    // Ecriture du fichier
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
                } else if (commande.equals(Project.CommandPrefix+"READ")){
                    // Lecture du fichier
                    ImplFileRW fileRW2 = new ImplFileRW(0, "/tmp/data/"+req[1], type);
                        fileRW2.open("r");
                        StringBuilder fragment = new StringBuilder();
                        String d = "";
                        while (true) {
                            d = String.valueOf(fileRW2.readtxt());
                            if (!Objects.equals(d, "null") && d != null) {
                                fragment.append(d).append("\n");
                            } else {
                                break;
                            }
                        }
                        ObjectOutputStream objectos = new ObjectOutputStream(socket.getOutputStream());
                        objectos.writeObject(fragment.toString());
                        fileRW2.close();
                        objectos.close();
                } else if (commande.equals(Project.CommandPrefix+"DELETE")){
                    // Suppression du fichier
                    File file = new File("/tmp/data/"+req[1]);
                    boolean deleted = file.delete();
                } else {
                    System.out.println("ATTENTION Commande inconnue");
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
