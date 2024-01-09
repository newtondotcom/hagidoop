package config;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Project {
    public static String PATH = "/home/raugerea2/Téléchargements/Hagidoop/";
    public static String TEMP_PATH = "/tmp/";

    private static String nameNode = "main.cfg";

    public static void main(String[] args) {
        Map<String, Integer> hash = new HashMap<>();
        try {
            FileOutputStream fichier = new FileOutputStream(nameNode);
            ObjectOutputStream objet = new ObjectOutputStream(fichier);
            objet.writeObject(hash);
            objet.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
