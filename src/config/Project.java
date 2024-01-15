package config;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Project {
    public static String PATH = "/home/raugerea2/Téléchargements/Hagidoop/";
    public static String TEMP_PATH = "/tmp/";

    public static String config = "main_n7.cfg";

    public static void main(String[] args) {
        Map<String, Integer> hash = new HashMap<>();
        try {
            FileOutputStream fichier = new FileOutputStream(config);
            ObjectOutputStream objet = new ObjectOutputStream(fichier);
            objet.writeObject(hash);
            objet.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties loadProperties(String _namenode) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(_namenode)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
