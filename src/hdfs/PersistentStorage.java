package hdfs;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PersistentStorage {
    public static String PATH = "C:\\Users\\helen\\Documents\\Github\\hagidoop\\";
    private static final String FILE_NAME = "dataMap.ser";

    private Map<String, Integer> fragments;

    public PersistentStorage() {
        fragments = deserializeData();
    }

    private void serializeData(Map<String, Integer> dataMap) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(PATH + FILE_NAME))) {
            oos.writeObject(dataMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Integer> deserializeData() {
        Map<String, Integer> restoredMap = new HashMap<>();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(PATH + FILE_NAME))) {
            restoredMap = (Map<String, Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return restoredMap;
    }

    public void addFragment(String filename, int number) {
        fragments.put(filename, number);
        serializeData(fragments);
    }

    public void removeFragment(String filename) {
        fragments.remove(filename);
        serializeData(fragments);
    }

    public int getNbFragments(String filename) {
        return fragments.getOrDefault(filename, 0);
    }

    public void ListFragments() {
        System.out.println("Liste des fragments :");
        for (Map.Entry<String, Integer> entry : fragments.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("Fin de la liste des fragments");
    }
}
