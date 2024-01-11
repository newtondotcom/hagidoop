import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PersistentStorage {

    private static final String FILE_NAME = "dataMap.ser";

    public PersistentStorage() {
        Map<String, Integer> dataMap = new HashMap<>();
        serializeData(dataMap);
    }

    private void serializeData(Map<String, Integer> dataMap) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(FILE_NAME))) {
            oos.writeObject(dataMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Integer> deserializeData() {
        Map<String, Integer> restoredMap = new HashMap<>();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(FILE_NAME))) {
            restoredMap = (Map<String, Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return restoredMap;
    }

    public void addData(String key, Integer value) {
        Map<String, Integer> existingData = deserializeData();
        existingData.put(key, value);
        serializeData(existingData);
    }
    
}
