import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PersistentStorage {

    public static void main(String[] args) {
        Map<String, Integer> dataMap = new HashMap<>();
        dataMap.put("One", 1);
        dataMap.put("Two", 2);
        dataMap.put("Three", 3);

        // Sérialisation
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("dataMap.ser"))) {
            oos.writeObject(dataMap);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Désérialisation
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream("dataMap.ser"))) {
            @SuppressWarnings("unchecked")
            Map<String, Integer> restoredMap = (Map<String, Integer>) ois.readObject();

            System.out.println(restoredMap);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
