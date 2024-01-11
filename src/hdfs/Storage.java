package hdfs;

import java.util.HashMap;
import java.util.Map;

public class Storage {
    Map<String,Integer> fragments;

    public Storage() {
        fragments = new HashMap<String,Integer>();
    }

    public void addFragment(String filename, int number) {
        fragments.put(filename, number);
    }

    public void removeFragment(String filename) {
        fragments.remove(filename);
    }

    public int getNbFragments(String filename) {
        return fragments.get(filename);
    }


}
