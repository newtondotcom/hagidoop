package application;

import java.util.HashMap;

import daemon.JobLauncher;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.MapReduce;
import interfaces.Reader;
import interfaces.Writer;

public class MyMapReduce implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that compute word counts
	public void map(Reader reader, Writer writer) {

		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		KV kv;
		while ((kv = reader.read()) != null) {
			String tokens[] = kv.v.split(" ");
			for (String tok : tokens) {
				if (hm.containsKey(tok)) hm.put(tok, hm.get(tok)+1);
				else hm.put(tok, 1);
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}

	public void reduce(Reader reader, Writer writer) {
		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}

	public static void main(String args[]) {
		long t1 = System.currentTimeMillis();
		JobLauncher.startJob(new MyMapReduce(), FileReaderWriter.FMT_TXT, args[0]);
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms ="+(t2-t1));
		System.exit(0);
	}
}