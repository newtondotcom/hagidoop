package impl;

import interfaces.KV;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import interfaces.FileReaderWriter;

public class ImplFileRW implements FileReaderWriter{

  private long index;
  private transient BufferedReader br;
  private transient BufferedWriter bw;
  // Nom du fichier sur HDFS 
  private String fName;

  public ImplFileRW(long _index, String _fName, String _mode){
    this.index = _index;
    this.fName = _fName;
    open(_mode);
  }

  public void open(String _mode){
    try{
        if (_mode.equals("r")){
          this.br = new BufferedReader(new FileReader(this.fName));
        } else {
          this.bw = new BufferedWriter(new FileWriter(this.fName));
        }
          System.out.println("Mode invalide");
    } catch (Exception e){
          e.printStackTrace();
    }
  }
  public void close(){
    try{
      if (this.br != null){
        this.br.close();
      } else {
        this.bw.close();
      }
    } catch (Exception e){
      e.printStackTrace();
    }
  }
  public void write(KV record) {
    try {
      String s = record.k+KV.SEPARATOR+record.v;
      bw.write(s, 0, s.length());
      bw.newLine();
      bw.flush();
      index += s.length();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public KV read() {
    KV kv = new KV();
    try {
      while (true) {
        String l = br.readLine();
        if (l == null) return null;
        index += l.length();
        String[] tokens = l.split(KV.SEPARATOR);
        if (tokens.length != 2) continue;
        kv.k = tokens[0];
        kv.v = tokens[1];
        return kv;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  
  public long getIndex(){
    return this.index;
  }
  public void setFname(String _fname){
    this.fName = _fname;
  }
	public String getFname(){
    return this.fName;
  }
}