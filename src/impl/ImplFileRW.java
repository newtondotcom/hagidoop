package impl;

import interfaces.KV;

import java.io.*;

import interfaces.FileReaderWriter;

public class ImplFileRW implements FileReaderWriter{

  private long index;
  private transient BufferedReader bufferedReader;
  private transient BufferedWriter bufferedWriter;

  private KV kv;

  // Nom du fichier sur HDFS
  private String fName;
  private String mode;

  public ImplFileRW(long _index, String _fName, int format){
    this.index = _index;
    this.fName = _fName;
  }

  public void open(String _mode){
    try{
      System.out.println("Ouverture du fichier " + this.fName + " en mode " + _mode);
        this.mode = _mode;
        if (_mode.equals("r")){
          this.bufferedReader = new BufferedReader(new FileReader(this.fName));
        }
        else if (_mode.equals("w")){
          this.bufferedWriter = new BufferedWriter(new FileWriter(this.fName));
        }
        else {
          System.out.println("Mode invalide");
        }
    } catch (Exception e){
          e.printStackTrace();
    }
  }
  public void close(){
    try{
      if (this.bufferedReader != null){
        this.bufferedReader.close();
      } else {
        this.bufferedWriter.close();
      }
    } catch (Exception e){
      e.printStackTrace();
    }
  }
  public void write(KV record) {
    try {
      String s = record.k+KV.SEPARATOR+record.v;
      bufferedWriter.write(s, 0, s.length());
      bufferedWriter.newLine();
      bufferedWriter.flush();
      index += s.length();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void write(String _record) {
    try {
      bufferedWriter.write(_record, 0, _record.length());
      bufferedWriter.newLine();
      bufferedWriter.flush();
      index += _record.length();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public KV read() {
    this.kv = new KV("", "");
    try {
      kv.k = String.valueOf(0);
      kv.v = bufferedReader.readLine();
      if (kv.v == null) return null;
      index += kv.v.length();
      return kv;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public String readtxt(){
    try {
      String l = bufferedReader.readLine();
      if (l == null) return null;
      index += l.length();
      return l;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public KV readkv(){
    this.kv = new KV("", "");
    try {
      while (true) {
          String l = bufferedReader.readLine();
          if (l == null) return null;
          index += l.length();
          String[] tokens = l.split(KV.SEPARATOR);
          if (tokens.length != 2) continue;
          this.kv.k = tokens[0];
          this.kv.v = tokens[1];
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