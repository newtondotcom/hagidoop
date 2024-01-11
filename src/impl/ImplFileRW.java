package impl;

import interfaces.KV;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import interfaces.FileReaderWriter;

public class ImplFileRW implements FileReaderWriter{

  private long index;

  // Nom du fichier sur HDFS 
  private String fName;

  // Port
  private int port;

  // ServerSocket et Socket
  ServerSocket server;
  Socket s; 

  // Les différents canaux 
  OutputStream os;
  InputStream is;

  public ImplFileRW(long _index, String _fName, String _mode){
    this.index = _index;
    this.fName = _fName;
    open(_mode);
  }

	public void open(String _mode){ 
    try {
      server = new ServerSocket(port);
      s = server.accept();

      if (_mode == "W") /* Mode Ecriture */ {
        OutputStream os = s.getOutputStream();
      } else if(_mode == "R") /* Mode Lecture */ {
        InputStream is = s.getInputStream();
      } else {
        System.err.println("Erreur mauvais mode");
      }
      } catch (IOException e) {
        e.printStackTrace();
      }
  }
	public void close(){
    try {
      server.close();
      s.close();
    } catch (IOException e){
      e.printStackTrace();
    }
  }
  public void write(KV record){

  } 
  public KV read(){
    return null;
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