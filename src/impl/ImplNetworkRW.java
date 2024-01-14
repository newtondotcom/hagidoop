package impl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import javax.swing.text.StyledEditorKit;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class ImplNetworkRW implements NetworkReaderWriter{

  public int port;

  // Stream
  OutputStream os;
  ObjectOutputStream oos;
  InputStream is;
  ObjectInputStream ois;

  public ServerSocket ssck;
  public Socket s;

  public String host;

  /* Constructor */
  public ImplNetworkRW(int _port, String _host, boolean _server){
    this.port = _port;
    this.host = _host;
    if (_server){
      openServer();
      accept();
      System.out.println("5");
    } 
  }

  public void openServer(){
    try{
      ServerSocket ssck = new ServerSocket(this.port);
      System.out.println("Server open sur le port " + this.port);
      this.ssck = ssck;
    } catch (Exception e){
      e.printStackTrace();
    }
  }
	public void openClient(){
    try{
      System.out.println(this.host + this.port);
      Socket ss = new Socket(this.host, this.port);
      System.out.println("Client open sur le port " + this.port);
      this.s = ss;
    } catch (Exception e){
      e.printStackTrace();
    }
  }
	public NetworkReaderWriter accept(){
    try {
      this.s = ssck.accept();
      this.is = s.getInputStream();
      this.ois = new ObjectInputStream(is);
      this.os = s.getOutputStream();
      this.oos = new ObjectOutputStream(os);
      System.out.println("Tous les flux sont ouverts");
      return new ImplNetworkRW(this.port, this.host, true);
    } catch (Exception e){
      e.printStackTrace();
    }
    return null;
  }
	public void closeServer(){
    try {
      ssck.close();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
	public void closeClient(){
    try {
      this.s.close();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(KV _record){
    System.out.println("Write" + _record);
    try {
      this.oos = new ObjectOutputStream(os);
      oos.writeObject(_record);
    } catch (IOException e) {
      e.printStackTrace();
    }
  } 
  @Override
  public KV read() {
    try {
        this.ois = new ObjectInputStream(is);
        Object object = ois.readObject();
        if (object instanceof KV) {
            return (KV) object;
        } else {
            System.err.println("Error: Unexpected object type read from the stream.");
            return null;
        }
    } catch (EOFException e) {
        // La fin du flux a été atteinte, c'est normal.
        // Vous pouvez traiter cette exception si nécessaire.
        return null;
    } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return null;
    }
}
}
