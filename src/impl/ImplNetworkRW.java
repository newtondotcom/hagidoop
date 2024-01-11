package impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class ImplNetworkRW implements NetworkReaderWriter{

  private String destFName;
  public int port;

  // Stream
  OutputStream os;
  InputStream is;

  public static ServerSocket ssck;
  public static Socket s;

  public static String host;

  /* Constructor */
  public ImplNetworkRW(int _port){
    this.port = _port;
  }

  public void openServer(){
    try(ServerSocket ssck = new ServerSocket(port)){
      this.ssck = ssck;
    } catch (Exception e){
      e.printStackTrace();
    }
  }
	public void openClient(){
    try(Socket ss = new Socket(host, port)){
      this.s = ss;
    } catch (Exception e){
      e.printStackTrace();
    }
  }
	public NetworkReaderWriter accept(){
    try {
      this.s = ssck.accept();
      is = s.getInputStream();
      os = s.getOutputStream();

      return new ImplNetworkRW(port);
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
      s.close();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(KV _record){
    try {
      ObjectOutputStream oos = new ObjectOutputStream(os);
      oos.writeObject(_record);
    } catch (IOException e) {
      e.printStackTrace();
    }
  } 
  @Override
  public KV read(){
    KV record = null;
    try {
      ObjectInputStream ois = new ObjectInputStream(is);
      Object object = ois.read();
      record = new KV("Undefined", (String)object);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return record;
  }
}
