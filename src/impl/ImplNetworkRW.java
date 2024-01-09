package impl;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class ImplNetworkRW implements NetworkReaderWriter{

  private String destFName;

  /* Constructor */
  public ImplNetworkRW(String _destFName){
    this.destFName = _destFName;
  }

  public void openServer(){

  }
	public void openClient(){

  }
	public NetworkReaderWriter accept(){
    return null;
  }
	public void closeServer(){

  }
	public void closeClient(){

  }
  
  public void write(KV record){

  } 
  public KV read(){
    return null;
  }
}
