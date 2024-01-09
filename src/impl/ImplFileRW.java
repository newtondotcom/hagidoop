package impl;

import interfaces.KV;
import interfaces.FileReaderWriter;

public class ImplFileRW implements FileReaderWriter{

  private long index;

  // Nom du fichier sur HDFS 
  private String fName;

  public ImplFileRW(long _index, String _fName){
    this.index = _index;
    this.fName = _fName;
  }

	public void open(String mode){ 

  }
	public void close(){

  }
	public void setFname(String fname){

  }
  public void write(KV record){

  } 
  public KV read(){
    return null;
  }

  
  public long getIndex(){
      return this.index;
  }
	public String getFname(){
    return this.fName;
  }
}