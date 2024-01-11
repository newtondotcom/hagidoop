package impl;

import java.io.*;

import interfaces.*;

public class ImplReaderWriter implements ReaderWriter {

  private InputStream is;
  private OutputStream os;

  public ImplReaderWriter(InputStream _is, OutputStream _os){
    this.is = _is;
    this.os = _os;
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
