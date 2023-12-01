package interfaces;

public interface NetworkReaderWriter extends ReaderWriter {
	public void openServer();
	public void openClient();
	public NetworkReaderWriter accept();
	public void closeServer();
	public void closeClient();
}
