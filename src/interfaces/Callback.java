package interfaces;
import java.io.Serializable;


public interface Callback extends Serializable{

	public void tacheFinie();

	public int getTachesFinies();
}