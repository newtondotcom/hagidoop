package interfaces;

import java.io.Serializable;

public interface Map extends Serializable {
	public void map(Reader reader, Writer writer);
}