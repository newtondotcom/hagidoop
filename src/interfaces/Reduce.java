package interfaces;

import java.io.Serializable;

public interface Reduce extends Serializable {
	public void reduce(Reader reader, Writer writer);
}
