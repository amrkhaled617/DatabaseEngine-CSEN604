import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private String pageId;
    private int numberOfRows;
    private Vector<Hashtable<String, Object>> rows;
}
