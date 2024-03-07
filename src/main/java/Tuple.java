import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private Hashtable<String,Object> record;

    @Override
    public String toString(){
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String key : record.keySet()) {
            if (!first) {
                result.append(",");
            }
            result.append(record.get(key));
            first = false;
        }
        return result.toString();
    }

}
