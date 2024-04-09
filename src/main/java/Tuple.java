import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private Hashtable<String,Object> record;

    public Tuple(Hashtable<String,Object> record){
        this.record=record;
    }

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
    public Hashtable<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Hashtable<String, Object> record) {
        this.record = record;
    }
}

