import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String tableName;
    private String clusteringKeyColumn;
    private Vector<String> pagesId;



    public Table loadTable() {
        Table table = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main" + tableName);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            table = (Table) objectInputStream.readObject();
            fileInputStream.close();
            objectInputStream.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }
        return table;
    }
    public void saveTable(Table table, String tableName){
        try{
            FileOutputStream fileOutputStream = new FileOutputStream("src/main/" + tableName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(table);
            objectOutputStream.close();
            fileOutputStream.close();
        }
        catch (IOException i){
            i.printStackTrace();
        }
    }

}
