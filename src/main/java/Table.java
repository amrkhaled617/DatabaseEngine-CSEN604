import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String strTableName;
    private String strClusteringKeyColumn;
    private Hashtable<String,String> htblColNameType;
    private Vector<Integer> pagesId;
    private Vector<String> indexedColumns;

    public Table(String strTableName,String strClusteringKeyColumn,Hashtable<String,String> htblColNameType){
        this.strTableName=strTableName;
        this.strClusteringKeyColumn=strClusteringKeyColumn;
        this.htblColNameType=htblColNameType;
    }
    public void insertRow(Hashtable<String,Object> htblColNameValue){

    }

    public Table loadTable() {
        Table table = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main" + strTableName + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            table = (Table) objectInputStream.readObject();
            fileInputStream.close();
            objectInputStream.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }
        return table;
    }
    public void unloadTable(){
        saveTable();
        this.strTableName=null;
        this.strClusteringKeyColumn=null;
        this.htblColNameType=null;
        this.pagesId=null;
        this.indexedColumns=null;
    }
    public void saveTable(){
        try{
            FileOutputStream fileOutputStream = new FileOutputStream("src/main/" + strTableName + ".class");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.close();
            fileOutputStream.close();
        }
        catch (IOException i){
            i.printStackTrace();
        }
    }


    public String getStrTableName() {
        return strTableName;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public String getStrClusteringKeyColumn() {
        return strClusteringKeyColumn;
    }

    public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
        this.strClusteringKeyColumn = strClusteringKeyColumn;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }

    public Vector<Integer> getPagesId() {
        return pagesId;
    }

    public void setPagesId(Vector<Integer> pagesId) {
        this.pagesId = pagesId;
    }

    public Vector<String> getIndexedColumns() {
        return indexedColumns;
    }

    public void setIndexedColumns(Vector<String> indexedColumns) {
        this.indexedColumns = indexedColumns;
    }
}
