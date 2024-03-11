import java.io.*;
import java.util.DuplicateFormatFlagsException;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private Integer pageId;
    private int numberOfRows;
    private Vector<Tuple> tuples;
    private String strTableName;

    public Page(Integer pageId) {
    }
    public void insertRowInPage(Hashtable<String,Object> htblColNameValue, String strClusteringKeyColumn) throws DBAppException {
        int lowTupleIndex = 0;
        int highTupleIndex = tuples.size() - 1;
        Object clusteringKeyVal = htblColNameValue.get(strClusteringKeyColumn);
        while(lowTupleIndex >= highTupleIndex) {
            int mid = lowTupleIndex + (highTupleIndex - lowTupleIndex) / 2;
            Object midElement = tuples.get(mid).getRecord().get(strClusteringKeyColumn);
            if (clusteringKeyVal instanceof String) {
                String castedClusteringKeyVal = (String) clusteringKeyVal;
                String castedmidElement = (String) midElement;
                int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
                if (comparisonResult == 1){
                    highTupleIndex=mid-1;
                }else if(comparisonResult == 0){
                    //Duplicate
                }else if(comparisonResult == -1){
                    lowTupleIndex=mid+1;
                }
            } else if (clusteringKeyVal instanceof Integer) {
                Integer castedClusteringKeyVal = (Integer) clusteringKeyVal;
                Integer castedmidElement = (Integer) midElement;
                int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
                if (comparisonResult == 1){
                    highTupleIndex = mid-1;
                }else if(comparisonResult == 0){
                    //Duplicate
                }else if(comparisonResult == -1){
                    lowTupleIndex = mid+1;
                }
            } else if (clusteringKeyVal instanceof Double) {
                Double castedClusteringKeyVal = (Double) clusteringKeyVal;
                Double castedmidElement = (Double) midElement;
                int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
                if (comparisonResult == 1){
                    highTupleIndex = mid-1;
                }else if(comparisonResult == 0){
                    //Duplicate
                }else if(comparisonResult == -1){
                    lowTupleIndex = mid+1;
                }
            } else {
                throw new DBAppException("clusteringKeyVal has a invalid Datatype");
            }
        }
    }



    @Override
    public String toString(){
        StringBuilder result = new StringBuilder();
        boolean firstTuple = true;
        for (Tuple tuple : tuples) {
            if (!firstTuple) {
                result.append(",");
            }
            result.append(tuple.toString());
            firstTuple = false;
        }

        return result.toString();
    }
    public void savePage(){
        File file = new File(strTableName+pageId+".class");
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException i){
            i.printStackTrace();
        }
    }
    public void deletePage() {
        File file = new File(strTableName + pageId + ".class");
        file.delete();
    }
    public void unloadPage(){
        savePage();
        this.numberOfRows=0;
        this.tuples=null;
        this.strTableName=null;
        this.pageId=0;
    }
    public Page loadPage(){
        Page page = null;
        try {
            FileInputStream fileInputStream = new FileInputStream(strTableName + pageId + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            page = (Page) objectInputStream.readObject();
            fileInputStream.close();
            objectInputStream.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }
        return page;
    }

    public Integer getPageId() {
        return pageId;
    }

    public void setPageId(Integer pageId) {
        this.pageId = pageId;
    }

    public int getNumberOfRows() {
        return numberOfRows;
    }

    public void setNumberOfRows(int numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public void setTuples(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    public String getStrTableName() {
        return strTableName;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }
}
