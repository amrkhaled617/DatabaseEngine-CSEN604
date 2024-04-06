import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private Vector<Integer> pagesId= new Vector<Integer>();
    private String strTableName;
    private String strClusteringKeyColumn;
    private Hashtable<String,String> htblColNameType;
    private int numOfPages;

    private Vector<String> indexedColumns = new Vector<String>();

    public Table(String strTableName,String strClusteringKeyColumn,Hashtable<String,String> htblColNameType){
        this.strTableName=strTableName;
        this.strClusteringKeyColumn=strClusteringKeyColumn;
        this.htblColNameType=htblColNameType;
    }
    public void insertRow(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        Object clusteringKeyVal = htblColNameValue.get(strClusteringKeyColumn);
        //find the page id where i should insert
        Page pageToInsertInto = findPageByBinarySearch(clusteringKeyVal);
        //load the page
        //insert the row in the page
        pageToInsertInto.insertRowInPage(htblColNameValue,strClusteringKeyColumn);
        //update indices(idk what this means lesa)

    }
    public void populateTree(bplustree bPlusTree,String strColName){

        for (int pageId : pagesId){
            Page page = Page.loadPage(strTableName,pageId);
            page.populateTreePage(bPlusTree,strColName);
        }
    }
    public Vector<Tuple> getRowsFromSQLTerm(SQLTerm sqlTerm) throws DBAppException {
        Vector<Tuple> rows = new Vector<Tuple>();

        for (int pageId : pagesId) {
            Page page = Page.loadPage(strTableName,pageId);
            Vector<Tuple> pageRows = page.getRowsFromSQLTerm(sqlTerm);
            rows.addAll(pageRows);
        }
        return rows;
    }




    public Page findPageByBinarySearch(Object clusteringKeyVal) throws DBAppException {
  //      Collections.sort(pagesId);
//        if(numOfPages==0){
//            pagesId.add(++numOfPages);
//            Page page = new Page(numOfPages) ;
//            page.getTuples().add()
//        }
//        System.out.print(pagesId.toString());
        int lowPageId=0;
        int highPageId=pagesId.size()-1;
        while(lowPageId <= highPageId){//lesa hzbt el condition dah
            int mid= lowPageId + (highPageId-lowPageId)/2;//mid page Index in the pagesID Vector
            Integer pageIdToGet = pagesId.get(mid);//the mid pageId
            Page pageToCheck = Page.loadPage(strTableName,pageIdToGet);//the actual mid Page
            Object firstClusteringKeyVal = pageToCheck.getTuples().firstElement().getRecord().get(strClusteringKeyColumn);//Gets the first Value of the Clustering key in the page
            Object lastClusteringKeyVal = pageToCheck.getTuples().lastElement().getRecord().get(strClusteringKeyColumn);//Gets the last Value of the Clustering key in the page
            if(clusteringKeyVal instanceof String){
                String castedFirstClusteringKeyVal = (String) firstClusteringKeyVal;
                String castedLastClusteringKeyVal = (String) lastClusteringKeyVal;
                String castedClusteringKeyVal=(String) clusteringKeyVal;
                int firstComparisonResult=castedFirstClusteringKeyVal.compareTo(castedClusteringKeyVal);
                int lastComparisonResult=castedLastClusteringKeyVal.compareTo(castedClusteringKeyVal);
                if(castedFirstClusteringKeyVal==castedLastClusteringKeyVal){
                    return pageToCheck;
                }
                if (firstComparisonResult > 0){
                    highPageId=mid-1;
                    //go back
                } else if (firstComparisonResult == 0){
                    //duplicate(throw DBException?)
                } else if (firstComparisonResult < 0 && lastComparisonResult > 0){
                    // in between
                    return pageToCheck;
                } else if (lastComparisonResult == 0){
                    //duplicate
                } else if (lastComparisonResult < 0){
                    if(DBApp.getMaximumRowsCountinPage()!=pageToCheck.getNumberOfRows()){
                        return pageToCheck;
                    }else{
                        lowPageId=mid+1;
                    }
                    //go forward
                }
            } else if(clusteringKeyVal instanceof Integer){
                Integer castedFirstClusteringKeyVal = (Integer) firstClusteringKeyVal;
                Integer castedLastClusteringKeyVal = (Integer) lastClusteringKeyVal;
                Integer castedClusteringKeyVal=(Integer) clusteringKeyVal;
                int firstComparisonResult=castedFirstClusteringKeyVal.compareTo(castedClusteringKeyVal);
                int lastComparisonResult=castedLastClusteringKeyVal.compareTo(castedClusteringKeyVal);
                if(castedFirstClusteringKeyVal==castedLastClusteringKeyVal){
                    return pageToCheck;
                }
                if (firstComparisonResult == 1){
                    highPageId=mid-1;
                    //go back
                } else if (firstComparisonResult == 0){
                    //duplicate(throw DBException?)
                } else if (firstComparisonResult == -1 && lastComparisonResult == 1){
                    // in between
                    return pageToCheck;
                } else if (lastComparisonResult == 0){
                    //duplicate
                } else if (lastComparisonResult == -1){
                    if(DBApp.getMaximumRowsCountinPage()!=pageToCheck.getNumberOfRows()){
                        return pageToCheck;
                    }else{
                        lowPageId=mid+1;
                    }
                }
            } else if(clusteringKeyVal instanceof Double){
                Double castedFirstClusteringKeyVal = (Double) firstClusteringKeyVal;
                Double castedLastClusteringKeyVal = (Double) lastClusteringKeyVal;
                Double castedClusteringKeyVal=(Double) clusteringKeyVal;
                int firstComparisonResult=castedFirstClusteringKeyVal.compareTo(castedClusteringKeyVal);
                int lastComparisonResult=castedLastClusteringKeyVal.compareTo(castedClusteringKeyVal);
                if(castedFirstClusteringKeyVal==castedLastClusteringKeyVal){
                    return pageToCheck;
                }
                if (firstComparisonResult == 1){
                    highPageId=mid-1;
                    //go back
                } else if (firstComparisonResult == 0){
                    //duplicate(throw DBException?)
                } else if (firstComparisonResult == -1 && lastComparisonResult == 1){
                    // in between
                    return pageToCheck;
                } else if (lastComparisonResult == 0){
                    //duplicate
                } else if (lastComparisonResult == -1){
                    if(DBApp.getMaximumRowsCountinPage()!=pageToCheck.getNumberOfRows()){
                        return pageToCheck;
                    }else{
                        lowPageId=mid+1;
                    }
                }
            }
        }
        throw new DBAppException("Couldn't find PageId");


    }

    public static Table loadTable(String strTableName) {
        Table table = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/" + strTableName + ".class");
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

    public int getNumOfPages() {
        return numOfPages;
    }

    public void setNumOfPages(int numOfPages) {
        this.numOfPages = numOfPages;
    }
}
