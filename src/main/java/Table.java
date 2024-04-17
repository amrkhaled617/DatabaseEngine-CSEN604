import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Vector;

public class Table implements Serializable {
    private Vector<Integer> pagesId= new Vector<>();
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
    public void deleteRowPrimary(Comparable htblColNameValuePK) throws DBAppException {
        //find a way to binary search for finding page to delete
        if(this.getPagesId().size()==0){
            throw new DBAppException("Table is empty");
        }
        Page deleteFrom=findPageByBinarySearchForDelete(htblColNameValuePK);
        deleteFrom.deleteRowFromPageBinary(htblColNameValuePK,strClusteringKeyColumn);

    }
    public void deleteRowSingle(Comparable val,String colname) throws DBAppException {
        if (this.getPagesId().size() == 0)
            throw new DBAppException("Table is empty");
        for(Integer pageId: pagesId ){
            Page page = Page.loadPage(strTableName, pageId);
            page.deleteRowFromPageLinear1(val,colname);
        }

    }
    public void deleteRowsMultiple(Hashtable<String,Object> htbl) throws DBAppException {
        for(Integer pageId: pagesId ){
            Page page = Page.loadPage(strTableName, pageId);
            page.deleteRowFromPageLinear2(htbl);

        }

    }

    public Page findPageByBinarySearchForUpdate(String clusteringKeyVal) throws DBAppException {
        int lowPageId=0;
        int highPageId=pagesId.size()-1;
        String type = htblColNameType.get(strClusteringKeyColumn);
        Object castedClusteringKeyVal=null;
        if(Objects.equals(type, "java.lang.String")){
             castedClusteringKeyVal = (String) clusteringKeyVal;
        }else if(Objects.equals(type, "java.lang.Integer")){
             castedClusteringKeyVal=(Integer)Integer.parseInt(clusteringKeyVal);
        }else if(Objects.equals(type, "java.lang.Double")){
             castedClusteringKeyVal=(Double)Double.parseDouble(clusteringKeyVal);
        }else{
            throw new DBAppException("Datatype wrong in findPageByBinarySearchForUpdate");
        }
        while(lowPageId <= highPageId){//lesa hzbt el condition dah
            int mid= lowPageId + (highPageId-lowPageId)/2;//mid page Index in the pagesID Vector
            Integer pageIdToGet = pagesId.get(mid);//the mid pageId
            Page pageToCheck = Page.loadPage(strTableName,pageIdToGet);//the actual mid Page
            Comparable firstClusteringKeyVal = (Comparable)pageToCheck.getTuples().firstElement().getRecord().get(strClusteringKeyColumn);//Gets the first Value of the Clustering key in the page
            Comparable lastClusteringKeyVal = (Comparable)pageToCheck.getTuples().lastElement().getRecord().get(strClusteringKeyColumn);//Gets the last Value of the Clustering key in the page
            int firstComparisonResult=firstClusteringKeyVal.compareTo(castedClusteringKeyVal);
            int lastComparisonResult=lastClusteringKeyVal.compareTo(castedClusteringKeyVal);
            if (firstComparisonResult > 0){
                highPageId=mid-1;
                //go back
            } else if (firstComparisonResult == 0){
                return pageToCheck;
            } else if (firstComparisonResult < 0 && lastComparisonResult > 0){
                // in between
                return pageToCheck;
            } else if (lastComparisonResult == 0){
                //duplicate
                return pageToCheck;
            } else if (lastComparisonResult < 0){
                    lowPageId=mid+1;
                //go forward
            }
        }
        throw new DBAppException("Couldn't find PageId");


    }



    public Page findPageByBinarySearch(Object clusteringKeyVal) throws DBAppException {
        int lowPageId=0;
        int highPageId=pagesId.size()-1;
        while(lowPageId <= highPageId){//lesa hzbt el condition dah
            int mid= lowPageId + (highPageId-lowPageId)/2;//mid page Index in the pagesID Vector
            Integer pageIdToGet = pagesId.get(mid);//the mid pageId
            Page pageToCheck = Page.loadPage(strTableName,pageIdToGet);//the actual mid Page
            Comparable firstClusteringKeyVal = (Comparable)pageToCheck.getTuples().firstElement().getRecord().get(strClusteringKeyColumn);//Gets the first Value of the Clustering key in the page
            Comparable lastClusteringKeyVal = (Comparable)pageToCheck.getTuples().lastElement().getRecord().get(strClusteringKeyColumn);//Gets the last Value of the Clustering key in the page
            Comparable castedClusteringKeyVal=(Comparable) clusteringKeyVal;
            int firstComparisonResult=firstClusteringKeyVal.compareTo(castedClusteringKeyVal);
            int lastComparisonResult=lastClusteringKeyVal.compareTo(castedClusteringKeyVal);
            if(firstClusteringKeyVal==lastClusteringKeyVal){
                return pageToCheck;
            }
            if (firstComparisonResult > 0){
                if(pageToCheck.getPageId()==1){
                    return pageToCheck;
                }
                highPageId=mid-1;
                //go back
            } else if (firstComparisonResult == 0){
                //duplicate(throw DBException?)
                throw new DBAppException("duplicate");
            } else if (firstComparisonResult < 0 && lastComparisonResult > 0){
                // in between
                return pageToCheck;
            } else if (lastComparisonResult == 0){
                //duplicate
                throw new DBAppException("duplicate");
            } else if (lastComparisonResult < 0){
                File file=new File("src/main/" + strTableName + (pageIdToGet+1) + ".class");
                if(DBApp.getMaximumRowsCountinPage()!=pageToCheck.getTuples().size()){
                    return pageToCheck;
                }else if(DBApp.getMaximumRowsCountinPage()==pageToCheck.getTuples().size() && !file.exists()) {
                    Page page = new Page(pageIdToGet+1,strTableName);
                    pagesId.add(pageIdToGet+1);
                    page.savePage();
                    return page;
                }else{
                    lowPageId=mid+1;
                }
                //go forward
            }
        }
        throw new DBAppException("Couldn't find PageId");


    }
    public Page findPageByBinarySearchForDelete(Object clusteringKeyVal) throws DBAppException {
        int lowPageId=0;
        int highPageId=pagesId.size()-1;
        while(lowPageId <= highPageId){//lesa hzbt el condition dah
            int mid= lowPageId + (highPageId-lowPageId)/2;//mid page Index in the pagesID Vector
            Integer pageIdToGet = pagesId.get(mid);//the mid pageId
            Page pageToCheck = Page.loadPage(strTableName,pageIdToGet);//the actual mid Page
            Comparable firstClusteringKeyVal = (Comparable)pageToCheck.getTuples().firstElement().getRecord().get(strClusteringKeyColumn);//Gets the first Value of the Clustering key in the page
            Comparable lastClusteringKeyVal = (Comparable)pageToCheck.getTuples().lastElement().getRecord().get(strClusteringKeyColumn);//Gets the last Value of the Clustering key in the page
            Comparable castedClusteringKeyVal=(Comparable) clusteringKeyVal;
            int firstComparisonResult=firstClusteringKeyVal.compareTo(castedClusteringKeyVal);
            int lastComparisonResult=lastClusteringKeyVal.compareTo(castedClusteringKeyVal);
            if(firstClusteringKeyVal==lastClusteringKeyVal){
                return pageToCheck;
            }
            if (firstComparisonResult > 0){
                if(pageToCheck.getPageId()==1){
                    return pageToCheck;
                }
                highPageId=mid-1;
                //go back
            } else if (firstComparisonResult == 0){
                return pageToCheck;
            } else if (firstComparisonResult < 0 && lastComparisonResult > 0){
                // in between
                return pageToCheck;
            } else if (lastComparisonResult == 0){
                return pageToCheck;
            } else if (lastComparisonResult < 0){
                File file=new File("src/main/" + strTableName + (pageIdToGet+1) + ".class");
                if(DBApp.getMaximumRowsCountinPage()!=pageToCheck.getTuples().size()){
                    return pageToCheck;
                }else if(DBApp.getMaximumRowsCountinPage()==pageToCheck.getTuples().size() && !file.exists()) {
                    Page page = new Page(pageIdToGet+1,strTableName);
                    pagesId.add(pageIdToGet+1);
                    page.savePage();
                    return page;
                }else{
                    lowPageId=mid+1;
                }
                //go forward
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
