import java.io.*;
import java.util.*;


public class Page implements Serializable {
    private Integer pageId;
    private int numberOfRows;
    private Vector<Tuple> tuples = new Vector<Tuple>();
    private String strTableName;

    public Page(Integer pageId, String strTableName) {
        this.pageId=pageId;
        this.strTableName=strTableName;
    }
    public void insertRowInPage(Hashtable<String,Object> htblColNameValue, String strClusteringKeyColumn) throws DBAppException {
        int lowTupleIndex = 0;
        int highTupleIndex = tuples.size() - 1;
        Object clusteringKeyVal = htblColNameValue.get(strClusteringKeyColumn);
        while(lowTupleIndex <= highTupleIndex) {
            int mid = lowTupleIndex + (highTupleIndex - lowTupleIndex) / 2;
            Object midElement = tuples.get(mid).getRecord().get(strClusteringKeyColumn);
                Comparable castedClusteringKeyVal = (Comparable) clusteringKeyVal;
                Comparable castedmidElement = (Comparable) midElement;
                int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
                if (comparisonResult > 0){
                    highTupleIndex=mid-1;
                }else if(comparisonResult == 0){
                    //Duplicate
                    throw new DBAppException("duplicate");
                }else if(comparisonResult < 0){
                    lowTupleIndex=mid+1;
                }
        }
        //now u have exited the while loop --> lowtupleindex = highindex
        //CHECK IF PAGE IS FULL
        int MaximumRowsCountinPage = DBApp.getMaximumRowsCountinPage();
        if(tuples.size()==MaximumRowsCountinPage) {
            Tuple t = tuples.lastElement();
            tuples.remove(tuples.size() - 1);
            File file = new File("src/main/"+(strTableName)+ (pageId+1) +".class");
            if (!file.exists()) {
                Page page = new Page((pageId+1), strTableName);
                Table table = Table.loadTable(page.strTableName);
                table.getPagesId().add(pageId+1);
                table.setNumOfPages(table.getNumOfPages() + 1);
                table.saveTable();
                page.savePage();
            }
            Page page = Page.loadPage(strTableName, (pageId+1));
            page.insertRowInPage(t.getRecord(), strClusteringKeyColumn);
        }
        int mid = lowTupleIndex;
        if(tuples.isEmpty()){
            Tuple tuple=new Tuple(htblColNameValue);
            tuples.add(0,tuple);
            numberOfRows++;
            this.savePage();
        }else {
            if(mid==tuples.size()){
                mid=tuples.size()-1;
            }
            Comparable midElement = (Comparable) tuples.get(mid).getRecord().get(strClusteringKeyColumn);
            if(midElement.compareTo(clusteringKeyVal) > 0){
                Tuple tuple=new Tuple(htblColNameValue);
                tuples.add(mid,tuple);
                numberOfRows++;
                this.savePage();
            }
            else {
                Tuple tuple=new Tuple(htblColNameValue);
                tuples.add(mid+1,tuple);
                numberOfRows++;
                this.savePage();
            }
        }
    }
    public void updateRowInPage(String clusteringKeyVal,String strClusteringKeyColumn,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        int lowTupleIndex = 0;
        int highTupleIndex = tuples.size() - 1;
        String type = Table.loadTable(strTableName).getHtblColNameType().get(strClusteringKeyColumn);
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
        while(lowTupleIndex <= highTupleIndex) {
            int mid = lowTupleIndex + (highTupleIndex - lowTupleIndex) / 2;
            Object midElement = tuples.get(mid).getRecord().get(strClusteringKeyColumn);
            Comparable castedmidElement = (Comparable) midElement;
            int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
            if (comparisonResult == 1) {
                highTupleIndex = mid - 1;
            } else if (comparisonResult == 0) {
                //update this
                htblColNameValue.put(strClusteringKeyColumn,castedClusteringKeyVal);
                tuples.get(mid).setRecord(htblColNameValue);
                return;
            } else if (comparisonResult == -1) {
                lowTupleIndex = mid + 1;
            }
        }
                throw new DBAppException("Couldnt find the row to update");
    }

    public void populateTreePage(bplustree bPlusTree,String strColName){
        for( Tuple tuple : tuples){
            Hashtable<String,Object> record = tuple.getRecord();
            Comparable key = (Comparable) record.get(strColName);
            Comparable value=(Comparable) (strTableName+pageId+".class");
            bPlusTree.insert(key,value);
            bPlusTree.saveBPlusTree(strTableName,strColName);
        }
    }
    public Vector<Tuple> getRowsFromSQLTerm(SQLTerm sqlTerm) {
        Vector<Tuple> result = new Vector<Tuple>();
        String strColumnName = sqlTerm._strColumnName;
        Object objValue = sqlTerm._objValue;
        String strOperator = sqlTerm._strOperator;
        for (int i = 0; i < tuples.size(); i++) {
            Hashtable<String, Object> row = tuples.get(i).getRecord();
            if (strOperator.equals("=")) {
                if (row.get(strColumnName).equals(objValue)) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            } else if (strOperator.equals("!=")) {
                if (!row.get(strColumnName).equals(objValue)) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            } else if (strOperator.equals(">")) {
                if (((Comparable) row.get(strColumnName)).compareTo(objValue) > 0) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            } else if (strOperator.equals(">=")) {
                if (((Comparable) row.get(strColumnName)).compareTo(objValue) >= 0) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            } else if (strOperator.equals("<")) {
                if (((Comparable) row.get(strColumnName)).compareTo(objValue) < 0) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            } else if (strOperator.equals("<=")) {
                if (((Comparable) row.get(strColumnName)).compareTo(objValue) <= 0) {
                    Tuple tuple = new Tuple(row);
                    result.add(tuple);
                }
            }
        }
        return result;
    }

    public void deleteRowFromPageBinary(Comparable PK,String strClusteringKeyColumn) throws DBAppException{
        int lowTupleIndex = 0;
        int highTupleIndex = tuples.size() - 1;
        Object clusteringKeyVal = PK;
        while(lowTupleIndex <= highTupleIndex) {
            int mid = lowTupleIndex + (highTupleIndex - lowTupleIndex) / 2;
            Object midElement = tuples.get(mid).getRecord().get(strClusteringKeyColumn);
            Comparable castedClusteringKeyVal = (Comparable) clusteringKeyVal;
            Comparable castedmidElement = (Comparable) midElement;
            int comparisonResult = castedmidElement.compareTo(castedClusteringKeyVal);
            if (comparisonResult == 1){
                highTupleIndex=mid-1;
            }else if(comparisonResult == 0){
                //Duplicate
                tuples.remove(mid);
                numberOfRows--;
                this.savePage();
                this.unloadPage();
                return;
            }else if(comparisonResult == -1){
                lowTupleIndex=mid+1;
            }
            else {
                throw new DBAppException("clusteringKeyVal has a invalid Datatype");
            }
        }
    }
    public void deleteRowFromPageLinear1(Comparable val,String colname) throws DBAppException{
        //t2reeban el loop btbooz 34an ana b3ml remove w b loop 3ala nafs el tuples
        int i=0;
        while(i<tuples.size()){
            Tuple tuple=tuples.get(i);
            Comparable tupleVal=(Comparable) tuple.getRecord().get(colname);
            if(tupleVal.compareTo(val)==0){
                tuples.remove(tuple);
                numberOfRows--;
                this.savePage();
            }else{
                i++;
            }
        }
    }
    public void deleteRowFromPageLinear2(Hashtable<String,Object> htbl) throws DBAppException{
        Enumeration keys=htbl.keys();
        int i=0;
        while(i<tuples.size()){
            Tuple tuple=tuples.get(i);
            Enumeration tuplekeys= tuple.getRecord().keys();
            ArrayList<Boolean> Flags = new ArrayList<>();
            Boolean flag=true;
            while (tuplekeys.hasMoreElements()){
                Comparable tupkey = (Comparable) tuplekeys.nextElement();
                while(keys.hasMoreElements()){
                    Comparable key = (Comparable) keys.nextElement();
                    Comparable val = (Comparable)htbl.get(key);

                    if (tupkey.equals(key)){

                        if (((Comparable)tuple.getRecord().get(tupkey)).equals(val)){
                            Flags.add(true);

                        }else{ Flags.add(false);}
                    }

                }keys=htbl.keys();

            }for(int j=0;j<Flags.size();j++){
                if(Flags.get(j).equals(false)){
                    flag=false;

                }
            }
            if(Flags.isEmpty()){
                flag=false;
            }
            if (flag==true){
                tuples.remove(tuple);
                numberOfRows--;
                this.savePage();
            }
            Flags.clear();
            flag=true;
            i++;
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
        File file = new File("src/main/" + strTableName+pageId+".class");
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
    public static Page loadPage(String strTableName , int pageId){
        Page page = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/" +strTableName + pageId + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            page = (Page) objectInputStream.readObject();
            fileInputStream.close();
            objectInputStream.close();
        } catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }
        return page;
    }
    public static Page loadPageForIndex(String nameOfPage){
        Page page = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/" + nameOfPage);
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
