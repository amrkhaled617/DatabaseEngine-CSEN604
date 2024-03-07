
/** * @author Wael Abouelsaadat */ 

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.*;


public class DBApp {
	private Vector<Table> tables;
	private File metadata;
	private int MaximumRowsCountinPage;


	public DBApp( ){
		
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		createMetadataFile();
		readConfig();
	}
	//Gets the MaximumRowsCountinPage from the DBApp.config
	public void readConfig(){
		Properties properties= new Properties();
		try (FileInputStream fileInputStream = new FileInputStream("src/main/resources/DBApp.config")){
			properties.load(fileInputStream);
			int MaximumRowsCountinPage= Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
		} catch (IOException i){
			i.printStackTrace();
		}
	}

	public void createMetadataFile() {
		metadata = new File("src/main/metadata.csv");
		try {
			metadata.createNewFile();
			try (FileWriter writer = new FileWriter(metadata)) {
				writer.write("TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType\n");
			} catch (IOException e) {
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType.
	// htblColNameType will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
		if(strTableName==null){
			throw new DBAppException("strTableName is null");
		}
		if(htblColNameType.get(strClusteringKeyColumn)==null){
			throw new DBAppException("strClusteringKeyColumn not found");
		}
		for (Table table : tables) {
			if (table.getStrTableName().equals(strTableName))
				throw new DBAppException("Table already exists");
		}
		Enumeration<String> keys = htblColNameType.keys();
		Boolean flagCluster = false;
		while(keys.hasMoreElements()) {
			String colName = keys.nextElement();
			String colType = htblColNameType.get(colName);
			Boolean flagType = false;
			if(colName.equals(strClusteringKeyColumn))
				flagCluster = true;
			if(colType == "java.lang.Integer" || colType == "java.lang.String" || colType == "java.lang.Double")
				flagType = true;
			else
				flagType = false;
			if(!flagType)
				throw new DBAppException("Column data Type is not String/Double/Integer");
		}
		if(!flagCluster)
			throw new DBAppException("There is no Cluster/Primary key");

		//Putting information in metadata CSV file
		Enumeration<String> keysCSV = htblColNameType.keys();
		Enumeration<String> elementsCSV = htblColNameType.elements();
		try (FileWriter writer = new FileWriter(metadata)) {
			while(keysCSV.hasMoreElements()) {
				String key = keysCSV.nextElement();
				String element = elementsCSV.nextElement();
				writer.write(strTableName + "," + key + "," + element + "," + (key == strClusteringKeyColumn ? "True" : "False") + "," + "NULL" + "," + "NULL" + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		//Creating the table instance and adding it to the tables Vector
		Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		tables.add(table);
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		//check if any paremeter is null
		if (strTableName == null)
			throw new DBAppException("strTableName is null");
		if (strColName == null)
			throw new DBAppException("strColName is null");
		if (strIndexName == null)
			throw new DBAppException("strIndexName is null");

		//check if table exists
		boolean existFlag=false;
		for (Table table : tables) {
            if (table.getStrTableName().equals(strTableName)) {
                existFlag = true;
                break;
            }
		}
		if(!existFlag)
			throw new DBAppException("Table doesn't exist");

		//check if the column already has an index
		Table table = getTableFromName(strTableName);
		Vector<String> tableIndexedColumns = table.getIndexedColumns();
		if(tableIndexedColumns.contains(strColName))
			throw new DBAppException("Column is already indexed");

		//Update MetadataCSV for the Index
		updateIndexCSV(strTableName,strColName,strIndexName);
		//Create the index

		//Add the columnname that will be indexed in the indexedcolumns Vector which is in the table class
		table.getIndexedColumns().add(strColName);

		throw new DBAppException("not implemented yet");
	}
	public void updateIndexCSV(String strTableName,String strColName,String strIndexName){
		try (CSVReader reader = new CSVReader(new FileReader("src/main/metadata.csv"))) {
			String[] header = reader.readNext(); // Read the header row
			String[] line;
			List<String[]> modifiedLines = new ArrayList<>();
			while ((line = reader.readNext()) != null) {
				if (line[1].equals(strColName) && line[0].equals(strTableName)) {
					line[4] = strIndexName;
					line[5] = "B+Tree";
				}
				modifiedLines.add(line);

			}

			// Write the updated data back to the CSV file
			try (CSVWriter writer = new CSVWriter(new FileWriter("src/main/metadata.csv"))) {
				writer.writeNext(header);
				writer.writeAll(modifiedLines);
			}
		} catch (IOException | CsvValidationException e) {
			throw new RuntimeException(e);
		}
	}

	public Table getTableFromName(String strTableName) throws DBAppException {
		for (Table table : tables) {
			if (table.getStrTableName().equals(strTableName)) {
				return table;
			}
		}
		throw new DBAppException("Table not found");
	}
	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException, IOException {
		if (strTableName == null)
			throw new DBAppException("strTableName is null");
		if (htblColNameValue == null)
			throw new DBAppException("htblColNameValue is null");
		//Check if the primary key has a value
		//I tried to check but you cant put a Value in the hashtable null anyway but i will leave it just in case
		Table table = getTableFromName(strTableName);
		String strClusteringKeyColumn = table.getStrClusteringKeyColumn();
		if(htblColNameValue.get(strClusteringKeyColumn)==null)
			throw new DBAppException("htblColNameValue doesn't include a value for the primary key");
		//i don't know if i should check inside the hashtable for nulls/wrong column names and datatypes
		//check if all Column names in the htblColNameValue exist
		Enumeration<String> keys= htblColNameValue.keys();
		Hashtable<String,String> htblColNameType=table.getHtblColNameType();
		Enumeration<String> tableKeys = htblColNameType.keys();
		while(keys.hasMoreElements()){
			String key=keys.nextElement();
			String tableKey=tableKeys.nextElement();
			if(!(key.equals(tableKey))){
				throw new DBAppException("Mismatch");
			}
		}
		//checking for if the datatypes are correct in the htblColNameValue
		try {
			BufferedReader br = new BufferedReader(new FileReader(metadata));
			keys = htblColNameValue.keys();
			Enumeration<Object> values = htblColNameValue.elements();
			while (keys.hasMoreElements()) {
				String colName = keys.nextElement();
				Object colValue = values.nextElement();
				String colType = table.getHtblColNameType().get(colName);
                switch (colType) {
                    case "java.lang.Integer" -> {
                        if (!(colValue instanceof Integer))
                            throw new DBAppException("The Column type and the column value doesnt match");
                    }
                    case "java.lang.Double" -> {
                        if (!(colValue instanceof Double))
                            throw new DBAppException("The Column type and the column value doesnt match");
                    }
                    case "java.lang.String" -> {
                        if (!(colValue instanceof String))
                            throw new DBAppException("The Column type and the column value doesnt match");
                    }
                }
				String line = br.readLine();
				while (line != null) {
					String[] arrValues = line.split(",");
					if (arrValues[0].equals(table.getStrTableName()) && arrValues[1].equals(colName)) {
						if (!(arrValues[2].equals(colType))) {
							throw new DBAppException("The Column type and the column value doesnt match regarding the metadata.csv file");
						}
						break;
					}
					line = br.readLine();
				}

			}
		} catch (IOException e) {
            e.printStackTrace();
        }

		//Insert row into table
		table.insertRow(htblColNameValue);

        throw new DBAppException("not implemented yet");
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	public static void main( String[] args ){
	
//	try{
//			String strTableName = "Student";
//			DBApp	dbApp = new DBApp( );
//
//			Hashtable htblColNameType = new Hashtable( );
//			htblColNameType.put("id", "java.lang.Integer");
//			htblColNameType.put("name", "java.lang.String");
//			htblColNameType.put("gpa", "java.lang.double");
//			dbApp.createTable( strTableName, "id", htblColNameType );
//			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 2343432 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 453455 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//		}
//		catch(Exception exp){
//			exp.printStackTrace( );
//		}
	}

}