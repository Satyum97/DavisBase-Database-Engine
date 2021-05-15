
import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;



public class DavisBase {

	static String prompt = "DavisBaseSql> ";
	static String catalogDirectory = "data/catalog";
	static String user_data = "data/user_data";
	

	static boolean isExit = false;
		
	public static int sizeOfpage = 512;
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
    public static void main(String[] args) {
    	init();
		
		Display_Screen();

		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			translateUserCommand(userCommand);
		}
		System.out.println("Exiting DavisBase...");


	}
	
    public static void Display_Screen() {
		System.out.println(line("=",80));
        System.out.println("Welcome to DavisBase");
		System.out.println("\nType \"Help;\" to display supported commands.");
		System.out.println("\nType \"Exit;\" to exit DavisBase.");
		System.out.println(line("=",80));
	}
	

	
	public static String line(String s,int num) {
		String x = "";
		for(int i=0;i<num;i++) {
			x += s;
		}
		return x;
	}
	
	
	public static void help() {
		System.out.println(line("=",80));
		System.out.println("FOLLOWING ARE THE SUPPORTED COMMANDS");
		System.out.println("All commands listed below are case insensitive");
		System.out.println();
		System.out.println("\tSHOW TABLES;                                               This Command Display all the tables in the database.");
		System.out.println("\tCREATE TABLE table_name (<column_name datatype> <NOT NULL/UNIQUE>); This Command  Create a new table in the database and First record must be primary key of type Int.");
		System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);         This Command Insert a new record into the table and First Column is primary key which has inbuilt auto increment function.");
		System.out.println("\tSELECT * FROM table_name;                                 This Command Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE column_name operator value;This Command Display records of the table when the given condition is satisfied.");
		System.out.println("\tDROP TABLE table_name;                                    This Command withdraw table data and its schema.");
		System.out.println("\tHELP;                                                     This Command Show this help information.");
		System.out.println("\tEXIT;                                                     This Command Exit the program.");
		System.out.println();
		System.out.println();
		System.out.println(line("=",80));
	}


	
	public static boolean table_Exists(String table_name){
		table_name = table_name+".tbl";
		
		try {
			
			
			File data_Directory = new File(user_data);
			if (table_name.equalsIgnoreCase("davisbase_tables.tbl") || table_name.equalsIgnoreCase("davisbase_columns.tbl"))
				data_Directory = new File(catalogDirectory) ;
			
			String[] previousTableFiles;
			previousTableFiles = data_Directory.list();
			for (int i=0; i<previousTableFiles.length; i++) {
				if(previousTableFiles[i].equals(table_name))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Cannot create data container directory");
			System.out.println(se);
		}

		return false;
	}

	public static void init(){
		try {
			File data_Directory = new File("data");
			if(data_Directory.mkdir()){
				System.out.println("Unable to find data base, initializing data base...");
				start();
			}
			else {
				data_Directory = new File(catalogDirectory);
				String[] previousTableFiles = data_Directory.list();
				boolean checkTable = false;
				boolean checkColumn = false;
				for (int i=0; i<previousTableFiles.length; i++) {
					if(previousTableFiles[i].equals("davisbase_tables.tbl"))
						checkTable = true;
					if(previousTableFiles[i].equals("davisbase_columns.tbl"))
						checkColumn = true;
				}
				
				if(!checkTable){
					System.out.println("Unable to find data base, initializing data base...");
					System.out.println();
					start();
				}
				
				if(!checkColumn){
					System.out.println("Unable to find data base, initializing data base...");
					System.out.println();
					start();
				}
				
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

	}
	
public static void start() {

		
		try {
			File data_Directory = new File(user_data);
			data_Directory.mkdir();
			data_Directory = new File(catalogDirectory);
			data_Directory.mkdir();
			String[] previousTableFiles;
			previousTableFiles = data_Directory.list();
			for (int i=0; i<previousTableFiles.length; i++) {
				File anpreviousFile = new File(data_Directory, previousTableFiles[i]); 
				anpreviousFile.delete();
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile tables_catalog = new RandomAccessFile(catalogDirectory+"/davisbase_tables.tbl", "rw");
			tables_catalog.setLength(sizeOfpage);
			tables_catalog.seek(0);
			tables_catalog.write(0x0D);
			tables_catalog.writeByte(0x02);
			
			int size1=24;
			int size2=25;
			
			int offsetTable=sizeOfpage-size1;
			int offsetColumn=offsetTable-size2;
			
			tables_catalog.writeShort(offsetColumn);
			tables_catalog.writeInt(0);
			tables_catalog.writeInt(0);
			tables_catalog.writeShort(offsetTable);
			tables_catalog.writeShort(offsetColumn);
			
			tables_catalog.seek(offsetTable);
			tables_catalog.writeShort(20);
			tables_catalog.writeInt(1); 
			tables_catalog.writeByte(1);
			tables_catalog.writeByte(28);
			tables_catalog.writeBytes("davisbase_tables");
			
			tables_catalog.seek(offsetColumn);
			tables_catalog.writeShort(21);
			tables_catalog.writeInt(2); 
			tables_catalog.writeByte(1);
			tables_catalog.writeByte(29);
			tables_catalog.writeBytes("davisbase_columns");
			
			tables_catalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
		
		try {
			RandomAccessFile columns_catalog = new RandomAccessFile(catalogDirectory+"/davisbase_columns.tbl", "rw");
			columns_catalog.setLength(sizeOfpage);
			columns_catalog.seek(0);       
			columns_catalog.writeByte(0x0D); 
			columns_catalog.writeByte(0x09); 
			
			int[] offset=new int[9];
			offset[0]=sizeOfpage-45;
			offset[1]=offset[0]-49;
			offset[2]=offset[1]-46;
			offset[3]=offset[2]-50;
			offset[4]=offset[3]-51;
			offset[5]=offset[4]-49;
			offset[6]=offset[5]-59;
			offset[7]=offset[6]-51;
			offset[8]=offset[7]-49;
			
			columns_catalog.writeShort(offset[8]); 
			columns_catalog.writeInt(0); 
			columns_catalog.writeInt(0); 
			
			for(int i=0;i<offset.length;i++)
				columns_catalog.writeShort(offset[i]);

			
			columns_catalog.seek(offset[0]);
			columns_catalog.writeShort(36);
			columns_catalog.writeInt(1); 
			columns_catalog.writeByte(6); 
			columns_catalog.writeByte(28); 
			columns_catalog.writeByte(17); 
			columns_catalog.writeByte(15); 
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_tables"); 
			columns_catalog.writeBytes("rowid"); 
			columns_catalog.writeBytes("INT"); 
			columns_catalog.writeByte(1); 
			columns_catalog.writeBytes("NO"); 
			columns_catalog.writeBytes("NO"); 
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[1]);
			columns_catalog.writeShort(42); 
			columns_catalog.writeInt(2); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(28);
			columns_catalog.writeByte(22);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_tables"); 
			columns_catalog.writeBytes("table_name"); 
			columns_catalog.writeBytes("TEXT"); 
			columns_catalog.writeByte(2);
			columns_catalog.writeBytes("NO"); 
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[2]);
			columns_catalog.writeShort(37); 
			columns_catalog.writeInt(3); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(17);
			columns_catalog.writeByte(15);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("rowid");
			columns_catalog.writeBytes("INT");
			columns_catalog.writeByte(1);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[3]);
			columns_catalog.writeShort(43);
			columns_catalog.writeInt(4); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(22);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("table_name");
			columns_catalog.writeBytes("TEXT");
			columns_catalog.writeByte(2);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[4]);
			columns_catalog.writeShort(44);
			columns_catalog.writeInt(5); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(23);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("column_name");
			columns_catalog.writeBytes("TEXT");
			columns_catalog.writeByte(3);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[5]);
			columns_catalog.writeShort(42);
			columns_catalog.writeInt(6); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(21);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("data_type");
			columns_catalog.writeBytes("TEXT");
			columns_catalog.writeByte(4);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[6]);
			columns_catalog.writeShort(52); 
			columns_catalog.writeInt(7); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(28);
			columns_catalog.writeByte(19);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("ordinal_position");
			columns_catalog.writeBytes("TINYINT");
			columns_catalog.writeByte(5);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.seek(offset[7]);
			columns_catalog.writeShort(44); 
			columns_catalog.writeInt(8); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(23);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("is_nullable");
			columns_catalog.writeBytes("TEXT");
			columns_catalog.writeByte(6);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
		

			columns_catalog.seek(offset[8]);
			columns_catalog.writeShort(42); 
			columns_catalog.writeInt(9); 
			columns_catalog.writeByte(6);
			columns_catalog.writeByte(29);
			columns_catalog.writeByte(21);
			columns_catalog.writeByte(16);
			columns_catalog.writeByte(4);
			columns_catalog.writeByte(14);
			columns_catalog.writeByte(14);
			columns_catalog.writeBytes("davisbase_columns");
			columns_catalog.writeBytes("is_unique");
			columns_catalog.writeBytes("TEXT");
			columns_catalog.writeByte(7);
			columns_catalog.writeBytes("NO");
			columns_catalog.writeBytes("NO");
			
			columns_catalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
}



	public static String[] translateEquation(String equation){
		String comparasion_operator[] = new String[3];
		String temp[] = new String[2];
		if(equation.contains("=")) {
			temp = equation.split("=");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = "=";
			comparasion_operator[2] = temp[1].trim();
		}
		
		if(equation.contains("<")) {
			temp = equation.split("<");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = "<";
			comparasion_operator[2] = temp[1].trim();
		}
		
		if(equation.contains(">")) {
			temp = equation.split(">");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = ">";
			comparasion_operator[2] = temp[1].trim();
		}
		
		if(equation.contains("<=")) {
			temp = equation.split("<=");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = "<=";
			comparasion_operator[2] = temp[1].trim();
		}

		if(equation.contains(">=")) {
			temp = equation.split(">=");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = ">=";
			comparasion_operator[2] = temp[1].trim();
		}
		
		if(equation.contains("!=")) {
			temp = equation.split("!=");
			comparasion_operator[0] = temp[0].trim();
			comparasion_operator[1] = "!=";
			comparasion_operator[2] = temp[1].trim();
		}

		return comparasion_operator;
	}
		
	public static void translateUserCommand (String userCommand) {
		
		ArrayList<String> comm_tokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (comm_tokens.get(0)) {

		    case "show":
			    ShowTables();
			    break;
			
		    case "create":
		    	switch (comm_tokens.get(1)) {
		    	case "table": 
		    		translateCreateString(userCommand);
		    		break;
		    	}
		    	break;

			case "insert":
				translateInsertString(userCommand);
				break;
				
			case "select":
				translateQueryString(userCommand);
				break;

			case "drop":
				dropTable(userCommand);
				break;	

			case "help":
				help();
				break;

			case "exit":
				isExit=true;
				break;
				
			case "quit":
				isExit=true;
				break;
	
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				System.out.println();
				break;
		}
	} 

	public static void ShowTables() {
		System.out.println("STUB: To Process Command, Calling a Method");
		System.out.println("Translating the string:\"show tables\"");
		
		String table = "davisbase_tables";
		String[] columns = {"table_name"};
		String[] comparasion_operator = new String[0];
		
		Table.select(table, columns, comparasion_operator,user_data+"/");
	}
	
    public static void translateCreateString(String createString) {
		
		System.out.println("STUB: Calling your method to process the command");
		System.out.println("Translating the string:\"" + createString + "\"");
		
		String[] tokens=createString.split(" ");
		String table_name = tokens[2];
		String[] temp = createString.split(table_name);
		String cols = temp[1].trim();
		String[] create_cols = cols.substring(1, cols.length()-1).split(",");
		
		for(int i = 0; i < create_cols.length; i++)
			create_cols[i] = create_cols[i].trim();
		
		if(table_Exists(table_name)){
			System.out.println("Table "+table_name+" already exists.");
		}
		else
			{
			Table.createTable(table_name, create_cols);		
			}

	}
    
    public static void translateInsertString(String insertStr) {
    	try{
		System.out.println("STUB: To Process Command, Calling a Method");
		System.out.println("Translating the string:\"" + insertStr + "\"");
		
		String[] tks=insertStr.split(" ");
		String table = tks[2];
		String[] temp = insertStr.split("values");
		String temp1=temp[1].trim();
		String[] insert_values = temp1.substring(1, temp1.length()-1).split(",");
		for(int i = 0; i < insert_values.length; i++)
			insert_values[i] = insert_values[i].trim();
	
		if(!table_Exists(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Table.insertInto(table, insert_values,user_data+"/");
		}
    	}
    	catch(Exception e)
    	{
    		System.out.println(e+e.toString());
    	}

	}
    
    public static void translateQueryString(String queryStr) {
		System.out.println("STUB: To Process Command, Calling a Method");
		System.out.println("Translating the string:\"" + queryStr + "\"");
		
		String[] comparator;
		String[] column;
		String[] temp = queryStr.split("where");
		if(temp.length > 1){
			String tmp = temp[1].trim();
			comparator = translateEquation(tmp);
		}
		else{
			comparator = new String[0];
		}
		String[] select = temp[0].split("from");
		String table_name = select[1].trim();
		String columns = select[0].replace("select", "").trim();
		if(columns.contains("*")){
			column = new String[1];
			column[0] = "*";
		}
		else{
			column = columns.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		
		if(!table_Exists(table_name)){
			System.out.println("Table "+table_name+" does not exist.");
		}
		else
		{
		    Table.select(table_name, column, comparator,user_data+"/");
		}
	}
	
	public static void dropTable(String dropTableStr) {
		System.out.println("STUB: To Process Command, Calling a Method");
		System.out.println("Translating the string:\"" + dropTableStr + "\"");
		
		String[] tks=dropTableStr.split(" ");
		String table_name = tks[2];
		if(!table_Exists(table_name)){
			System.out.println("Table "+table_name+" does not exist.");
		}
		else
		{
			Table.drop(table_name);
		}		

	}

}