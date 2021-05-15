
import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table{
	
	public static int sizeOfpage = 512;
	public static String date_layout = "yyyy-MM-dd_HH:mm:ss";
	public static String catalogDirectory = "data/catalog/";
	public static String user_data = "data/user_data/";
	
	public static int numberOfrecords;
	
	public static int pages(RandomAccessFile file){
		int numberOfpages = 0;
		try{
			numberOfpages = (int)(file.length()/(new Long(sizeOfpage)));
		}catch(Exception e){
			System.out.println(e);
		}

		return numberOfpages;
	}

	public static void drop(String table){
		try{
			
			RandomAccessFile file = new RandomAccessFile(catalogDirectory+"davisbase_tables.tbl", "rw");
			int num_of_pages = pages(file);
			for(int page = 1; page <= num_of_pages; page ++){
				file.seek((page-1)*sizeOfpage);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] AddressOfCell = BTree.getArraycell(file, page);
					int k = 0;
					for(int i = 0; i < AddressOfCell.length; i++)
					{
						long lcn = BTree.getLocationcell(file, page, i);
						String[] values = reedemValues(file, lcn);
						String tb = values[1];
						if(!tb.equals(table))
						{
							BTree.setOffsetCell(file, page, k, AddressOfCell[i]);
							k++;
						}
					}
					BTree.setNumCell(file, page, (byte)k);
				}
				else
					continue;
			}

			file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			num_of_pages = pages(file);
			for(int page = 1; page <= num_of_pages; page ++){
				file.seek((page-1)*sizeOfpage);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] AddressOfCell = BTree.getArraycell(file, page);
					int k = 0;
					for(int i = 0; i < AddressOfCell.length; i++)
					{
						long lcn = BTree.getLocationcell(file, page, i);
						String[] values = reedemValues(file, lcn);
						String tb = values[1];
						if(!tb.equals(table))
						{
							BTree.setOffsetCell(file, page, k, AddressOfCell[i]);
							k++;
						}
					}
					BTree.setNumCell(file, page, (byte)k);
				}
				else
					continue;
			}

			File anpreviousFile = new File(user_data, table+".tbl"); 
			anpreviousFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}
    
	public static String[] reedemValues(RandomAccessFile file, long loc){
		
		String[] values = null;
		try{
			
			SimpleDateFormat datelayout = new SimpleDateFormat (date_layout);

			file.seek(loc+2);
			int key = file.readInt();
			int numberOfcolumns = file.readByte();
			
			byte[] serialcode = new byte[numberOfcolumns];
			file.read(serialcode);
			
			values = new String[numberOfcolumns+1];
			
			values[0] = Integer.toString(key);
			
			for(int i=1; i <= numberOfcolumns; i++){
				switch(serialcode[i-1]){
					case 0x00:  file.readByte();
					            values[i] = "null";
								break;

					case 0x01:  file.readShort();
					            values[i] = "null";
								break;

					case 0x02:  file.readInt();
					            values[i] = "null";
								break;

					case 0x03:  file.readLong();
					            values[i] = "null";
								break;

					case 0x04:  values[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  values[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  values[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  values[i] = Long.toString(file.readLong());
								break;

					case 0x08:  values[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  values[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  Long temp = file.readLong();
								Date dateTime = new Date(temp);
								values[i] = datelayout.format(dateTime);
								break;

					case 0x0B:  temp = file.readLong();
								Date date = new Date(temp);
								values[i] = datelayout.format(date).substring(0,10);
								break;

					default:    int len = new Integer(serialcode[i-1]-0x0C); //why 12 is subtracted?
								byte[] bytes = new byte[len];
								file.read(bytes);
								values[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return values;
	}

	public static void createTable(String table, String[] col){
		try{	
			
			RandomAccessFile file = new RandomAccessFile(user_data+table+".tbl", "rw");
			file.setLength(sizeOfpage);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			
			file = new RandomAccessFile(catalogDirectory+"davisbase_tables.tbl", "rw");
			
			int num_of_pages = pages(file);
			int page=1;
			for(int i = 1; i <= num_of_pages; i++){
				int r = BTree.get_rightMost(file, i);
				if(r == 0)
					page = i;
			}
			
			int[] keys = BTree.getArraykey(file, page);
			int l = keys[0];
			for(int i = 0; i < keys.length; i++)
				if(keys[i]>l)
					l = keys[i];
			file.close();
			
			String[] values = {Integer.toString(l+1), table};
			insertInto("davisbase_tables", values,catalogDirectory);

			file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			
			num_of_pages = pages(file);
			page=1;
			for(int i = 1; i <= num_of_pages; i++){
				int r = BTree.get_rightMost(file, i);
				if(r == 0)
					page = i;
			}
			
			keys = BTree.getArraykey(file, page);
			l = keys[0];
			for(int i = 0; i < keys.length; i++)
				if(keys[i]>l)
					l = keys[i];
			file.close();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] tkn = col[i].split(" ");
				String column_name = tkn[0];
				String tu = tkn[1].toUpperCase();
				String position = Integer.toString(i+1);
				String invalid;
				String unique="NO";
				if(tkn.length > 2)
				{
					invalid = "NO";
					if(tkn[2].toUpperCase().trim().equals("UNIQUE"))
						unique = "YES";
					else
						unique = "NO";
				}
				else
					 invalid = "YES";
				
				
				String[] value = {Integer.toString(l), table, column_name, tu, position, invalid, unique};
				insertInto("davisbase_columns", value,catalogDirectory);
			}
	
		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void insertInto(String table, String[] values,String dir_s){
		try{
			RandomAccessFile file = new RandomAccessFile(dir_s+table+".tbl", "rw");
			insertInto(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void insertInto(RandomAccessFile file, String table, String[] values){
		String[] data_type = getDataType(table);
		String[] invalid = getInvalid(table);
		String[] unique = getUnique(table);
		
		int num_of_pages = pages(file);
		int pages=1;
		for(int i = 1; i <= num_of_pages; i++){
			int r = BTree.get_rightMost(file, i);
			if(r == 0)
				pages = i;
		}
		
		int[] keys = BTree.getArraykey(file, pages);
		int l = 0;
		for(int i = 0; i < keys.length; i++)
			if(keys[i]>l)
				l = keys[i];
		
		if (values[0].isEmpty())
			values[0]=String.valueOf(l+1);

		for(int i = 0; i < invalid.length; i++)
			if(values[i].equals("null") && invalid[i].equals("NO")){
				System.out.println("There is a NULL-value constraint violation");
				System.out.println();
				return;
			}
		
		for(int i = 0; i < unique.length; i++)
			if(unique[i].equals("YES")){
				System.out.println("Examining for unique constraint violation");
				System.out.println();
				String path = user_data ;
				
				try {
				
				RandomAccessFile uni = new RandomAccessFile(path+table+".tbl", "rw");
				
				String[] column_name = getColumnName(table);
				String[] type = getDataType(table);
				
				Records records = new Records();
				
				String[] comparator = {column_name[i],"=",values[i]};
				
				filter(uni, comparator, column_name, type, records);
				
				if (records.numberOfrows>0)
				{
					System.out.println("A Duplicate Key for "+column_name[i].toString()+" is found");
					
					System.out.println();
					return;
				}
				 
				uni.close();
				
				}catch (Exception e)
				{
					System.out.println(e);
				}
				
			}

		int key = new Integer(values[0]);
		int page = findPageKey(file, key);
		if(page != 0)
			if(BTree.hasKey(file, page, key)){
				System.out.println("There is an Uniqueness constraint violation");
				return;
			}
		if(page == 0)
			page = 1;
		
		byte[] serialcode = new byte[data_type.length-1];
		short sizeOfpageLocation = (short) compute_payload(table, values, serialcode);
		int cell_size = sizeOfpageLocation + 6;
		int offset = BTree.check_leaf_space(file, page, cell_size);
		
		if(offset != -1){
			BTree.insertLeafcell(file, page, offset, sizeOfpageLocation, key, serialcode, values);

		}else{
			BTree.leaf_split(file, page);
			insertInto(file, table, values);
		}
	}

	public static int compute_payload(String table, String[] values, byte[] serialcode){
		String[] dataType = getDataType(table);
		int size =dataType.length;
		for(int i = 1; i < dataType.length; i++){
			serialcode[i - 1]= getserialcode(values[i], dataType[i]);
			size = size + LengthOfField(serialcode[i - 1]);
		}
		return size;
	}
	

	public static byte getserialcode(String value, String dataType){
		if(value.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	

    public static short LengthOfField(byte serialcode){
		switch(serialcode){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(serialcode - 0x0C);
		}
	}


	
public static int findPageKey(RandomAccessFile file, int key){
		int val = 1;
		try{
			int num_pages = pages(file);
			for(int page = 1; page <= num_pages; page++){
				file.seek((page - 1)*sizeOfpage);
				byte page_type = file.readByte();
				if(page_type == 0x0D){
					int[] keys = BTree.getArraykey(file, page);
					if(keys.length == 0)
						return 0;
					int rm = BTree.get_rightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}


	
	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			Records records = new Records();
			String[] column_name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable","is_unique"};
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, column_name, records);
			HashMap<Integer, String[]> ctn = records.ctn;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] x : ctn.values()){
				array.add(x[3]);
			}
			int size=array.size();
			dataType = array.toArray(new String[size]);
			file.close();
			return dataType;
		}catch(Exception e){
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColumnName(String table){
		String[] cols = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			Records records = new Records();
			String[] column_name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable","is_unique"};
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, column_name, records);
			HashMap<Integer, String[]> ctn = records.ctn;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : ctn.values()){
				array.add(i[2]);
			}
			int size=array.size();
			cols = array.toArray(new String[size]);
			file.close();
			return cols;
		}catch(Exception e){
			System.out.println(e);
		}
		return cols;
	}

	public static String[] getInvalid(String table){
		String[] invalid = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			Records records = new Records();
			String[] column_name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, column_name, records);
			HashMap<Integer, String[]> ctn = records.ctn;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : ctn.values()){
				array.add(i[5]);
			}
			int size=array.size();
			invalid = array.toArray(new String[size]);
			file.close();
			return invalid;
		}catch(Exception e){
			System.out.println(e);
		}
		return invalid;
	}
	
	public static String[] getUnique(String table){
		String[] unique = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile(catalogDirectory+"davisbase_columns.tbl", "rw");
			Records records = new Records();
			String[] column_name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable", "is_unique"};
			String[] comparator = {"table_name","=",table};
			filter(file, comparator, column_name, records);
			HashMap<Integer, String[]> ctn = records.ctn;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : ctn.values()){
				array.add(i[6]);
			}
			int size=array.size();
			unique = array.toArray(new String[size]);
			file.close();
			return unique;
		}catch(Exception e){
			System.out.println(e);
		}
		return unique;
	}

	public static void select(String table, String[] cols, String[] comparator,String dir_s){
		try{
			String path = user_data ;
			if (table.equalsIgnoreCase("davisbase_tables") || table.equalsIgnoreCase("davisbase_columns"))
				path = catalogDirectory ;
			
			RandomAccessFile file = new RandomAccessFile(path+table+".tbl", "rw");
			String[] column_name = getColumnName(table);
			String[] type = getDataType(table);
			
			Records records = new Records();
			
			if (comparator.length > 0 && comparator[1].equals("=") && comparator[2].equalsIgnoreCase("null")) 
			{
				System.out.println("Empty Set");
				return ;
			}
			
			if (comparator.length > 0 && comparator[1].equals("!=") && comparator[2].equalsIgnoreCase("null")) 
			{
				comparator = new String[0];
			}
			
			filter(file, comparator, column_name, type, records);
			records.display(cols); 
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}


	public static void filter(RandomAccessFile file, String[] comparator, String[] column_name, String[] type, Records records){
		try{
			
			int num_of_pages = pages(file);
			
			for(int page = 1; page <= num_of_pages; page++){
				
				file.seek((page-1)*sizeOfpage);
				byte page_type = file.readByte();
				
					if(page_type == 0x0D){
						
					byte numOfCells = BTree.getNumberofCell(file, page);

					 for(int i=0; i < numOfCells; i++){
						long loc = BTree.getLocationcell(file, page, i);
						String[] values = reedemValues(file, loc);
						int rowid=Integer.parseInt(values[0]);
						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								values[j] = "'"+values[j]+"'";
						
						boolean check = comparatorChecker(values, rowid , comparator, column_name);

						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								values[j] = values[j].substring(1, values[j].length()-1);

						if(check)
							records.add(rowid, values);
					 }
				   }
				    else
						continue;
			}

			records.column_name = column_name;
			records.layout = new int[column_name.length];

		}catch(Exception e){
			System.out.println("Error found at filter");
			e.printStackTrace();
		}

	}


	public static void filter(RandomAccessFile file, String[] comparator, String[] column_name, Records records){
		try{
			
			int num_of_pages = pages(file);
			for(int page = 1; page <= num_of_pages; page++){
				
				file.seek((page-1)*sizeOfpage);
				byte page_type = file.readByte();
				if(page_type == 0x0D)
				{
					byte numOfCells = BTree.getNumberofCell(file, page);

					for(int i=0; i < numOfCells; i++){
						
						long loc = BTree.getLocationcell(file, page, i);	
						String[] values = reedemValues(file, loc);
						int rowid=Integer.parseInt(values[0]);

						boolean check = comparatorChecker(values, rowid, comparator, column_name);
						
						if(check)
							records.add(rowid, values);
					}
				}
				else
					continue;
			}

			records.column_name = column_name;
			records.layout = new int[column_name.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}


	
	public static boolean comparatorChecker(String[] values, int rowid, String[] comparator, String[] column_name){

		boolean check = false;
		
		if(comparator.length == 0){
			check = true;
		}
		else{
			int column_position = 1;
			for(int i = 0; i < column_name.length; i++){
				if(column_name[i].equals(comparator[0])){
					column_position = i + 1;
					break;
				}
			}
			
			if(column_position == 1){
				int val = Integer.parseInt(comparator[2]);
				String operator = comparator[1];
				switch(operator){
					case "=": if(rowid == val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= val) 
						        check = true;
					          else
					  	        check = false;	
					          break;
					case "<": if(rowid < val) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<=": if(rowid <= val) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "!=": if(rowid != val)  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(comparator[2].equals(values[column_position-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}
	
}


