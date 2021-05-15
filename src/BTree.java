// import packages
import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;

public class BTree{
	public static int page_size = 512;
	public static final String date_format = "yyyy-MM-dd_HH:mm:ss";
	public static String catalogDirectory = "data/catalog/";
	public static String user_created_table = "data/user_created_table/";

	
	public static short compute_payload(String[] values, String[] dataType){
		int val = dataType.length; 
		for(int i = 1; i < dataType.length; i++){
			String dt = dataType[i];
			switch(dt){
				case "TINYINT":
					val = val + 1;
					break;
				case "SMALLINT":
					val = val + 2;
					break;
				case "INT":
					val = val + 4;
					break;
				case "BIGINT":
					val = val + 8;
					break;
				case "REAL":
					val = val + 4;
					break;		
				case "DOUBLE":
					val = val + 8;
					break;
				case "DATETIME":
					val = val + 8;
					break;
				case "DATE":
					val = val + 8;
					break;
				case "TEXT":
					String text = values[i];
					int len = text.length();
					val = val + len;
					break;
				default:
					break;
			}
		}
		return (short)val;
	}

	public static int construct_inner_page(RandomAccessFile file){
		int num_of_pages = 0;
		try{
			num_of_pages = (int)(file.length()/(new Long(page_size)));
			num_of_pages = num_of_pages + 1;
			file.setLength(page_size * num_of_pages);
			file.seek((num_of_pages-1)*page_size);
			file.writeByte(0x05); 
		}catch(Exception e){
			System.out.println(e);
		}

		return num_of_pages;
	}

		public static int search_mid_key(RandomAccessFile file, int page){
		int val = 0;
		try{
			file.seek((page-1)*page_size);
			byte page_type = file.readByte();
			int num_of_cells = getNumberofCell(file, page);
			int mid = (int) Math.ceil((double) num_of_cells / 2);
			long loc = getLocationcell(file, page, mid-1);
			file.seek(loc);

			switch(page_type){
				case 0x05:
					file.readInt(); 
					val = file.readInt();
					break;
				case 0x0D:
					file.readShort();
					val = file.readInt();
					break;
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static int construct_leaf_page(RandomAccessFile file){
		int num_of_pages = 0;
		try{
			num_of_pages = (int)(file.length()/(new Long(page_size)));
			num_of_pages = num_of_pages + 1;
			file.setLength(page_size * num_of_pages);
			file.seek((num_of_pages-1)*page_size);
			file.writeByte(0x0D); 
		}catch(Exception e){
			System.out.println(e);
		}

		return num_of_pages;

	}

	
	public static void leaf_page_split(RandomAccessFile file, int curr_page, int new_page){
		try{
			
			int num_of_cells = getNumberofCell(file, curr_page);
			
			int mid = (int) Math.ceil((double) num_of_cells / 2);

			int num_cell_A = mid - 1;
			int num_cell_B = num_of_cells - num_cell_A;
			int content = 512;

			for(int i = num_cell_A; i < num_of_cells; i++){
				long loc = getLocationcell(file, curr_page, i);
				file.seek(loc);
				int cell_size = file.readShort()+6;
				content = content - cell_size;
				file.seek(loc);
				byte[] cell = new byte[cell_size];
				file.read(cell);
				file.seek((new_page-1)*page_size+content);
				file.write(cell);
				setOffsetCell(file, new_page, i - num_cell_A, content);
			}

			
			file.seek((new_page-1)*page_size+2);
			file.writeShort(content);

			
			short offset = getOffsetCell(file, curr_page, num_cell_A-1);
			file.seek((curr_page-1)*page_size+2);
			file.writeShort(offset);

			
			int rightMost = get_rightMost(file, curr_page);
			set_rightMost(file, new_page, rightMost);
			set_rightMost(file, curr_page, new_page);

			
			int parent = getParent(file, curr_page);
			setParent(file, new_page, parent);

			
			byte num = (byte) num_cell_A;
			setNumCell(file, curr_page, num);
			num = (byte) num_cell_B;
			setNumCell(file, new_page, num);
			
		}catch(Exception e){
			System.out.println(e);
			
		}
	}
	
	public static void inner_split_page(RandomAccessFile file, int curr_page, int new_page){
		try{
			
			int num_of_cells = getNumberofCell(file, curr_page);
			
			int mid = (int) Math.ceil((double) num_of_cells / 2);

			int num_cell_A = mid - 1;
			int num_cell_B = num_of_cells - num_cell_A - 1;
			short content = 512;

			for(int i = num_cell_A+1; i < num_of_cells; i++){
				long loc = getLocationcell(file, curr_page, i);
				short cell_size = 8;
				content = (short)(content - cell_size);
				file.seek(loc);
				byte[] cell = new byte[cell_size];
				file.read(cell);
				file.seek((new_page-1)*page_size+content);
				file.write(cell);
				file.seek(loc);
				int page = file.readInt();
				setParent(file, page, new_page);
				setOffsetCell(file, new_page, i - (num_cell_A + 1), content);
			}
			
			int tmp = get_rightMost(file, curr_page);
			set_rightMost(file, new_page, tmp);
			
			long midLoc = getLocationcell(file, curr_page, mid - 1);
			file.seek(midLoc);
			tmp = file.readInt();
			set_rightMost(file, curr_page, tmp);
			
			file.seek((new_page-1)*page_size+2);
			file.writeShort(content);
			
			short offset = getOffsetCell(file, curr_page, num_cell_A-1);
			file.seek((curr_page-1)*page_size+2);
			file.writeShort(offset);

			
			int parent = getParent(file, curr_page);
			setParent(file, new_page, parent);
			
			byte num = (byte) num_cell_A;
			setNumCell(file, curr_page, num);
			num = (byte) num_cell_B;
			setNumCell(file, new_page, num);
			
		}catch(Exception e){
			System.out.println(e);
		}
	}

	
	public static void leaf_split(RandomAccessFile file, int page){
		int new_page = construct_leaf_page(file);
		int mid_key = search_mid_key(file, page);
		leaf_page_split(file, page, new_page);
		int parent = getParent(file, page);
		if(parent == 0){
			int root_page = construct_inner_page(file);
			setParent(file, page, root_page);
			setParent(file, new_page, root_page);
			set_rightMost(file, root_page, new_page);
			cell_inner_insert(file, root_page, page, mid_key);
		}else{
			long ploc = getLocationPointer(file, page, parent);
			setLocationPointer(file, ploc, parent, new_page);
			cell_inner_insert(file, parent, page, mid_key);
			sortArraycell(file, parent);
			while(check_inner_space(file, parent)){
				parent = inner_split(file, parent);
			}
		}
	}

	public static int inner_split(RandomAccessFile file, int page){
		int new_page = construct_inner_page(file);
		int mid_key = search_mid_key(file, page);
		inner_split_page(file, page, new_page);
		int parent = getParent(file, page);
		if(parent == 0){
			int root_page = construct_inner_page(file);
			setParent(file, page, root_page);
			setParent(file, new_page, root_page);
			set_rightMost(file, root_page, new_page);
			cell_inner_insert(file, root_page, page, mid_key);
			return root_page;
		}else{
			long ploc = getLocationPointer(file, page, parent);
			setLocationPointer(file, ploc, parent, new_page);
			cell_inner_insert(file, parent, page, mid_key);
			sortArraycell(file, parent);
			return parent;
		}
	}

	
	public static void sortArraycell(RandomAccessFile file, int page){
		 byte num = getNumberofCell(file, page);
		 int[] array_key = getArraykey(file, page);
		 short[] array_cell = getArraycell(file, page);
		 int left_tmp;
		 short right_tmp;

		 for (int i = 1; i < num; i++) {
            for(int j = i ; j > 0 ; j--){
                if(array_key[j] < array_key[j-1]){

                    left_tmp = array_key[j];
                    array_key[j] = array_key[j-1];
                    array_key[j-1] = left_tmp;

                    right_tmp = array_cell[j];
                    array_cell[j] = array_cell[j-1];
                    array_cell[j-1] = right_tmp;
                }
            }
         }

         try{
         	file.seek((page-1)*page_size+12);
         	for(int i = 0; i < num; i++){
				file.writeShort(array_cell[i]);
			}
         }catch(Exception e){
         	System.out.println("Error found in sortArraycell");
         }
	}

	public static int[] getArraykey(RandomAccessFile file, int page){
		int num = new Integer(getNumberofCell(file, page));
		int[] array = new int[num];

		try{
			file.seek((page-1)*page_size);
			byte page_type = file.readByte();
			byte offset = 0;
			switch(page_type){
			    case 0x0d:
				    offset = 2;
				    break;
				case 0x05:
					offset = 4;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num; i++){
				long loc = getLocationcell(file, page, i);
				file.seek(loc+offset);
				array[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return array;
	}
	
	public static short[] getArraycell(RandomAccessFile file, int page){
		int num = new Integer(getNumberofCell(file, page));
		short[] array = new short[num];

		try{
			file.seek((page-1)*page_size+12);
			for(int i = 0; i < num; i++){
				array[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return array;
	}

	
	public static long getLocationPointer(RandomAccessFile file, int page, int parent){
		long val = 0;
		try{
			int num_of_cells = new Integer(getNumberofCell(file, parent));
			for(int i=0; i < num_of_cells; i++){
				long loc = getLocationcell(file, parent, i);
				file.seek(loc);
				int childPage = file.readInt();
				if(childPage == page){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setLocationPointer(RandomAccessFile file, long loc, int parent, int page){
		try{
			if(loc == 0){
				file.seek((parent-1)*page_size+4);
			}else{
				file.seek(loc);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println(e);
		}
	} 

	
	public static void cell_inner_insert(RandomAccessFile file, int page, int child, int key){
		try{
			
			file.seek((page-1)*page_size+2);
			short content = file.readShort();
			
			if(content == 0)
				content = 512;
			
			content = (short)(content - 8);
			
			file.seek((page-1)*page_size+content);
			file.writeInt(child);
			file.writeInt(key);
			
			file.seek((page-1)*page_size+2);
			file.writeShort(content);
			
			byte num = getNumberofCell(file, page);
			setOffsetCell(file, page ,num, content);
			
			num = (byte) (num + 1);
			setNumCell(file, page, num);

		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void insertLeafcell(RandomAccessFile file, int page, int offset, short pageloc_size, int key, byte[] sc, String[] values){
		try{
			String s;
			file.seek((page-1)*page_size+offset);
			file.writeShort(pageloc_size);
			file.writeInt(key);
			int col = values.length - 1;
			file.writeByte(col);
			file.write(sc);
			for(int i = 1; i < values.length; i++){
				switch(sc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(values[i]));
						break;
					case 0x05:
						file.writeShort(new Short(values[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(values[i]));
						break;
					case 0x07:
						file.writeLong(new Long(values[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(values[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(values[i]));
						break;
					case 0x0A:
						s = values[i];
						Date temp = new SimpleDateFormat(date_format).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = values[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(date_format).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(values[i]);
						break;
				}
			}
			int n = getNumberofCell(file, page);
			byte tmp = (byte) (n+1);
			setNumCell(file, page, tmp);
			file.seek((page-1)*page_size+12+n*2);
			file.writeShort(offset);
			file.seek((page-1)*page_size+2);
			int content = file.readShort();
			if(content >= offset || content == 0){
				file.seek((page-1)*page_size+2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void updateLeafcell(RandomAccessFile file, int page, int offset, int pageloc_size, int key, byte[] sc, String[] values){
		try{
			String s;
			file.seek((page-1)*page_size+offset);
			file.writeShort(pageloc_size);
			file.writeInt(key);
			int col = values.length - 1;
			file.writeByte(col);
			file.write(sc);
			for(int i = 1; i < values.length; i++){
				switch(sc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(values[i]));
						break;
					case 0x05:
						file.writeShort(new Short(values[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(values[i]));
						break;
					case 0x07:
						file.writeLong(new Long(values[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(values[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(values[i]));
						break;
					case 0x0A:
						s = values[i];
						Date temp = new SimpleDateFormat(date_format).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = values[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(date_format).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(values[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}

	
	public static boolean check_inner_space(RandomAccessFile file, int page){
		byte num_of_cells = getNumberofCell(file, page);
		if(num_of_cells > 30)
			return true;
		else
			return false;
	}

	public static int check_leaf_space(RandomAccessFile file, int page, int size){
		int val = -1;

		try{
			file.seek((page-1)*page_size+2);
			int content = file.readShort();
			if(content == 0)
				return page_size - size;
			int num_of_cells = getNumberofCell(file, page);
			int space = content - 20 - 2*num_of_cells;
			if(size < space)
				return content - size;
			
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	
	public static int getParent(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*page_size+8);
			val = file.readInt();
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setParent(RandomAccessFile file, int page, int parent){
		try{
			file.seek((page-1)*page_size+8);
			file.writeInt(parent);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static int get_rightMost(RandomAccessFile file, int page){
		int rl = 0;

		try{
			file.seek((page-1)*page_size+4);
			rl = file.readInt();
		}catch(Exception e){
			System.out.println("Error Found in get_rightMost");
		}

		return rl;
	}

	public static void set_rightMost(RandomAccessFile file, int page, int rightLeaf){

		try{
			file.seek((page-1)*page_size+4);
			file.writeInt(rightLeaf);
		}catch(Exception e){
			System.out.println("Error found in set_rightMost");
		}

	}

	public static boolean hasKey(RandomAccessFile file, int page, int key){
		int[] keys = getArraykey(file, page);
		for(int i : keys)
			if(key == i)
				return true;
		return false;
	}
	
	public static long getLocationcell(RandomAccessFile file, int page, int id){
		long loc = 0;
		try{
			file.seek((page-1)*page_size+12+id*2);
			short offset = file.readShort();
			long orig = (page-1)*page_size;
			loc = orig + offset;
		}catch(Exception e){
			System.out.println(e);
		}
		return loc;
	}

	public static byte getNumberofCell(RandomAccessFile file, int page){
		byte val = 0;

		try{
			file.seek((page-1)*page_size+1);
			val = file.readByte();
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setNumCell(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*page_size+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static byte getPagetype(RandomAccessFile file, int page){
		byte type=0x05;
		try {
			file.seek((page-1)*page_size);
			type = file.readByte();
		} catch (Exception e) {
			System.out.println(e);
		}
		return type;
	}

	public static short getOffsetCell(RandomAccessFile file, int page, int id){
		short offset = 0;
		try{
			file.seek((page-1)*page_size+12+id*2);
			offset = file.readShort();
		}catch(Exception e){
			System.out.println(e);
		}
		return offset;
	}

	public static void setOffsetCell(RandomAccessFile file, int page, int id, int offset){
		try{
			file.seek((page-1)*page_size+12+id*2);
			file.writeShort(offset);
		}catch(Exception e){
			System.out.println(e);
		}
	}
    

}















