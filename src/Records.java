// import packages
import java.util.HashMap;

class Records{
	
	public int numberOfrows; 
	public HashMap<Integer, String[]> ctn;
	public String[] column_name; 
	public int[] layout; 
	
	public Records(){
		numberOfrows = 0;
		ctn = new HashMap<Integer, String[]>();
	}

	public void add(int rowid, String[] val){
		ctn.put(rowid, val);
		numberOfrows = numberOfrows + 1;
	}

	public void updateLayout(){
		for(int i = 0; i < layout.length; i++)
			layout[i] = column_name[i].length();
		for(String[] i : ctn.values())
			for(int j = 0; j < i.length; j++)
				if(layout[j] < i[j].length())
					layout[j] = i[j].length();
	}

	public String fix(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}


	public void display(String[] col){
		
		if(numberOfrows == 0){
			System.out.println("Empty set.");
		}
		else{
			updateLayout();
			
			if(col[0].equals("*")){
				
				for(int l: layout)
					System.out.print(DavisBase.line("-", l+3));
				
				System.out.println();
				
				for(int i = 0; i< column_name.length; i++)
					System.out.print(fix(layout[i], column_name[i])+"|");
				
				System.out.println();
				
				for(int l: layout)
					System.out.print(DavisBase.line("-", l+3));
				
				System.out.println();

				for(String[] i : ctn.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(layout[j], i[j])+"|");
					System.out.println();
				}
			
			}
			else{
				int[] ctrl = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < column_name.length; i++)
						if(col[j].equals(column_name[i]))
							ctrl[j] = i;

				for(int j = 0; j < ctrl.length; j++)
					System.out.print(DavisBase.line("-", layout[ctrl[j]]+3));
				
				System.out.println();
				
				for(int j = 0; j < ctrl.length; j++)
					System.out.print(fix(layout[ctrl[j]], column_name[ctrl[j]])+"|");
				
				System.out.println();
				
				for(int j = 0; j < ctrl.length; j++)
					System.out.print(DavisBase.line("-", layout[ctrl[j]]+3));
				
				System.out.println();
				
				for(String[] i : ctn.values()){
					for(int j = 0; j < ctrl.length; j++)
						System.out.print(fix(layout[ctrl[j]], i[ctrl[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}