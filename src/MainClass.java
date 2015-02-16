import java.io.IOException;


public class MainClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Master objMaster=new Master();
		objMaster.setMasterObject(objMaster);
		try {
			objMaster.getInput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("***ERROR: while reading input from file");
			e.printStackTrace();
		}
		objMaster.masterThread("Master");
	}
}