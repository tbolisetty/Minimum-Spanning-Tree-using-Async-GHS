import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Master implements Runnable {
	// Variables copied from synchronous project
	int  iPID[];
	float iGraph[][];
	int iNumberOfProcessCompleted, iNumberOfProcesses;
	public Process[] oProc;
	private Scanner in;
	private static String[] sArrProcessPulseStatus;
	String[] sArrStatusValues = { "PulseCompleted", "PulseNotCompleted",
	"ProcessCompleted" };
	boolean bAllProcessesReplied, bInWaitState;
	String sStatus;
	Thread tMaster;
	Master oMaster;

	// Variables created in asynchronous project
	Long iPulseNumber;
	int iPulseWidth;

	// Methods copied from synchronous project
	public Master() {
		// set the initial status to RoundCompleted
		sStatus = sArrStatusValues[0];
		// declare the scanner object
		in = new Scanner(System.in);
		iNumberOfProcessCompleted = 0;
		bAllProcessesReplied = false;
		bInWaitState = false;
		iPulseNumber = 0L;
		iPulseWidth = 10;
	}

	public void setMasterObject(Master oMas) {
		this.oMaster = oMas;
	}

	public void masterThread(String sThreadName) {
		tMaster = new Thread(this, sThreadName);
		tMaster.start();
	}

	public void getInput() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("Test.txt"));
		ArrayList list = new ArrayList();
		String line = null;
		int iCount = 0;
		int xCount = 0;

		int gCount = 0;
		line = reader.readLine();
		String[] parts = line.split("\\s");
		iNumberOfProcesses = Integer.parseInt(parts[gCount++]);
		line = reader.readLine();
		line.replaceAll("\\t+", "");
		parts = line.split("\\t+");
		
		iPID = new int[iNumberOfProcesses];
		iGraph = new float[iNumberOfProcesses][iNumberOfProcesses];
		for (; xCount < iNumberOfProcesses; xCount++) {
			iPID[xCount] = Integer.parseInt(parts[xCount]);
		}
		while ((line = reader.readLine()) != null) {
			parts = line.split("\\t+");
			for (int jCount = 0; jCount < iNumberOfProcesses; jCount++)
				iGraph[iCount][jCount] = Float.parseFloat(parts[jCount]);
			iCount++;
		}

		// calling initializeProcess() method
		initalizeProcesses();
	}

	// Initialize all the processes with their PID and their neighbors
	public void initalizeProcesses() {
		Process[] p;
		oProc = new Process[iNumberOfProcesses];
		int iDeclareProcess = 1, iNumberOfNbrs = 0, iRow, iPrs = 1;
		float arrNbrs[][];
		// initialize the processes
		// send them their neighbor PID's
		for (iDeclareProcess = 1; iDeclareProcess <= iNumberOfProcesses; iDeclareProcess++) {
			iNumberOfNbrs = 0;
			// count the number of neighbors
			for (iPrs = 1; iPrs <= iNumberOfProcesses; iPrs++) {
				if (iGraph[iDeclareProcess - 1][iPrs - 1] > 0) {
					iNumberOfNbrs++;
				}
			}
			// initialize the array that will be sent to the Process
			// column 0-process index id
			// column 1-process pid
			// column 2-edge weight
			arrNbrs = new float [iNumberOfNbrs][3];
			iRow = 0;
			for (iPrs = 1; iPrs <= iNumberOfProcesses; iPrs++) {
				if (iGraph[iDeclareProcess - 1][iPrs - 1] > 0) {
					arrNbrs[iRow][0] = iPrs;
					arrNbrs[iRow][1] = iPID[iPrs - 1];
					arrNbrs[iRow][2] = iGraph[iDeclareProcess - 1][iPrs - 1];
					iRow++;
				}
			}
			// creating the process
			oProc[iDeclareProcess - 1] = new Process(iPID[iDeclareProcess - 1],
					iDeclareProcess, arrNbrs);
		}
		// send each process about how to communicate to their neighbors
		// send an array of neighbor objects to the process
		// first count, how many number of neighbors each process has. Then add
		// each neighbor object to the array and send it to the Process.
		iNumberOfNbrs = 0;
		for (iDeclareProcess = 1; iDeclareProcess <= iNumberOfProcesses; iDeclareProcess++) {
			iNumberOfNbrs = 0;
			for (iPrs = 1; iPrs <= iNumberOfProcesses; iPrs++) {
				if (iGraph[iDeclareProcess - 1][iPrs - 1] > 0) {
					iNumberOfNbrs++;
				}
			}
			p = new Process[iNumberOfNbrs];
			iRow = 0;
			for (iPrs = 1; iPrs <= iNumberOfProcesses; iPrs++) {
				if (iGraph[iDeclareProcess - 1][iPrs - 1] > 0) {
					p[iRow] = oProc[iPrs - 1];
					iRow++;
				}
			}
			oProc[iDeclareProcess - 1].setNeighborObjects(p);
			// send the reference of the master to each process
			oProc[iDeclareProcess - 1].setMaster(oMaster);
		}
	}

	// reset the sArrProcessPulseStatus to False at the end of each round
	// make the static variable to zero
	private void resetProcessPulseStatus(String sStatus) {
		// TODO Auto-generated method stub
		int iCount = 1;
		for (iCount = 1; iCount <= iNumberOfProcesses; iCount++) {
			sArrProcessPulseStatus[iCount - 1] = sStatus;
		}
		iNumberOfProcessCompleted = 0;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int iCount;
		try {
			tMaster.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Start of Master thread");
		// Initiate each thread
		for (iCount = 1; iCount <= iNumberOfProcesses; iCount++) {
			oProc[iCount - 1].processThread("T" + iPID[iCount - 1]);
			//			oProc[iCount - 1].printNeighborPID();
		}
		// Pass the control to controlPulse
		controlPulse();
		System.out.println("\n\n\nID		Parent		Children");
		for(iCount=1;iCount<=iNumberOfProcesses;iCount++){
			oProc[iCount-1].printYourTree();
		}
		System.out.println("Master thread terminated");
	}

	public synchronized void setProcessPulseStatus(int iIndex) {
		System.out.println("Start of fucntion: setProcessRoundStatus");
		//		sArrProcessPulseStatus[iIndex - 1] = sStatus_temp;
		System.out.println("End of fucntion: setProcessRoundStatus");
		iNumberOfProcessCompleted++;
	}

	// Methods created in asynchronous project
	private synchronized void generatePulse() {
		iPulseNumber++;
	}

	// Control the pulse

	public void controlPulse() {
		int iCount;
		while(iNumberOfProcessCompleted != iNumberOfProcesses){
			generatePulse();
			informPulseToProcesses();
			try {
				// System.out.println("Master entering sleep in controlPulse()");
				tMaster.sleep(iPulseWidth);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("***Error in controlPulse of Master");
			}
		}
	}

	public void informPulseToProcesses() {
		// System.out.println("Start of informPulseToProcesses(): Master Thread");
		int iCount = 0;
		for (iCount = 1; iCount <= iNumberOfProcesses; iCount++) {
			oProc[iCount - 1].receivePulse(iPulseNumber);
		}
		// System.out.println("End of informPulseToProcesses(): Master Thread");
	}
}