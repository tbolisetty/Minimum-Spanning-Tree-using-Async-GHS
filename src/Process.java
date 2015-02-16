import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Random;

public class Process implements Runnable {
	int iNumberOfNbrs, iMyPID, iMyLevel, iRowToFind, iNumberOfChildren, iReceiveFindMWOE_ConvergeCast;
	int iReceiveNewLeader_ConvergeCast, iNewLeader_Broadcast;
	float iMaxWeight;
	float[] iComponenetID=new float[3];
	float[] iMWOE_ComponentID=new float[3];
	float[] iMyMWOE_ComponentID=new float[3];
	String sMyStatus=new String();
	String[] sStatusValues=new String[3];
	Long iCurrentPulse;
	HashMap<Long,ArrayList> hMessage=new HashMap<Long,ArrayList>();
	HashMap<Integer, Integer> hNeighborIndex=new HashMap<Integer, Integer>();
	Long[] arrNbrLatestPulse;
	Random randomGenerator = new Random();
	boolean bAnyNewChildAtTheEndOfThisRound, bReceiveFindMWOE_Broadcast, bReceiveFindMWOE_ConvergeCast;
	boolean bReceiveConnectMWOE_Broadcast, bShouldIConnect;
	boolean bWaitForReply_RequestForConnect, breceiveNewLeader_Broadcast, breceiveNewLeader_ConvergeCast;
	boolean bShouldICopyToNewObject, bRepliedForMergeMessage, bRepliedToCoreEdgeProcess;
	boolean bTerminate;
	PriorityQueue<Message> pqDeferredMessages=new PriorityQueue<>(levelComparator);
	String sWaitForMWOEReply=new String();
	String[] sWaitForMWOEReply_Values=new String[4];
	String sMyCurrentState=new String();
	//		Copied from Synchronous project
	float[][] mCon;
	Thread tProcess;
	Process[] oProc;
	Master oMaster;
	//		Message class
	public class Message{
		Message(){
			bShouldITerminate=false;
		}
		String sTypeOfMessage;
		int iNeighborUID, iSenderID;
		float[] arrMwoe=new float[3];
		int iLevel;
		float[] iMessageComponentID=new float[3];
		String connectionStatus=new String();
		String sShouldIFindMWOE=new String();
		boolean bShouldITerminate;
	}
	Message objConnectMWOE_Broadcast=new Message();
	Message objRequestForConnect=new Message();
	ArrayList<Message> arrMergeMessages=new ArrayList<Message>();
	Message objRequestForConnect_2=new Message();

	Process(int id, int iRowToFind,float nbrs[][]){
		//		Declare temp. variables used in constructor
		int iVar,iRow,iCount;
		//		Initialize all the process defined variables
		iMyPID=id;
		//		Initialize maxWeight
		iMaxWeight=10000;
		this.iRowToFind=iRowToFind;
		iNumberOfNbrs=nbrs.length;

		iCurrentPulse=(long) 0;
		//		Declare Array size
		//		This holds the latest pulse number at which the message has to be sent for each member
		arrNbrLatestPulse=new Long[iNumberOfNbrs];
		//		Initialize the array to zero
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			arrNbrLatestPulse[iCount-1]=0L;
		}
		//Declare number of neighboring processes
		oProc=new Process[iNumberOfNbrs];
		System.out.println("Number of neighbors for Process id-"+iMyPID+" is: "+iNumberOfNbrs);
		//		initialize the matrix with neighbor rows * 3
		mCon=new float[iNumberOfNbrs][4];
		/*  
		 * Column0 - index of nbrs
		 * Column1 - ID of nbrs
		 * Column2 is- 0 for null, 1-for parent and 2 for child. 
		 * Initially column 2 is  to null
		 * 			0-NULL
		 * 			1-Parent
		 * 			2-Child
		 * 			3-Child to be added at the end of the round
		 * 			4-Neighbor is in the same component but i have no relation with it
		 *           Converge cast is initialized to 0
		 * Column 3 has the edge cost connecting the neighbors*/

		//Copy the information sent by Master to the mCon Matrix
		iRow=1;
		for(iVar=1;iVar<=nbrs.length;iVar++){
			mCon[iRow-1][0]=nbrs[iVar-1][0];
			mCon[iRow-1][1]=nbrs[iVar-1][1];
			mCon[iRow-1][3]=nbrs[iVar-1][2];
			iRow++;
		}
		bAnyNewChildAtTheEndOfThisRound=false;
		//		Initialize the component details and the least MWOE_Component details
		iComponenetID[0]=iComponenetID[2]=0;
		iComponenetID[1]=iMyPID;
		iMWOE_ComponentID[0]=iMWOE_ComponentID[1]=iMWOE_ComponentID[2]=-1;
		iMyMWOE_ComponentID[0]=iMyMWOE_ComponentID[1]=iMyMWOE_ComponentID[2]=iMaxWeight;
		//		Initiate the values for MWOE replies
		sWaitForMWOEReply_Values[0]="wait";
		sWaitForMWOEReply_Values[1]="true";
		sWaitForMWOEReply_Values[2]="false";
		sWaitForMWOEReply_Values[3]="unknown";
		//		Initiate the sArrWaitForReply_RequestForConnect reply values
		bWaitForReply_RequestForConnect=false;
		// update the possible status for a process
		sStatusValues[0]="root";
		sStatusValues[1]="intermediate";
		sStatusValues[2]="leaf";
		//initially everybody is a root to itself
		sMyStatus=sStatusValues[0];
		//		Initialize level to 0
		iMyLevel=0;
		//		Call function to create the hash map to find Neighbor index
		createNeighborIndexHashMap();
		//		Initialize numberOfChildren to 0 and few other variables
		iNumberOfChildren=0;
		iNewLeader_Broadcast=0;
		bReceiveFindMWOE_ConvergeCast=false;
		iReceiveFindMWOE_ConvergeCast=0;
		iReceiveNewLeader_ConvergeCast=0;
		bReceiveConnectMWOE_Broadcast=false;
		bShouldIConnect=false;
		breceiveNewLeader_Broadcast=false;
		breceiveNewLeader_ConvergeCast=false;
		bRepliedForMergeMessage=false;
		bRepliedToCoreEdgeProcess=false;
		bTerminate=false;
		//initialize to unknown
		sMyCurrentState="unknown";
		bReceiveFindMWOE_Broadcast=false;
	}

	public void createNeighborIndexHashMap(){
		int iCount;
		//		Add each neighbor to the hash map
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			hNeighborIndex.put((int)mCon[iCount-1][1], iCount);
		}
	}

	public void setNeighborObjects(Process[] p){
		int iCount;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			oProc[iCount-1]=p[iCount-1];
		}
	}

	//	set the reference to master object
	public void setMaster(Master m){
		this.oMaster=m;
	}

	public void printNeighborPID(){
		System.out.println("Start of printNeighborPID in "+tProcess.getName());
		int iRow;
		System.out.println("My Process ID is-"+iMyPID);
		System.out.println("My neighbors has-");
		for(iRow=1;iRow<=iNumberOfNbrs;iRow++){ 
			System.out.print("index-" +(int)mCon[iRow-1][0]+"	");
			System.out.print("PID: "+(int)mCon[iRow-1][1]);
			System.out.println();
			oProc[iRow-1].printPID(iMyPID);
		}
		System.out.println("End of printNeighborPID in "+tProcess.getName());
	}

	private void printPID(int iSenderID) {
		// TODO Auto-generated method stub
		System.out.println("My neighbor-"+iSenderID+" called me. My PID is-"+iMyPID);
	}

	//    to print the mCon matrix
	public synchronized void printConMatrix(){
		//		System.out.println("I received "+iMessagesRcvd_count+" messages");
		System.out.println("---"+tProcess.getName()+"---");

		int iRow=1, iCol;
		for(iRow=1;iRow<=iNumberOfNbrs;iRow++){
			if(iRow==1)
			{
				System.out.println("");
			}
			System.out.println();
			for(iCol=1;iCol<=5;iCol++){
				System.out.print(mCon[iRow-1][iCol-1]+"		");
			}
		}
	}

	void reset(String sColumnToReset){
		System.out.println("Start function: reset: "+tProcess.getName());

		System.out.println("End function: reset: "+tProcess.getName());
	}

	void resetAll(){
		setNumberOfChildren();
		bReceiveFindMWOE_ConvergeCast=false;
		bAnyNewChildAtTheEndOfThisRound=false;
		bReceiveConnectMWOE_Broadcast=false;
		bShouldIConnect=false;
		bShouldIConnect=false;
		bWaitForReply_RequestForConnect=false;
		breceiveNewLeader_Broadcast=false;
		breceiveNewLeader_ConvergeCast=false;
		bRepliedForMergeMessage=false;
		bRepliedToCoreEdgeProcess=false;

		iReceiveFindMWOE_ConvergeCast=0;
		iReceiveNewLeader_ConvergeCast=0;

		iMWOE_ComponentID[0]=iMWOE_ComponentID[1]=iMWOE_ComponentID[2]=-1;
		iMyMWOE_ComponentID[0]=iMyMWOE_ComponentID[1]=iMyMWOE_ComponentID[2]=iMaxWeight;
		iNewLeader_Broadcast=0;

		sMyCurrentState="unknown";
	}

	public void processThread(String sThreadName){
		tProcess=new Thread(this,sThreadName);
		tProcess.start();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int iRun=0;
		try{
			tProcess.sleep(1000);
		}catch(Exception e){
			System.out.println("Exception caught in run() of Thread: "+tProcess.getName());
		}
		System.out.println("Start of thread: "+tProcess.getName());
		while( bTerminate == false){
			controlFlow();
			System.out.println();
		}
		oMaster.setProcessPulseStatus(iRowToFind);
		System.out.println("End of thread: "+tProcess.getName());
	}

	//----------------------------------------------------------------------------------------------------------------------
	//		Methods created in asynchronous project

	public void controlFlow(){
		System.out.println("Start of controlFlow() of Thread:"+tProcess.getName());
		//		Reset all your parameters before the start of this round
		resetAll();

		//check if you have any 3
		//	if yes, add it as a child
		//	ask him to find MWOE
		sendRepliesToDeferredMessages();
		if(checkForPendingNeighbours()){
			send3NeighboursToFindMWOE(false);
			addPendingNeighbor();
			setMyStatus();
			setNumberOfChildren();
			//		send a message to find MWOE
		}
		String sTempStatus=getMyStatus();
		//If you are a root, initiate the broadcast message
		if(sTempStatus.equals("root")){
			bReceiveFindMWOE_Broadcast=false;
			sendFindMWOE_Broadcast();
			findMWOE();
			waitForMWOE_ConvergeCast();
			copyMWOEToBroadcast();
			sendMWOEConnect_Broadcast();
			if(checkToTerminate()==false){
				checkIfHaveToConnect();
				controlFlow_TillEndOfPhase();
			}
			else{
				System.out.println("Terminating. Thread:"+tProcess.getName());
				bTerminate=true;
			}

		}
		else if(sTempStatus.equals("intermediate")){
			waitForFindMWOE_Broadcast();
			bReceiveFindMWOE_Broadcast=false;
			sendFindMWOE_Broadcast();
			findMWOE();
			waitForMWOE_ConvergeCast();
			sendMWOE_ConvergeCast();
			waitForMWOEConnect_Broadcast();
			sendMWOEConnect_Broadcast();
			if(checkToTerminate() == false){
				checkIfHaveToConnect();
				controlFlow_TillEndOfPhase();
			}
			else{
				System.out.println("Terminating. Thread:"+tProcess.getName());
				bTerminate=true;
			}
		}
		else{
			waitForFindMWOE_Broadcast();
			bReceiveFindMWOE_Broadcast=false;
			findMWOE();
			sendMWOE_ConvergeCast();
			waitForMWOEConnect_Broadcast();
			if(checkToTerminate() == false){
				checkIfHaveToConnect();
				controlFlow_TillEndOfPhase();
			}
			else{
				System.out.println("Terminating. Thread:"+tProcess.getName());
				bTerminate=true;
			}
		}
		if(checkForPendingNeighbours()){
			send3NeighboursToFindMWOE(true);
			addPendingNeighbor();
			setMyStatus();
			setNumberOfChildren();
			//		send a message to find MWOE
		}
		System.out.println("End of controlFlow() of Thread:"+tProcess.getName());
	}

	public void send3NeighboursToFindMWOE(boolean bLocalTerminate){
		Message objMessage = new Message();
		objMessage.iLevel=iMyLevel;
		objMessage.iMessageComponentID[0]=iComponenetID[0];
		objMessage.iMessageComponentID[1]=iComponenetID[1];
		objMessage.iMessageComponentID[2]=iComponenetID[2];
		objMessage.sShouldIFindMWOE="true";
		objMessage.connectionStatus="merge";
		objMessage.bShouldITerminate=bLocalTerminate;
		for(int iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==3){
				scheduleMessage(createMessage("newLeader", (int)mCon[iCount-1][1], objMessage));
			}
		}
	}
	
	public void controlFlow_TillEndOfPhase(){
		boolean bContinueToAbsorb=true;
		System.out.println("In controlFlow_TillEndOfPhase in Thread:"+tProcess.getName());
		//		Check if you have merge messages
		replyForPendingMergeMessages();
		addPendingNeighbor();
		sMyCurrentState="waitingForRequestConnect";

		if(bShouldIConnect==true){
			//		connect with the process on the other side
			if(bRepliedForMergeMessage==false){
				connectWithProcessOfOtherComponent();
				System.out.println("I sent my requestForConnect message. Thread:"+tProcess.getName());
				sMyCurrentState="waitingForRequestConnect";
				waitForReply_requestForConnect();
			}
			else{
				sMyCurrentState="waitingForRequestConnect";
			}
			//			sMyCurrentState="unknown";
			if(objRequestForConnect.connectionStatus.equals("absorb")==true){
				System.out.println("I'm in Absorb. Thread:"+tProcess.getName());
				//		broadcast the message to your old component trees
				iNewLeader_Broadcast++;
				addPendingNeighbor();
				resetOldParentToChild(); //Make you parent as your child now
				updateComponentDetails();
				setNumberOfChildren();
				setYourNewLevel();
				sendRepliesToDeferredMessages();
				setMyStatus();
				broadcastNewLeaderDetails();
				waitForNewLeader_ConvergeCast();
				setYourNewParent(objRequestForConnect.iSenderID);
				controlFlow_TillNextMerge();
			}
			else if(objRequestForConnect.connectionStatus.equals("wait")==true){
				//		wait till you get a reply
				bWaitForReply_RequestForConnect=false;
				waitForReply_requestForConnect();
				if(objRequestForConnect.connectionStatus.equals("absorb")==true){
					iNewLeader_Broadcast++;
					addPendingNeighbor();
					System.out.println("I'm in Wait-->Absorb. Thread:"+tProcess.getName());
					resetOldParentToChild();//Make you parent as your child now
					updateComponentDetails();
					setNumberOfChildren();
					setYourNewLevel();
					sendRepliesToDeferredMessages();
					setMyStatus();
					broadcastNewLeaderDetails();
					waitForNewLeader_ConvergeCast();
					setYourNewParent(objRequestForConnect.iSenderID);
					controlFlow_TillNextMerge();
				}
				else if(objRequestForConnect.connectionStatus.equals("merge")==true){
					System.out.println("I'm in Wait-->Merge. Thread:"+tProcess.getName());
					//		Decide who is the leader
					waitUntilYouReplyToCoreEdgeProcess();
					addPendingNeighbor();
					int iMinID=(int)objRequestForConnect.iMessageComponentID[1], iMaxID;
					if(iMinID>(int)objRequestForConnect.iMessageComponentID[2]){
						iMaxID=iMinID;
						iMinID=(int)objRequestForConnect.iMessageComponentID[2];
					}
					else{
						iMaxID=(int)objRequestForConnect.iMessageComponentID[2];
					}

					if(iMyPID==iMinID){
						//I'm the leader
						// wait until you merge
						resetOldParentToChild();
						setNewChild(iMaxID);
						setMyStatus();
						setNumberOfChildren();
						//		Increase the level
						iMyLevel++;
						//		Update the component ID
						iComponenetID[0]=iMyMWOE_ComponentID[0];
						iComponenetID[1]=iMinID;
						iComponenetID[2]=iMaxID;
						//		Broadcast the new leader message
						//		create objRequestForConnect details
						objRequestForConnect.iLevel=iMyLevel;
						objRequestForConnect.iMessageComponentID[0]=iComponenetID[0];
						objRequestForConnect.iMessageComponentID[1]=iComponenetID[1];
						objRequestForConnect.iMessageComponentID[2]=iComponenetID[2];
						objRequestForConnect.sShouldIFindMWOE="false";

						broadcastNewLeaderDetails();
						waitForNewLeader_ConvergeCast();
						//		wait for converge cast
					}
					else{
						//I'm not the leader
						addPendingNeighbor();
						waitForNewLeader_Broadcast();
						resetOldParentToChild();//		Will make your old parent to your child
						setYourNewLevel();
						setYourNewParent(objRequestForConnect.iSenderID); //		Will set your new parent
						updateComponentDetails();
						setNumberOfChildren();
						setMyStatus();
						broadcastNewLeaderDetails();
						waitForNewLeader_ConvergeCast();
						breceiveNewLeader_ConvergeCast=false;
						sendNewLeader_ConvergeCast();
						//		wait for broadcast
						//		wait for converge cast
						//		fwd. the converge cast to the new leader
					}
				}
			}

			else if(objRequestForConnect.connectionStatus.equals("merge")){
				System.out.println("I'm in Merge. Thread:"+tProcess.getName());
				waitUntilYouReplyToCoreEdgeProcess();
				addPendingNeighbor();
				//		Decide who is the leader
				int iMinID=(int)objRequestForConnect.iMessageComponentID[1], iMaxID;
				if(iMinID>objRequestForConnect.iMessageComponentID[2]){
					iMaxID=iMinID;
					iMinID=(int)objRequestForConnect.iMessageComponentID[2];
				}
				else{
					iMaxID=(int)objRequestForConnect.iMessageComponentID[2];
				}

				if(iMyPID==iMinID){
					//I'm the leader
					resetOldParentToChild();
					setNewChild(iMaxID);
					setMyStatus();
					setNumberOfChildren();
					//		Increase the level
					iMyLevel++;
					//		Update the component ID
					iComponenetID[0]=iMyMWOE_ComponentID[0];
					iComponenetID[1]=iMinID;
					iComponenetID[2]=iMaxID;
					//		Add the pending children
					//		Broadcast the new leader message
					//		create objRequestForConnect details
					objRequestForConnect.iLevel=iMyLevel;
					objRequestForConnect.iMessageComponentID[0]=iComponenetID[0];
					objRequestForConnect.iMessageComponentID[1]=iComponenetID[1];
					objRequestForConnect.iMessageComponentID[2]=iComponenetID[2];

					objRequestForConnect.sShouldIFindMWOE="false";

					broadcastNewLeaderDetails();
					waitForNewLeader_ConvergeCast();
					//		wait for converge cast
				}
				else{
					//I'm not the leader
					System.out.println("I'm not the leader. Thread:"+tProcess.getName());
					waitForNewLeader_Broadcast();
					resetOldParentToChild();//		Will make your old parent to your child
					setYourNewLevel();
					setYourNewParent(objRequestForConnect.iSenderID); //		Will set your new parent
					updateComponentDetails();
					setNumberOfChildren();
					setMyStatus();
					broadcastNewLeaderDetails();
					waitForNewLeader_ConvergeCast();
					breceiveNewLeader_ConvergeCast=false;
					sendNewLeader_ConvergeCast();
					//		wait for broadcast
					//		wait for converge cast
					//		fwd. the converge cast to the new leader
				}

			}
			else{
				System.out.println("***ERROR. Neither merge nor absorb. Received:"+objRequestForConnect.connectionStatus+" .Thread"+tProcess.getName());
			}
			//		Decide the leader and broadcast the details to all
			//		Wait for converge cast
			//		Done
		}
		else{
			addPendingNeighbor();
			waitForNewLeader_Broadcast();
			resetOldParentToChild();//		Will make your old parent to your child
			setYourNewLevel();
			setYourNewParent(objRequestForConnect.iSenderID); //		Will set your new parent
			updateComponentDetails();
			setNumberOfChildren();
			setMyStatus();
			broadcastNewLeaderDetails();
			waitForNewLeader_ConvergeCast();
			breceiveNewLeader_ConvergeCast=false;
			sendNewLeader_ConvergeCast();
			if(objRequestForConnect.connectionStatus.equals("absorb")){
				controlFlow_TillNextMerge();
			}
			//		Wait for the new leader message
			//		Wait for the converge cast and fwd. to parent
		}
	}

	public void waitUntilYouReplyToCoreEdgeProcess(){
		if(bRepliedToCoreEdgeProcess==false){
			while(bRepliedToCoreEdgeProcess==false){
				try {
					tProcess.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else{
			System.out.println("I have already replied to my core edge");
		}
	}
	public void setNewChild(int iMaxID) {
		// TODO Auto-generated method stub
		int iCount;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][1]==iMaxID){
				mCon[iCount-1][2]=2;
			}
		}
	}

	public void replyForPendingMergeMessages() {
		// TODO Auto-generated method stub
		int iCount=1;
		Message objMessage=new Message();
		Message objMessage_reply=new Message();
		//		Check if you have any pending messages
		if(arrMergeMessages.size() != 0){
			if(bShouldIConnect==true){
				while(arrMergeMessages.size() != 0){
					objMessage=arrMergeMessages.get(iCount-1);
					if(compareComponentID(iMyMWOE_ComponentID, objMessage.iMessageComponentID)==true){
						//reply merge
						bRepliedForMergeMessage=true;
						objRequestForConnect.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
						objRequestForConnect.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
						objRequestForConnect.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
						objRequestForConnect.connectionStatus="merge";
						objMessage_reply.connectionStatus="merge";
						objMessage_reply.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
						objMessage_reply.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
						objMessage_reply.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
						bRepliedToCoreEdgeProcess=true;
						objMessage_reply.sShouldIFindMWOE="false";
						System.out.println("Replying merge. Thread:"+tProcess.getName());
						scheduleMessage(createMessage("requestForconnect_lateReply", objMessage.iSenderID, objMessage_reply));
					}
					else{
						//	reply absorb
						//	find the ID of the other process
						setNeighborRelationTo3(objMessage.iSenderID);
						objMessage_reply.connectionStatus="absorb";
						objMessage_reply.iMessageComponentID[0]=iComponenetID[0];
						objMessage_reply.iMessageComponentID[1]=iComponenetID[1];
						objMessage_reply.iMessageComponentID[2]=iComponenetID[2];
						objMessage_reply.iLevel=iMyLevel;
						objMessage_reply.sShouldIFindMWOE="false";;
						System.out.println("Replying true-->absorb. Thread:"+tProcess.getName());
						scheduleMessage(createMessage("requestForconnect_lateReply", objMessage.iSenderID, objMessage_reply));
					}
					arrMergeMessages.remove(iCount-1);
				}
			}
			else if(bShouldIConnect==false){
				//reply absorb to every merge message
				setNeighborRelationTo3(objMessage.iSenderID);
				while(arrMergeMessages.size() != 0){
					objMessage=arrMergeMessages.get(iCount-1);
					setNeighborRelationTo3(objMessage.iSenderID);
					objMessage_reply.connectionStatus="absorb";
					objMessage_reply.iMessageComponentID[0]=iComponenetID[0];
					objMessage_reply.iMessageComponentID[1]=iComponenetID[1];
					objMessage_reply.iMessageComponentID[2]=iComponenetID[2];
					objMessage_reply.iLevel=iMyLevel;
					objMessage_reply.sShouldIFindMWOE="false";
					arrMergeMessages.remove(iCount-1);
					System.out.println("Replying absorb. Thread:"+tProcess.getName());
					scheduleMessage(createMessage("requestForconnect_lateReply", objMessage.iSenderID, objMessage_reply));
				}
			}
		}
	}

	public Message replyForPendingMergeMessages_() {
		// TODO Auto-generated method stub
		int iCount=1;
		Message objMessage=new Message();
		Message objMessage_reply=new Message();
		//		Check if you have any pending messages
		if(bShouldIConnect==true){
			objMessage=arrMergeMessages.get(iCount-1);
			if(compareComponentID(iMyMWOE_ComponentID, objMessage.iMessageComponentID)==true){
				//reply merge
				bRepliedForMergeMessage=true;
				objRequestForConnect.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
				objRequestForConnect.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
				objRequestForConnect.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
				objRequestForConnect.connectionStatus="merge";
				objMessage_reply.connectionStatus="merge";
				objMessage_reply.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
				objMessage_reply.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
				objMessage_reply.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
				objMessage_reply.sShouldIFindMWOE="false";
				bRepliedToCoreEdgeProcess=true;
				System.out.println("Replying merge. Thread:"+tProcess.getName());
				arrMergeMessages.remove(iCount-1);
				return objMessage_reply;
			}
			else{
				//	reply absorb
				//	find the ID of the other process
				setNeighborRelationTo3(objMessage.iSenderID);
				objMessage_reply.connectionStatus="absorb";
				objMessage_reply.iMessageComponentID[0]=iComponenetID[0];
				objMessage_reply.iMessageComponentID[1]=iComponenetID[1];
				objMessage_reply.iMessageComponentID[2]=iComponenetID[2];
				objMessage_reply.iLevel=iMyLevel;
				objMessage_reply.sShouldIFindMWOE="false";
				System.out.println("Replying true-->absorb.(_) Thread:"+tProcess.getName());
				arrMergeMessages.remove(iCount-1);
				return objMessage_reply;
			}
		}
		else {
			//reply absorb to every merge message
			setNeighborRelationTo3(objMessage.iSenderID);
			while(arrMergeMessages.size() != 0){
				objMessage=arrMergeMessages.get(iCount-1);
				setNeighborRelationTo3(objMessage.iSenderID);
				objMessage_reply.connectionStatus="absorb";
				objMessage_reply.iMessageComponentID[0]=iComponenetID[0];
				objMessage_reply.iMessageComponentID[1]=iComponenetID[1];
				objMessage_reply.iMessageComponentID[2]=iComponenetID[2];
				objMessage_reply.iLevel=iMyLevel;
				objMessage_reply.sShouldIFindMWOE="false";
				arrMergeMessages.remove(iCount-1);
				System.out.println("Replying absorb.(_) Thread:"+tProcess.getName());
				return objMessage_reply;
			}
		}
		return objMessage_reply;
	}

	public void addPendingMergeMessages(Message objMessage){
		arrMergeMessages.add(objMessage);
	}

	public void controlFlow_TillNextMerge() {
		// wait for new leader message()
		//	broadcast it
		//converge cast it
		// if the new leader message is absorb
		// continue this
		System.out.println("in controlFlow_tillMerge. Thread:"+tProcess.getName());
		reset_controlFlowTillNextMerge();

		if(iNewLeader_Broadcast == 2){
			breceiveNewLeader_Broadcast=true;
			objRequestForConnect=objRequestForConnect_2;
			iNewLeader_Broadcast=0;
		}
		else{
			breceiveNewLeader_Broadcast=false;
			iNewLeader_Broadcast=0;
			waitForNewLeader_Broadcast();
		}
		if(objRequestForConnect.sShouldIFindMWOE.equals("false")){
			setYourNewParent(objRequestForConnect.iSenderID);
			updateComponentDetails();
			setYourNewLevel();
			setMyStatus();
			broadcastNewLeaderDetails();
			waitForNewLeader_ConvergeCast();
			System.out.println("Came out of wait in controlFlowTillNextMerge.Thread:"+tProcess.getName());
			sendNewLeader_ConvergeCast();
			if(objRequestForConnect.connectionStatus.equals("absorb")){
				iNewLeader_Broadcast=1;
				controlFlow_TillNextMerge();
			}
			else{
				//start next phase
			}
		}
		else{
			setYourNewParent(objRequestForConnect.iSenderID);
			updateComponentDetails();
			setYourNewLevel();
			setMyStatus();
			broadcastNewLeaderDetails();
			if(objRequestForConnect.bShouldITerminate==true){
				bTerminate=true;
			}
		}
		
	}

	public void addPendingNeighbor(){
		int iCount;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==3){
				mCon[iCount-1][2]=2;
				System.out.println("Adding T"+mCon[iCount-1][1]+" as my child. Thread:"+tProcess.getName());
			}
		}
	}

	public boolean checkForPendingNeighbours(){
		for(int iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==3){
				return true;
			}
		}
		return false;
	}
	public void reset_controlFlowTillNextMerge() {
		// TODO Auto-generated method stub
		breceiveNewLeader_Broadcast=false;
		breceiveNewLeader_ConvergeCast=false;
		iReceiveNewLeader_ConvergeCast=0;
	}

	//		Forward the new. leader converge cast to your parent
	public void sendNewLeader_ConvergeCast(){
		int iCount=-1;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==1){
				scheduleMessage(createMessage("newLeader_ConvergeCast",(int)mCon[iCount-1][1]));
				System.out.println("In "+tProcess.getName()+". Sending a convergeCast to "+mCon[iCount-1][1]);
				break;
			}
		}
	}

	//		Receive new_leader convergeCast
	public synchronized void updateComponentDetails(){
		//		Set your new component details
		iComponenetID[0]=objRequestForConnect.iMessageComponentID[0];
		iComponenetID[1]=objRequestForConnect.iMessageComponentID[1];
		iComponenetID[2]=objRequestForConnect.iMessageComponentID[2];
	}

	public void broadcastNewLeaderDetails() {
		int iCount=-1;
		System.out.println("I'm broadcasting new leader details. Thread:"+tProcess.getName());
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==2){
				//		Schedule a message
				System.out.println("Bro, i sent a message to T"+mCon[iCount-1][1]+" .Thread:"+tProcess.getName());
				scheduleMessage(createMessage("newLeader",(int) mCon[iCount-1][1], objRequestForConnect));
			}
		}
	}

	public void receiveNewLeader_ConvergeCast(){
		System.out.println("In receiveNewLeader_ConvergeCast of thread:"+tProcess.getName());
		iReceiveNewLeader_ConvergeCast++;
		if(iReceiveNewLeader_ConvergeCast==iNumberOfChildren){
			breceiveNewLeader_ConvergeCast=true;
		}
	}
	
	public void receiveNewLeader_Broadcast(Message objMessage) {
		System.out.println("In receiveNewLeader_Broadcast. Have the iNewLeaderBroadcast as: "+iNewLeader_Broadcast+".Thread:"+tProcess.getName());
		System.out.println("Sent by "+objMessage.iSenderID+". Thread: "+tProcess.getName());
		if(iNewLeader_Broadcast==1){
			objRequestForConnect_2=objMessage;
			iNewLeader_Broadcast++;
		}
		else{
			objRequestForConnect=objMessage;
		}
		breceiveNewLeader_Broadcast=true;
	}

	public void waitForNewLeader_Broadcast() {
		while(breceiveNewLeader_Broadcast==false){
			try {
				tProcess.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void  waitForNewLeader_ConvergeCast() {
		if(iNumberOfChildren==0){
			return;
		}
		while(breceiveNewLeader_ConvergeCast==false){
			try {
				tProcess.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void setYourNewParent(int iID){
		int iCount=-1;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(iID==mCon[iCount-1][1]){
				mCon[iCount-1][2]=1;
				break;
			}
		}
	}

	public void setYourNewLevel(){
		iMyLevel=objRequestForConnect.iLevel;
	}

	//		Make your parent to child
	public void resetOldParentToChild(){
		int iCount=-1;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==1){
				mCon[iCount-1][2]=2;
				break;
			}
		}
	}

	public void waitForReply_requestForConnect(){
		System.out.println("In "+tProcess.getName()+". Entering waitForReply_requestForConnect()");
		while(bWaitForReply_RequestForConnect==false){
			try {
				tProcess.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("***ERROR: In waitForReply_requestForConnect() in thread:"+tProcess.getName());
				e.printStackTrace();
			}
		}
		System.out.println("In "+tProcess.getName()+". Exiting waitForReply_requestForConnect()");
	}
	public void connectWithProcessOfOtherComponent(){
		int iConnectProcessID=(int)iMyMWOE_ComponentID[1];
		float[] arrDuplicate=new float[3];
		//		Find the ID of the process with whom you want to connect
		if(iConnectProcessID == iMyPID){
			iConnectProcessID=(int)iMyMWOE_ComponentID[2];
		}
		arrDuplicate[0]=iMyMWOE_ComponentID[0];
		arrDuplicate[1]=iMyMWOE_ComponentID[1];
		arrDuplicate[2]=iMyMWOE_ComponentID[2];
		scheduleMessage(createMessage("requestForconnect", iConnectProcessID, iMyLevel, arrDuplicate));
		System.out.println("Scheduling a message to "+iConnectProcessID+" from Thread:"+tProcess.getName());
	}

	public Message requestForConnect(Message objMessage){
		Message objMessage_reply=new Message();
		System.out.println("In "+tProcess.getName()+": Received a requestForConnect message from "+objMessage.iSenderID);
		//		Send "absorb" if your level is greater than the level of the process that wants to connect
		if(iMyLevel>objMessage.iLevel){
			//		Make the relationship with this neighbor to 3
			setNeighborRelationTo3(objMessage.iSenderID);
			objMessage_reply.connectionStatus="absorb";
			objMessage_reply.iMessageComponentID[0]=iComponenetID[0];
			objMessage_reply.iMessageComponentID[1]=iComponenetID[1];
			objMessage_reply.iMessageComponentID[2]=iComponenetID[2];
			objMessage_reply.iLevel=iMyLevel;
			objMessage_reply.sShouldIFindMWOE="false";
		}
		else if (iMyLevel==objMessage.iLevel){
			//		add the message to the array list
			System.out.println("Added requestForConnect message from "+objMessage.iSenderID+" to pendingMessages. Thread:"+tProcess.getName());
			addPendingMergeMessages(objMessage);
			if(sMyCurrentState.equals("waitingForRequestConnect")){
				objMessage_reply=replyForPendingMergeMessages_();
			}
			else{
				objMessage_reply.connectionStatus="wait";
			}
		}
		else{
			System.out.println("***ERROR: In requestForConnect() in Thread:"+tProcess.getName());
		}
		return objMessage_reply;
	}

	public void requestForConnect_lateReply(Message objMessage){
		System.out.println("In requestForConnect_lateReply of "+tProcess.getName()+": Received from "+objMessage.iSenderID);
		objRequestForConnect=objMessage;
		objRequestForConnect.iLevel=objMessage.iLevel;
		objRequestForConnect.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
		objRequestForConnect.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
		objRequestForConnect.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
		objRequestForConnect.sShouldIFindMWOE=objMessage.sShouldIFindMWOE;

		bWaitForReply_RequestForConnect=true;
		bRepliedToCoreEdgeProcess=true;
	}

	public void setNeighborRelationTo3(int iID){
		if(hNeighborIndex.containsKey(iID)==true){
			mCon[(hNeighborIndex.get(iID))-1][2]=3;
		}
		else{
			System.out.println("***ERROR: No match with the hash in setNeighborRelationTo3 of Thread:"+tProcess.getName());
		}
	}

	public void copyMWOEToBroadcast(){
		objConnectMWOE_Broadcast.iMessageComponentID[0]=iMWOE_ComponentID[0];
		objConnectMWOE_Broadcast.iMessageComponentID[1]=iMWOE_ComponentID[1];
		objConnectMWOE_Broadcast.iMessageComponentID[2]=iMWOE_ComponentID[2];
		System.out.println("Copying values ***"+iMWOE_ComponentID[0]+"***"+iMWOE_ComponentID[1]+"***"+iMWOE_ComponentID[2]);
		System.out.println("Copied. The new values are:*** "+objConnectMWOE_Broadcast.iMessageComponentID[0]+"***"+objConnectMWOE_Broadcast.iMessageComponentID[1]+"***"+objConnectMWOE_Broadcast.iMessageComponentID[2]);
	}

	public void checkIfHaveToConnect(){
		if(compareComponentID(iMyMWOE_ComponentID, objConnectMWOE_Broadcast.iMessageComponentID)==true){
			bShouldIConnect=true;
		}
	}

	public void waitForMWOEConnect_Broadcast(){
		int iWaitTime=200;
		while(bReceiveConnectMWOE_Broadcast==false){
			try{
				tProcess.sleep(iWaitTime);
			}catch(Exception e){
				System.out.println("***ERROR: While entering sleep in waitForMWOEConnectBroadcast() of Thread:"+tProcess.getName());
			}
		}
	}

	public void sendMWOEConnect_Broadcast(){
		//		Broadcast the MWOE connect message to each of your children in the tree
		System.out.println("Start sendConnect_Broadcast() of Thread:"+tProcess.getName());
		int iCount=0;
		float [] arrDuplicate=new float[3];
		arrDuplicate[0]=objConnectMWOE_Broadcast.iMessageComponentID[0];
		arrDuplicate[1]=objConnectMWOE_Broadcast.iMessageComponentID[1];
		arrDuplicate[2]=objConnectMWOE_Broadcast.iMessageComponentID[2];
		System.out.println("--"+objConnectMWOE_Broadcast.iMessageComponentID[0]+"--"+objConnectMWOE_Broadcast.iMessageComponentID[1]+"--"+objConnectMWOE_Broadcast.iMessageComponentID[2]);
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==2){
				//schedule a message to child
				scheduleMessage(createMessage("sendConnectMWOE_Broadcast", (int)mCon[iCount-1][1], arrDuplicate));
			}
		}
		System.out.println("End of sendConnectMWOE_Broadcast of Thread:"+tProcess.getName());
	}
	//		Set MWOE_Component
	public synchronized void setMWOE_Component(float iMessageComponentID, int iProcessID1, int iProcessID2){
		int iCount=0, iSwap;
		boolean bSetMWOE=false;
		//		If processID1 is greater than the proceeID2, then swap them
		if(iMessageComponentID != -1){
			if(iProcessID1 > iProcessID2){
				iSwap=iProcessID1;
				iProcessID1=iProcessID2;
				iProcessID2=iSwap;
			}
			if(iMWOE_ComponentID[0] == -1){
				iMWOE_ComponentID[0]=iMessageComponentID;
				iMWOE_ComponentID[1]=iProcessID1;
				iMWOE_ComponentID[2]=iProcessID2;
			}
			else{
				//		Check if you have to setMWOEComponent or not
				if(iMessageComponentID == iMWOE_ComponentID[0]){
					if(iProcessID1 < iMWOE_ComponentID[1]){
						bSetMWOE=true;
					}
				}
				else if(iMessageComponentID < iMWOE_ComponentID[0]){
					bSetMWOE=true;
				}
				//		Copy the array to iMWOE_Component
				if(bSetMWOE == true){
					iMWOE_ComponentID[0]=iMessageComponentID;
					iMWOE_ComponentID[1]=iProcessID1;
					iMWOE_ComponentID[2]=iProcessID2;	
				}
			}
		}
	}

	//		Wait until you receive convergeCast from all your children
	public void waitForMWOE_ConvergeCast(){
		if(iNumberOfChildren==0){
			System.out.println("I have no children to wait, hence not waiting for MWOE_ConvergeCast. Thread:"+tProcess.getName());
			return;
		}
		int iWaitTime=100;
		while(bReceiveFindMWOE_ConvergeCast==false){
			try{
				tProcess.sleep(iWaitTime);
			}catch(Exception E){
				System.out.println("***ERROR: Exception while entering sleep in waitForMWOE_ConvergeCast of Thread:"+tProcess.getName());
			}
		}
	}

	//		Send convergeCast to your parent
	public void sendMWOE_ConvergeCast(){
		int iCount, iParentID=-1;
		float [] arrDuplicate=new float[3];
		//		Find the index of your parent
		System.out.println("Sending this MWOE to my parent: --"+iMWOE_ComponentID[0]+"--"+iMWOE_ComponentID[1]+"--"+iMWOE_ComponentID[2]);
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==1){
				iParentID=(int)mCon[iCount-1][1];
				break;
			}
		}
		arrDuplicate[0]=iMWOE_ComponentID[0];
		arrDuplicate[1]=iMWOE_ComponentID[1];
		arrDuplicate[2]=iMWOE_ComponentID[2];
		if(iParentID==-1){
			System.out.println("***ERROR: I have no parent to sendMWOE_ConvergeCast Thread:"+tProcess.getName());
			System.out.println("Exiting the function. Thread:"+tProcess.getName());
			return;
		}
		//		Send the message to your parent
		scheduleMessage(createMessage("sendMWOE_ConvergeCast",iParentID,arrDuplicate));
	}

	//		Receive converge cast from children
	public synchronized void receiveMWOE_ConvergeCast(Message objMessage){
		setMWOE_Component(objMessage.iMessageComponentID[0], (int)objMessage.iMessageComponentID[1], (int)objMessage.iMessageComponentID[2]);
		iReceiveFindMWOE_ConvergeCast++;
		if(iReceiveFindMWOE_ConvergeCast==iNumberOfChildren){
			bReceiveFindMWOE_ConvergeCast=true;
		}
	}
	//		Find MWOE
	public boolean findMWOE(){
		System.out.println("Start of findMWOE() of Thread:"+tProcess.getName());
		int iCount,  iWaitTime=200, iMWOEID=-1, iIndex=-1;
		float iMinWeight;
		boolean bFindMWOE=false;

		try{
			
		
		//		Set sWaitForMWOEReply=none
		while(bFindMWOE==false){
			sWaitForMWOEReply=sWaitForMWOEReply_Values[3];
			iMinWeight=1000000;
			iMWOEID=-1;
			iIndex=-1;
			for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
				//		Find the neighbor that has the least edge cost
				try{
				if(mCon[iCount-1][2]==0){
					if(iMinWeight == mCon[iCount-1][3]){
						if(iMWOEID > (int)mCon[iCount-1][1]){
							iMWOEID=(int)mCon[iCount-1][1];
							iIndex=iCount;
						}
					}
					else if(iMinWeight > mCon[iCount-1][3]){
						iMinWeight=mCon[iCount-1][3];
						iMWOEID=(int)mCon[iCount-1][1];
						iIndex=iCount;
					}
				}
				}catch(Exception E){
					System.out.println("at here in findMWOE(). Thread:"+tProcess.getName());
					System.out.println("value of iCount is:"+iCount+" Thread:"+tProcess.getName());
				}
			}
			//		Send a test message to the neighbor if there is a MWOE
			if(iMWOEID!=-1){
				//		schedule a message- type-receiveTestMessage, iNeighborID, iComponentdetails, iLevel
				float [] arrDuplicate=new float[3];
				arrDuplicate[0]=iComponenetID[0];
				arrDuplicate[1]=iComponenetID[1];
				arrDuplicate[2]=iComponenetID[2];
				scheduleMessage(createMessage("receiveTestMessage",iMWOEID,iMyLevel,arrDuplicate));
				while(sWaitForMWOEReply.equals("unknown")){
					try {
						tProcess.sleep(iWaitTime);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println("In try/catch of findMWOE() of Thread:"+tProcess.getName()+" while entering sleep");
						e.printStackTrace();
					}
				}
				if(sWaitForMWOEReply.equals("true")){
					System.out.println(tProcess.getName()+" received a reply true from Process:"+iMWOEID);
					bFindMWOE=true;
					//		Set your leastMWOE_Component
					setMyMWOE_Component(mCon[iIndex-1][3],iMyPID,(int)mCon[iIndex-1][1]);
					setMWOE_Component(mCon[iIndex-1][3],iMyPID,(int)mCon[iIndex-1][1]);
				}
				else if(sWaitForMWOEReply.equals("false")){
					System.out.println(tProcess.getName()+" received a reply false from Process:"+iMWOEID);
					mCon[iIndex-1][2]=4;
					bFindMWOE=false;
				}
				//		If you are getting a reply to wait
				else{
					System.out.println(tProcess.getName()+" received a reply wait from Process:"+iMWOEID);
					System.out.println("Entering sleep until i get a reply."+tProcess.getName());
					//		Wait till you get the reply
					while(sWaitForMWOEReply.equals("wait")){
						try {
							tProcess.sleep(iWaitTime);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							System.out.println("In try/catch of findMWOE() of Thread:"+tProcess.getName()+" while entering sleep");
							e.printStackTrace();
						}
					}
					if (sWaitForMWOEReply.equals("true")) {
						bFindMWOE=true;
						setMyMWOE_Component(mCon[iIndex-1][3],iMyPID,(int)mCon[iIndex-1][1]);
						setMWOE_Component(mCon[iIndex-1][3],iMyPID,(int)mCon[iIndex-1][1]);
						bFindMWOE=true;
					}
					else{
						//		Update the matrix to have no relation with the neighbor in the tree, update the value to 4
						mCon[iCount-1][2]=4;
						bFindMWOE=false;
					}
				}				
			}
			else{
				//		You have no neighbors to connect with
				//		Reply you have no messages to send
				setMyMWOE_Component(-1, -1, -1);
				setMWOE_Component(-1, -1, -1);
				bFindMWOE=false;
				break;
			}
		}
		}catch(Exception E){
			System.out.println("In catch of findMWOE:Thread:"+tProcess.getName());
		}
		System.out.println("My MWOE: --"+iMyMWOE_ComponentID[0]+"--"+iMyMWOE_ComponentID[1]+"--"+iMyMWOE_ComponentID[2]);
		System.out.println("End of findMWOE() of Thread:"+tProcess.getName());
		return bFindMWOE;
	}

	public void printMyComponent(){
		System.out.println("Start of printMyComponent of Thread:"+tProcess.getName());
		System.out.println("Core edge:"+iMyMWOE_ComponentID[0]);
		System.out.println("ProcessID1:"+iMyMWOE_ComponentID[1]);
		System.out.println("ProcessID2:"+iMyMWOE_ComponentID[2]);
	}

	public void setMyMWOE_Component(float mCon2, int iProcessID1, int iProcessID2){
		int iSwap;
		//		If processID1 is greater than the proceeID2, then swap them
		if(iProcessID1 > iProcessID2){
			iSwap=iProcessID1;
			iProcessID1=iProcessID2;
			iProcessID2=iSwap;
		}
		//		Copy the array to iMyMWOE_Component
		iMyMWOE_ComponentID[0]=mCon2;
		iMyMWOE_ComponentID[1]=iProcessID1;
		iMyMWOE_ComponentID[2]=iProcessID2;
	}

	public synchronized String receiveTestMessage(Message objMessage){
		//		Reply immediately if your level is greater than (or) equal to sender's level
		if(iMyLevel>=objMessage.iLevel){
			//		Check if the component ID is also same
			//		If yes, return false, else return true
			if(compareComponentID(iComponenetID, objMessage.iMessageComponentID)){
				return "false";
			}
			else{
				return "true";
			}
		}
		//		My level is lower than the sender's level, so defer the reply to the message
		else{
			addMessageToDeferredReplies(objMessage);
			return "wait";
		}
	}

	//		Add message to the list of deferred messages
	public void addMessageToDeferredReplies(Message objMessage){
		//		Add the message to the pqDeferrerdMessages (Priority Queue)
		pqDeferredMessages.offer(objMessage);
	}

	//		Check if there are any deferred messages to be replied for after your level has been changed
	public boolean areThereAnyDeferredMessages(int iLevel){
		//		Peek the first element to check if it myCurrentLevel is greater than equal to its level
		//		If yes, return true, else return false
		if(pqDeferredMessages.size()> 0){
			if(iLevel>=pqDeferredMessages.peek().iLevel){
				return true;
			}
		}
		return false;
	}

	//		Send replies to deferred messages
	public void sendRepliesToDeferredMessages(){
		boolean bAnyMessagesToSend=true;
		int iLevel=iMyLevel;
		while(bAnyMessagesToSend){
			//		Check if there are any messages to reply for in this level
			if(areThereAnyDeferredMessages(iLevel)==true){
				Message objMessage=new Message();
				objMessage=pqDeferredMessages.poll();
				int iCount=0, iRowIndex=-1;
				//		Find the index by matching against sender id
				for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
					if(mCon[iCount-1][1]==objMessage.iSenderID){
						iRowIndex=iCount;
						break;
					}
				}
				if(iRowIndex!=-1){
					//		Reply false if the component ID's are same, Else, return true
					if((compareComponentID(iComponenetID, objMessage.iMessageComponentID))==true){
						oProc[iRowIndex-1].receiveDeferredReply("false");
					}
					else{
						oProc[iRowIndex-1].receiveDeferredReply("true");
					}
				}
				else{
					System.out.println("***ERROR: Cannot match senderID with any of my neighbor's list. sendRepliesToDeferredMessages. Thread:"+tProcess.getName());
				}
			}
			else{
				//		No deferred messages to reply for at this level
				bAnyMessagesToSend=false;
			}
		}
	}

	//		Compare the component ID and return true if they are same, else return false
	public boolean compareComponentID(float[] iMyMWOE_ComponentID2, float[] iMessageComponentID){
		//		If all 3 parameters of the array are same, return true, else return false
		if(iMyMWOE_ComponentID2[0]==iMessageComponentID[0]){
			if(iMyMWOE_ComponentID2[1]==iMessageComponentID[1]){
				if(iMyMWOE_ComponentID2[2]==iMessageComponentID[2]){
					return true;
				}
			}
		}
		return false;
	}

	//		Create message to send


	//		This messages is to send findMWOE_Broadcast
	public  Message createMessage(String sType, int iNeighborID){
		Message objMessage = new Message();
		objMessage.sTypeOfMessage=sType;
		objMessage.iNeighborUID=iNeighborID;
		objMessage.iSenderID=iMyPID;
		return objMessage;
	}

	//		This message is to send testMessage()
	public Message createMessage(String sType, int iNeighborID, int iLevel, float[] arrDuplicate){
		int iCount=0;
		Message objMessage = new Message();
		objMessage.sTypeOfMessage=sType;
		objMessage.iNeighborUID=iNeighborID;
		objMessage.iLevel=iLevel;
		objMessage.iSenderID=iMyPID;
		for(iCount=1;iCount<=arrDuplicate.length;iCount++){
			objMessage.iMessageComponentID[iCount-1]=arrDuplicate[iCount-1];
		}
		return objMessage;
	}

	//	This messages is to send MWOE_ConvergeCast to your parent
	public  Message createMessage(String sType, int iNeighborID, float[] arrDuplicate){
		int iCount=0;
		Message objMessage = new Message();
		objMessage.sTypeOfMessage=sType;
		objMessage.iNeighborUID=iNeighborID;
		objMessage.iSenderID=iMyPID;
		for(iCount=1;iCount<=arrDuplicate.length;iCount++){
			objMessage.iMessageComponentID[iCount-1]=arrDuplicate[iCount-1];
		}
		return objMessage;
	}


	public  Message createMessage(String sType, int iNeighborID, int iLevel){
		Message objMessage = new Message();
		objMessage.sTypeOfMessage=sType;
		objMessage.iNeighborUID=iNeighborID;
		objMessage.iLevel=iLevel;
		objMessage.iSenderID=iMyPID;
		return objMessage;
	}

	//		This is to broadcast new leader message
	public Message createMessage(String sType, int iNeighborID, Message objMessage){
		Message returnMessage=new Message();
		returnMessage.sTypeOfMessage=sType;
		returnMessage.iSenderID=iMyPID;
		returnMessage.iNeighborUID=iNeighborID;
		returnMessage.connectionStatus=objMessage.connectionStatus;
		returnMessage.iMessageComponentID[0]=objMessage.iMessageComponentID[0];
		returnMessage.iMessageComponentID[1]=objMessage.iMessageComponentID[1];
		returnMessage.iMessageComponentID[2]=objMessage.iMessageComponentID[2];
		returnMessage.iLevel=objMessage.iLevel;
		returnMessage.arrMwoe[0]=objMessage.arrMwoe[0];
		returnMessage.arrMwoe[1]=objMessage.arrMwoe[1];
		returnMessage.arrMwoe[2]=objMessage.arrMwoe[2];
		returnMessage.sShouldIFindMWOE=objMessage.sShouldIFindMWOE;
		returnMessage.connectionStatus=objMessage.connectionStatus;

		System.out.println("Broadcasting to neighbor: "+returnMessage.iNeighborUID+" Thread:"+tProcess.getName());
		return returnMessage;
	}

	public synchronized void receiveDeferredReply(String sReplyFromNeighbor){
		if(sReplyFromNeighbor.equals("true")){
			sWaitForMWOEReply=sWaitForMWOEReply_Values[1];
		}
		else if(sReplyFromNeighbor.equals("false")){
			sWaitForMWOEReply=sWaitForMWOEReply_Values[2];
		}
		else{
			System.out.println("***Error in receiveDeferredReply in Thread:"+tProcess.getName());
		}
	}

	public synchronized void receiveFindMWOE_Broadcast(){
		bReceiveFindMWOE_Broadcast=true;
	}

	//		Receive connectMWOE_Broadcast
	public void receiveConnectMWOE_Broadcast(Message objMessage){
		objConnectMWOE_Broadcast=objMessage;
		bReceiveConnectMWOE_Broadcast=true;
	}

	public void waitForFindMWOE_Broadcast(){
		int iWaitTime=200;
		while(bReceiveFindMWOE_Broadcast==false){
			try {
				tProcess.sleep(iWaitTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("***ERROR occurred when thread is going to sleep in waitForFindMWOE_Broadcast()");
				e.printStackTrace();
			}
		}
	}

	public void sendFindMWOE_Broadcast(){
		//		Broadcast the message to each of your children in the tree
		System.out.println("Start sendFindMWOE_Broadcast() of Thread:"+tProcess.getName());
		int iCount=0;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==2){
				//call child function
				scheduleMessage(createMessage("sendFindMWOE_Broadcast", (int)mCon[iCount-1][1]));
			}
		}
		System.out.println("End of sendFindMWOE_Broadcast of Thread:"+tProcess.getName());
	}

	public String getMyStatus(){
		return sMyStatus;
	}

	public void setMyStatus(){
		int iCount=-1;
		boolean bImRoot=true, bImLeaf=true;;

		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==1){
				bImRoot=false;
			}
			else if(mCon[iCount-1][2]==2){
				bImLeaf=false;
			}
		}

		if(bImRoot==true){
			sMyStatus="root";
		}
		else if(bImLeaf==true){
			sMyStatus="leaf";
		}
		else{
			sMyStatus="intermediate";
		}
		System.out.println("Setting my status to "+sMyStatus+".Thread:"+tProcess.getName());
	}

	public void receivePulse(Long iPulseNumber){
		setPulse(iPulseNumber);
		sendMessagesForThisPulse();
	}

	public boolean anyMessagesToBeSentInThisPulse() {
		//		Check hash map to see if there are messages in this pulse
		if(hMessage.containsKey(iCurrentPulse)==true){
			return true;
		}
		else{
			return false;
		}
	}

	//		The below two methods setPulse and getPulse are synchronized
	public synchronized void setPulse(Long iPulseNumber) {
		//		System.out.println("Start of setPulse in: "+tProcess.getName());
		iCurrentPulse=iPulseNumber;
	}

	public synchronized long getPulse() {
		//		System.out.println("Start of getPulse in: "+tProcess.getName());
		return iCurrentPulse;
	}

	public synchronized int generateRandomNumber(){
		//		System.out.println("Start of generateRandomNumber in: "+tProcess.getName());
		return randomGenerator.nextInt(20)+2;	
	}

	public synchronized void scheduleMessage(Message objMessage){
		//		System.out.println("Start of scheduleMessage in: "+tProcess.getName());
		int iRandomNumber, iProcessIndex=0;
		Long iSetTime;

		if(hNeighborIndex.containsKey(objMessage.iNeighborUID)){
			iProcessIndex=hNeighborIndex.get(objMessage.iNeighborUID);
			if(objMessage.iNeighborUID==10){
				System.out.println("Scheduling a msg to 10"+tProcess.getName());
			}
		}
		else{
			System.out.println("***ERROR: Neighbor ID not found in the Neighbor Index hash map");
			System.out.println("Going out of the function scheduleMessage(). Thread: "+tProcess.getName());
			return;
		}
		iRandomNumber=randomGenerator.nextInt(20)+2;
//		iRandomNumber=generateRandomNumber();
		//		Get the current pulse and add the random number to it
		iSetTime=getPulse()+iRandomNumber;
		//		If set time is less than the latest messages to be sent to this neighbor, increase setTime by 1 and add it to hash
		if(iSetTime<=arrNbrLatestPulse[iProcessIndex-1]){
			iSetTime=arrNbrLatestPulse[iProcessIndex-1];
			//		Increment the setTime to next Pulse and also update the pulse of the neighbor
			iSetTime++;
			arrNbrLatestPulse[iProcessIndex-1]++;
			//		Add key to the hash
			if(hMessage.containsKey(iSetTime)==true){
				hMessage.get(iSetTime).add(objMessage);
			}
			else{
				hMessage.put(iSetTime, new ArrayList<>());
				hMessage.get(iSetTime).add(objMessage);
			}
		}
		else{	//		Add key to the hash
			if(hMessage.containsKey(iSetTime)==true){

				hMessage.get(iSetTime).add(objMessage);
			}
			else{
				hMessage.put(iSetTime, new ArrayList<>());
				hMessage.get(iSetTime).add(objMessage);
			}
			//		Update the neighbor latest pulse
			arrNbrLatestPulse[iProcessIndex-1]=iSetTime;
		}
		System.out.println("Message scheduled at: "+iSetTime+" :"+tProcess.getName());
		//		System.out.println("End of scheduleMessage in: "+tProcess.getName());
	}

	public void receiceMessage(int iNbrID, int iID){
		System.out.println("Start of receiveMessage in "+tProcess.getName()+" called by "+iNbrID);
		System.out.println("It sent me an ID: "+iID);
		System.out.println("End of receiveMessage in "+tProcess.getName()+" called by "+iNbrID);
	}

	public void sendMessagesForThisPulse(){
		//		System.out.println("Start of sendMessagesForThisPulse() in "+tProcess.getName());
		//		Return if you have no messages to send in this pulse
		if(anyMessagesToBeSentInThisPulse()==false){//		Send the messages that are scheduled for this pulse and delete them
			return; //you have no messages to send in this pulse
		}

		int iCountMessages, iNumberOfMessagesSent=0;
		ArrayList arrMessage=new ArrayList();
		Message objMessage=new Message();
		//		Count how many messages you have to send in this pulse
		iCountMessages=hMessage.get(iCurrentPulse).size();
		for(iNumberOfMessagesSent=1;iNumberOfMessagesSent<=iCountMessages;iNumberOfMessagesSent++){
			arrMessage=hMessage.get(iCurrentPulse);
			objMessage=(Message) arrMessage.get(iNumberOfMessagesSent-1);
			//Identify the type of the message and call the appropriate function
			switch(objMessage.sTypeOfMessage){
			case "receiveTestMessage":
				sWaitForMWOEReply=oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveTestMessage(objMessage);
				break;
			case "sendFindMWOE_Broadcast":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveFindMWOE_Broadcast();
				break;
			case "sendMWOE_ConvergeCast":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveMWOE_ConvergeCast(objMessage);
				break;
			case "sendConnectMWOE_Broadcast":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveConnectMWOE_Broadcast(objMessage);
				break;
			case "requestForconnect":
				objRequestForConnect=oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].requestForConnect(objMessage);
				bWaitForReply_RequestForConnect=true;
				break;
			case "requestForconnect_lateReply":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].requestForConnect_lateReply(objMessage);
				break;
			case "newLeader":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveNewLeader_Broadcast(objMessage);
				break;
			case "newLeader_ConvergeCast":
				oProc[(hNeighborIndex.get(objMessage.iNeighborUID))-1].receiveNewLeader_ConvergeCast();
				break;
			}
		}
		deleteMessagesForThisPulse(iCurrentPulse);
		//		System.out.println("End of sendMessagesForThisPulse() in "+tProcess.getName());
	}

	public void deleteMessagesForThisPulse(Long iPulseNumber){
		//		System.out.println("Start of deleteMessagesForThisPulse() in "+tProcess.getName());
		if(hMessage.containsKey(iPulseNumber)){
			hMessage.remove(iPulseNumber);
		}
		else{//		No messages found for this pulse
			System.out.println("***WARNING: No messages are found with the key: "+iPulseNumber);

		}
		//		System.out.println("End of deleteMessagesForThisPulse() in "+tProcess.getName());
	}

	public void setNumberOfChildren(){
		int iCount=0;
		iNumberOfChildren=0;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==2){
				iNumberOfChildren++;
			}
		}
	}

	public boolean checkToTerminate(){
		System.out.println("In terminate(). Thread:"+tProcess.getName());
		if(objConnectMWOE_Broadcast.iMessageComponentID[0] == -1){
			//			if(objConnectMWOE_Broadcast.iMessageComponentID[1]==-1){
			//				if(objConnectMWOE_Broadcast.iMessageComponentID[2]==-1){
			System.out.println("Returning true. Thread:"+tProcess.getName());
			return true;
			//				}
			//			}
		}
		System.out.println("Returning false. Thread:"+tProcess.getName());
		return false;
	}

	public void printYourTree(){
		System.out.print(iMyPID+"		");
		System.out.print("   ");
		printParent();
		System.out.print("		");
		System.out.print("    ");
		printChildren();
		if(sMyStatus.equals("root")){
			System.out.print("		Level:"+iMyLevel+"\t\tComponent ID:"+iComponenetID[0]+"\t"+(int)iComponenetID[1]+"\t"+(int)iComponenetID[2]);
		}
		System.out.println();
	}

	public void printParent(){
		int iCount=-1,iParentIndex=-1;
		boolean bHasParent=false;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==1){
				bHasParent=true;
				iParentIndex=iCount;
				break;
			}
		}
		if(bHasParent==true){
			System.out.print((int)mCon[iParentIndex-1][1]);
		}
		else{
			System.out.print("ROOT");
		}
	}

	public void printChildren(){
		int iCount,iCounter=1;;
		for(iCount=1;iCount<=iNumberOfNbrs;iCount++){
			if(mCon[iCount-1][2]==2){
				if(iCounter==1){
					iCounter++;
					System.out.print((int)mCon[iCount-1][1]);
					continue;
				}
				else{
					System.out.print(",");
				}
				System.out.print((int)mCon[iCount-1][1]);
			}
		}
	}

	public static Comparator<Message> levelComparator = new Comparator<Message>(){
		@Override
		public int compare(Message m1, Message m2) {
			System.out.println("Q1:"+m1.iLevel);
			System.out.println("Q2:"+m2.iLevel);
			return (int) (m1.iLevel-m2.iLevel);
		}
	};
}