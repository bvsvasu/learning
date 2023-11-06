/**
 * 
 */
package com.deere.dwis.ccms.ccms_comman_udfs;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.io.Text;

public class EventFlattener extends GenericUDTF {

	private static final String COMPLETED = "Completed";

	private static final int NUM_COLS = 27; //no of ouput column 10 ts 10 ids 1 case 1 workbasket 1 clousre reason

	//SP70128: All CASE STATUSES [START]
	private static final String New = "New";
	private static final String Owned = "Owned";
	private static final String PendingInformationRequested = "Pending-Information Requested";
	private static final String PendingInformationProvided = "Pending-Information Provided";
	private static final String LikelyResolutionProvided = "Likely Resolution Provided";
	private static final String PendingDealerSurvey = "Pending-Dealer Survey";
	private static final String Reopened = "Reopened";
	private static final String Resolved = "Resolved"; //should contain Resolved-Dealer Closed,Resolved-Completed,Resolved-Withdrawn
	private static final String Escalated = "Escalated";
	private static final String ResolvedDealerClosed="Resolved-Dealer Closed"; 
	private static final String ResolvedCompleted="Resolved-Completed";
	private static final String ResolvedWithdrawn="Resolved-Withdrawn";
	private static final String ResolvedDAClosed="Resolved-DAClosed";
	private static final String ResolvedSystemClosed="Resolved-System Closed";
	private static final String NewDraft="New-Draft";
	
	//SP70128: All CASE STATUSES [END]

	//SP70128: output row columns details[START]
	private Text[] retCols; // array of returning flattened column values
	private ObjectInspector[] inputOIs; // input ObjectInspectors
	private HiveVarchar prevUserId, prevWrkBsktNm, prevCasePriority, prevCaseStatus, prevCaseId, preCaseClasification, preCaseWellPreFlag; // variable to hold previous row String columns from last iteration
	private int prevCaseStatusKey, currentStatusKey; //variable to hold previous row integer columns from last iteration
	private Map<String, String> status = new HashMap<String, String>(); //Map for holding status
	private Map<String, String> users = new HashMap<String, String>(); //Map for holding users
	private boolean isFirstRow = true; // first row execution flag
	private boolean isFirstRowNew = false; // first row execution flag
	//SP70128: output row columns details[END]

	// SP70128 : input row column types[START]
	private int LINE_ID; //variables to get the integer column from the source table
	private Timestamp ACTV_TS, prevACTV_TS; //variables to get the timestamp column from the source table
	private HiveVarchar CASE_ID, USER_ID, WORK_BASKET_NM, CASE_STAT, CASE_PRRTY, CASE_WELL_PREPARE_FLG, CASE_CLSFN, CASE_CLOSE_TYP; //variables to get the string column from the source table
	private int occ_id = 1; //variable to change when new case row is added-new case occurance
	// SP70128 : input row column types[END]
	
	private String owningWorkBasket=null;

	//SP70128 : Status Map to hold status along with key for comparison if any case status sequence changed or new status added please add/modify this code[START]
	private static Map<Integer, List<String>> statusMap = new HashMap<Integer, List<String>>();
	static {
		statusMap.put(1, Arrays.asList(New, Reopened));
		statusMap.put(2, Arrays.asList(Owned));
		statusMap.put(3, Arrays.asList(PendingInformationRequested,Escalated,LikelyResolutionProvided));
		statusMap.put(4, Arrays.asList(PendingInformationProvided));
		statusMap.put(5, Arrays.asList(PendingDealerSurvey));
		statusMap.put(6, Arrays.asList(ResolvedDealerClosed, ResolvedCompleted,ResolvedWithdrawn,ResolvedDAClosed,ResolvedSystemClosed)); //should contain Resolved-Dealer Closed,Resolved-Completed,Resolved-Withdrawn at same key
	}
	//SP70128 : Status Map to hold status along with key for comparison if any case status sequence changed or new status added please add/modify this code[END]

	@Override
	public StructObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
		inputOIs = ois; //input row columns

		// construct the output column data holders
		retCols = new Text[NUM_COLS];
		for (int i = 0; i < NUM_COLS; ++i) {
			retCols[i] = new Text();
		}

		// construct output object inspector
		List<String> fieldNames = new ArrayList<String>(NUM_COLS);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(NUM_COLS);
		for (int i = 0; i < NUM_COLS; ++i) {
			fieldNames.add("CoulmnName " + i); // column name can be anything since it will be named by UDTF 'as' clause			
			fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); //all returned type will be Text
		}
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void close() throws HiveException {
		createOutputRow(); //to create a last row we have to add this its important
	}

	@Override
	public void process(Object[] args) throws HiveException {
		// Storing all input values of current row in resp. variables
		CASE_ID = ((WritableHiveVarcharObjectInspector) inputOIs[0]).getPrimitiveJavaObject(args[0]);
		LINE_ID = ((IntObjectInspector) inputOIs[1]).get(args[1]);
		USER_ID = ((WritableHiveVarcharObjectInspector) inputOIs[2]).getPrimitiveJavaObject(args[2]);
		ACTV_TS = ((TimestampObjectInspector) inputOIs[3]).getPrimitiveJavaObject(args[3]);
		WORK_BASKET_NM = ((WritableHiveVarcharObjectInspector) inputOIs[4]).getPrimitiveJavaObject(args[4]);
		CASE_STAT = ((WritableHiveVarcharObjectInspector) inputOIs[5]).getPrimitiveJavaObject(args[5]);
		CASE_PRRTY = ((WritableHiveVarcharObjectInspector) inputOIs[6]).getPrimitiveJavaObject(args[6]);
		CASE_WELL_PREPARE_FLG = ((WritableHiveVarcharObjectInspector) inputOIs[7]).getPrimitiveJavaObject(args[7]);
		CASE_CLSFN = ((WritableHiveVarcharObjectInspector) inputOIs[8]).getPrimitiveJavaObject(args[8]);

		//SP70128:Hold the key for status for comparison of status
		for (Map.Entry<Integer, List<String>> entry : statusMap.entrySet()) {
			List<String> listValue = entry.getValue();
			if (listValue.contains(CASE_STAT.toString())) {
				currentStatusKey = entry.getKey();
			}
		}
		//sp70128: In case of first row, store correct values in previous variables		
		if (isFirstRow) {
			isFirstRow = false;
			if (CASE_STAT.toString().startsWith(New)){
				isFirstRowNew=true;
			}
			evalStatus(CASE_STAT,ACTV_TS,USER_ID);
			assignAllRowElements();			
		}else{
			if (prevCaseId!=CASE_ID) {
				isFirstRowNew=false; 
			} else if (prevCaseId==CASE_ID && CASE_STAT.toString().startsWith(New)) {
				isFirstRowNew=true; 
			}
			if((prevCaseStatusKey==3 && (currentStatusKey==4 || currentStatusKey==2 || currentStatusKey==1)) || (prevCaseStatusKey==2 && currentStatusKey==1) || (prevCaseStatusKey==6 && currentStatusKey==1) || (prevCaseStatusKey==2 && currentStatusKey==4)
					|| !(prevCaseId.equals(CASE_ID))){
				newRowCreation();
			}
			if ((this.prevCaseStatusKey == this.currentStatusKey) && (this.prevWrkBsktNm != null) && (this.WORK_BASKET_NM != null) && (this.prevWrkBsktNm.getValue().toString().equals(this.WORK_BASKET_NM.getValue().toString())))
			{

		    }
		    else
		    {
		      evalStatus(this.CASE_STAT, this.ACTV_TS, this.USER_ID);
		    }
			assignAllRowElements();
		}		
	}

	/**
	 * SP70128::Assigns all the case details after creating new row
	 */
	public void assignAllRowElements() {
		prevCaseId = CASE_ID;
		prevUserId = USER_ID;
		prevWrkBsktNm = WORK_BASKET_NM !=null ? WORK_BASKET_NM : prevWrkBsktNm;
		prevCaseStatus = CASE_STAT;
		prevCaseStatusKey = currentStatusKey;
		prevCasePriority = CASE_PRRTY !=null ? CASE_PRRTY : prevCasePriority;
		preCaseClasification = CASE_CLSFN;
		preCaseWellPreFlag = CASE_WELL_PREPARE_FLG;
		prevACTV_TS = ACTV_TS;
	}
	
	/**
	 * SP70128::New row creation for all the conditions
	 * @throws HiveException
	 */
	public void newRowCreation() throws HiveException {
		createOutputRow();
		occ_id++;
		status = null;
		CASE_CLOSE_TYP = null; //Assign as null so that same closure code will not be updated to next new row
		status = new HashMap<String, String>();
		users = new HashMap<String, String>();
		owningWorkBasket=null;
		if (!prevCaseId.equals(CASE_ID)) {
			occ_id = 1; 
		} 
	}

	/**
	 * SP70128::Evaluate status and add that into Map for Display in Flat
	 * structure
	 * 
	 * @param CURR_STAT
	 * @param ACTV_TS
	 * @param USER_ID 
	 * @throws HiveException
	 */
	private void evalStatus(HiveVarchar CURR_STAT, Timestamp ACTV_TS, HiveVarchar USER_ID) throws HiveException {
		String actv_ts = ACTV_TS != null ? ACTV_TS.toString() : "";
		String user_id = USER_ID !=null ? USER_ID.toString() :"";
		String currentStatus = CURR_STAT!=null?CURR_STAT.toString():"";
			if (CURR_STAT.toString().startsWith(New)) {
				updateStatusTimestamp(New, actv_ts);			
			} else if (CURR_STAT.toString().startsWith(Owned)) {
				updateStatusTimestamp(Owned, actv_ts);
				owningWorkBasket = WORK_BASKET_NM!=null?WORK_BASKET_NM.toString():null;			
			} else if (CURR_STAT.toString().startsWith(PendingInformationRequested)) {
				updateStatusTimestamp(PendingInformationRequested, actv_ts);
			} else if (CURR_STAT.toString().startsWith(PendingInformationProvided)) {
				updateStatusTimestamp(PendingInformationProvided, actv_ts);
			} else if (CURR_STAT.toString().startsWith(LikelyResolutionProvided)) {
				updateStatusTimestamp(LikelyResolutionProvided, actv_ts);
			} else if (CURR_STAT.toString().startsWith(PendingDealerSurvey)) {
				updateStatusTimestamp(PendingDealerSurvey, actv_ts);
			} else if (CURR_STAT.toString().startsWith(Resolved)) {
				updateStatusTimestamp(Resolved, actv_ts);
				CASE_CLOSE_TYP = new HiveVarchar();
				String[] statusParts = CURR_STAT.toString().split("-");
				CASE_CLOSE_TYP.setValue(statusParts != null ? statusParts[1] : "");
			} else if (CURR_STAT.toString().startsWith(Reopened)) {
				updateStatusTimestamp(Reopened, actv_ts);
			} else if (CURR_STAT.toString().startsWith(Escalated)) {
				updateStatusTimestamp(Escalated, actv_ts);
			}
			updateUserIds(currentStatus,user_id);
				
	}

	/**
	 * @param new2
	 * @param user_id2
	 */
	private void updateUserIds(String currentStatus, String user_id) {
		users.put(currentStatus, user_id);		
	}

	/**
	 * SP70128::Status evaluation logic
	 * 
	 * @param CurrentStatus
	 * @param CurrentStatus
	 *            Timestamp
	 * @throws HiveException
	 */
	public void updateStatusTimestamp(String currentStatus, String active_ts) throws HiveException {
			status.put(currentStatus, active_ts);
	}

	/**
	 * SP70128::Output row creation using column array 
	 * @throws HiveException
	 */
	private void createOutputRow() throws HiveException {
		// Set values in array to return as output row
		retCols[0].set(prevCaseId!=null?prevCaseId.getValue():"");
		retCols[1].set("" + occ_id);
		if(owningWorkBasket!=null){
			retCols[2].set(owningWorkBasket);
		}else{
			retCols[2].set(prevWrkBsktNm != null ? prevWrkBsktNm.getValue():"");
		}		
		retCols[3].set(prevCasePriority != null ? prevCasePriority.getValue():"");
		retCols[4].set(preCaseWellPreFlag != null ? preCaseWellPreFlag.getValue():"");
		retCols[5].set(preCaseClasification!=null?preCaseClasification.getValue():"");
		
		//SP70128 : User Id Update Columns [START]		
		retCols[6].set(users.containsKey(New) == true ? users.get(New) : "");
		retCols[7].set(users.containsKey(Reopened) == true ? users.get(Reopened) : "");
		retCols[8].set(users.containsKey(Owned) == true ? users.get(Owned) : "");
		retCols[9].set(users.containsKey(PendingInformationRequested) == true ? users.get(PendingInformationRequested) : "");
		retCols[10].set(users.containsKey(PendingInformationProvided) == true ? users.get(PendingInformationProvided) : "");
		retCols[11].set(users.containsKey(LikelyResolutionProvided) == true ? users.get(LikelyResolutionProvided) : "");
		retCols[12].set(users.containsKey(Escalated) == true ? users.get(Escalated) : "");
		retCols[13].set(users.containsKey(PendingDealerSurvey) == true ? users.get(PendingDealerSurvey) : "");
		
        retCols[14].set(users.containsKey(ResolvedCompleted) == true? users.get(ResolvedCompleted) : "");

		if(users.containsKey(Resolved) == true){
            retCols[15].set(users.get(Resolved));
	    }else if(users.containsKey(ResolvedDealerClosed) == true){
	            retCols[15].set(users.get(ResolvedDealerClosed));
	    }else if(users.containsKey(ResolvedCompleted) == true){
	            retCols[15].set(users.get(ResolvedCompleted));
	    }else if(users.containsKey(ResolvedWithdrawn) == true){
	            retCols[15].set(users.get(ResolvedWithdrawn));
	    }else if(users.containsKey(ResolvedDAClosed) == true){
	            retCols[15].set(users.get(ResolvedDAClosed));
	    }else if(users.containsKey(ResolvedSystemClosed) == true){
	            retCols[15].set(users.get(ResolvedSystemClosed));
	    }else {
	            retCols[15].set("");
	    }
		

		//SP70128 : Status Timestamp Columns [START]
		retCols[16].set(status.containsKey(New) == true ? status.get(New) : "");
		retCols[17].set(status.containsKey(Reopened) == true ? status.get(Reopened) : "");
		retCols[18].set(status.containsKey(Owned) == true ? status.get(Owned) : "");
		retCols[19].set(status.containsKey(PendingInformationRequested) == true ? status.get(PendingInformationRequested) : "");
		retCols[20].set(status.containsKey(PendingInformationProvided) == true ? status.get(PendingInformationProvided) : "");
		retCols[21].set(status.containsKey(LikelyResolutionProvided) == true ? status.get(LikelyResolutionProvided) : "");
		retCols[22].set(status.containsKey(Escalated) == true ? status.get(Escalated) : "");
		retCols[23].set(status.containsKey(PendingDealerSurvey) == true ? status.get(PendingDealerSurvey) : "");
		
		retCols[24].set(status.containsKey(Resolved)==true && CASE_CLOSE_TYP.toString().equals(COMPLETED)? status.get(Resolved): "");
		
		retCols[25].set(status.containsKey(Resolved) == true ? status.get(Resolved) : "");
		//SP70128 : Status Timestamp Columns [END]

		//sp70128:Closure type for case if case closed
		retCols[26].set(CASE_CLOSE_TYP != null ? CASE_CLOSE_TYP.getValue():"");

		//SP70128: Push the output row
		forward(retCols);
	}

}
