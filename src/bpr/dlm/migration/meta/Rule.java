/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Rule {
	String mName;
	Migration mParent;
	
	List mTargetFieldSet;
	Map mSourceFieldSet;

	HashMap mmapVariable;
	HashMap mmapVariables;

	Vector vVariables;
	

	long mCompleteIndexCount = -1;
	
	private Map mProperties;
	private Map mConnections;
	private Map mSams;
	private Map mFunctions;
	private Map mCodes;

	private List mActionList;
	
	private SamReader mSamReader;
	private String mSource;

	private String mReportFile;
	private SamFooter mReporter;
	
	private boolean mbElapsed = false;

	private long mErrTotalCount;

	private Logger log;

	Rule(String name, String sourceTable, String reportfile, Logger log){
		mTargetFieldSet = new ArrayList();
		mSourceFieldSet = new HashMap();
		mmapVariable = new HashMap();
		mmapVariables = new HashMap();
		
		vVariables = new Vector();
		
		mActionList = new ArrayList();

		this.log = log;
		
		mName = name;
		mSource = sourceTable;
		mReportFile = reportfile;
	}
	
	public void addSourceField(String name, String group, String position) throws Exception{
		SourceField sourceField = (SourceField) mSourceFieldSet.get(group+"."+name);
		if (sourceField == null){
			mSourceFieldSet.put(group+"."+name, new SourceField(name, group, position));
		} else {
			throw new Exception("duplicated source field:"+ group + "." + name);
		}
	}
	public void addVariable(String name, int type, String value) throws Exception{
		Variable var = new Variable(type, value);
		mmapVariable.put(name,  var);
		vVariables.add(var);
	}
	public void addVariables(String db, String name, String value, String groupid) throws Exception{
		mmapVariables.put(name,  new Variables(db, value, groupid));
	}
	public void addTargetFieldSet(TargetFieldSet fieldset){
		mTargetFieldSet.add(fieldset);
	}
	
	public void addAction(Action action){
		mActionList.add(action);
	}
	
	public void init(Migration migration) throws Exception{
		this.mProperties = migration.mProperties;
		this.mConnections = migration.mConnectionList;
		this.mSams = migration.mSamList;
		this.mFunctions = migration.mFunctionList;
		this.mCodes = migration.mCodeList;
		
		this.mParent = migration;
		
		if (mReportFile == null || mReportFile.equals("")){
			throw new Exception("please specify samfile name for error output!");
		}
		mReporter = new SamFooter(mReportFile);
		try{
			mErrTotalCount = Long.parseLong((String)mProperties.get("error-count"));
		} catch(Exception e){
			mErrTotalCount = 0;
		}
		if (mErrTotalCount == 0)
			mErrTotalCount = 1000;
		
		String elapsed = (String)mProperties.get("elapsed-time");
		if (elapsed != null && elapsed.equals("yes")){
			mbElapsed = true;
		}

	}
	
	public void run() throws Exception {
		SamReader reader = (SamReader)mSams.get(mSource);
		if (reader == null)
			throw new Exception("해당되는 SAM 파일이 없습니다");
		
		long readIndex = 0;
//		long reportIndex = -1;
		long current_index = 0;
		long startIndex = 0;
		long start_time = 0;
		
		if (mParent.mStatusManager != null)
			startIndex = mParent.mStatusManager.getCompleteCount(mName);

		if (mbElapsed){
			start_time = System.currentTimeMillis();
		}

		try {
			mReporter.setSamReader(reader);

			// 시작 인덱스를 status파일로 부터 읽어들인 값으로 세팅한다.
			if (startIndex != 0){
				if (mParent.mStatusManager != null)
					mParent.mStatusManager.getTableCount(mName, mTargetFieldSet);
			}
			
			reader.init(startIndex);
			log.info("시작 인덱스:"+(startIndex+1));
			
			// 커밋 카운트를 설정.
//			int commit_count = Integer.parseInt((String)mProperties.get("commit-count"));
			readIndex = startIndex;
			
			while(true){
				
				current_index = reader.next();
				// 읽은 데이터가 한개도 없으므로 다음 그룹으로..
				if (reader.getGroupCount() < 1){
					continue;
				}
				
				// 전 단계 시작 인덱스에 다다를때까지 이부분 실행.
				if (current_index != 0 && current_index <= startIndex){
//					if (current_index > reportIndex)
//						reportIndex = mReporter.getNext();
					
//					if (current_index != reportIndex){
						readIndex = current_index;
						continue;
//					}
				}
				
				// SAM파일 READ에러.
				if (current_index < 0){
					throw new Exception("SAM파일 READ 에러"+reader.mFiles.get(reader.mfileIndex));
				}

				// 그룹 단위로 SAM파일 데이터 얻는다. 준비..
				Iterator i = mSourceFieldSet.values().iterator();
				while(i.hasNext()){
					// 컬럼 값들을 읽어들여 원본필드에 셋한다. 스몰그룹에 따라 값이 여러개일 수도..
					SourceField sourceField= (SourceField)i.next();

					int size = reader.getSize(sourceField.mGroup);
					if (size <= 0){
//						log.debug("존재하지 않는 그룹아이디입니다:" + sourceField.mGroup);
						sourceField.setValue(null);
						continue;
					}
					String[] value = new String[size]; 
					for (int j = 0; j < size; j++) {
						try {
							value[j] = reader.getValue(sourceField, j);
						} catch (Exception e) {
							log.error("컬럼수가 잘 못 세팅되어 있을 가능성이 있습니다:"+sourceField.mGroup+":"+sourceField.mPosition,e);
							throw e;
						}
					}
					sourceField.setValue(value);
				}
				
				// 변수 설정
				// 내부변수 초기화
//				i = mmapVariable.values().iterator();
//				while(i.hasNext()){
//					Variable before = (Variable)i.next();
//					if (before.mType == Variable.ARRAYLIST){ // ArrayList, internal string
//						if (before.mArrayList != null)
//							before.mArrayList.clear();
//						continue;
//					}else if (before.mType == Variable.INTERNAL_STRING){ // ArrayList, internal string
//							before.mAfter = "";
//							continue;
//					}
//
//					// 무조건 첫번째 값만, 그룹은 마스터를 기준으로 하기 때문에 비워둔다.
//					before.mAfter = (String)parseTargetValue(0, before.mBefore, "", 1, null);
//				}
				
				for (int j = 0; j < vVariables.size(); j++) {
					Variable before = (Variable)vVariables.get(j);
					if (before.mType == Variable.ARRAYLIST){ // ArrayList, internal string
						if (before.mArrayList != null)
							before.mArrayList.clear();
						continue;
					}else if (before.mType == Variable.INTERNAL_STRING){ // ArrayList, internal string
							before.mAfter = "";
							continue;
					}

					// 무조건 첫번째 값만, 그룹은 마스터를 기준으로 하기 때문에 비워둔다.
					before.mAfter = (String)parseTargetValue(0, before.mBefore, "", 1, null);
				}
				

				// 배열 변수 설정.
				i = mmapVariables.values().iterator();
				while(i.hasNext()){
					Variables before = (Variables)i.next();
					int rowsize = reader.getSize(before.mGroup);
					// value1|value2|value3 형식으로.. 리턴된다. rowsize > 1이므로
					before.mAfter = (String)parseTargetValue(0, before.mBefore, "", rowsize, null);
				}

				// 매핑..
				boolean bInserted = true;
				i = mTargetFieldSet.iterator();
B_POINT:		while(i.hasNext()){
					TargetFieldSet targetFieldSet= (TargetFieldSet)i.next();
					targetFieldSet.mCurrentInsertedRow = 0;
					
					Connection parent_conn = (Connection)mConnections.get(targetFieldSet.mDB);
					if (parent_conn == null)
						throw new Exception("DB 커넥션 에러");
					
					int rowsize = reader.getSize(targetFieldSet.mGroup);
					int targetFieldNum = targetFieldSet.mField.size();
					
IGNORE_POINT:		for (int k = 0; k < rowsize; k++) {
						String[] targetFieldValues = new String[targetFieldNum];
						for (int j = 0; j < targetFieldNum; j++) {
							Field targetField = (Field)targetFieldSet.mField.get(j);
							try {
								String value = (String)parseTargetValue(k, targetField.mValueBefore, targetFieldSet.mGroup, 1, targetFieldSet);

								targetFieldValues[j] = value;
								targetField.mValueAfter = value;
							} catch (IgnoreException e) {
								break IGNORE_POINT;
							}
						}
						
						// 값들을 DB에 입력한다.
						int col_count = insert2Target(parent_conn, current_index, targetFieldSet, targetFieldValues);
						if (col_count > 0)
							targetFieldSet.mCurrentInsertedRow ++;
						
						if (col_count < 1)
							bInserted = false;
						
						if (!bInserted){
							break B_POINT;
						}
					}
				}
				
				// 성공여부 설정.
				boolean bSuccess = false;
				if (bInserted){
					bSuccess = true;
				}
				
				// action 처리..
				i = mActionList.iterator();
				Action action;
				while (i.hasNext()) {
					action = (Action) i.next();
					String[] params = action.getBeforeParams();
					Object[] after_params = new Object[params.length];
					for (int j = 0; j < params.length; j++) {
						Object value = parseTargetValue(0, params[j], "", 1, null);
						after_params[j] = value;
					}
					try {
						action.action(bSuccess, after_params);
					} catch (Exception e) {
						log.warn("action:"+current_index+":", e);
						
						if (bSuccess){
							mReporter.addGroupContents();
							mParent.mbErrorSam = true;
							long line = mReporter.getCurrentGroup();
							if (line > mErrTotalCount){
								throw new Exception("허용하는 Error SamFile 출력 그룹수를 벗어납니다.");
							}
						}
						
						setLastErrorVariable(e.getMessage());
						
						// db rollback
						mParent.rollbackAll();
						// image rollback
						if (mParent.mImageUploader != null)
							mParent.mImageUploader.rollback();

						bSuccess = false;
						
					}
				}
				
				// 정상적으로 모두 Insert 되었을때만 commit 처리한다.
				if (bInserted == true && current_index != 0){
					// 100개 단위로 완료 메세지 뿌린다.
					if (current_index % 100 == 0){
						// 걸린 시간 계산.
						if (mbElapsed){
							printElapsedTime(start_time);
							start_time = System.currentTimeMillis();
						}
						
						log.info(reader.m_current_filename+" 완료 INDEX:"+current_index);
					}
					
					//=============현재는 그룹단위로 COMMIT!!=====
					mParent.commitAll();
					//============================================

					// commit 되었으므로, table출력 수 증가.
					i = mTargetFieldSet.iterator();
					while(i.hasNext()){
						TargetFieldSet targetFieldSet= (TargetFieldSet)i.next();
//						int rowsize = reader.getSize(targetFieldSet.mGroup);
						if (targetFieldSet.mCurrentInsertedRow > 0)
						targetFieldSet.mlCount += targetFieldSet.mCurrentInsertedRow;
//						for (int k = 0; k < rowsize; k++) {
//							targetFieldSet.mlCount++;
//						}
					}
				
				}


				
				// 모든 SAM파일이 다 읽혀졌다면, 종료.
				if (current_index == 0){	//정상 종료...
					readIndex++;
					break;
				}else if (current_index > 0){
					readIndex = current_index;
				}
			}
		} catch (Exception e) {
			// readIndex+1 인 이유는 오류가 나면, readIndex=current_index; 가 실행되기 전이기 때문에 
			log.error("INDEX:"+(current_index), e);
			throw e;
		} finally {
			if (readIndex >= startIndex)
				mParent.checkComplete(mName, readIndex, mTargetFieldSet);
			reader.shutdown();
		}
		
		// 정상 완료 처리
		// mCompleteIndexCount가 commit완료한 인덱스를 가지고 있음.
		try{
			mParent.commitAll();
			if (current_index != 0)
				mParent.checkComplete(mName, readIndex, mTargetFieldSet);
			log.info("정상완료:"+mName+", "+readIndex);

			// 걸린 시간 계산.
			if (mbElapsed){
				printElapsedTime(start_time);
			}
		} catch (Exception e) {
			throw e;
		}
	}
	
	private void printElapsedTime(long start){
		long elapsed_time = System.currentTimeMillis() - start;
    	SimpleDateFormat dateformat = new SimpleDateFormat("mmss.SS");
    	String formatted = dateformat.format(new Date(elapsed_time));
		log.info("elaped time = " + formatted);
	}
	
	private Object parseTargetValue(int row, String before, String group, int size_value, TargetFieldSet targetFieldSet) throws IgnoreException, Exception{
		if (before.length() == 0){
			return before;
		} else if (before.charAt(0) == '='){
			SourceField sourceField;
			sourceField = (SourceField)mSourceFieldSet.get(before.substring(1));
			
			if (sourceField == null){
				throw new Exception("존재하지 않는 source-field입니다:"+before.substring(1));
			}
			if (sourceField.mValue == null){
//				throw new Exception("데이터에 매핑할 값이 없습니다:"+before.substring(1));
//				return null;
				return "NULL";
			} else {
				// 그룹이 같으면 같은 행의 값을 리턴.
				if (sourceField.mGroup.equals(group)){
					return sourceField.mValue[row];
				}else{
					return sourceField.mValue[0];
				}
			}
		} else if (before.charAt(0) == '!'){	// 변수
			Variable variable;
			variable = (Variable)mmapVariable.get(before.substring(1));
			
			if (variable == null){
				if (before.substring(1).equalsIgnoreCase("GROUPCOUNT")){
//					int size = reader.getSize(sourceField.mGroup);
					return ""+size_value;
				} else if (before.substring(1).equalsIgnoreCase("MIG_ERROR_MSG")){
//						int size = reader.getSize(sourceField.mGroup);
						return "";
				}else{
//					throw new Exception("존재하지 않는 변수명:"+before.substring(1));
					return "";
				}
			}
			
			if (variable.mType == Variable.ARRAYLIST){
				return variable.mArrayList;
			}
			
			return variable.mAfter;
		} else if (before.charAt(0) == '@'){	// 배열 변수
			Variables variables;
			variables = (Variables)mmapVariables.get(before.substring(1));
			// 그룹이 같으면 같은 행의 값을 리턴.
			
			if (variables.mAfter == null){
				throw new Exception("not defined variables:" + before.substring(1));
			}
			String[] splited = variables.mAfter.split("\\|");
			return splited[row];
		} else if (before.charAt(0) == '$'){	// 함수
			String ret = "";
			for (int j = 0; j < size_value; j++) {
				String param = null;
				Function func;
				int pos = before.indexOf(",");
				if (pos < 0){
					func = (Function)mFunctions.get(before.substring(1));
				} else {
					func = (Function)mFunctions.get(before.substring(1,pos));
					param = before.substring(pos+1);
				}
				
				// function 파라메터 준비..
				String[] params = null;
				String[] settedparams = null;
				if(param != null){
					params = param.split(",");
					settedparams = new String[params.length];

					for (int i = 0; i < params.length; i++) {
						settedparams[i] = (String)parseTargetValue(row, params[i], group, size_value, targetFieldSet);
//						if (params[i].charAt(0) == '='){
//							SourceField sourceField;
//							sourceField = (SourceField)mSourceFieldSet.get(params[i].substring(1));
//							if (sourceField == null)
//								throw new Exception("필드값이 존재하지 않습니다. 룰파일 오류:"+params[i].substring(1));
//							
//							if (sourceField.mValue == null){
//								throw new Exception("데이터에 매핑할 값이 없습니다:"+params[i].substring(1));
////								settedparams[i] = null;
//							} else {
//								// 그룹이 같으면 같은 행의 값을 리턴.
//								if (sourceField.mGroup.equals(group)){
//									settedparams[i] = sourceField.mValue[row];
//								}else{
//									settedparams[i] = sourceField.mValue[0];
//								}
//							}
//						}else{
//							settedparams[i] = params[i];
//						}
					}
				}
				
				try{
					if (func == null){
						throw new Exception("정의되지 않은 함수입니다:"+before);
					}
					ret += func.getValue(settedparams);
					if (j < size_value-1){
						ret += "|";
					}
				} catch(IgnoreException e){
					throw e;
				} catch(Exception e){
					log.error("함수에러: 값을 가져 올 수 없습니다:"+before);
					throw e;
				}
			}

			return ret;
		} else if (before.charAt(0) == '~'){	// 함수
			String ret = null;
			for (int j = 0; j < size_value; j++) {
				String param = null;
				String func;

				int pos = before.indexOf("|");
				if (pos < 0){
					func = before.substring(1);
				} else {
					func = before.substring(1,pos);
					param = before.substring(pos+1);
				}
				
				try{
					String[] params = null;
					if(param != null){
						params = param.split("\\|");
					}

					if (func.equalsIgnoreCase("DECODE_PROC")){
						// function 파라메터 준비..
						String[] settedparams = null;
						if (params.length < 3){
							throw new Exception("DECODE_PROC: not sufficient parameter");
						}
						
						String cond = (String)parseTargetValue(row, params[0], group, size_value, targetFieldSet);
						log.debug("DECODE_PROC condition="+cond);
						for (int i = 1; i < params.length; i+=2) {
							String lvalue = cond;
							if (lvalue.equals("null"))
								lvalue = "";
							if (lvalue.equalsIgnoreCase(params[i])){
								// 수행만 할 뿐, 리턴값을 무시하자.
								parseTargetValue(row, params[i+1], group, size_value, targetFieldSet);
								log.debug("inserted com_mig011"+params[i+1]);
//								if (j < size_value-1){
//									ret += "|";
//								}
								ret = lvalue;
								break;
							}
						}
						if (ret == null){
							ret = cond;
						}
					} else if (func.equalsIgnoreCase("VAR_INPUT")){
						if (params.length < 2){
							throw new Exception("VAR_INPUT: not sufficient parameter");
						}
						Variable var =  (Variable) mmapVariable.get(params[0]);
						if (var == null){
							var = new Variable(Variable.ARRAYLIST, params[1]);
							var.mArrayList = new ArrayList();
							mmapVariable.put( params[0], var);
						}
						var.mArrayList.add(parseTargetValue(row, params[1], group, size_value, targetFieldSet));
					} else if (func.equalsIgnoreCase("VAR_SINGLE_INPUT")){
						if (params.length < 2){
							throw new Exception("VAR_SINGLE_INPUT: not sufficient parameter");
						}
						Variable var =  (Variable) mmapVariable.get(params[0]);
						if (var == null){
							var = new Variable(Variable.INTERNAL_STRING, params[1]);
							mmapVariable.put( params[0], var);
						}
						var.mAfter = (String) parseTargetValue(row, params[1], group, size_value, targetFieldSet);
					}
				} catch(IgnoreException e){
//					log.error("후처리 함수(~) 에러: 값을 가져 올 수 없습니다:"+before+", "+e.getMessage());
					throw e;
				} catch(Exception e){
					log.error("후처리 함수(~) 에러: 값을 가져 올 수 없습니다:"+before+", "+e.getMessage());
					throw e;
				}
			}
			
			return ret;
		} else if (before.charAt(0) == '#'){	// 함수
			if (targetFieldSet == null){
				return "";
			}
			
			int nth = Integer.parseInt(before.substring(1));
			Field targetField = (Field)targetFieldSet.mField.get(nth);
			return targetField.mValueAfter;
		} else{	// 단지 값.
			return before;
		}

	}
	
	private void setLastErrorVariable(String message){
		Variable var = new Variable(Variable.INTERNAL_STRING, " ");
		var.mAfter = message;
		mmapVariable.put( "MIG_ERROR_MSG", var);
	}
	
	// param1: 테이블, 필드정보를 위해..
	private int insert2Target(Connection dbconn, long current_index, TargetFieldSet targetFieldSet, String[] targetFieldValues) throws Exception{
		PreparedStatement stmt = null;
		String sql = null;
		int count = 0;

		try{
			sql = getPreparedStatement(targetFieldSet.mTable, targetFieldSet.mField);
			stmt = dbconn.prepareStatement(sql);
			
			for (int i = 0; i < targetFieldValues.length; i++) {
				Field targetField = (Field)targetFieldSet.mField.get(i);

				// 무시 필드 처리
				if (targetField.mName.equals("IGNORE_THIS_FIELD")){
					continue;
				}
				// 값이 null일경우, 매핑 자체를 하지 말자. 20060322
				if (targetField.mValueAfter == null){
					continue;
				}

				count++;
				
				if (targetFieldValues[i] == null){
					throw new Exception("매핑되지 않았습니다:"+targetField.mValueBefore);
				}
				
				String value = targetFieldValues[i].trim();
				if (value.equals("NULL")){
					value = "";
				}
				if (targetField.mType.equals("string")) {
					stmt.setString(count, value);
				} else if (targetField.mType.equals("double")) {
					if (value.equals(""))
						stmt.setDouble(count, 0);
					else
						stmt.setDouble(count, Double.parseDouble(value));
					// TODO: 일단, ""이면 0으로 처리..
				} else if (targetField.mType.equals("int")) {
					if (value.equals(""))
						stmt.setLong(count, 0);
					else
						stmt.setLong(count, Long.parseLong(value));
					// TODO: 일단, ""이면 0으로 처리..
				} else if (targetField.mType.equals("date")) {
					stmt.setDate(count, java.sql.Date.valueOf(value));
				} else if (targetField.mType.equals("timestamp")) {
					stmt.setTimestamp(count, Timestamp.valueOf(value));
//					stmt.setObject(count, value);
				} else if (targetField.mType.equals("blob")) {
					stmt.setObject(count, value);
				} else {
					stmt.setObject(count, value);
				}
			}

			stmt.execute();
		} catch(SQLException e){
			String out = targetFieldValues[0];
			for (int i = 1; i < targetFieldValues.length; i++) {
				out += ","+targetFieldValues[i];
			}
			log.warn("sql="+sql);
			log.warn("insert2target error:"+current_index+":"+out,e);
			
			// 에러메세지를 변수에 등록
			setLastErrorVariable(e.getMessage());

//			dbconn.rollback();
			mParent.rollbackAll();
			stmt.close();
			
			mReporter.addGroupContents();
			mParent.mbErrorSam = true;
			long line = mReporter.getCurrentGroup();
			if (line > mErrTotalCount){
				throw new Exception("허용하는 Error SamFile 출력 그룹수를 벗어납니다.");
			}

			return 0;
			
			// Report만 남기고, 진행하기위한 처리
//			throw new Exception(targetFieldSet.mTable+":"+e.getMessage());
		} catch(Exception ex){
			mParent.rollbackAll();
//			dbconn.rollback();
			if (log.isDebugEnabled()){
				for (int i = 0; i < targetFieldValues.length; i++) {
					log.warn(i + "=" + targetFieldValues[i]);
				}
				ex.printStackTrace();
			}
			throw new Exception(targetFieldSet.mTable+":"+ex.getClass().getName() + ":"+ex.getMessage());
	    }finally{
	    	stmt.close();
	    }
	    
	    return count;
	}

	private String getPreparedStatement(String table, List fields) throws SQLException {
		StringBuffer sql = new StringBuffer("insert into ");
		sql = sql.append(table);
		sql = sql.append(" (");
		Iterator i = fields.iterator();
		int count = 0;
		Field field;
		while (i.hasNext()) {
			field = (Field) i.next();
			if (field.mName.equals("IGNORE_THIS_FIELD")){
				continue;
			}
			// 값이 null일경우, 매핑 자체를 하지 말자. 20060322
			if (field.mValueAfter == null){
				continue;
			}

			count++;
			if (count != 1){
				sql = sql.append(", ");
			}
			sql = sql.append(field.mName);
		}
		sql = sql.append(") values (");
		for (int j = 0; j < count; j++) {
			sql = sql.append("?");
			if (j < count - 1) {
				sql = sql.append(", ");
			}
		}
		sql = sql.append(")");
		
		return sql.toString();
	}
}
