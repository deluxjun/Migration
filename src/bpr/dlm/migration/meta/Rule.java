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
			throw new Exception("�ش�Ǵ� SAM ������ �����ϴ�");
		
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

			// ���� �ε����� status���Ϸ� ���� �о���� ������ �����Ѵ�.
			if (startIndex != 0){
				if (mParent.mStatusManager != null)
					mParent.mStatusManager.getTableCount(mName, mTargetFieldSet);
			}
			
			reader.init(startIndex);
			log.info("���� �ε���:"+(startIndex+1));
			
			// Ŀ�� ī��Ʈ�� ����.
//			int commit_count = Integer.parseInt((String)mProperties.get("commit-count"));
			readIndex = startIndex;
			
			while(true){
				
				current_index = reader.next();
				// ���� �����Ͱ� �Ѱ��� �����Ƿ� ���� �׷�����..
				if (reader.getGroupCount() < 1){
					continue;
				}
				
				// �� �ܰ� ���� �ε����� �ٴٸ������� �̺κ� ����.
				if (current_index != 0 && current_index <= startIndex){
//					if (current_index > reportIndex)
//						reportIndex = mReporter.getNext();
					
//					if (current_index != reportIndex){
						readIndex = current_index;
						continue;
//					}
				}
				
				// SAM���� READ����.
				if (current_index < 0){
					throw new Exception("SAM���� READ ����"+reader.mFiles.get(reader.mfileIndex));
				}

				// �׷� ������ SAM���� ������ ��´�. �غ�..
				Iterator i = mSourceFieldSet.values().iterator();
				while(i.hasNext()){
					// �÷� ������ �о�鿩 �����ʵ忡 ���Ѵ�. �����׷쿡 ���� ���� �������� ����..
					SourceField sourceField= (SourceField)i.next();

					int size = reader.getSize(sourceField.mGroup);
					if (size <= 0){
//						log.debug("�������� �ʴ� �׷���̵��Դϴ�:" + sourceField.mGroup);
						sourceField.setValue(null);
						continue;
					}
					String[] value = new String[size]; 
					for (int j = 0; j < size; j++) {
						try {
							value[j] = reader.getValue(sourceField, j);
						} catch (Exception e) {
							log.error("�÷����� �� �� ���õǾ� ���� ���ɼ��� �ֽ��ϴ�:"+sourceField.mGroup+":"+sourceField.mPosition,e);
							throw e;
						}
					}
					sourceField.setValue(value);
				}
				
				// ���� ����
				// ���κ��� �ʱ�ȭ
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
//					// ������ ù��° ����, �׷��� �����͸� �������� �ϱ� ������ ����д�.
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

					// ������ ù��° ����, �׷��� �����͸� �������� �ϱ� ������ ����д�.
					before.mAfter = (String)parseTargetValue(0, before.mBefore, "", 1, null);
				}
				

				// �迭 ���� ����.
				i = mmapVariables.values().iterator();
				while(i.hasNext()){
					Variables before = (Variables)i.next();
					int rowsize = reader.getSize(before.mGroup);
					// value1|value2|value3 ��������.. ���ϵȴ�. rowsize > 1�̹Ƿ�
					before.mAfter = (String)parseTargetValue(0, before.mBefore, "", rowsize, null);
				}

				// ����..
				boolean bInserted = true;
				i = mTargetFieldSet.iterator();
B_POINT:		while(i.hasNext()){
					TargetFieldSet targetFieldSet= (TargetFieldSet)i.next();
					targetFieldSet.mCurrentInsertedRow = 0;
					
					Connection parent_conn = (Connection)mConnections.get(targetFieldSet.mDB);
					if (parent_conn == null)
						throw new Exception("DB Ŀ�ؼ� ����");
					
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
						
						// ������ DB�� �Է��Ѵ�.
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
				
				// �������� ����.
				boolean bSuccess = false;
				if (bInserted){
					bSuccess = true;
				}
				
				// action ó��..
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
								throw new Exception("����ϴ� Error SamFile ��� �׷���� ����ϴ�.");
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
				
				// ���������� ��� Insert �Ǿ������� commit ó���Ѵ�.
				if (bInserted == true && current_index != 0){
					// 100�� ������ �Ϸ� �޼��� �Ѹ���.
					if (current_index % 100 == 0){
						// �ɸ� �ð� ���.
						if (mbElapsed){
							printElapsedTime(start_time);
							start_time = System.currentTimeMillis();
						}
						
						log.info(reader.m_current_filename+" �Ϸ� INDEX:"+current_index);
					}
					
					//=============����� �׷������ COMMIT!!=====
					mParent.commitAll();
					//============================================

					// commit �Ǿ����Ƿ�, table��� �� ����.
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


				
				// ��� SAM������ �� �������ٸ�, ����.
				if (current_index == 0){	//���� ����...
					readIndex++;
					break;
				}else if (current_index > 0){
					readIndex = current_index;
				}
			}
		} catch (Exception e) {
			// readIndex+1 �� ������ ������ ����, readIndex=current_index; �� ����Ǳ� ���̱� ������ 
			log.error("INDEX:"+(current_index), e);
			throw e;
		} finally {
			if (readIndex >= startIndex)
				mParent.checkComplete(mName, readIndex, mTargetFieldSet);
			reader.shutdown();
		}
		
		// ���� �Ϸ� ó��
		// mCompleteIndexCount�� commit�Ϸ��� �ε����� ������ ����.
		try{
			mParent.commitAll();
			if (current_index != 0)
				mParent.checkComplete(mName, readIndex, mTargetFieldSet);
			log.info("����Ϸ�:"+mName+", "+readIndex);

			// �ɸ� �ð� ���.
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
				throw new Exception("�������� �ʴ� source-field�Դϴ�:"+before.substring(1));
			}
			if (sourceField.mValue == null){
//				throw new Exception("�����Ϳ� ������ ���� �����ϴ�:"+before.substring(1));
//				return null;
				return "NULL";
			} else {
				// �׷��� ������ ���� ���� ���� ����.
				if (sourceField.mGroup.equals(group)){
					return sourceField.mValue[row];
				}else{
					return sourceField.mValue[0];
				}
			}
		} else if (before.charAt(0) == '!'){	// ����
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
//					throw new Exception("�������� �ʴ� ������:"+before.substring(1));
					return "";
				}
			}
			
			if (variable.mType == Variable.ARRAYLIST){
				return variable.mArrayList;
			}
			
			return variable.mAfter;
		} else if (before.charAt(0) == '@'){	// �迭 ����
			Variables variables;
			variables = (Variables)mmapVariables.get(before.substring(1));
			// �׷��� ������ ���� ���� ���� ����.
			
			if (variables.mAfter == null){
				throw new Exception("not defined variables:" + before.substring(1));
			}
			String[] splited = variables.mAfter.split("\\|");
			return splited[row];
		} else if (before.charAt(0) == '$'){	// �Լ�
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
				
				// function �Ķ���� �غ�..
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
//								throw new Exception("�ʵ尪�� �������� �ʽ��ϴ�. ������ ����:"+params[i].substring(1));
//							
//							if (sourceField.mValue == null){
//								throw new Exception("�����Ϳ� ������ ���� �����ϴ�:"+params[i].substring(1));
////								settedparams[i] = null;
//							} else {
//								// �׷��� ������ ���� ���� ���� ����.
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
						throw new Exception("���ǵ��� ���� �Լ��Դϴ�:"+before);
					}
					ret += func.getValue(settedparams);
					if (j < size_value-1){
						ret += "|";
					}
				} catch(IgnoreException e){
					throw e;
				} catch(Exception e){
					log.error("�Լ�����: ���� ���� �� �� �����ϴ�:"+before);
					throw e;
				}
			}

			return ret;
		} else if (before.charAt(0) == '~'){	// �Լ�
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
						// function �Ķ���� �غ�..
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
								// ���ุ �� ��, ���ϰ��� ��������.
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
//					log.error("��ó�� �Լ�(~) ����: ���� ���� �� �� �����ϴ�:"+before+", "+e.getMessage());
					throw e;
				} catch(Exception e){
					log.error("��ó�� �Լ�(~) ����: ���� ���� �� �� �����ϴ�:"+before+", "+e.getMessage());
					throw e;
				}
			}
			
			return ret;
		} else if (before.charAt(0) == '#'){	// �Լ�
			if (targetFieldSet == null){
				return "";
			}
			
			int nth = Integer.parseInt(before.substring(1));
			Field targetField = (Field)targetFieldSet.mField.get(nth);
			return targetField.mValueAfter;
		} else{	// ���� ��.
			return before;
		}

	}
	
	private void setLastErrorVariable(String message){
		Variable var = new Variable(Variable.INTERNAL_STRING, " ");
		var.mAfter = message;
		mmapVariable.put( "MIG_ERROR_MSG", var);
	}
	
	// param1: ���̺�, �ʵ������� ����..
	private int insert2Target(Connection dbconn, long current_index, TargetFieldSet targetFieldSet, String[] targetFieldValues) throws Exception{
		PreparedStatement stmt = null;
		String sql = null;
		int count = 0;

		try{
			sql = getPreparedStatement(targetFieldSet.mTable, targetFieldSet.mField);
			stmt = dbconn.prepareStatement(sql);
			
			for (int i = 0; i < targetFieldValues.length; i++) {
				Field targetField = (Field)targetFieldSet.mField.get(i);

				// ���� �ʵ� ó��
				if (targetField.mName.equals("IGNORE_THIS_FIELD")){
					continue;
				}
				// ���� null�ϰ��, ���� ��ü�� ���� ����. 20060322
				if (targetField.mValueAfter == null){
					continue;
				}

				count++;
				
				if (targetFieldValues[i] == null){
					throw new Exception("���ε��� �ʾҽ��ϴ�:"+targetField.mValueBefore);
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
					// TODO: �ϴ�, ""�̸� 0���� ó��..
				} else if (targetField.mType.equals("int")) {
					if (value.equals(""))
						stmt.setLong(count, 0);
					else
						stmt.setLong(count, Long.parseLong(value));
					// TODO: �ϴ�, ""�̸� 0���� ó��..
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
			
			// �����޼����� ������ ���
			setLastErrorVariable(e.getMessage());

//			dbconn.rollback();
			mParent.rollbackAll();
			stmt.close();
			
			mReporter.addGroupContents();
			mParent.mbErrorSam = true;
			long line = mReporter.getCurrentGroup();
			if (line > mErrTotalCount){
				throw new Exception("����ϴ� Error SamFile ��� �׷���� ����ϴ�.");
			}

			return 0;
			
			// Report�� �����, �����ϱ����� ó��
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
			// ���� null�ϰ��, ���� ��ü�� ���� ����. 20060322
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
