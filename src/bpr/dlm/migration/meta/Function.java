/*
 * Created on 2006. 1. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import bpr.dlm.migration.db.DBCommand;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Function {
	String sql;
	String mValue;
	int mType;
	
	private Connection m_db_conn;
	private Migration m_mig;
	
	private boolean mbIgnoreQuotation;

	private Logger log;
	
	public final static int NEWIMAGEKEY = 1;
	public final static int NEWSEQUENCE = 2;
	public final static int LEFT = 3;
	public final static int RIGHT = 4;
	public final static int MERGECOMMA = 5;
	public final static int INSERTINTO = 6;
	public final static int TRUNCATE = 7;
	public final static int SP_CALL = 8;
	public final static int IGNORE_INSERT = 9;
	public final static int MERGE_STR = 10;
	
	public final static int NORMAL_SQL = 11;
	public final static int INTERNAL = 12;
	public final static int UPDATE_SQL = 13;
	public final static int NEWSEQUENCE2 = 14;
	public final static int GETUNIQUE = 15;
	
	Function(Migration parent, int type, Logger log){
		this.log = log;
		this.m_mig = parent;
		
		mType = type;
		mValue = null;
	}
	
	Function(int type, String db, String sql, String ignore_quotation, Migration parent, Logger log){
		this.log = log;
		this.m_mig = parent;

		m_db_conn = (Connection) parent.getConnectionList().get(db);
		this.sql = sql;
		mValue = null;
		mType = type;
		
		if (ignore_quotation != null && ignore_quotation.equalsIgnoreCase("yes")){
			mbIgnoreQuotation = true;
		}
	}
	
	public String getValue(String[] params) throws IgnoreException, Exception{
		mValue = "";

		if (mType == NORMAL_SQL){
			String ret = null;
			Statement stmt = null;
			Connection dbconn = null;
			String newsql;
			
			if(params != null){
				newsql = parseSQL(sql, params);
			}else{
				newsql = sql;
			}
			
			try{
				dbconn = m_db_conn;
				
				stmt = dbconn.createStatement();
				
				ResultSet rs = stmt.executeQuery(newsql);
				log.debug("func query: " + newsql);

//				rs.next();
//				mValue = rs.getString(1);
				
				if (rs.next()){
					mValue = rs.getString(1);
					log.debug("func value : " + mValue);
				}
				else{
					mValue = "";
				}
			}
			catch(SQLException e){
				throw new Exception(newsql+"::"+e.getMessage());
		    }finally{
		    	stmt.close();
		    }
		} else if (mType == UPDATE_SQL){
				String ret = null;
				Statement stmt = null;
				Connection dbconn = null;
				String newsql;
				
				if(params != null){
					newsql = parseSQL(sql, params);
				}else{
					newsql = sql;
				}
				
				try{
					dbconn = m_db_conn;
					
					stmt = dbconn.createStatement();
					
					int count = stmt.executeUpdate(newsql);
					log.debug("func query: " + newsql);

					mValue = "" + count;
				}
				catch(SQLException e){
					throw new Exception(newsql+"::"+e.getMessage());
			    }finally{
			    	stmt.close();
			    }
		} else if (mType == INTERNAL){
			String ret = null;
			String stmt;
			
			if(params != null){
				stmt = parseSQL(sql, params);
			}else{
				stmt = sql;
			}
			
			try{
//				stmt = stmt.replaceAll("'","");
				mValue = internalFunction(stmt);
			}
			catch(Exception e){
				throw new Exception(stmt+":"+e.getMessage());
		    }
		} else if (mType == NEWSEQUENCE){
			mValue = "";
			int i_size = 0;
			// 일련번호 생성 작업.
			if (params == null || params.length < 2)
				throw new Exception("NEWSEQUENCE 파라미터 오류.");
			try {
				for (int i = 0; i < params.length - 1; i++) {
					mValue += params[i];
				}
				i_size = Integer.parseInt(params[params.length-1]);
			} catch (Exception e) {
				log.warn("NEWSEQUENCE 연산:"+e.getMessage());
				throw e;
			}
			
			mValue += getSequence(i_size);
			log.debug("NEWSEQUENCE : " + mValue);
			return mValue;
		} else if (mType == NEWSEQUENCE2){
			mValue = "";
			int i_size = 0;
			String id = "0";
			String format = "yyyyMMddHHmmss";
			// 일련번호 생성 작업.
			if (params == null || params.length < 4)
				throw new Exception("NEWSEQUENCE2 파라미터 오류.");
			try {
				for (int i = 0; i < params.length - 3; i++) {
					mValue += params[i];
				}
				format = params[params.length-3];
				id = params[params.length-2];
				i_size = Integer.parseInt(params[params.length-1]);
			} catch (Exception e) {
				log.warn("NEWSEQUENCE2 연산:"+e.getMessage());
				throw e;
			}
			
			mValue += getSequence(id, format, i_size);
			log.debug("NEWSEQUENCE2 : " + mValue);
			return mValue;
		} else if (mType == GETUNIQUE){
			mValue = "";
			int i_size = 0;
			String value = "";
			// 일련번호 생성 작업.
			if (params == null || params.length < 2)
				throw new Exception("GETUNIQUE 파라미터 오류.");
			try {
//				for (int i = 0; i < params.length - 2; i++) {
//					mValue += params[i];
//				}
				value = params[0];
				i_size = Integer.parseInt(params[1]);
			} catch (Exception e) {
				log.warn("GETUNIQUE 연산:"+e.getMessage());
				throw e;
			}
			
			mValue = getSequence2(value, i_size);
			log.debug("GETUNIQUE : " + mValue);
			return mValue;
		} else if (mType == NEWIMAGEKEY){
			mValue = "";
			// TODO:이미지키 생성작업...
			if (params == null)
				throw new Exception("NEWIMAGEKEY 파라미터가 없습니다.");
			try {
				for (int i = 0; i < params.length; i++) {
					mValue += params[i];
				}
			} catch (Exception e) {
				log.warn("NEWIMAGEKEY 연산:"+e.getMessage());
				throw e;
			}
			
			mValue += getSequence(3);
			return mValue;
		} else if (mType == INSERTINTO){
			mValue = "";
			// TODO:이미지키 생성작업...
			if (params == null || params.length < 4)
				throw new Exception("INSERTINTO 파라미터가 부족합니다.");
			
			Connection dbconn = null;

			String table = params[1];
			String query;
			Statement stmt = null;
			int count = 0;
			int i;

			try{
				if (m_db_conn == null)
					m_db_conn = (Connection) m_mig.getConnectionList().get(params[0]);

				dbconn = m_db_conn;

				query = "insert into " + table;
				query += " (" + params[2];
				for (i = 4; i < params.length; i+=2) {
					query += ", " + params[i];
				}
				query = query + ")";
				
				query = query + " values ('" + params[3];
				for (i = 5; i < params.length; i+=2) {
					query += "','" + params[i];
				}
				query = query + "')";
				
				log.debug("insert into:"+ query);

				stmt = dbconn.createStatement();
				count = stmt.executeUpdate(query);
			} catch (SQLException ex) {
				if (log != null) log.warn(ex.getMessage());
//				if (ex.getErrorCode() == 2627){	// 이미 존재
//					return 2;
//				}
				return -1+"";
			} finally{
				if (stmt != null)
					stmt.close();
			}
			
			return count + "";
		} else if (mType == LEFT){	//LEFT 연산
			if (params == null)
				throw new Exception("LEFT 파라미터가 없습니다.");
			try {
				if (params[0].equals(""))
					return "";
				mValue = params[0].substring(0,Integer.parseInt(params[1])); 
			} catch (Exception e) {
				log.warn("LEFT 연산:"+e.getMessage());
				throw e;
			}
			return mValue;
		} else if (mType == RIGHT){	//RIGHT 연산
			if (params == null)
				throw new Exception("RIGHT 파라미터가 없습니다.");
			try {
				if (params[0].equals(""))
					return "";
				int pos = Integer.parseInt(params[1]);
				mValue = params[0].substring(params[0].length()-pos); 
			} catch (Exception e) {
				log.warn("RIGHT 연산:"+e.getMessage());
				throw e;
			}
			return mValue;
		} else if (mType == MERGECOMMA){	//MERGECOMMA 연산
			if (params == null)
				throw new Exception("MERGECOMMA 파라미터가 없습니다.");
			try {
				mValue = params[0];
				for (int i = 1; i < params.length; i++) {
					mValue += "," + params[i];
				}
			} catch (Exception e) {
				log.warn("MERGECOMMA 연산:"+e.getMessage());
				throw e;
			}
			return mValue;
		} else if (mType == MERGE_STR){	//MERGECOMMA 연산
			if (params == null || params.length < 2)
				throw new Exception("MERGE_STR 파라미터가 부족합니다.");
			try {
				String delimiter = params[0];
				mValue = params[1];
				for (int i = 2; i < params.length; i++) {
					mValue += delimiter + params[i];
				}
			} catch (Exception e) {
				log.warn("MERGE_STR 연산:"+e.getMessage());
				throw e;
			}
			log.debug("MERGE_STR : " + mValue);
			return mValue;
		} else if (mType == TRUNCATE){	//MERGECOMMA 연산
			if (params == null || params.length < 3)
				throw new Exception("TRUNCATE 파라미터가 부족합니다.");
			
			try {
				if (params[0] == null)
					return "NULL";
				String delimiter = params[1];
				String[] variables = params[0].split(delimiter);
				int ind = Integer.parseInt(params[2]);
				
				mValue = variables[ind];
			} catch (Exception e) {
				log.warn("TRUNCATE 연산:",e);
				return "NULL";
//				throw e;
			}
			return mValue;
		} else if (mType == IGNORE_INSERT){	//MERGECOMMA 연산
			throw new IgnoreException("Ignore");
		} else if (mType == SP_CALL){
			if (params == null || params.length < 4)
				throw new Exception("SP_CALL 파라미터가 없습니다.");
			
			mValue = "";
			Connection dbconn = null;

			try {
				if (m_db_conn == null)
					m_db_conn = (Connection) m_mig.getConnectionList().get(params[0]);

				dbconn = m_db_conn;

				String spname = params[1];	// sp name
				int nInput = Integer.parseInt(params[2]); // output number
				int nOutput = Integer.parseInt(params[3]); // output number
				String[] values = new String[nInput];
				String[] outputs = new String[nOutput];
				
				for (int i = 0; i < nInput; i++) {
					try{
//						values[i] = "'" + params[i+4] + "'";
						values[i] = params[i+4].trim();
					}catch(Exception e){
						values[i] = "";
					}
				}
				
				DBCommand.sp_execute(dbconn, spname, values, outputs);
				int i;
				for ( i = 0; i < outputs.length - 1; i++) {
					mValue += outputs[i] + "^";
				}
				mValue += outputs[i];
				
				if (log.isDebugEnabled()){
					String inputs = "";
					for (i = 0; i < values.length; i++) {
						inputs += values[i]+"^";
					}
					log.debug(spname+","+inputs+","+mValue);
				}
			} catch (Exception e) {
				log.warn("SP_CALL 연산:"+e.getMessage());
				throw e;
			} finally{
			}
			return mValue;
		}

	    return mValue;
	}
	
	private String saveDate;
	private String saveUniqueValue;
	private int sequenceNumber;
	private int sequenceNumber2;
	
	private Hashtable hSavedUniques = new Hashtable();
	private Hashtable hSavedValues = new Hashtable();
	private String getSequence(int size){
		// TODO:이미지키 생성작업...
		
	    Date date = new Date();
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
	    String value = formatter.format(date);

	    // 일련번호 생성하여 붙이기
	    if (saveDate != null && !saveDate.equals(value)){
	    	saveDate = value;
	    	sequenceNumber = 0;
	    }else{
	    	if (saveDate == null)
	    		saveDate = value;
	    }

	    if (size > 0){
	    	String s_tmp = "";
	    	for (int i = 0; i < size; i++) {
				s_tmp += "0";
			}
	    	NumberFormat numformatter = new DecimalFormat(s_tmp);
	    	value += numformatter.format(sequenceNumber++);
    	}
	    return value;
	}
	private String getSequence(String id, String format, int size){
		// TODO:이미지키 생성작업...
		
	    Date date = new Date();
	    SimpleDateFormat formatter = new SimpleDateFormat(format);
	    String value = formatter.format(date);

	    String unique = (String)hSavedUniques.get(id);
	    if (unique == null)
	    	hSavedUniques.put(id, value);

	    Long lSavedValue = (Long)hSavedValues.get(id);
	    if (lSavedValue == null){
	    	lSavedValue = new Long("0");
	    	hSavedValues.put(id, lSavedValue);
	    }

	    
	    // 일련번호 생성하여 붙이기
	    if (unique != null && !unique.equals(value)){
	    	hSavedUniques.put(id, value);
	    	
	    	lSavedValue = new Long("0");
	    	hSavedValues.put(id, lSavedValue);
	    }

	    if (size > 0){
	    	String s_tmp = "";
	    	for (int i = 0; i < size; i++) {
				s_tmp += "0";
			}
	    	NumberFormat numformatter = new DecimalFormat(s_tmp);
	    	value += numformatter.format(lSavedValue.longValue());
	    	
	    	hSavedValues.put(id, new Long(lSavedValue.longValue()+1));
    	}
	    return value;
	}

	private String getSequence2(String value, int size){
		String rValue = "";
		
	    // 일련번호 생성하여 붙이기
	    if (saveUniqueValue!= null && !saveUniqueValue.equals(value)){
	    	saveUniqueValue = value;
	    	sequenceNumber2 = 0;
	    }else{
	    	if (saveUniqueValue == null)
	    		saveUniqueValue = value;
	    }

	    if (size > 0){
	    	String s_tmp = "";
	    	for (int i = 0; i < size; i++) {
				s_tmp += "0";
			}
	    	NumberFormat numformatter = new DecimalFormat(s_tmp);
	    	rValue = numformatter.format(sequenceNumber2++);
    	}
	    return rValue;
	}	
	private String parseSQL(String sql, String[] base_values){
	    String patternStr = "\\$[0-9]+";
	    StringBuffer buf = new StringBuffer();
	    
	    // Compile regular expression
	    Pattern pattern = Pattern.compile(patternStr);
	    Matcher matcher = pattern.matcher(sql);
	    
	    boolean found = false;
	    while ((found = matcher.find())) {
	        // Get the match result
	        String replaceStr = matcher.group();
	    
	        // Convert to uppercase
	        try{
	        	int index = Integer.parseInt(replaceStr.substring(1));
	        	if (mbIgnoreQuotation){
	        		base_values[index] = base_values[index].replaceAll("'","");
	        	}
	        	matcher.appendReplacement(buf, base_values[index]);
	        }catch(Exception e){
	        	return null;
	        }
	    }
	    matcher.appendTail(buf);
	    
	    return buf.toString();
	}
	
	private String internalFunction(String stmt) throws Exception{
		String func_name = getFuncName(stmt);
		
		if (func_name != null){
			String[] params = getParameters(stmt);
			if (params == null){
				throw new Exception("function parameter error:"+stmt);
			}
			String[] newparams = new String[params.length];
			
			for (int i = 0; i < params.length; i++) {
				newparams[i] = internalFunction(params[i]);
			}

			// decode 구현.
			if (func_name.equalsIgnoreCase("decode")){
				String value = newparams[0];
				for (int i = 1; i < newparams.length - 1; i+=2) {
					if (value.equals(newparams[i])){
						return newparams[i+1];
					}
				}
				return newparams[newparams.length - 1];
			}
			// substr 구현
			else if (func_name.equalsIgnoreCase("substr")){
				String value = newparams[0];
				value = value.replaceAll("'","");
				
//				if (value.charAt(0) != '\'' || value.charAt(value.length()-1) != '\''){
//					throw new Exception("substr 구문 오류: "+stmt);
//				}
				try {
					int first = Integer.parseInt(newparams[1]);
					int len = Integer.parseInt(newparams[2]);
					int last = first-1+len;
					if ((first-1+len) > value.length())
						last = value.length();
					
					return value.substring(first-1, last);
				} catch (Exception e) {
//					throw new Exception("substr 구문 오류: "+stmt +","+e.getMessage());
					return "NULL";
				}
			}
			// LPAD 구현
			else if (func_name.equalsIgnoreCase("lpad")){
				String value = newparams[0];
				value = value.replaceAll("'","");
				
				try {
					int size = Integer.parseInt(newparams[1]);
					String ch = newparams[2];
					
					StringBuffer sb = new StringBuffer(value);
					for (int i = value.length(); i < size; i++) {
						sb.insert(0, ch);
					}
					
					return sb.toString();
				} catch (Exception e) {
//					throw new Exception("substr 구문 오류: "+stmt +","+e.getMessage());
					return "NULL";
				}
			}
			// substr 구현
			else if (func_name.equalsIgnoreCase("truncate")){
				String value = newparams[0];
				value = value.replaceAll("'","");
//				if (value.charAt(0) != '\'' || value.charAt(value.length()-1) != '\''){
//					throw new Exception("substr 구문 오류: "+stmt);
//				}
				try {
					if (value == null)
						return "NULL";
					
					String delimiter = newparams[1];
					String[] variables = newparams[0].split(delimiter);
					int ind = Integer.parseInt(newparams[2]);
					
					return variables[ind];
				} catch (Exception e) {
//					throw new Exception("substr 구문 오류: "+stmt +","+e.getMessage());
					return "NULL";
				}
			}
			// length 구현
			else if (func_name.equalsIgnoreCase("length")){
				String value = newparams[0];
				value = value.replaceAll("'","");
//				if (value.charAt(0) != '\'' || value.charAt(value.length()-1) != '\''){
//					throw new Exception("length statement error");
//				}
				return ""+(newparams[0].length());
			}
			// 정의 되지 않았으므로 오류
			else {
				throw new Exception("not defined internal function!");
			}
			
		} else {
			if (stmt.indexOf("||") != -1){
				String[] splits = stmt.split("\\|\\|");
				String ret = "";
				for (int i = 0; i < splits.length; i++) {
					ret += splits[i];
				}
				ret = ret.replaceAll("'","");
				return ret;
			} else {
				stmt = stmt.replaceAll("'","");
				return stmt;
			}
		}
	}
	
	private String[] getParameters(String stmt) throws Exception{
		Vector params = new Vector();
		String[] retstrs;

		int start_pos = stmt.indexOf('(');
		if (start_pos != -1){ // 함수
			int last_pos = stmt.lastIndexOf(')');
			if (last_pos == -1){
				return null;
			} else {
				String naka = stmt.substring(start_pos+1, last_pos);
				char[] bytes = new char[naka.length()]; 
				naka.getChars(0, naka.length(), bytes, 0);
				
				int brace_count = 0;
				int current_pos = -1;
				boolean quto = false;
				
				for (int i = 0; i < bytes.length; i++) {
					if (bytes[i] == '('){
						brace_count ++;
					} else if (bytes[i] == ')'){
						brace_count --;
					}
					
					if (bytes[i] == '\''){
						quto = !quto;
					}
					if (quto == true){
						continue;
					}
					
					if (brace_count == 0 && bytes[i] == ','){
						params.add(naka.substring(current_pos+1, i).trim());
						current_pos = i;
					}
				}
				params.add(naka.substring(current_pos+1, bytes.length).trim());

				// error check
				if (brace_count != 0){
					throw new Exception("function statement error:"+stmt);
				}
				
				naka.indexOf('(');
			}
			
			retstrs = new String[params.size()];
			params.copyInto(retstrs);
			return retstrs;
		}
		
		return null;
	}
	
	private String getFuncName(String stmt){
		int pos = stmt.indexOf("(");
		if (pos == -1){
			return null;
		} else {
			return stmt.substring(0, pos).trim();
		}
	}
	
    public static void main(String[] args) {
    }
}
