/*
 * Created on 2006. 1. 4.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.dumpsam;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import bpr.dlm.migration.db.DBCommand;
import bpr.dlm.migration.util.CommonUtil;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Main {
	private Logger log;
//	private DBConnectionPool mDB;
//	private DBConnectionPool mDB2;
	private ConfigData mConfig;
	private SamWriter mWriter;
	private IniFileWriter mLogWriter;
	private IniFileWriter mInfoWriter;
	
	private Hashtable mColumnNameMap;
	
	private String[] m_aBaseValues;
	private String[] m_aMasterValues;
	private String[] m_aBeforeValues;

	private Hashtable m_htBuff;
	
	private Connection m_conn1, m_conn2;
	private Connection m_conn_grp_1, m_conn_grp_2;

	private boolean mbInfo = false;

//	private Hashtable hSizes;
	
	Main(){
    	PropertyConfigurator.configure("log.properties");
        log = Logger.getLogger("main");		// 로그 기록		
	}
	
    public boolean init(String configfile){
//    	hSizes = new Hashtable();
    	try {
			mConfig = new ConfigData(configfile);
			
			log.info("=========== [STARTED: "+mConfig.sFileHead+" ] ===========");

			// 출력폴더 초기화
			File output = new File(mConfig.sOutFolder);
			if (!mConfig.bOverwrite){
				if (output.exists()){
					throw new Exception("출력 폴더가 이미 존재");
				}
			} else {
				output.mkdirs();
			}
			
			// db 초기화
//        	mDB = new DBConnectionPool(mConfig.sDBURL, mConfig.sDBUser, mConfig.sDBPassword, mConfig.sDBClassName, 2, 0, log);
        	m_conn1 = DBCommand.getConnection(mConfig.sDBClassName, mConfig.sDBURL, mConfig.sDBUser, mConfig.sDBPassword);
        	m_conn_grp_1 = DBCommand.getConnection(mConfig.sDBClassName, mConfig.sDBURL, mConfig.sDBUser, mConfig.sDBPassword);
        	if (mConfig.sDB2ClassName != null){
//        		mDB2 = new DBConnectionPool(mConfig.sDB2URL, mConfig.sDB2User, mConfig.sDB2Password, mConfig.sDB2ClassName, 2, 0, log);
            	m_conn2 = DBCommand.getConnection(mConfig.sDB2ClassName, mConfig.sDB2URL, mConfig.sDB2User, mConfig.sDB2Password);
            	m_conn_grp_2 = DBCommand.getConnection(mConfig.sDB2ClassName, mConfig.sDB2URL, mConfig.sDB2User, mConfig.sDB2Password);
        	}
        	
        	mWriter = new SamWriter(mConfig.sOutFolder, mConfig.sFileHead, "bak", log);

        	mLogWriter = new IniFileWriter(mConfig.sLogFile);
        	mLogWriter.addSection("LOG");

        	if (mbInfo){
        		mInfoWriter = new IniFileWriter(mConfig.sInfoFile);
        	}
        	
        	mColumnNameMap = new Hashtable();
        	m_htBuff = new Hashtable();
        	
    	} catch (Exception e){
    		log.fatal("초기화 에러", e);
    		return false;
    	}
    	
    	return true;
	}
    
    public void setPrintInfo(boolean bInfo){
    	mbInfo = bInfo;
    }
	
	private int addGroupResult(long index, int group_count, String header, String sql){
		Statement stmt = null;
		Connection dbconn = null;
		int count = 0;
		Vector vValues = new Vector(0);

		try{
			int pos = mConfig.sDB2Target.indexOf(header);
			if (pos >= 0)
				dbconn = m_conn_grp_2;
			else
				dbconn = m_conn_grp_1;
			if (dbconn == null)
				throw new SQLException("DB Connection 에러");

			// connect
			stmt = dbconn.createStatement();

			ResultSet rs = stmt.executeQuery(sql);

			// 컬럼 정보를 얻는다. 메핑룰 작성을 위해
			int nCol = rs.getMetaData().getColumnCount();
			if (index == 1){
				String[] aColumnName = (String[])mColumnNameMap.get(header);
				if (aColumnName == null)
					aColumnName = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					aColumnName[i] = rs.getMetaData().getColumnName(i+1);
				}
				mColumnNameMap.put(header, aColumnName);
			}

			while(rs.next()){
				count ++;
				String[] values = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					values[i] = (String)rs.getString(i+1);
				}
				
				// 원장정보로 나머지 쿼리들의 값을 대치하기 위해..
				if (header.equalsIgnoreCase("M")){
					m_aMasterValues = values;
				}
				m_aBeforeValues = values;

				
				// 그룹 출력
				mWriter.writeRow(index, group_count+count-1, header, count, values, mConfig.sDelimiter);
			}
			
		}
		catch(SQLException e){
			log.error(e.getMessage(), e);
			count = -1;	//	SQL 에러.
	    }finally{
	    	try {
		    	if (stmt != null)
		    		stmt.close();
			} catch (Exception e) {
			}
	    }
	    
	    return count;
	}
	
	private boolean parseCondition(String condition, String[] base_values, String[] m_values, String[] before_values) throws Exception{
		//$18<>0000000000000000 and $19<>00000000 and except_code = '00'
	    String patternStr = "[\\$\\#][0-9]+";
	    StringBuffer buf = new StringBuffer();
	    
	    // Compile regular expression
	    Pattern pattern = Pattern.compile(patternStr);
	    Matcher matcher = pattern.matcher(condition);
	    
	    boolean found = false;
	    while ((found = matcher.find())) {
	        // Get the match result
	        String replaceStr = matcher.group();
	    
	        // Convert to uppercase
	        try{
	        	if(replaceStr.charAt(0) == '#'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	matcher.appendReplacement(buf, base_values[index]);
	        	} else if(replaceStr.charAt(0) == '$'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	matcher.appendReplacement(buf, m_values[index]);
	        	} else if(replaceStr.charAt(0) == '@'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	matcher.appendReplacement(buf, before_values[index]);
	        	}
	        }catch(Exception e){
	        	log.error("설정파일 Condition 에러");
	        	throw e;
	        }
	    }
	    matcher.appendTail(buf);
	    
	    String parsed_cond = buf.toString();
	    
	    // and 있을경우.
	    String[] aCond = parsed_cond.split(" and ");
	    if (aCond.length >= 2){
		    for (int i = 0; i < aCond.length; i++) {
				String[] values = aCond[i].split("<>");
				// <> 이므로 같으면, 막바로 false 리턴.
				if (values.length == 2){
					if (values[0].trim().equals(values[1].trim())){
							return false;
					}
				}
				values = aCond[i].split("=");
				// = 이므로 같지 않으면, 막바로 false 리턴.
				if (values.length == 2){
					if (!values[0].trim().equals(values[1].trim())){
							return false;
					}
				}
			}
		    return true;
	    }

	    return true;
	}
	
	private String parseSQL(String sql, String[] base_values, String[] m_values, String[] before_values){
	    String patternStr = "[\\$\\#][0-9]+";
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
	        	if(replaceStr.charAt(0) == '#'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	if (base_values[index] == null){
		        		base_values[index] = "NULL";
		        	}
		        	matcher.appendReplacement(buf, base_values[index]);
	        	} else if(replaceStr.charAt(0) == '$'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	if (m_values[index] == null){
		        		m_values[index] = "NULL";
		        	}
		        	matcher.appendReplacement(buf, m_values[index]);
	        	} else if(replaceStr.charAt(0) == '@'){
		        	int index = Integer.parseInt(replaceStr.substring(1));
		        	if (before_values[index] == null){
		        		before_values[index] = "NULL";
		        	}
		        	matcher.appendReplacement(buf, before_values[index]);
	        	}
	        }catch(Exception e){
	        	log.error(e);
	        	return null;
	        }
	    }
	    matcher.appendTail(buf);
	    
	    return buf.toString();
	}
	
	public Logger getLogger(){
		return log;
	}
	
	public boolean run(){
		// TODO: SAM파일 작성. config문 파싱
		boolean bRet = true;
		Statement stmt = null;
		Connection base_dbconn = null;
		String[] aLogOrder;
		
		// Log 출력 순서 정의.
		aLogOrder = new String[5+mConfig.aSQLHeader.length];
		aLogOrder[0] = "START_TIME";
		aLogOrder[1] = "MASTER_QUERY";
		aLogOrder[2] = "END_TIME";
		aLogOrder[3] = "COUNT_PER_SECOND";
		aLogOrder[4] = "BASE";
		for (int i = 5; i < aLogOrder.length; i++) {
			aLogOrder[i] = mConfig.aSQLHeader[i-5];
		}
		
		int group_size = mConfig.mSQLMap.size();
		
		if (group_size < 1){
			log.error("그룹 SQL을 지정해야 합니다.");
			return false;
		}
		
		long base_row_index = 0;
		long rowindex[] = new long[mConfig.aSQLHeader.length];

		String base_query = (String)mConfig.mSQLMap.get("BASE");
		log.debug("base query : "+base_query);

		
		// 초기화
		for (int i = 0; i < rowindex.length; i++) {
			rowindex[i] = 0;
		}

	    try{
	    	// 시작 시간 출력.
			mLogWriter.setValue("LOG", aLogOrder[0], CommonUtil.getNowTime("yy/MM/dd HH:mm:ss"));
			// 조건 출력.
			mLogWriter.setValue("LOG", aLogOrder[1], base_query);
	    	// 종료 시간 출력.
			mLogWriter.setValue("LOG", aLogOrder[2], "");
			mLogWriter.save();

		    long start = System.currentTimeMillis();

			int pos = mConfig.sDB2Target.indexOf("BASE");
			if (mConfig.sDB2Target != null && pos != -1)
				base_dbconn = m_conn2;
			else
				base_dbconn = m_conn1;

			if (base_dbconn == null)
				throw new SQLException("DB Connection 에러");
			
			log.info("DB Connection OK!");
			
			stmt = base_dbconn.createStatement();

			ResultSet rs = stmt.executeQuery(base_query);

			log.info("BASE Query executed!");

			int nCol = rs.getMetaData().getColumnCount();

			if (mConfig.aSQLHeader.length == 0){
				String[] aColumnName = (String[])mColumnNameMap.get("BASE");
				if (aColumnName == null)
					aColumnName = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					aColumnName[i] = rs.getMetaData().getColumnName(i+1);
				}
				mColumnNameMap.put("BASE", aColumnName);
			}

			while(rs.next()){

				// row 인덱스 증가
				base_row_index ++;
				
				// 진행상태 보여주기 위해..
				if (base_row_index%1000 == 0){
					log.info("DUMP COUNT : " + base_row_index);
				}

				String[] values = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					values[i] = rs.getString(i+1);
				}
				m_aBaseValues = values;
				m_aMasterValues = values;
				m_aBeforeValues = values;


				// 다음 그룹들의 쿼리를 실행하고 출력
				int group_count = 1;

				// base 만 있을때
				if (mConfig.bBaseWrite)
					mWriter.writeRow(base_row_index, group_count, "BASE", 1, m_aMasterValues, mConfig.sDelimiter);

				for (int i = 0; i < mConfig.aSQLHeader.length; i++) {
					String string = (String)mConfig.mSQLMap.get((String)mConfig.aSQLHeader[i]);
					if (string == null)
						continue;
					
					String before_sql;
					boolean bSearchCondition = true;
					
					// I 일 때만 조건 처리하자.
					if (mConfig.aSQLHeader[i].equalsIgnoreCase("I")){
					
						String[] strings = string.split("\\|");	// string[0]은 상태, string[1]은 sql
						
						// 검색 여부 체크
						if (strings.length > 1 ){
							before_sql = strings[1];
							bSearchCondition = parseCondition(strings[0], m_aBaseValues, m_aMasterValues,m_aBeforeValues);
						} else {
							before_sql = strings[0];
						}
					} else {
						before_sql = string;
					}
					
					int rstCount = 0;
					if (bSearchCondition){
						String parsed_sql = parseSQL(before_sql, m_aBaseValues, m_aMasterValues,m_aBeforeValues);

						if (parsed_sql == null || parsed_sql.equals("")){
							throw new Exception("파싱에러:"+mConfig.aSQL[i]);
						}

						// 원장정보로 나머지 쿼리들의 값을 대치하기 위해..
						if (mConfig.aSQLHeader[i].equalsIgnoreCase("M")){
							m_aMasterValues = null;
						}
						
						// 시간계산
						long grp_start = 0;
						if (base_row_index <= mConfig.iComputeCount){
						    grp_start = System.currentTimeMillis();
						}

						// 실행 및 출력
						rstCount = addGroupResult(base_row_index, group_count, mConfig.aSQLHeader[i], parsed_sql);
						if (rstCount < 0){
							throw new Exception("추가 그룹 등록 에러 :"+parsed_sql);
						}
						// M이 없으므로, 다음 BASE 진행.
						if (m_aMasterValues == null){
							base_row_index --;
							break;
						}

						// 시간계산
						if (base_row_index <= mConfig.iComputeCount){
							log.info(mConfig.aSQLHeader[i] + " Elasped time = "+ (System.currentTimeMillis()-grp_start));
						}
						
						// 그룹일련번호 증가를 위하여.
						group_count += rstCount;
						// 전체 로그 출력을 위해.
						rowindex[i] += rstCount;
						
					}
				}
				
				// base 만 있을때
				if (mConfig.aSQLHeader.length == 0)
					mWriter.writeRow(base_row_index, group_count, "BASE", 1, m_aMasterValues, mConfig.sDelimiter);


				if (mConfig.iFlushCount == 1 || base_row_index%mConfig.iFlushCount == 0){
					mWriter.flush();
				}

				// 제한 row 수를 넘으면 다음파일로
				if (base_row_index != 0 && (base_row_index % (mConfig.iPartRowNum)) == 0){
					log.debug("to next file.." + base_row_index + "%" + mConfig.iPartRowNum);
					if (mWriter.next() != 0){
						throw new Exception(mWriter.getLastError());
					}
				}
			}

			// check count 출력
//			for (int i = 0; i < totalrow.length; i++) {
//				if (totalrow[0] != -1){
//					mLogWriter.setValue("REPORT", "MASTER_TOTAL", Long.toString(totalrow[i]));
//			    }
//			}
			// 각 그룹별 총 출력값
			for (int i = 0; i < rowindex.length; i++) {
				String string = (String)mConfig.mSQLMap.get((String)mConfig.aSQLHeader[i]);
				if (string == null)
					continue;
				mLogWriter.setValue("LOG", aLogOrder[i+5], Long.toString(rowindex[i]));
				log.info(aLogOrder[i+5] + " writed="+rowindex[i]);
			}
			// base 만 있을때
			if (mConfig.aSQLHeader.length == 0){
				mLogWriter.setValue("LOG", aLogOrder[4], Long.toString(base_row_index));
				log.info(aLogOrder[4] + " writed="+base_row_index);
			}
			
		    
		    // Get elapsed time in milliseconds
		    long elapsedTimeMillis = System.currentTimeMillis()-start;
		    // Get elapsed time in seconds
		    long elapsedTimeSec = elapsedTimeMillis/1000;

		    
		    // 필드 정보 출력
			Enumeration k = mColumnNameMap.keys();
			Enumeration e = mColumnNameMap.elements();
			
			Vector vecForSort = new Vector();
        	if (mbInfo){
				mInfoWriter.addSection("INFO");
				for (; e.hasMoreElements();) {
					String[] aNames = (String[])e.nextElement();
					String key = (String)k.nextElement();
					for (int i = 0; i < aNames.length; i++) {
						String thiskey = key+" "+i;
						mInfoWriter.setValue("INFO", thiskey, "<source-field name=\""+aNames[i]+"\" group=\""+key+"\" position=\""+i+"\"/>");
						vecForSort.add(thiskey);
					}
				}
				String[] keysForSort = new String[vecForSort.size()];
				vecForSort.copyInto(keysForSort);
				
				mInfoWriter.save(keysForSort);
        	}
			
//			// 성공
//			if (rowindex.length > 0){
//				log.info("DUMP COUNT : " + rowindex[0]);
//				CommonUtil.createEmptyFile(mConfig.sOutFolder + "/completed"+rowindex[0]);
//			}
//
//			// base 만 있을때
//			if (mConfig.aSQLHeader.length == 0){
//				log.info("DUMP COUNT : " + base_row_index);
//				CommonUtil.createEmptyFile(mConfig.sOutFolder + "/completed"+base_row_index);
//			}

	    	// 종료 시간 출력.
			if (elapsedTimeSec != 0)
				mLogWriter.setValue("LOG", aLogOrder[3], Long.toString(base_row_index/elapsedTimeSec));
			else
				mLogWriter.setValue("LOG", aLogOrder[3], "0");
			mLogWriter.setValue("LOG", "END_TIME", CommonUtil.getNowTime("yy/MM/dd HH:mm:ss"));

			log.info("=========== [TERMINATED] ===========");

			if (mWriter.mWritedCount > 0)
				System.out.println("Dumpsam Success");

	    }
		catch(Exception e){
			log.error(e.getMessage(), e);
			bRet = false;
	    }finally{
	    	try {
		    	if (stmt != null)
		    		stmt.close();
			} catch (Exception e) {}
    		
    		try{
    			mLogWriter.save(aLogOrder);
    		}catch(Exception ex){
    			log.error("로그 출력 오류", ex);
    		}
    		mWriter.close();
		}
	    
		return bRet;
	}
	
	public boolean terminate(){
//		if (mDB != null)
//			mDB.releaseConnection(m_conn1);
//		if (mDB2 != null)
//			mDB2.releaseConnection(m_conn2);
//		
//		if (mDB != null)
//			mDB.releaseAll();
//		if (mDB2 != null)
//			mDB2.releaseAll();
		
		try {
			if (m_conn1 != null)
				m_conn1.close();
			if (m_conn2 != null)
				m_conn2.close();
			if (m_conn_grp_1 != null)
				m_conn_grp_1.close();
			if (m_conn_grp_2 != null)
				m_conn_grp_2.close();
		} catch (SQLException e) {
			log.error(e);
		}
		
		if (mWriter != null)
			mWriter.close();

		mConfig = null;
//		mDB = null;
//		mDB2 = null;
		
		return true;
	}

    /**
     * 실행 옵션을 프린트
     */
    private static void usageInfo() {
        System.out.println("Usage:");
        System.out.println("java bpr.dlm.migration.dumpsam.Main [config file]");
    }
    public static void main(String[] args) {
		if (args.length < 1){
			usageInfo();
			return;
		}
		
		Main prg = new Main();
		
		if (args.length > 1 && args[1].equalsIgnoreCase("info"))
			prg.setPrintInfo(true);
		
		if (!prg.init(args[0])){
			prg.getLogger().fatal("Init error");
			prg.terminate();
			return;
		}


		try {
			if (!prg.run()){
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			prg.terminate();
		}
	}
}
