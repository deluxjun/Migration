/*
 * 동시에 작업할 작업목록을 읽어들이고 ,
 * 작업별로 쓰레드를 생성하여 업무를 진행한다. 
 */
package bpr.dlm.migration.imagemove;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import bpr.dlm.migration.db.DBConnectionPool;
import bpr.dlm.migration.imagemove.resource.DocumentumDriver;
import bpr.dlm.migration.imagemove.resource.XtormDriver;
import bpr.dlm.migration.util.CommonFileWriter;
import bpr.dlm.migration.util.CommonUtil;
import bpr.dlm.migration.util.IniFile;

import com.documentum.fc.client.IDfSession;
import com.windfire.apis.asysConnectData;

public class AgentControl {
	List m_thread_list;
	private Logger log;
	private AgentStartup m_parent;
	public DBConnectionPool m_pool;

	public boolean init(AgentStartup parent, String ini, Logger log) throws Exception{
		m_parent = parent;
		m_thread_list = new ArrayList();
		IniFile inifile = new IniFile(ini);
		
		String db_class = inifile.getKeyValue("COMMON", "CLASSNAME");
		String db_url = inifile.getKeyValue("COMMON", "URL");
		String db_user = inifile.getKeyValue("COMMON", "USER");
		String db_password = inifile.getKeyValue("COMMON", "PASSWORD");
		
		m_pool = new DBConnectionPool(db_url, db_user, db_password, db_class, 20, 5, log);

		parent.m_downpath = inifile.getKeyValue("COMMON", "DownFolder");
		if (!parent.m_downpath.endsWith("/")){
			parent.m_downpath += File.separatorChar;
		}
		String m_base_table = inifile.getKeyValue("COMMON", "BaseTable");
		
		String worknum = inifile.getKeyValue("COMMON", "DoWork");
		String[] works = worknum.split("\\|");
		
		for (int i = 0; i < works.length; i++) {
			int work_id = Integer.parseInt(works[i]);
			String s_work_section = "WORK"+work_id;
			
			String s_desc = inifile.getKeyValue(s_work_section, "DESCRIPTION");
			String s_search_option = inifile.getKeyValue(s_work_section, "SEARCH");
			String s_table = inifile.getKeyValue(s_work_section, "TABLE");
			String s_docbase_section = inifile.getKeyValue(s_work_section, "DOCBASE");
			String s_folder_section = inifile.getKeyValue(s_work_section, "FOLDER");
			String s_xtorm_section = inifile.getKeyValue(s_work_section, "XTORM");
			int i_sleep_count = inifile.getKeyIntValue(s_work_section, "SLEEPCOUNT");

			String xtorm_server = inifile.getKeyValue(s_xtorm_section, "Server");
			int xtorm_port = inifile.getKeyIntValue(s_xtorm_section, "Port");
			String xtorm_user = inifile.getKeyValue(s_xtorm_section, "Username");
			String xtorm_password = inifile.getKeyValue(s_xtorm_section, "Password");
			String xtorm_indexid = inifile.getKeyValue(s_xtorm_section, "IndexId");
			String xtorm_cc = inifile.getKeyValue(s_xtorm_section, "ContentClassId");

			if (s_folder_section != null){
				String folder_path = inifile.getKeyValue(s_folder_section, "BaseFolder");
				createWork(s_desc, i_sleep_count,
						m_pool,
						new DBHandler(m_base_table, s_search_option, s_table),
						new FolderHandler(folder_path),
						new XtormHandler(xtorm_server, xtorm_port, xtorm_user, xtorm_password, xtorm_indexid, xtorm_cc));
			} else {
				String docbase_user = inifile.getKeyValue(s_docbase_section, "Username");
				String docbase_password = inifile.getKeyValue(s_docbase_section, "Password");
				
				createWork(s_desc, i_sleep_count,
						m_pool,
						new DBHandler(m_base_table, s_search_option, s_table),
						new DocumentumHandler(s_docbase_section, docbase_user, docbase_password),
						new XtormHandler(xtorm_server, xtorm_port, xtorm_user, xtorm_password, xtorm_indexid, xtorm_cc));
			}

			

		}
		this.log = log;
		inifile = null;
		return true;
	}
	
	private void setLogConfig(String s_desc) throws  Exception{
		Properties prop = new Properties();

		prop.put("log4j.logger."+s_desc, "INFO, R");
		prop.put("log4j.appender.R", "org.apache.log4j.RollingFileAppender");
		prop.put("log4j.appender.R.File", "log/"+s_desc+".log");
		prop.put("log4j.appender.R.MaxFileSize", "5MB");
		prop.put("log4j.appender.R.MaxBackupIndex", "100");
		prop.put("log4j.appender.R.layout", "org.apache.log4j.PatternLayout");
		prop.put("log4j.appender.R.layout.ConversionPattern", "%5p %d{[yyyy.MM.dd,HH:mm:ss]} - %m%n");
		
		PropertyConfigurator.configure(prop);
	}
	
	private boolean createWork(String s_desc, int sleep_count,
			DBConnectionPool pool,
			DBHandler db, DocumentumHandler documentum, XtormHandler xtorm) throws Exception{
	
		setLogConfig(s_desc);
        Logger log = Logger.getLogger(s_desc);		// 로그 기록
		
        AgentThread thread = new AgentThread(s_desc, sleep_count, pool, db, documentum, xtorm, m_parent.m_downpath, log);
		m_thread_list.add(thread);
		thread.start();
		
		return true;
	}
	
	private boolean createWork(String s_desc, int sleep_count,
			DBConnectionPool pool,
			DBHandler db, FolderHandler folder, XtormHandler xtorm) throws Exception{
		
		setLogConfig(s_desc);
        Logger log = Logger.getLogger(s_desc);		// 로그 기록
		
        AgentThread thread = new AgentThread(s_desc, sleep_count, pool, db, folder, xtorm, m_parent.m_downpath, log);
		m_thread_list.add(thread);
		thread.start();
		
		return true;
	}

	public void printThreadInfo(){
		log.info("[running thread list]");
    	StringBuffer buff = new StringBuffer("");

    	Iterator i = m_thread_list.iterator();
		while(i.hasNext()){
			AgentThread th = (AgentThread)i.next();
			if(th.isAlive()){
				buff.append(th.m_desc+" alive"+CommonUtil.LS);
				buff.append(" - completed image count : " + th.m_success_count+CommonUtil.LS);
				buff.append(" - fail image count : " + th.m_fail_count+CommonUtil.LS);
			} else{
				buff.append(th.m_desc+" not alive"+CommonUtil.LS);
			}
		}
		log.info(buff.toString());
		
		// to file
		try {
			CommonFileWriter writer = new CommonFileWriter("status.txt", false);
			writer.writeln("[running thread list]");
			writer.writeln(buff.toString());
			writer.flush();
			writer.close();
		} catch (Exception e) {
		}
	}
	
	public void terminate(){
		Iterator i = m_thread_list.iterator();
		// 중지 신호 보내고.
		while(i.hasNext()){
			AgentThread th = (AgentThread)i.next();
			th.terminate();
		}
		
		// 중지할때까지 기다린다.
		while(i.hasNext()){
			AgentThread th = (AgentThread)i.next();
			try {
				th.join();
			} catch (Exception e) {
				log.fatal("cannot join thread :" + th.m_desc);
			}
		}
	}
	
	public boolean checkAliveThreads(){
		Iterator i = m_thread_list.iterator();
		while(i.hasNext()){
			AgentThread th = (AgentThread)i.next();
			if(th.isAlive())
				return true;
		}
		return false;
	}


	/////////////////////////////////////////////////////////////
	// classes
	
    class DBHandler{
    	String m_base_table;
    	String m_search;
    	String m_com_table;
    	
    	DBHandler(String base_table, String search_option, String com_table) throws Exception{
//    		m_conn = DBCommand.getConnection(driver, url, user, password);
//    		if (m_conn == null){
//    			String error = DBCommand.getLastError();
//    			throw new Exception(error);
//    		}
    		m_base_table = base_table;
    		m_search = search_option;
    		m_com_table = com_table;
    	}
    	
    	public void queryExecute(Connection conn, String query) throws Exception
		{
			Statement stmt = null;
			int count = 0;

			try {
				// connect
				stmt = conn.createStatement();
				count = stmt.executeUpdate(query);
			} catch (SQLException e) {
				throw e;
			} finally{
				if (stmt != null){
					stmt.close();
				}
			}
			if (count < 1){
				throw new Exception("not updated");
			}
		}
    }
    
    
    class FolderHandler {
    	String s_base;
    	String m_lasterror;

    	FolderHandler(String basepath){
    		s_base = basepath;
    	}
    	
    	public String getImage(String s_info){
			try {
	    		NumberFormat formatter = new DecimalFormat("00000000");
				//작업일자_블럭번호_블럭내일련번호
				String[] arrFilePath = s_info.split("_");
				int i_Temp = Integer.parseInt(arrFilePath[2]);
				String s_OcrFilePath = s_base + "\\" + arrFilePath[0] + "\\" + arrFilePath[1] + "\\" + "JIB" + formatter.format(i_Temp) + ".TIF";
				File file = new File(s_OcrFilePath);
				if (file.exists() == false || file.length() == 0){
					throw new Exception("file not exist or length is zero");
				}
				return s_OcrFilePath;
			} catch(Exception e){
				m_lasterror = e.getMessage();
				return null;
			}
    		
    	}
    	
    	public String getLastError(){
    		if (m_lasterror == null)
    			return "";
    		
    		return m_lasterror;
    	}
    }
    
    class DocumentumHandler extends DocumentumDriver{
    	IDfSession m_session;
    	
    	DocumentumHandler(String docbase, String user, String password) throws Exception{
    		m_session = comConnect(docbase, user, password);
    		if (m_session == null){
    			throw new Exception(getLastError());
    		}
    	}
    	
    	/* (non-Javadoc)
		 * @see bpr.dlm.migration.imagemove.resource.DocumentumDriver#comDisconnect(com.documentum.fc.client.IDfSession)
		 */
		public boolean disconnect() {
			return super.comDisconnect(m_session);
		}
		
		/* (non-Javadoc)
		 * @see bpr.dlm.migration.imagemove.resource.DocumentumDriver#comDownObject(com.documentum.fc.client.IDfSession, java.lang.String, java.lang.String, java.lang.String)
		 */
		public boolean umIsConnect() {
			if (m_session == null) {
				return false;
			} else {
				try {
					if (comIsConnect(m_session, "/System")) {
						return true;
					} else {
						return false;
					}
				} catch(Exception e) {
					return false;
				}
			}
		}
		
		public boolean getImage(String s_ObjectId, String s_Path, String s_FileNames) {
			return super.comDownObject(m_session, s_ObjectId, s_Path, s_FileNames);
		}
		public boolean getImage(String s_ObjectId, int idx, String s_Path, String s_FileName) {
			return super.comDownObject(m_session, s_ObjectId, idx, s_Path, s_FileName);
		}
    }

    class XtormHandler extends XtormDriver{
    	asysConnectData m_conn;
    	String m_indexid;
    	String m_cc;
    	
    	XtormHandler(String server, int port, String user, String password, String indexid, String cc) throws Exception{
    		
    		m_conn = comConnect(server, port, user, password, "XTORM_MAIN");
    		if (m_conn == null)
    			throw new Exception(getLastError());
    		m_indexid = indexid;
    		m_cc = cc;
    	}
    	
    	/* (non-Javadoc)
		 * @see bpr.dlm.migration.imagemove.resource.XtormDriver#comDisconnect(com.windfire.apis.asysConnectData)
		 */
		public void disconnect() {
			super.comDisconnect(m_conn);
		}
		
		/* (non-Javadoc)
		 * @see bpr.dlm.migration.imagemove.resource.XtormDriver#createImage(com.windfire.apis.asysConnectData, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
		 */
		public String createImage(String fileLoc, String imgKey, String formCode, String imageVersion) throws Exception{
			return super.createImage(m_conn, "XTORM_MAIN", fileLoc, m_cc, m_indexid, imgKey, formCode, imageVersion);
		}
		
		/* (non-Javadoc)
		 * @see bpr.dlm.migration.imagemove.resource.XtormDriver#deleteImage(com.windfire.apis.asysConnectData, java.lang.String, java.lang.String)
		 */
		public boolean deleteImage(String eid) {
			return super.deleteImage(m_conn, "XTORM_MAIN", eid);
		}
		
    }
}
