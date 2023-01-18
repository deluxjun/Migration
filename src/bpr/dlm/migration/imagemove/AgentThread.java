package bpr.dlm.migration.imagemove;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Vector;

import org.apache.log4j.Logger;

import bpr.dlm.migration.db.DBConnectionPool;
import bpr.dlm.migration.imagemove.AgentControl.DBHandler;
import bpr.dlm.migration.imagemove.AgentControl.DocumentumHandler;
import bpr.dlm.migration.imagemove.AgentControl.FolderHandler;
import bpr.dlm.migration.imagemove.AgentControl.XtormHandler;

public class AgentThread extends Thread {
	public boolean		m_stop = false;
	private DBHandler m_db;
	private DocumentumHandler m_docu;
	private FolderHandler m_folder;
	private XtormHandler m_xtorm;
	private String m_downpath;
	public String m_desc;
	
	private int m_sleep_count;
	
	public long m_success_count = 0;
	public long m_fail_count = 0;
	
	private DBConnectionPool m_dbpool;
	
	private Logger log;

	//생성자
	public AgentThread(String s_desc, int sleep_count,
			DBConnectionPool dbpool,
			DBHandler db, DocumentumHandler documentum, XtormHandler xtorm,
			String downpath, Logger log) throws Exception{
		m_desc = s_desc;
		m_sleep_count = sleep_count;

		m_dbpool = dbpool;
		m_db = db;
		m_docu = documentum;
		m_xtorm = xtorm;
		m_downpath = downpath;

		this.log = log;
	}
	
	public AgentThread(String s_desc, int sleep_count,
			DBConnectionPool dbpool,
			DBHandler db, FolderHandler folder, XtormHandler xtorm,
			String downpath, Logger log) throws Exception{
		m_desc = s_desc;
		m_sleep_count = sleep_count;

		m_dbpool = dbpool;
		m_db = db;
		m_folder = folder;
		m_xtorm = xtorm;
		m_downpath = "";

		this.log = log;
	}
	
	public void run() {
		String[] imagekeys = null;
		String s_eid = null;
		
		log.info("["+m_desc + ": started]");
		
		while (!m_stop) {
			Connection dbconn = null;
			try {
				dbconn = m_dbpool.getConnection();
				imagekeys = null;
				// 이관 대상 base 이미지키 얻어오기
				imagekeys = getBaseImagekey();
				if (imagekeys == null){
					log.info(m_desc+": No more works.");
					log.info(m_desc+": completed :" + m_success_count);
					log.info(m_desc+": failed :" + m_fail_count);
					break;
				}
				
				log.debug("base selected:"+imagekeys.length);
				
				for (int i = 0; i < imagekeys.length; i++) {
					Connection dbconn2 = null;
					Statement stmt = null;
					Vector vec_result = new Vector();
					
					// 이관 대상 이미지 정보 얻어오기
					try{
						dbconn2 = m_dbpool.getConnection();
						stmt = dbconn2.createStatement();
						ResultSet rs = stmt.executeQuery(
								"SELECT IMG_KEY, DOC_FORM_C, IMG_VER, IPS_ACT_C, IPS_IMG_ID, IMG_IDX, DESC_CTNT1 FROM "+ m_db.m_base_table +
								" WHERE IMG_KEY = '"+ imagekeys[i] +"' ORDER BY IPS_IMG_ID, IMG_IDX");

						int nCol = rs.getMetaData().getColumnCount();

						while(rs.next()){
							String[] values = new String[nCol];
							for (int j = 0; j < nCol; j++) {
								values[j] = rs.getString(j+1);
							}
							vec_result.add(values);
						}
						rs.close();
					}catch(Exception e){
				    	throw new Exception("cannot get image infos," ,e);
				    }finally{
				    	if (stmt != null){
				    		stmt.close();
				    	}
				    	if (dbconn2 != null){
				    		m_dbpool.releaseConnection(dbconn2);
				    	}
				    }
					
				    // 실제 한 row씩 이관 처리한다.
					for (int j = 0; j < vec_result.size(); j++) {
						String[] values = (String[]) vec_result.get(j);
						
						String imagekey = values[0];
						String formcode = values[1];
						String imageversion = values[2];
						String ips_imageid = values[4];
						int ips_imageidx = 0;
						try {
							ips_imageidx = Integer.parseInt(values[5]);
						} catch (Exception e) {
							ips_imageidx = 0;
						}
						String s_desc = values[6];
						
						// image down
						String image_filename = null;
						if (m_folder != null){
							log.debug("processing: " + imagekey + "," + formcode+","+imageversion + "||" + s_desc);

							image_filename = m_folder.getImage(s_desc);
							if (image_filename == null){
								String message = "get image error: " + m_folder.getLastError();
								log.error(message);

								updateError(imagekey, formcode, imageversion, message);
								continue;
							}
						} else {
							log.debug("processing: " + imagekey + "," + formcode+","+imageversion + "||" + ips_imageid + ","+ips_imageidx);

							image_filename = ips_imageid+"_"+ips_imageidx + ".tif";
							if (!m_docu.getImage(ips_imageid, ips_imageidx-1, m_downpath, image_filename)){
								if (!m_docu.umIsConnect())
									throw new Exception("documentum disconnected.");
								
								String message = "get image error: view log.";
								log.error(message + m_docu.getLastError());
								
								updateError(imagekey, formcode, imageversion, message);
								continue;
							}
						}
						
						// image up
						s_eid = null;
						s_eid = m_xtorm.createImage(m_downpath+image_filename, imagekey, formcode, imageversion);
						if (s_eid == null){
							String message = "create image error: view log.";
							log.error(message + m_xtorm.getLastError());
							
							File imagefile = new File(m_downpath+image_filename);
							imagefile.delete();
							updateError(imagekey, formcode, imageversion, message);
							continue;
						}
						
						// update com_img0xx
						try {
							m_db.queryExecute(dbconn, "update " + m_db.m_com_table + " set IMG_MVO_YN='0', IMG_DOC_NO='" + s_eid + "'"+
									" where IMG_KEY='"+imagekey+"' and DOC_FORM_C='"+formcode+"' and IMG_VER='"+imageversion+"'");
						} catch (Exception e) {
							dbconn.rollback();
							rollbackImage(s_eid);
							s_eid = null;
							String message = "com_img00x IMG_DOC_NO 세팅 error: view log.";
							log.error(message + e.getMessage());

							File imagefile = new File(m_downpath+image_filename);
							imagefile.delete();
							updateError(imagekey, formcode, imageversion, message);
							continue;
						}
						
						// update com_mig001
						try {
							m_db.queryExecute(dbconn, "update " + m_db.m_base_table + " set IMG_MVO_YN='0', IMG_MVO_DT=TO_CHAR(SYSDATE,'YYYYMMDD')"+
									" where IMG_KEY='"+imagekey+"' and DOC_FORM_C='"+formcode+"' and IMG_VER='"+imageversion+"'");
						} catch (Exception e) {
							dbconn.rollback();
							rollbackImage(s_eid);
							s_eid = null;
							String message = "com_mig001 이관 완료 세팅 error: view log.";
							log.error(message + e.getMessage());

							File imagefile = new File(m_downpath+image_filename);
							imagefile.delete();
							updateError(imagekey, formcode, imageversion, message);
							continue;
						}
						
						// commit
						dbconn.commit();
						
						// completed. rename file
						File imagefile = new File(m_downpath+image_filename);
						imagefile.delete();
//						CommonUtil.renameFile(m_downpath+image_filename, m_downpath+image_filename+"_OK");
						
						log.info("ended:(" + m_success_count + ") " + imagekey + "," + formcode+","+imageversion + "," + image_filename);
						m_success_count ++;
						
						// emds 부하때문에 sleep
						try {
							sleep(m_sleep_count);
						} catch (Exception e) {
							log.warn("sleep error: ",e);
						}
					}
					vec_result.clear();
					vec_result = null;
				}
				
				imagekeys = null;

			} catch (Exception e) {
				try {
					dbconn.rollback();
				} catch (Exception ex) {
				}
				
				if (s_eid != null){
					rollbackImage(s_eid);
					s_eid = null;
				}
				log.fatal(m_desc+" :terminated :",e);
				break;
			} finally{
		    	if (dbconn != null){
		    		m_dbpool.releaseConnection(dbconn);
		    	}
			}
		}
		
		m_stop = true;
	}
	
	//현재 클래스를 중지한다.
	public synchronized void terminate() {
		m_stop = true;
	}
	
	private void rollbackImage(String eid){
		if(!m_xtorm.deleteImage(eid)){
			log.warn(m_desc+": cannot rollback image, "+ m_xtorm.getLastError());
		}
	}
	
	private void updateError(String imagekey, String formcode, String imageversion, String message) throws Exception{
		Connection dbconn = null;
		// update com_mig001 error message
		try {
			dbconn = m_dbpool.getConnection();
			m_db.queryExecute(dbconn, "update " + m_db.m_base_table + " set IMG_MVO_YN='2', DESC_CTNT2='" + message + "'"+
					" where IMG_KEY='"+imagekey+"' and DOC_FORM_C='"+formcode+"' and IMG_VER='"+imageversion+"'");
			dbconn.commit();
			log.warn(m_desc+"updated error:"+imagekey+","+formcode+","+imageversion+":"+message);
			m_fail_count ++;
		} catch (Exception e) {
//			log.fatal(m_desc+"update_error_message failure:" + e.getMessage());
			throw new Exception("update_error_message failure:" + e.getMessage());
		} finally{
	    	if (dbconn != null){
	    		m_dbpool.releaseConnection(dbconn);
	    	}
		}
	}
	
	private String[] getBaseImagekey() throws Exception{
		Statement stmt = null;
		Connection dbconn = null;
		Vector vec = new Vector();

		try{
			dbconn = m_dbpool.getConnection();
			stmt = dbconn.createStatement();

//			SELECT IMG_KEY FROM COM_MIG001
//			WHERE IPS_ACT_C LIKE 'D%' AND IMG_MVO_YN = '1' AND ROWNUM < 101
//			GROUP BY IMG_KEY
			ResultSet rs = stmt.executeQuery("SELECT /*+ INDEX(COM_MIG001 IX_COMMIG001_02) */ IMG_KEY FROM "+m_db.m_base_table+
					" WHERE "+ m_db.m_search + " AND IMG_MVO_YN = '1' AND ROWNUM < 101 GROUP BY IMG_KEY");

			while(rs.next()){
				String value = rs.getString(1);
				if (value != null){
					vec.add(value);
				}
			}
			rs.close();
	    }catch(Exception e){
	    	log.error(e);
	    	throw e;
	    }finally{
	    	if (dbconn != null){
	    		m_dbpool.releaseConnection(dbconn);
	    	}
	    	if (stmt != null){
	    		stmt.close();
	    	}
	    }
	    if (vec.size() < 1){
	    	return null;
	    }
	    
	    String[] a_ret = new String[vec.size()];
	    vec.copyInto(a_ret);
	    
	    return a_ret;
	}
}
