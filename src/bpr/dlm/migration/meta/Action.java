/*
 * Created on 2006. 3. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import bpr.dlm.migration.util.CommonUtil;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Action {
	private String m_type;
	private Migration m_mig;
	private String[] m_before_params;
	private Object[] m_after_params;
	
	private Logger log;
	
	public Action(Migration mig, String type, Vector param, Logger log){
		m_type = type;
		m_mig = mig;
		m_before_params = new String[param.size()];
		param.copyInto(m_before_params);
		
		this.log = log;
	}
	
	public void action(boolean bSuccess, Object[] params) throws ParseException, Exception{
		if (m_type.equals("IMAGE_DOWN")){
			if (!bSuccess)
				return;
			try {
				m_mig.mImageDownloader.getImage((String)params[1], (String)params[2], CommonUtil.listtoArray((List)params[3]), (String)params[4]);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw e;
			}
		} else if (m_type.equals("IMAGE_DOWN_FTP")){
			if (!bSuccess)
				return;
			try {
				m_mig.mImageDownloader.getImageFTP((String)params[1], CommonUtil.listtoArray((List)params[2]), (String)params[3]);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw e;
			}

		} else if (m_type.equals("IMAGE_UP")){
			if (!bSuccess)
				return;

			Object[] keys = ((List)params[1]).toArray();
			String[] eids = m_mig.mImageUploader.createImage(keys, (String)params[2]);

			Connection dbconn = null;
			Statement stmt = null;
			String query;
			int count = 0;
			
			dbconn = (Connection) m_mig.getConnectionList().get((String)(String)params[3]);

			for (int i = 0; i < eids.length; i++) {
				String[] splits = ((String)keys[i]).split("_");
				String imagekey = splits[0];
				String formcode = splits[1];
				String imageversion = splits[2];
				try{
//					update BPRIFTABLE set move_gubun = '1', move_date = '3'
//					WHERE gaejung_code = '$0' AND place_no = '$1' AND img_id = '$2'

					query = "update " + (String)params[4];
					query += " set IMG_DOC_NO='" + eids[i] + "'";
					
					query += " where IMG_KEY='" + imagekey + "' AND ";
					query += "DOC_FORM_C='" + formcode + "' AND ";
					query += "IMG_VER='" + imageversion + "'";

					stmt = dbconn.createStatement();
					count = stmt.executeUpdate(query);
				} catch (SQLException ex) {
					if (log != null) log.warn(ex.getMessage());
					throw ex;
				} finally{
					if (stmt != null)
						stmt.close();
				}
			}

		} else if (m_type.equals("MIGRATION_SUCCESS_QUERY")){
			if (!bSuccess)
				return;

			// TODO:이미지키 생성작업...
			if (params == null || params.length < 3)
				throw new Exception("MIGRATION_SUCCESS_QUERY 파라미터가 부족합니다.");
			
			String[] values = new String[params.length-2];
			for (int i = 2; i < params.length; i++) {
				values[i-2] = (String)params[i];
			}
			String query = CommonUtil.parseTextWithParam((String)params[1], values);
			Connection dbconn = null;
			Statement stmt = null;
			int count = 0;
			int i;

			try{
				dbconn = (Connection) m_mig.getConnectionList().get((String)params[0]);

				stmt = dbconn.createStatement();
				count = stmt.executeUpdate(query);
			} catch (SQLException ex) {
				if (log != null) log.warn(query+":"+ex.getMessage());
				throw ex;
			} finally{
				if (stmt != null)
					stmt.close();
			}
			if (count != 1){
				throw new Exception("Migration Success write error:"+query);
			}
		} else if (m_type.equals("MIGRATION_FAIL_QUERY")){
			if (bSuccess)
				return;

			// TODO:이미지키 생성작업...
			if (params == null || params.length < 3)
				throw new Exception("MIGRATION_FAIL_QUERY 파라미터가 부족합니다.");
			
			String[] values = new String[params.length-2];
			for (int i = 2; i < params.length; i++) {
				values[i-2] = (String)params[i];
			}
			String query = CommonUtil.parseTextWithParam((String)params[1], values);
			Connection dbconn = null;
			Statement stmt = null;
			int count = 0;
			int i;

			try{
				dbconn = (Connection) m_mig.getConnectionList().get(params[0]);

				stmt = dbconn.createStatement();
				count = stmt.executeUpdate(query);
				dbconn.commit();
			} catch (SQLException ex) {
				if (log != null) log.warn(ex.getMessage());
				throw new Exception("Migration fail write error:"+query+":"+ex.getMessage());
			} finally{
				if (stmt != null)
					stmt.close();
			}
		} else if (m_type.equals("CALL")){
			if (!bSuccess)
				return;
			
			if (params == null || params.length < 2)
				throw new Exception("CALL 파라미터가 부족합니다.");
			
			if (!params[0].equals(params[1])){
				throw new Exception("CALL returned error:"+params[0]);
			}
			log.debug("CALL success:"+params[0]);
		}
		
	}
	
	public String[] getBeforeParams(){
		return m_before_params;
	}
	public void setAfterParams(Object[] params){
		m_after_params = params;
	}
}
