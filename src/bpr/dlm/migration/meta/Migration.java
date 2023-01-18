/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import bpr.dlm.migration.db.DBConnectionPool;
import bpr.dlm.migration.image.ImageDownloader;
import bpr.dlm.migration.image.ImageUploader;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Migration {
	Map mProperties;
	Map mDBList;
	Map mConnectionList;
	Map mSamList;
	Map mFunctionList;
	Map mCodeList;
	List mRuleList;
	
	StatusManager mStatusManager;
	String mStatusFileName;
	
	ImageDownloader mImageDownloader;
	ImageUploader mImageUploader;
	
	private Logger log;
	public boolean mbErrorSam;

	Migration(Logger log){
		this.log = log;
		mProperties = new HashMap();
		
		mDBList = new HashMap();
		mConnectionList = new HashMap();
		mSamList = new HashMap();
		mFunctionList = new HashMap();
		mCodeList = new HashMap();
		mRuleList = new ArrayList();
		
		mbErrorSam = false;
	}
	
	public Map getProperties(){
		return mProperties;
	}
	public Map getDBList(){
		return mDBList;
	}
	public Map getConnectionList(){
		return mConnectionList;
	}
	public Map getSamList(){
		return mSamList;
	}
	public Map getFunctionList(){
		return mFunctionList;
	}
	public Map getCodeList(){
		return mCodeList;
	}
	
	public void addRule(Rule rule){
		mRuleList.add(rule);
	}

	
	public void setImageUploader(ImageUploader xtorm){
		mImageUploader = xtorm;
	}
	public void setImageDownloader(ImageDownloader ftp){
		mImageDownloader = ftp;
	}
	
	public void init() throws Exception{
		if (mConnectionList == null || mSamList == null || mFunctionList == null || mCodeList == null)
			throw new Exception("�ʿ��� ������ ���õ��� �ʾҽ��ϴ�.");
		
		// ���� ���翩�� üũ
		Iterator iter = mSamList.values().iterator();
		while(iter.hasNext()){
			SamReader reader = (SamReader)iter.next();
			for (int i = 0; i < reader.mFiles.size(); i++) {
				String filename = (String)reader.mFiles.get(i);
				if(!(new File(filename)).exists()){
					log.error("������ �������� �ʽ��ϴ�."+filename);
					throw new Exception("������ SAM������ �����ϴ�");
				}
			}	
		}

		// autocommit�� false��...
		iter = mConnectionList.values().iterator();
		while(iter.hasNext()){
			Connection conn = (Connection)iter.next();
			conn.setAutoCommit(false);
		}

		// add get image key �Լ�
		mFunctionList.put("NEW_IMAGEKEY", new Function(this, Function.NEWIMAGEKEY,log));
		mFunctionList.put("NEW_SEQUENCE", new Function(this, Function.NEWSEQUENCE,log));
		mFunctionList.put("NEW_SEQUENCE2", new Function(this, Function.NEWSEQUENCE2,log));
		mFunctionList.put("GETUNIQUE", new Function(this, Function.GETUNIQUE,log));
		mFunctionList.put("LEFT", new Function(this, Function.LEFT,log));
		mFunctionList.put("RIGHT", new Function(this, Function.RIGHT,log));
		mFunctionList.put("MERGECOMMA", new Function(this, Function.MERGECOMMA,log));
		mFunctionList.put("INSERTINTO", new Function(this, Function.INSERTINTO,log));
		mFunctionList.put("TRUNCATE", new Function(this, Function.TRUNCATE,log));
		mFunctionList.put("SP_CALL", new Function(this, Function.SP_CALL,log));
		mFunctionList.put("IGNORE_INSERT", new Function(this, Function.IGNORE_INSERT,log));
		mFunctionList.put("MERGE_STR", new Function(this, Function.MERGE_STR,log));
		
		String status_file = (String)mProperties.get("status-file");
		if (status_file != null)
			mStatusManager = new StatusManager(status_file);
	}
	
	public void checkComplete(String rule, long complete_count, List listTargetField) throws Exception{
		try{
			if (mStatusManager == null)
				return;
			mStatusManager.setCompleteCount(rule, complete_count);
			// ���̺� �Էµ� ī��Ʈ ���� ���Ѵ�
			mStatusManager.setTableCount(rule, listTargetField);
			mStatusManager.save();
		}catch(SQLException e){
			throw e;
		}catch(Exception io){
			log.error(complete_count + " index ���� �Է��� �Ϸ��Ͽ�����, status���� ��¿� �����Ͽ����ϴ�.",io);
		}
	}
	
	public void commitAll() throws SQLException{
		Iterator iter = mConnectionList.values().iterator();
		while(iter.hasNext()){
			Connection conn = (Connection)iter.next();
			if (conn == null)
				throw new SQLException("Connection is null");
			try {
				conn.commit();
			} catch (SQLException e) {
				conn.rollback();
				throw e;
			}
		}
	}
	public void rollbackAll() throws Exception{
		Iterator iter = mConnectionList.values().iterator();
		while(iter.hasNext()){
			Connection conn = (Connection)iter.next();
			conn.rollback();
		}
	}
	
	private void shutdown(){
		// release all db connection.
		Iterator i = mDBList.values().iterator();
		DBConnectionPool dbpool;
		while (i.hasNext()) {
			dbpool = (DBConnectionPool) i.next();
			dbpool.releaseAll();
		}
		
		i = mSamList.values().iterator();
		while (i.hasNext()) {
			SamReader reader = (SamReader) i.next();
			reader.shutdown();
		}
	}
	
	public void run() throws Exception {
		try {
			Iterator i = mRuleList.iterator();
			Rule rule;
			while (i.hasNext()) {
				rule = (Rule) i.next();
				
				rule.init(this);
				
				rule.run();
				
				commitAll();
			}

		} catch (Exception e) {
			rollbackAll();
			log.info("Rollback!");
			throw e;
		} finally {
			shutdown();
		}
	}

}
