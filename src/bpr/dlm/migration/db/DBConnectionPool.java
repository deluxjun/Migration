package bpr.dlm.migration.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;


/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class DBConnectionPool {
	private Hashtable htConnections;				// ������ �����ϴ� �ؽ� ���̺�
	private int nIncrement;						// ���� ��ü�� ������ ��� ���� ġ

	private String dbURL, szUser, szPassword;

	private Logger log;

	public DBConnectionPool(String db_url, String user, String passwd, String driverClassName, int init_connections, int increment, Logger log)
			throws SQLException, ClassNotFoundException {
		/* ����̹� �ε� */
		Class.forName(driverClassName);

		/* ���� ���� ���� */
		this.dbURL = db_url;
		this.szUser = user;
		this.szPassword = passwd;
		this.nIncrement = increment;
		
		this.log = log;

		htConnections = new Hashtable();

		makeConnection(init_connections);	// �ʱ� ���� ����..
	}

	public synchronized Connection getConnection() throws SQLException {
		Connection con = null;
		Enumeration cons = null;
		long current_time = System.currentTimeMillis();

		/* ������ �ʴ� Ŀ�ؼ��� ã�´� */
		cons = htConnections.keys();
		while (cons.hasMoreElements()) {
			con = (Connection) cons.nextElement();
			ConnData data = (ConnData) htConnections.get(con);

			if (data.used == false) {
				// ������ �ʴ� Connection ��ü�� ���� �̻��� ����.
				if (isConnectionOK(con) == false) {
					htConnections.remove(con);
					if (con.isClosed() == false)
						con.close();

					log.info("reconnecting db connection..");
					con = DriverManager.getConnection(dbURL, szUser, szPassword);
					con.setAutoCommit(false);
				}

				// Connection�� ��볻���� ����Ѵ�.
				data.count ++;
				data.used = true;
				data.time = current_time;
				htConnections.put(con, data);
				return con;
			}
		}

		// ���� ��� �ִ� Ŀ�ؼ��� �߰����� ���� ��� nIncrement��ŭ ���ο� Connection ��ü�� �����.
		// �� �������� ������ �д�. �ʱ� Ǯ �̻��̸� ����ϰ� ����..
//		makeConnection(nIncrement);
//
//		/* �ٽ� �� �� ������ �ʴ� Ŀ�ؼ��� ã�´� */
//		cons = htConnections.keys();
//		while (cons.hasMoreElements()) {
//			con = (Connection) cons.nextElement();
//			ConnData data = (ConnData) htConnections.get(con);
//
//			if (data.used == false) {
//				// Connection�� ��볻���� ����Ѵ�.
//				data.count ++;
//				data.used = true;
//				htConnections.put(con, data);
//				return con;
//			}
//		}

		return null;
	}

	private synchronized void makeConnection(int count) throws SQLException {
		for (int i = 0; i < count; i++) {
			Connection con = DriverManager.getConnection(dbURL, szUser, szPassword);
			con.setAutoCommit(false);
			htConnections.put(con, new ConnData(0, false, 0));
		}
	}

	/**
	 * �־��� Connection ��ü�� ������ ���������� �����Ѵ�
	 * @param con
	 * @return
	 */
	private synchronized boolean isConnectionOK(Connection con) {
		Statement testStmt = null;
		try {
//			if (!con.isClosed()) {
				testStmt = con.createStatement();
				testStmt.executeQuery("select 1 from dual");
				testStmt.close();
//			} else {
//				return false;
//			}
		} catch (SQLException e) {
			if (testStmt != null) {
				try {
					testStmt.close();
				} catch (SQLException es) {
				}
			}
			return false;
		}
		return true;
	}

	public synchronized void releaseConnection(Connection returned) {
		Connection con;
		Enumeration cons = htConnections.keys();
		while (cons.hasMoreElements()) {
			con = (Connection) cons.nextElement();
			if (con == returned) {
				ConnData data = (ConnData) htConnections.get(con);
				data.used = false;
				htConnections.put(con, data);
				break;
			}
		}
	}

	public synchronized void releaseAll() {
		try {
			Connection con = null;
			Enumeration cons = htConnections.keys();
			while (cons.hasMoreElements()) {
				con = (Connection) cons.nextElement();
				htConnections.remove(con);
				con.close();
			}
			htConnections = null;
		} catch (SQLException e) {

		}
	}
	
	public synchronized String getStatusString(){
		StringBuffer buff = new StringBuffer("");
		int count = 0;
		Connection con;
		Enumeration cons = htConnections.keys();
		while (cons.hasMoreElements()) {
			con = (Connection) cons.nextElement();
			ConnData data = (ConnData) htConnections.get(con);
			buff.append(++count + "(" + data.used +","+ data.count + ") ");
		}
		return buff.toString();
	}
	
	public void releaseOldConnections(){
		Connection con = null;
		Enumeration cons = null;
		long current_time = System.currentTimeMillis();

		cons = htConnections.keys();
		while (cons.hasMoreElements()) {
			con = (Connection) cons.nextElement();
			ConnData data = (ConnData) htConnections.get(con);

			if (data.used == true) {
				if (current_time - data.time > 10000)
					data.used = false;

				htConnections.put(con, data);
			}
		}		
	}
	
	private class ConnData{
		long count;
		long time;
		boolean used;
		ConnData(long count, boolean used, long time){
			this.count = count;
			this.used = used;
			this.time = time;
		}
	}
}
