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
	private Hashtable htConnections;				// 연결을 저장하는 해시 테이블
	private int nIncrement;						// 연결 객체가 부족할 경우 증가 치

	private String dbURL, szUser, szPassword;

	private Logger log;

	public DBConnectionPool(String db_url, String user, String passwd, String driverClassName, int init_connections, int increment, Logger log)
			throws SQLException, ClassNotFoundException {
		/* 드라이버 로딩 */
		Class.forName(driverClassName);

		/* 연결 정보 설정 */
		this.dbURL = db_url;
		this.szUser = user;
		this.szPassword = passwd;
		this.nIncrement = increment;
		
		this.log = log;

		htConnections = new Hashtable();

		makeConnection(init_connections);	// 초기 연결 생성..
	}

	public synchronized Connection getConnection() throws SQLException {
		Connection con = null;
		Enumeration cons = null;
		long current_time = System.currentTimeMillis();

		/* 사용되지 않는 커넥션을 찾는다 */
		cons = htConnections.keys();
		while (cons.hasMoreElements()) {
			con = (Connection) cons.nextElement();
			ConnData data = (ConnData) htConnections.get(con);

			if (data.used == false) {
				// 사용되지 않는 Connection 객체의 연결 이상을 점검.
				if (isConnectionOK(con) == false) {
					htConnections.remove(con);
					if (con.isClosed() == false)
						con.close();

					log.info("reconnecting db connection..");
					con = DriverManager.getConnection(dbURL, szUser, szPassword);
					con.setAutoCommit(false);
				}

				// Connection을 사용내역을 기록한다.
				data.count ++;
				data.used = true;
				data.time = current_time;
				htConnections.put(con, data);
				return con;
			}
		}

		// 만약 놀고 있는 커넥션을 발견하지 못한 경우 nIncrement만큼 새로운 Connection 객체를 만든다.
		// 이 서버에서 제약을 둔다. 초기 풀 이상이면 대기하게 하자..
//		makeConnection(nIncrement);
//
//		/* 다시 한 번 사용되지 않는 커넥션을 찾는다 */
//		cons = htConnections.keys();
//		while (cons.hasMoreElements()) {
//			con = (Connection) cons.nextElement();
//			ConnData data = (ConnData) htConnections.get(con);
//
//			if (data.used == false) {
//				// Connection을 사용내역을 기록한다.
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
	 * 주어진 Connection 객체의 연결이 정상적인지 점검한다
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
