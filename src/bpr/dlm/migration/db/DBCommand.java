package bpr.dlm.migration.db;

import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Vector;

import org.apache.log4j.Logger;


public class DBCommand {
	private static String lastErrorMessage;
	private static Logger log;

	public DBCommand() {
	}

	public synchronized String ko(String str) {
		try {
			str = new String(str.getBytes("8859_1"), "euc-kr");
		}catch (UnsupportedEncodingException e) {
		}
		return str;
	}
	
	public synchronized static void setLog(Logger flog){
		log = flog;
	}
	
	public synchronized static String getLastError(){
		return lastErrorMessage;
	}

	public synchronized static Connection getConnection(String dbdriver, String url, String user, String password){
		Connection dbconn = null;

		try{
			Class.forName(dbdriver).newInstance();

			dbconn = DriverManager.getConnection(url, user, password);
			if(dbconn == null){
				lastErrorMessage = "can not connect db";
			}
		}
		catch(Exception ex){
			lastErrorMessage = ex.getMessage();
			return null;
		}

		return dbconn;
	}

	public synchronized static String[] getColumnNames(Connection conn, String table){
		String[] columns = null;

		try {
			// Create a result set
			Statement stmt = conn.createStatement();
			ResultSet rs1 = stmt.executeQuery("SELECT * FROM " + table);
	    
			// Get result set meta data
			ResultSetMetaData rsmd = rs1.getMetaData();
			int numColumns = rsmd.getColumnCount();

			columns = new String[numColumns];
			for (int i=0; i<numColumns; i++) {
				columns[i] = rsmd.getColumnName(i+1);
			}
	    }
		catch (Exception e) {
			lastErrorMessage = e.getMessage();
			e.printStackTrace();
	    	return null;
	    }

		return columns;
	}

	public synchronized static int queryInsert(Connection dbconn, String table, String[] names, Object[] values){
		String query;
		PreparedStatement pstmt = null;
		int i;

		try{
			query = "insert into " + table;
			if (names != null){
				query += " (";
				for (i = 0; i < names.length-1; i++) {
					query = query + names[i] + ", ";
				}
				query = query + names[i] + ")";
			}
			
			query = query + " values (";
			for (i = 0; i < values.length-1; i++) {
				query = query + "?, ";
			}
			query = query + "?)";

			pstmt = dbconn.prepareStatement(query);

			for (i = 0; i < values.length; i++) {
				if (values[i] != null)
					pstmt.setObject(i+1, values[i]);
				else
					pstmt.setObject(i+1, "");
			}

			pstmt.executeUpdate();
		}
		catch (SQLException ex) {
			String message = "[";
			for (int j = 0; j < values.length; j++) {
				message += values[j] + ",";
			}
			message += "]";
			if (log != null) log.error(ex.getMessage());
			lastErrorMessage = message + " " + ex.getMessage();
			if (ex.getErrorCode() == 2627){	// 이미 존재
				return 2;
			}
			return 1;
		}

		return 0;
	}
	
	public synchronized static Vector querySelect(Connection dbconn, String sql){
		Statement stmt = null;
		Vector retv = new Vector();

		try{
			// connect
			stmt = dbconn.createStatement();

			ResultSet rs = stmt.executeQuery(sql);
			int fetchsize = 0;
			if(rs.last()){
				fetchsize = rs.getRow();
				int nCol = rs.getMetaData().getColumnCount();
				rs.first();
				if(fetchsize != 0){
					do{
						String[] values = new String[nCol];
						for (int i = 0; i < nCol; i++) {
							values[i] = (String)rs.getString(i+1);
						}
						retv.addElement(values);
					}
					while(rs.next());
				}
				else
					retv = null;
			}
			else
				retv = null;
		}
		catch(SQLException e){
			if (log != null) log.error(e.getMessage());
			lastErrorMessage = e.getMessage();
	    	return null;
	    }

		return retv;
	}	
	
	public synchronized static Vector[] querySelect(Connection dbconn, String table, String[] names, String wherestmt){
		Statement stmt = null;
		Vector[] retv = null;
		String query;
		int index = 0;

		try{
			// connect
			stmt = dbconn.createStatement();

			query = "select ";
			int i = 0;
			for (i = 0; i < names.length-1; i++) {
				query = query + names[i] + ",";
			}
			query = query + names[i] + " from " + table + wherestmt;

			ResultSet rs = stmt.executeQuery(query);
			int fetchsize = 0;
			if(rs.last()){
				fetchsize = rs.getRow();
				rs.first();
				if(fetchsize != 0){
					retv = new Vector[fetchsize];
					do{
						Vector vi = new Vector();
						for (i = 0; i < names.length; i++) {
							vi.addElement((String)rs.getString(names[i]));
						}
						retv[index++] = vi;
					}
					while(rs.next());
				}
				else
					retv = null;
			}
			else
				retv = null;
		}
		catch(SQLException e){
			if (log != null) log.error(e.getMessage());
			lastErrorMessage = e.getMessage();
	    	return null;
	    }

		return retv;
	}	
	
	public synchronized static int queryExecute(Connection conn, String query) throws Exception
	{
		Statement stmt = null;
		int count = 0;

		try {
			// connect
			stmt = conn.createStatement();
			count = stmt.executeUpdate(query);
		}
		catch (SQLException e) {
			if (log != null) log.error(e.getMessage());
			lastErrorMessage = e.getMessage();
			throw e;
		}
		
		return count;
	}
	
	public synchronized static void sp_execute(Connection conn, String spname, String[] values, String[] output) throws Exception {

		CallableStatement cst = null;
		int i_return = 1;

		try {
			String sql = "{call " + spname + "(";
			for (int i = 0; i < values.length+output.length-1; i++) {
				sql += "?,";
			}
			sql += "?)}";
			
			cst = conn.prepareCall(sql);
			
			for (int i = 0; i < values.length; i++) {
				cst.setString(i+1, values[i]);
			}
			for (int i = 0; i < output.length; i++) {
				cst.registerOutParameter(values.length+i+1, Types.VARCHAR);
			}
			
			cst.execute();
			
			for (int i = 0; i < output.length; i++) {
				output[i] = cst.getString(values.length+i+1);
			}

		} catch (Exception e) {
			throw e;
		} finally {
			if (cst != null){
				try {
					cst.close();
				} catch (Exception e) {
				}
			}
		}
	}
}
