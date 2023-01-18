/*
 * Created on 2006. 3. 7.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.dumpsam;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import bpr.dlm.migration.db.DBCommand;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class ExeQuery {
	
	ExeQuery(String sql) throws Exception{
		Statement stmt = null;
		Vector retv = new Vector();

		Connection dbconn = null;
		try{
			dbconn = DBCommand.getConnection("com.jnetdirect.jsql.JSQLDriver", "jdbc:JSQLConnect://21.101.4.145/database=cardips", "sa", "cardips");
			// connect
			stmt = dbconn.createStatement();

			ResultSet rs = stmt.executeQuery(sql);
			int nCol = rs.getMetaData().getColumnCount();
			while(rs.next()){
				String[] values = new String[nCol];
				for (int i = 0; i < nCol; i++) {
					System.out.print((String)rs.getString(i+1));
					System.out.print(",");
				}
				System.out.println("");
			}
		}
		catch(SQLException e){
			e.printStackTrace();
	    } finally{
	    	if (stmt != null){
	    		stmt.close();
	    	}
	    	if (dbconn != null){
	    		dbconn.close();
	    	}
	    }
	}

	public static void main(String[] args) throws Exception{
		new ExeQuery("select cust_no,seq_no,card_no,trd_brn_no,req_date from card_send where req_date = '20041108'  and insert_div <> 0");
	}
}
