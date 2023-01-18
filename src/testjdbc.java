import java.sql.Connection;
import java.sql.DriverManager;


public class testjdbc {

	testjdbc() throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection con = DriverManager.getConnection(
						"jdbc:oracle:thin:@(DESCRIPTION =(ENABLE=BROKEN)(FAILOVER = ON)(LOAD_BALANCE = OFF)(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.172.76)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.172.78)(PORT = 1521)))(CONNECT_DATA = (SERVICE_NAME = DSPPRA)))",
						"PPR000",
						"fo479685");
		con.setAutoCommit(false);
		con.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new testjdbc();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("OK!");
	}

}
