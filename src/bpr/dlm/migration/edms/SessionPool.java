/*
 * Created on 2005. 10. 18.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.edms;

import java.sql.DriverManager;
import java.util.Vector;

import com.windfire.apis.asysConnectData;

/**
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SessionPool {
	private String CLIENT_NAME = "AGENT";
    private String m_gateway = "XTORM_MAIN";
    private String m_server = "localhost";
    private int m_port = 2102;
    private String m_user = "SUPER";
    private String m_pwd = "";
    private int m_size = 10;	// default = 10;

    private String m_ErrorMessage = new String("");
    private Vector m_pool = null;

    public SessionPool(String gateway, String server, int port, String user, String pwd, int size) {
		this.m_gateway = gateway.trim();
		this.m_server = server.trim();
		this.m_user = user.trim();
		this.m_pwd = pwd.trim();
		this.m_port = port;
		this.m_size = size;
    }
    
    public SessionPool(String gateway, String server, int port, String user, String pwd) {
		this.m_gateway = gateway.trim();
		this.m_server = server.trim();
		this.m_user = user.trim();
		this.m_pwd = pwd.trim();
		this.m_port = port;
    }    

    public SessionPool() {
    }
	
	public String getLastError() {
		return m_ErrorMessage;
	}
	
	public int getPoolsize() {
		if (m_pool == null) {
			return 0;
		}else{
			return m_pool.size();
		}
	}
	
    public void setGateway(String value) {
        if (value != null) {
            m_gateway = value;
        }
    }

    public String getGateway() {
        return m_gateway;
    }

    public void setSize(int value) {
        if (value > 0) {
            m_size = value;
        }
    }

    public int getSize() {
    	if (m_pool != null && m_size != m_pool.size())
    		m_size = m_pool.size();
    	
        return m_size;
    }

    public void setUsername(String value) {
        if (value != null) {
            m_user = value;
        }
    }

    public String getUserName() {
        return m_user;
    }

    public void setPassword(String value) {
        if (value != null) {
            m_pwd = value;
        }
    }

    public String getPassword() {
        return m_pwd;
    }

    public synchronized void initializePool() throws Exception {
        if (m_gateway == null) {
            throw new Exception("No gateway Specified!");
        }
        if (m_size < 1) {
            throw new Exception("Pool size is less than 1!");
        }

        try {
            for (int x=0;x<m_size;x++) {
                asysConnectData _sess = XtormCommon.getNewSession(CLIENT_NAME, m_gateway, m_server, m_port, m_user, m_pwd);
                Session ps = new Session(_sess);
                addSession(ps);
            }
        } catch (Exception e) {
            throw new Exception("init pool error : " + e.getMessage());
        }
    }

    private void addSession(Session value) {
        if (m_pool == null) {
            m_pool = new Vector(m_size);
        }
        m_pool.addElement(value);
    }

    public synchronized void releaseSession(asysConnectData sess) {
        for (int x=0;x<m_pool.size();x++) {
            Session ps = (Session)m_pool.elementAt(x);
            if (ps.getSession() == sess ) {
                ps.setInUse(false);
                break;
            }
        }
    }

    public synchronized asysConnectData getSession() throws Exception{
        Session ps = null;
        for (int x=0;x<m_pool.size();x++) {
            ps = (Session)m_pool.elementAt(x);
            if (ps.inUse() == false) {
				if (XtormCommon.isAlive(ps.getSession()) == false) {
					m_pool.remove(ps);
					ps.close();

	                asysConnectData _sess = XtormCommon.getNewSession(CLIENT_NAME, m_gateway, m_server, m_port, m_user, m_pwd);
	                ps = new Session(_sess);
	                addSession(ps);
				}
                ps.setInUse(true);
                return ps.getSession();
            }
        }
        
        // 빈 세션이 없으면 null 리턴.
        return null;

//        try {
//            asysConnectData _sess = XtormCommon.getNewSession(CLIENT_NAME, m_gateway, m_server, m_port, m_user, m_pwd);
//            ps = new Session(_sess);
//            ps.setInUse(true);
//            m_pool.addElement(ps);
//		} catch (Exception e) {
//        	throw new Exception("get session error : " + e.getMessage());
//		}

//		return ps.getSession();
    }

    public synchronized void emptyPool() {
    	if (m_pool == null)
    		return;
        for (int x=0;x<m_pool.size();x++) {
            System.out.println("Closing Xtorm Session " + x);
            Session ps = (Session)m_pool.elementAt(x);
            if (ps.inUse() == false) {
                ps.close();
            } else {
                try {
                    java.lang.Thread.sleep(100);
                    ps.close();
                } catch (InterruptedException e) {
                	m_ErrorMessage = "close session error : " + e.getMessage();
                }
            }
        }
    }
    
    public Vector getSessionStatus() {
    	Vector clsVecReturn = new Vector();
    	if (m_pool == null) {
    		clsVecReturn.add("Pool size 0.");
    	} else {
	    	for (int x=0;x<m_pool.size();x++) {
	    		Session ps = (Session)m_pool.elementAt(x);
	    		if (ps == null) {
	    			clsVecReturn.add(Integer.toString(x+1) + " error");
	    		} else {
	    			long l_Return = ps.getCount();
	    			clsVecReturn.add(Integer.toString(x+1) + "("+ps.inUse()+","+Long.toString(l_Return)+")");
	    		}
			}
		}
		return clsVecReturn;
	}
    
	
	/**
	 * Session
	 */
	class Session{
	    private asysConnectData sess = null;
	    private boolean inuse = false;
	    private long m_jobcount = 0L;

	    public Session(asysConnectData value) {
	        if ( value != null ) {
	         sess = value;
	        }
	    }

	    public asysConnectData getSession() {
	        return sess;
	    }

	    public void setInUse(boolean value) {
	    	if (value) {
	    		// 몇 번 사용된 세션인가를 기억한다.
	    		m_jobcount++;
	    	}
	        inuse = value;
	    }

	    public boolean inUse() {
	        return inuse;
	    }

	    public void close() {
            sess.close();
	    }
	    
	    public long getCount() {
	    	return m_jobcount;
	    }
	}
}
