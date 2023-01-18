/*
 * Created on 2005. 10. 20.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.edms;

import com.windfire.apis.asysConnectData;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class PooledInterface extends XtormCommon{
	private SessionPool m_sessionPool;
	private int m_poolsize = 10;	// default size = 10
	
	public PooledInterface(String gateway, String server, int port, String user, String passwd) throws XtormException{
		setGateway(gateway);
		
		m_sessionPool = new SessionPool(gateway, server, port, user, passwd, m_poolsize);
		try {
			m_sessionPool.initializePool();
		} catch (Exception e) {
			super.lastError = e.getMessage();
			throw new XtormException(e.getMessage());
		}
	}

	public PooledInterface(int poolsize, String gateway, String server, int port, String user, String passwd) throws XtormException{
		m_poolsize = poolsize;
		setGateway(gateway);
		
		m_sessionPool = new SessionPool(gateway, server, port, user, passwd, poolsize);
		try {
			m_sessionPool.initializePool();
		} catch (Exception e) {
			super.lastError = e.getMessage();
			throw new XtormException(e.getMessage());
		}
	}
	
	public int getPoolSize(){
		return m_poolsize;
	}
	
	public SessionPool getSessionPool(){
		return m_sessionPool;
	}
	
	public void terminate(){
		m_sessionPool.emptyPool();
	}

	
//////////////////////////////////////////////////////////////////////
// re-define inherited modules
//
//////////////////////////////////////////////////////////////////////
	
	public synchronized asysConnectData getSession(){
		asysConnectData cd;
		
		try {
			cd = m_sessionPool.getSession();
		} catch (Exception e) {
			super.lastError = e.getMessage();
			return null;
		}
		return cd;
	}
	
	public synchronized void releaseSession(asysConnectData cd){
		m_sessionPool.releaseSession(cd);
	}
}
