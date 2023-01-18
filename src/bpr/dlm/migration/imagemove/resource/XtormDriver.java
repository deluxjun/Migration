/*
 * Created on 2006. 3. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.imagemove.resource;

import com.windfire.apis.asysConnectData;
import com.windfire.apis.asys.asysUsrElement;
import com.windfire.apis.asysadmin.asysAdmAuthMngrColl;
import com.windfire.apis.asysadmin.asysAdmEngine;
import com.windfire.apis.asysadmin.asysAdmEngineColl;
import com.windfire.apis.asysadmin.asysAdmPostProcColl;
import com.windfire.base.asysTransact;

/**
 * @author Administrator
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class XtormDriver {

	private String s_ErrorMessage = null;
	
	public String getLastError() {
		return this.s_ErrorMessage;
	}
	
	public asysConnectData comConnect(String S_Server, int i_Port, String s_UID, String S_PWD, String gateway) {
		asysConnectData con = new asysConnectData(S_Server, i_Port, "IMAGE", s_UID, S_PWD);
		
		int ret = handleEngines(gateway, con);
		if (ret != asysTransact.RCODE_OK){
			return null;
		}
		return con;
	}

	private int handleEngines(String gateway, asysConnectData cd)	{
		int i = 0;

		asysAdmEngineColl asysadmenginecoll = new asysAdmEngineColl(cd, true);
		int ret = asysadmenginecoll.retrieveEngines();
		if (ret != asysTransact.RCODE_OK){
			s_ErrorMessage = asysadmenginecoll.getLastError();
			return ret;
		}

		asysAdmAuthMngrColl asysadmauthmngrcoll = new asysAdmAuthMngrColl(cd, true);
		ret = asysadmauthmngrcoll.retrieveAuthMngrs();
		if(ret != asysTransact.RCODE_OK){
			s_ErrorMessage = asysadmenginecoll.getLastError();
			return ret;
		}
		
		asysAdmPostProcColl asysadmpostproccoll = new asysAdmPostProcColl(cd, true);
		ret = asysadmpostproccoll.retrievePostProcs();
		if(ret != asysTransact.RCODE_OK){
			s_ErrorMessage = asysadmpostproccoll.getLastError();
			return ret;
		}
		
		asysAdmEngine asysadmengine = asysadmenginecoll.findEngine(gateway);
		if (asysadmengine == null){
			s_ErrorMessage = gateway+"엔진을 찾을 수 없습니다.";
			return asysTransact.RCODE_ERROR;
		}
		ret = asysadmengine.retrieveProperties();
		if(ret != asysTransact.RCODE_OK){
			s_ErrorMessage = asysadmengine.getLastError();
			return ret;
		}
		return asysTransact.RCODE_OK;
	}
	
	public void comDisconnect(asysConnectData con) {
		if(con != null) con.close();
	}
	
	public String createImage(asysConnectData con, String gateway, String fileLoc,
			String cClassid, String indexId, String imgKey, String formCode, String imageVersion) throws Exception {

		if (con == null){
			s_ErrorMessage = "connection is null";
			return null;
		}
		asysUsrElement uePage = new asysUsrElement(con);
		uePage.setInfo("", "PUBLIC", "IMAGE", fileLoc);
		uePage.addIndexValue(indexId, "ImageKey", imgKey);
		uePage.addIndexValue(indexId, "FormCode", formCode);
		uePage.addIndexValue(indexId, "ImageVersion", imageVersion);
		uePage.m_cClassId = cClassid;
		int ret = uePage.create(gateway);
		if (ret != 0){
			s_ErrorMessage = uePage.getLastError();
			if (con.m_transmit == null){
				throw new Exception("connection failure");
			}

			return null;
		} else {
	        String[] arrString = uePage.m_elementId.split("::");
			String sRet = arrString[1].trim();
			return sRet;
		}
		
	}
	
	public boolean deleteImage(asysConnectData con, String gateway, String eid) {
		if (con == null){
			s_ErrorMessage = "connection is null";
			return false;
		}
		
		if (eid.indexOf("::") == -1){
			eid = gateway + "::" + eid + "::" + "IMAGE";
		}
		
		asysUsrElement e = new asysUsrElement(con);
		e.m_elementId = eid;
		int ret = e.delete();
		if (ret != 0){
			s_ErrorMessage = e.getLastError();
			return false;
		}

		return true;
	}
}
