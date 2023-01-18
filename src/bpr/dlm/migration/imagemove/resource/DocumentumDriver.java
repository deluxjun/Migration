/*
 * Created on 2006. 3. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.imagemove.resource;

import com.documentum.fc.client.DfClient;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfLoginInfo;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfLoginInfo;

/**
 * @author Administrator
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class DocumentumDriver {
	
	private String s_ErrorMessage = null;
	
	public String getLastError() {
		return this.s_ErrorMessage;
	}
	
	public IDfSession comConnect(String s_Docbase,	String s_UID, String s_PWD) {
		IDfSession sess = null;
		try {
			IDfClient client = DfClient.getLocalClient();
			IDfLoginInfo li = new DfLoginInfo();
			li.setUser(s_UID);
			li.setPassword(s_PWD);
			sess = client.newSession(s_Docbase, li);
			if (sess.isConnected()) {}
		} catch (DfException dfe) {
			s_ErrorMessage = dfe.toString();
			return null;
		}
		return	sess;
	}		
	
	public boolean comDisconnect(IDfSession sess) {
		boolean b_Return = false;
		try {
			if (sess.isConnected()) {
				sess.disconnect();
				if (!sess.isConnected()) {
					b_Return = true;
				} else {
					b_Return = false;
					s_ErrorMessage = "EDMS API Disconnect Failure";
				}
			} else {
				b_Return = false;
			}
		} catch (DfException dfe) {
			s_ErrorMessage = dfe.toString();
			b_Return = false;
		}
		return b_Return;
	}	

	public boolean comDownObject(IDfSession sess, String s_ObjectId, String s_Path, String s_FileNames) {
		int 			i_FileSu		= -1;
		String 			s_Return 		= null;
		IDfId 			sysObjId 		= null;
		IDfSysObject 	sysObj 			= null;
		
		try {
			sysObjId = new DfId(s_ObjectId);
			sysObj = (IDfSysObject)sess.getObject(sysObjId);
			String[] arrFiles = s_FileNames.split(",");
			for (int i=0; i<arrFiles.length; i++) {
				s_Return = sysObj.getFileEx(s_Path + arrFiles[i], "tiff", i, false);
				if (s_Return == "") {
					sysObj = null;
					sysObjId = null;
					s_ErrorMessage = "EDMS API Return NULL";
					return false;
				}
			}
		} catch (DfException dfe) {
			s_ErrorMessage = dfe.toString();
			return false;
  		}
      	
		sysObj = null;
		sysObjId = null;
		return true;
	}
	
	public boolean comDownObject(IDfSession sess, String s_ObjectId, int idx, String s_Path, String s_FileName) {
		int 			i_FileSu		= -1;
		String 			s_Return 		= null;
		IDfId 			sysObjId 		= null;
		IDfSysObject 	sysObj 			= null;
		
		try {
			sysObjId = new DfId(s_ObjectId);
			sysObj = (IDfSysObject)sess.getObject(sysObjId);
//			String[] arrFiles = s_FileNames.split(",");
//			for (int i=0; i<arrFiles.length; i++) {
//				s_Return = sysObj.getFileEx(s_Path + "/" + arrFiles[i], "tiff", i, false);
//				if (s_Return == "") {
//					sysObj = null;
//					sysObjId = null;
//					s_ErrorMessage = "EDMS API Return NULL";
//					return false;
//				}
//			}
			s_Return = sysObj.getFileEx(s_Path + s_FileName, "tiff", idx, false);
			if (s_Return == "") {
				sysObj = null;
				sysObjId = null;
				s_ErrorMessage = "EDMS API Return NULL";
				return false;
			}
		} catch (DfException dfe) {
			s_ErrorMessage = dfe.toString();
			return false;
  		}
      	
		sysObj = null;
		sysObjId = null;
		return true;
	}
	

	//서버와 세션을 유지하기 위해 내부함수를 날린다.
	public boolean comIsConnect(IDfSession sess, String s_Link)
	{
		boolean retVal = false;
		try {
			if (sess.isConnected()) {
				IDfFolder clsFolder = (IDfFolder)sess.getFolderByPath(s_Link);
				if (clsFolder == null) {
					retVal = true;
				} else {
					clsFolder = null;
				}
				retVal = true;
			} else {
				s_ErrorMessage = "EDMS API Session Not Connected";
				retVal = false;
			}
		} catch (DfException dfe) {
			s_ErrorMessage = dfe.toString();
			retVal = false;
		}
		return retVal;
	}
}
