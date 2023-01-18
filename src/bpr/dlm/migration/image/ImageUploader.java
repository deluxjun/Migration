/*
 * Created on 2006. 3. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.image;

import java.io.File;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import bpr.dlm.migration.edms.XtormCommon;

import com.windfire.apis.asysConnectData;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class ImageUploader {
	asysConnectData m_conn;
	XtormCommon xtorm;
	
	private String m_gateway;
	private String m_indexid;
	private String m_sclassid;
	private String m_cclassid;
	private String m_user;
	private String m_password;
	private String[] m_fields;
	private String[] m_eids;
	
	private Logger log;
	
//	host="172.24.110.121"
//	port="2102"
//	gateway="XTORM_MAIN"
//	indexid="JANGPYO_MASTER"
//	sclassid="PUBLIC"
//	cclassid="jangpyo_99y_cc"
//	user="xtorm"
//	password="shbpr"
//	fields="ImageKey,FormCode,ImageVersion"
	
	public ImageUploader(String host, int port, String gateway, String indexid,
			String sclassid, String cclassid, String user, String password, String fields, Logger log) throws Exception{
		
		xtorm = new XtormCommon(gateway, host, port, user, password);
		
		m_gateway = gateway;
		m_indexid = indexid;
		m_sclassid = sclassid;
		m_cclassid = cclassid;
		m_user = user;
		m_password = password;
		m_fields = fields.split(",");
		
		this.log = log;
	}

	public String[] createImage(Object[] files, String path) throws Exception{
		if (!path.endsWith("/")){
			path += "/";
		}
		String[] eids = new String[files.length];
		Hashtable htIndex = new Hashtable(3);

		try {
			for (int i = 0; i < files.length; i++) {
				String[] splits = ((String)files[i]).split("_");
				if(splits.length != m_fields.length){
					throw new Exception("index field number is defferent to file info:"+ files[i]);
				}
				
				// formcode 가 null이면 continue;
				if (splits[1].equals("null")){
//					throw new Exception("cannot create image because null:"+ files[i]);
					continue;
				}
				for (int j = 0; j < splits.length; j++) {
					htIndex.put(m_fields[j], splits[j]);
				}
				eids[i] = xtorm.createImage("", m_sclassid, path+files[i]+".tif", htIndex, "", m_cclassid, m_indexid, m_gateway);
                log.debug("image created in xtorm:"+eids[i]);
			}
		} catch (Exception e) {
			// rollback. 지금까지 추가했던 이미지 삭제
			for (int j = 0; j < eids.length; j++) {
				try {
					if (eids[j] == null)
						break;
					xtorm.deleteImage(eids[j]);
				} catch (Exception ex) {
				}
			}
			eids = null;
			m_eids = eids;

			throw new Exception("cannot create image in xtorm:"+e.getMessage());
		} finally {
            // delete image file
        	for (int i = 0; i < files.length; i++) {
                try {
    				File file = new File(path+files[i]+".tif");
    				file.delete();
    			} catch (Exception e) {
    			}
			}
		}
		
		m_eids = eids;
		
		return eids;
	}
	
	public void deleteImage(String[] a_eid) {
		int ret = 0;

		try {
			for (int i = 0; i < a_eid.length; i++) {
				try {
					xtorm.deleteImage(a_eid[i]);
				} catch (Exception e) {
				}
			}
		} catch (Exception e) {
		} finally {
		}
	}
	
	public void rollback(){
		if (m_eids == null)
			return;
		
		deleteImage(m_eids);
		m_eids = null;
	}
}
