package bpr.dlm.migration.edms;

import java.io.File;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import com.windfire.apis.asysConnectData;
import com.windfire.apis.asys.asysUsrElement;
import com.windfire.apis.asysadmin.asysAdmAuthMngrColl;
import com.windfire.apis.asysadmin.asysAdmEngine;
import com.windfire.apis.asysadmin.asysAdmEngineColl;
import com.windfire.apis.asysadmin.asysAdmPostProcColl;
import com.windfire.apis.data.asysDataField;
import com.windfire.apis.data.asysDataFieldColl;
import com.windfire.apis.data.asysDataIndex;
import com.windfire.apis.data.asysDataIndexColl;
import com.windfire.apis.data.asysDataResult;
import com.windfire.base.asysTransact;

/**
 * @author deluxjun
 *
 */
public class XtormCommon{
	String	APP_NAME = "IMAGE";
	String	DEFAULT_GATEWAY = "XTORM_MAIN";
	String	TYPE_IMAGE = "IMAGE";
	
	String ERR_INDEX = "인덱스관련 오류입니다.";
	
	asysConnectData con;
	String	gateway, server, user, passwd;
	int		port;
	
	String lastError;

	
	/**
	 * @param gateway
	 * @param server
	 * @param port
	 * @param user
	 * @param pswd
	 */
	public XtormCommon(String gateway, String server, int port, String user, String pswd)
	{
		this.gateway = gateway.trim();
		this.server = server.trim();
		this.user = user.trim();
		this.passwd = pswd.trim();
		this.port = port;

		con = connect();
	}
	/**
	 * @param server
	 * @param port
	 * @param user
	 * @param pswd
	 */
	public XtormCommon(String server, int port, String user, String pswd)
	{
		this.gateway = DEFAULT_GATEWAY;
		this.server = server.trim();
		this.user = user.trim();
		this.passwd = pswd.trim();
		this.port = port;

		con = connect();
	}
	public XtormCommon(){
		// default settings
		this.gateway = DEFAULT_GATEWAY;
		this.server = "localhost";
		this.user = "SUPER";
		this.passwd = "";
		this.port = 2102;
	}
	
	
	public void setGateway(String gateway) {
		this.gateway = gateway.trim();
	}

	public String getGateway() {
		return this.server;
	}

	public void setServer(String svr) {
		this.server = svr.trim();
	}

	public String getServer() {
		return this.server;
	}

	public void setUser(String user) {
		this.user = user.trim();
	}

	public String getUser() {
		return this.user;
	}

	public void setPswd(String pswd) {
		this.passwd = pswd.trim();
	}

	public String getPswd() {
		return this.passwd;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return this.port;
	}
	
	public String getLastError() {
		return lastError;
	}
	
	public asysConnectData getConnection(){
		return con;
	}
	
	public asysConnectData getSession(){
		if (con == null){
			con = connect();
		}
		return con;
	}
	public void releaseSession(asysConnectData cd){
		return;
	}

	public static boolean isAlive(asysConnectData cd){
		if (cd.m_transmit == null)
			return false;

		return true;
	}
	
	public static asysConnectData getNewSession(String client, String gateway, String server, int port, String user, String pswd) throws XtormException{
		asysConnectData cd = new asysConnectData(server, port, client, user, pswd);
		
		asysAdmEngineColl asysadmenginecoll = new asysAdmEngineColl(cd, true);
		int ret = asysadmenginecoll.retrieveEngines();
		if (ret != asysTransact.RCODE_OK){
			throw new XtormException(asysadmenginecoll.getLastError());
		}

		asysAdmAuthMngrColl asysadmauthmngrcoll = new asysAdmAuthMngrColl(cd, true);
		ret = asysadmauthmngrcoll.retrieveAuthMngrs();
		if(ret != asysTransact.RCODE_OK){
			throw new XtormException(asysadmauthmngrcoll.getLastError());
		}
		
		asysAdmPostProcColl asysadmpostproccoll = new asysAdmPostProcColl(cd, true);
		ret = asysadmpostproccoll.retrievePostProcs();
		if(ret != asysTransact.RCODE_OK){
			throw new XtormException(asysadmpostproccoll.getLastError());
		}
		
		asysAdmEngine asysadmengine = asysadmenginecoll.findEngine(gateway);
		if (asysadmengine == null){
			throw new XtormException(gateway+"엔진을 찾을 수 없습니다.");
		}
		ret = asysadmengine.retrieveProperties();
		if(ret != asysTransact.RCODE_OK){
			throw new XtormException(asysadmengine.getLastError());
		}
		
		return cd;
	}

	/**
	 * @return
	 */
	private asysConnectData connect()
	{
		asysConnectData acd = new asysConnectData(server, port, APP_NAME, user, passwd);
		
		int ret = handleEngines(gateway, acd);
		if (ret != asysTransact.RCODE_OK){
			return null;
		}
		return acd;
	}
	
	private int handleEngines(String gateway, asysConnectData cd)	{
		int i = 0;

		asysAdmEngineColl asysadmenginecoll = new asysAdmEngineColl(cd, true);
		int ret = asysadmenginecoll.retrieveEngines();
		if (ret != asysTransact.RCODE_OK){
			lastError = asysadmenginecoll.getLastError();
			return ret;
		}

		asysAdmAuthMngrColl asysadmauthmngrcoll = new asysAdmAuthMngrColl(cd, true);
		ret = asysadmauthmngrcoll.retrieveAuthMngrs();
		if(ret != asysTransact.RCODE_OK){
			lastError = asysadmenginecoll.getLastError();
			return ret;
		}
		
		asysAdmPostProcColl asysadmpostproccoll = new asysAdmPostProcColl(cd, true);
		ret = asysadmpostproccoll.retrievePostProcs();
		if(ret != asysTransact.RCODE_OK){
			lastError = asysadmpostproccoll.getLastError();
			return ret;
		}
		
		asysAdmEngine asysadmengine = asysadmenginecoll.findEngine(gateway);
		if (asysadmengine == null){
			lastError = gateway+"엔진을 찾을 수 없습니다.";
			return asysTransact.RCODE_ERROR;
		}
		ret = asysadmengine.retrieveProperties();
		if(ret != asysTransact.RCODE_OK){
			lastError = asysadmengine.getLastError();
			return ret;
		}
		return asysTransact.RCODE_OK;
	}
	
	/**
	 * 
	 */
	public void terminate(){
		if(con != null) con.close();
	}
	
	public void close(){
		
	}

	/**
	 * @param sel
	 * @return
	 */
	public Vector getResult(asysDataResult sel)
	{
		Vector res = new Vector(10, 5);
		String[] aRecord;
		int colCount = sel.getColCount();

		// 컬럼 레이블 인쇄 -- sel.nextRow() 전에 실행해야 함
		aRecord = new String[colCount+1];
		int tmp = 0;
		for (int i = 0; i < colCount; i++) {
			// record.put((String)sel.getColLabel(i),(String)sel.getColValue(i));
			aRecord[i] = sel.getColLabel(i);
			tmp++;
		}
		// ElementID 레이블 추가
//		record.put("L" + Integer.toString(tmp), "ElementID");
		aRecord[colCount] = "ElementID";
		res.addElement(aRecord);
	
		// 컬럼값 입력
		while (sel.nextRow()) {
			aRecord = new String[colCount+1];
			for (int i = 0; i < colCount; i++) {
				aRecord[i] = sel.getColValue(i);
			}
			// ElementID 추가
//			record.put("V" + Integer.toString(tmp), sel.getElementId());
			aRecord[colCount] = sel.getElementId();
			res.addElement(aRecord);
		}

		return res;
	}

	/**
	 * 조건을 만들어 해당되는 내용의 레코드들을 가져와 벡터로 반환한다.
	 * @param search
	 * @param etc
	 * @param idxID
	 * @param fld
	 * @param everything
	 * @return
	 */
	public Vector getRecords(asysConnectData con, Vector fld, Vector criteria, Vector value, Vector etc_fld, String index_id, boolean all)
	{
		if (con == null){
			return null;
		}

		// 전체 테이블정보 ic
		asysDataIndexColl ic = new asysDataIndexColl(con, false);

		asysDataFieldColl fc = new asysDataFieldColl(con, false);
		int rCode = fc.retrieveFields(gateway +  "::" + index_id + "::DEF", "USER_*");
		if (rCode != asysTransact.RCODE_OK) {
			lastError = "asysDataFieldColl.retrieveFields : " + fc.getLastError();
			return null;
		}

		rCode = ic.retrieveAll(1, "USER_SEARCH");
		if (rCode != asysTransact.RCODE_OK) {
			lastError = "asysDataIndexColl.retrieveAll : " + fc.getLastError();
			return null;
		}

		asysDataIndex idx = ic.findFullIndex(gateway +  "::" + index_id + "::DEF");
		if (idx == null){
			lastError = index_id + " " + ERR_INDEX;
			return null;
		}

		idx.clearResultFields();
		idx.clearSearchFields();

		// 결과필드 설정
		for (int i = 0; i < fc.getCollCount(); i++) {
			asysDataField fld1 = (asysDataField) fc.getCollObject(i);
			idx.addResultField(fld1.m_userColumn);
		}

		if (!all) {
			// 추가필드 추가. $ELEMENTID$, $DESCR$, $SCLASS$, $ECLASS$
			if (etc_fld != null) {
				Enumeration fields = etc_fld.elements();
				while (fields.hasMoreElements()){
					idx.addResultField((String)fields.nextElement());
				}
			}
			// 검색필드 criteria 추가
			if (fld != null) {
				Enumeration fields = fld.elements();
				Enumeration criterias = criteria.elements();
				Enumeration values = value.elements();
				while (fields.hasMoreElements() && criterias.hasMoreElements() && values.hasMoreElements()) {
					String fieldname = (String)fields.nextElement();
					String sel = (String)criterias.nextElement();
					String valuetext = (String)values.nextElement();

					if ((fieldname != null) && (valuetext != null) && (sel != null))
						idx.addSearchField(index_id, fieldname, sel, valuetext, "and");
				}
			}
		// 모든항목 검색
		} else {

			if (fld != null) {
				Enumeration fields = fld.elements();
				while (fields.hasMoreElements()) {
					String fieldname = (String) fields.nextElement();

					if (fieldname != null) {
						idx.addSearchField(index_id, fieldname, "like", "%", "and");
						break;
					}
				}
			}
		}

		// int rCode = idx.open(1000, "USER_VIEW", "order by ID");
		rCode = idx.open(1000, "USER_VIEW", "");
		if (rCode == asysTransact.RCODE_OK) {
			// 결과를 sel에 저장
			asysDataResult sel = idx.getResults();
			return getResult(sel);
		} else {
			lastError = "index open : " + idx.getLastError();
			return null;
		}
	}

	/**
	 * @param eid
	 * @param filepath
	 * @param fullpath
	 * @return
	 */
	public String getImage(asysConnectData con, String eid, String filepath, boolean fullpath)
	{
		if (con == null){
			return null;
		}
		
		String shorteid;
		String fulleid;
		int pos = eid.indexOf("::");
		if (pos != -1){
			fulleid = eid;
			String Engine = eid.substring(0, pos);
			String temp = eid.substring(pos + 2, eid.length());

			pos = temp.indexOf("::");
			shorteid = temp.substring(0, pos);
		}
		else{
			shorteid = eid;
			fulleid = gateway + "::" + shorteid + "::" + TYPE_IMAGE;
		}

		String imgName;
		if (fullpath){
			imgName = filepath;
		}
		else{
			// 폴더 없으면 생성한다.
		    (new File(filepath)).mkdirs();
			
			char tmp = filepath.charAt(filepath.length()-1);
			if (tmp != '/' && tmp != '\\')
				imgName = filepath + "/" + shorteid;
			else
				imgName = filepath + shorteid;
		}

		asysUsrElement ue = new asysUsrElement(con);
		ue.m_elementId = fulleid.trim();
		int ret = ue.getContent(imgName);
		if (ret == 0) {
			return shorteid;
		}
		else {
			lastError = ue.getLastError();
			return null;
		}
	}

	/**
	 * @param desc
	 * @param sclass
	 * @param fileLoc
	 * @param Indexes
	 * @param archiveid
	 * @param classid
	 * @param indexid
	 * @param gateway
	 * @return
	 */
	public String createImage(asysConnectData con, String desc, String sclass, String fileLoc,
			Hashtable indexes, String archiveid, String classid, String indexid, String gateway) throws Exception{

		if (con == null){
			throw new Exception("connection is null");
		}
		asysUsrElement uePage = new asysUsrElement(con);
		uePage.setInfo(desc, sclass, TYPE_IMAGE, fileLoc);
		// Xtorm Index info
		Enumeration values = indexes.elements();
		Enumeration keys = indexes.keys();

		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = (String) values.nextElement();
			uePage.addIndexValue(indexid, key, value);
		}
		if (classid != null && !classid.equals("")) {
			uePage.m_cClassId = classid;
		} else {
			uePage.m_archive = archiveid;
		}

		int ret = uePage.create(gateway);
		if (ret != 0){
			throw new Exception(uePage.getLastError());
		}
		
//		int pos = uePage.m_elementId.indexOf("::");
		String sRet = getShortID(uePage.m_elementId);
		
		return sRet;
	}
	public String createImage(String desc, String sclass, String fileLoc,
			Hashtable indexes, String archiveid, String classid, String indexid, String gateway) throws Exception{
		return createImage(con, desc, sclass, fileLoc, indexes, archiveid, classid, indexid, gateway);
	}
	

    public String getShortID(String id) {
        java.util.StringTokenizer stk =  new java.util.StringTokenizer(id, "::");
        String gNm = stk.nextToken(); //gateway
        String elemId  = stk.nextToken();
        return elemId;
    }
    
	/**
	 * @param eid
	 * @return
	 */
	public int deleteImage(asysConnectData con, String eid) throws Exception{
		if (con == null){
			throw new Exception("connection is null");
		}
		
		if (eid.indexOf("::") == -1){
			eid = gateway + "::" + eid + "::" + TYPE_IMAGE;
		}
		
		asysUsrElement e = new asysUsrElement(con);
		e.m_elementId = eid;
		int ret = e.delete();
		if (ret != 0)
			throw new Exception(e.getLastError());

		return ret;
	}
	public int deleteImage(String eid) throws Exception{
		return deleteImage(con, eid);
	}

	/**
	 * @param eid
	 * @param fileName
	 * @return
	 */
	public int replaceImage(asysConnectData con, String eid, String fileName)
	{
		if (con == null){
			return 1;
		}

		if (eid.indexOf("::") == -1){
			eid = gateway + "::" + eid + "::" + TYPE_IMAGE;
		}
		
		asysUsrElement e = new asysUsrElement(con);
		e.m_elementId = eid;

		int ret = e.replaceContent(fileName);
		if (ret != 0)
			lastError = e.getLastError();

		return ret;
	}
	
	public Vector getFieldsOfIndex(asysConnectData con, String indexid)
	{
		if (con == null){
			return null;
		}

		Vector res = new Vector(10, 5);
		Hashtable fields;
		
		asysDataFieldColl fc = new asysDataFieldColl(con, false);
		if (fc != null) {
			int rCode = fc.retrieveFields(gateway + "::" + indexid + "::DEF", "USER_*");
			if (rCode == asysTransact.RCODE_OK) {
				for (int i = 0; i < fc.getCollCount(); i++) {
					fields = new Hashtable(12);
					asysDataField fld = (asysDataField) fc.getCollObject(i);
					fields.put("USERCOLUMN", fld.getField("USERCOLUMN"));
					fields.put("FIELDTYPEID", fld.getField("FIELDTYPEID"));
					fields.put("DESCR", fld.getField("DESCR"));
					fields.put("USERSCLASS", fld.getField("USERSCLASS"));
					fields.put("HASSUBFIELDS", fld.getField("HASSUBFIELDS"));
					fields.put("SQLTYPE", fld.getField("SQLTYPE"));
					fields.put("ENTRYLENGTH", fld.getField("ENTRYLENGTH"));
					fields.put("ENTRYMASK", fld.getField("ENTRYMASK"));
					fields.put("CANSEARCH", fld.getField("CANSEARCH"));
					fields.put("REQUIRED", fld.getField("REQUIRED"));
					fields.put("LISTYPE", fld.getField("LISTYPE"));
					fields.put("HASDEPTH", fld.getField("HASDEPTH"));
					res.addElement(fields);
				}
				return res;
			}
			else {
				lastError = fc.getLastError();
				return null;
			}
		}
		else {
			lastError = fc.getLastError();
			return null;
		}
	}

}
