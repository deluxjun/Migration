package bpr.dlm.migration.dumpsam;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import bpr.dlm.migration.util.CommonUtil;
import bpr.dlm.migration.util.IniFile;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class ConfigData {
	public String sFileHead;
	public String sOutFolder;
	public String sDelimiter;
	public String sTableForCheck;
	public boolean bOverwrite;
	public boolean bBaseWrite = false;
	public int iPartRowNum;
	public int iFlushCount;
	public int iComputeCount;

//	public String sErrFile;
	public String sLogFile;
	public String sInfoFile;

	public String sDBClassName;
	public String sDBURL;
	public String sDBUser;
	public String sDBPassword;
	public Map mSQLMap;
	
	public String[] aSQLHeader;// = {"M","S","H","I","B","L"};
	
	public String sDB2ClassName;
	public String sDB2URL;
	public String sDB2User;
	public String sDB2Password;
	public String sDB2Target;
	
	public String[] aSQL;
	
	public ConfigData(String inifile) throws Exception{
		init(new FileInputStream(inifile));
	}
	
	public ConfigData(File fIniFile) throws Exception{
		init(new FileInputStream(fIniFile));
	}
	
	
	private void init(InputStream stream) throws Exception{
		IniFile prop = new IniFile(stream);

		// db info
		sFileHead = prop.getKeyValue("COMMON", "HEADERNAME");
		sOutFolder = prop.getKeyValue("COMMON", "OUTFOLDER");
		sDelimiter = prop.getKeyValue("COMMON", "DELIMITER");
		bOverwrite = prop.getKeyValue("COMMON", "OVERWRITE").equals("TRUE");
		bBaseWrite = prop.getKeyValue("COMMON", "BASEWRITE").equals("TRUE");
		iPartRowNum = prop.getKeyIntValue("COMMON", "PART_ROW");
		iFlushCount = prop.getKeyIntValue("COMMON", "FLUSH");
		iComputeCount = prop.getKeyIntValue("COMMON", "COMPUTE_COUNT");
		sTableForCheck = prop.getKeyValue("COMMON", "CHECKCOUNT_SQL");
		
		if (iFlushCount <= 0)
			iFlushCount = 1;
		if (iComputeCount <= 0)
			iComputeCount = 1;
		
		sLogFile = prop.getKeyValue("COMMON", "LOG_PATH");
    	sInfoFile = sLogFile;
		
		// log 파일 설정
    	sLogFile += "/" + sFileHead + ".log";
   	
    	sInfoFile += "/" + sFileHead + ".info";

		String sHeaders = prop.getKeyValue("COMMON", "HEADERS");
		if (sHeaders != null && sHeaders.length() != 0){
			aSQLHeader = sHeaders.split(",");
		} else {
			aSQLHeader = new String[0];
		}

		sDBClassName = prop.getKeyValue("DB", "CLASSNAME");
		sDBURL = prop.getKeyValue("DB", "URL");
		sDBUser = prop.getKeyValue("DB", "USER");
		sDBPassword = prop.getKeyValue("DB", "PASSWORD");

		sDB2ClassName = prop.getKeyValue("DB2", "CLASSNAME");
		sDB2URL = prop.getKeyValue("DB2", "URL");
		sDB2User = prop.getKeyValue("DB2", "USER");
		sDB2Password = prop.getKeyValue("DB2", "PASSWORD");
		sDB2Target = prop.getKeyValue("DB2", "TARGET");
		if (sDB2Target == null)
			sDB2Target = "";

		mSQLMap = new HashMap();
		mSQLMap.put("BASE", prop.getKeyValue("GROUP", "BASE"));
		for (int i = 0; i < aSQLHeader.length; i++) {
			mSQLMap.put(aSQLHeader[i], prop.getKeyValue("GROUP", aSQLHeader[i]));
		}
	}
	
}
