/*
 * Created on 2006. 1. 20.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.dumpsam;

import bpr.dlm.migration.util.IniFile;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class IniFileWriter {
	IniFile mIniFile;
	String mStatusFileName;
	
	IniFileWriter(String filename) throws Exception{
		// status파일 초기화
		mStatusFileName = filename;
		mIniFile = new IniFile();
	}

	public void save() throws Exception{
		mIniFile.save(mStatusFileName);
	}
	public void save(String[] orders) throws Exception{
		mIniFile.save(mStatusFileName, orders);
	}

	public void addSection(String section){
		mIniFile.addSection(section);
	}
	public void setValue(String section, String name, String value) throws Exception{
		mIniFile.setKeyValue(section, name, value);
	}
}
