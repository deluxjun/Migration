/*
 * Created on 2006. 1. 20.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import bpr.dlm.migration.util.IniFile;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class StatusManager {
	IniFile mIniFile;
	String mStatusFileName;
	Map mCompleteCounts;
	
	StatusManager(String filename) throws Exception{
		mCompleteCounts = new HashMap();
		
		// status파일 초기화
		mStatusFileName = filename;
		mIniFile = new IniFile();
		try {
			mIniFile.load(mStatusFileName);
			Vector rules = mIniFile.getKeyVectorValue("COMMON", "RULE");
			for (int i = 0; i < rules.size(); i++) {
				Long count = new Long(mIniFile.getKeyValue((String)rules.get(i), "COMPLETE"));
				mCompleteCounts.put(rules.get(i), count);
			}
		} catch (FileNotFoundException e) {
			mIniFile.addSection("COMMON");
		}
	}

	public void save() throws Exception{
		mIniFile.save(mStatusFileName);
	}
	
	public void setCompleteCount(String name, long count) throws Exception{
		mCompleteCounts.put(name, new Long(count));
		if (mIniFile.getSection(name) == null){
			mIniFile.setKeyValue("COMMON", "RULE", name);
			mIniFile.addSection(name);
		}
		mIniFile.setKeyValue(name, "COMPLETE", Long.toString(count));
	}
	
	public long getCompleteCount(String name){
		Long count = (Long)mCompleteCounts.get(name);
		if (count == null)
			return 0;
		return count.longValue();
	}
	
	public void setTableCount(String rule, List listTargetFieldSet) throws Exception{
		Iterator iter = listTargetFieldSet.iterator();
		int k = 0;
		while(iter.hasNext()){
			TargetFieldSet targetFieldSet= (TargetFieldSet)iter.next();
			mIniFile.setKeyValue(rule, targetFieldSet.mTable+"_"+targetFieldSet.mGroup, Long.toString(targetFieldSet.mlCount));
		}
	}
	public void getTableCount(String name, List listTargetFieldSet) throws Exception{
		Iterator iter = listTargetFieldSet.iterator();
		int k = 0;
		while(iter.hasNext()){
			TargetFieldSet targetFieldSet= (TargetFieldSet)iter.next();
			try{
				String value = mIniFile.getKeyValue(name, targetFieldSet.mTable+"_"+targetFieldSet.mGroup);
				if (value == null)
					targetFieldSet.mlCount = 0;
				else {
					Long count = new Long(value);
					targetFieldSet.mlCount = count.longValue();
				}
			}catch(Exception e){
				throw e;
			}
		}
	}
}
