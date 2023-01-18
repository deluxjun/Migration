/*
 * Created on 2006. 1. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.util.ArrayList;
import java.util.List;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class TargetFieldSet {
	String mDB;
	String mTable;
	String mGroup;
	List mField;
	long mlCount;
	int mCurrentInsertedRow = 0;

	TargetFieldSet(String db, String table, String groupid) throws Exception{
		mDB = db;
		mTable = table;
		mGroup = groupid;
		mlCount = 0;
		
		mField = new ArrayList();
	}
	
	public void addField(String name, String type, String value){
		mField.add(new Field(name, type, value));
	}

}
