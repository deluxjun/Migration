/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Field {
	String mName;
	String mType;
	String mValueBefore;
	String mValueAfter;
	
	Field(String name, String type, String value){
		mName = name;
		mType = type;
		mValueBefore = value;
	}
}
