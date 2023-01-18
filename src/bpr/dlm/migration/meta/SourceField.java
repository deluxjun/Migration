/*
 * Created on 2006. 1. 18.
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
public class SourceField{
	int mPosition;
	String mGroup;
	String[] mValue;
	
	SourceField(String name, String group, String position) throws Exception{
		mPosition = Integer.parseInt(position);
		mGroup = group;
	}
	
	public void setValue(String[] value){
		mValue = value;
	}
}