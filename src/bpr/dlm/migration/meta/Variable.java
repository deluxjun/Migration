/*
 * Created on 2006. 1. 18.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.util.List;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Variable{
	public static final int STRING = 0;
	
	public static final int ARRAYLIST = 1;	// 나중에 생긴것..
	public static final int INTERNAL_STRING = 2;	// 나중에 생긴것..
	
	int mType;
	String mBefore;
	String mAfter;
	List mArrayList;
	
	Variable(int type, String before){
		mType = type;
		mBefore = before;
	}
}