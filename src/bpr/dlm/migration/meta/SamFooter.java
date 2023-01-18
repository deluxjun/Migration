/*
 * Created on 2006. 1. 20.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.Vector;

import bpr.dlm.migration.util.CommonFileWriter;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SamFooter extends CommonFileWriter{
	private boolean bDirty = false;
	
	private SamReader mReader;
	
	private long mIndex = 1;
	
	SamFooter(String filename) throws Exception{
		super(filename, false);
	}
	
	public void setSamReader(SamReader reader){
		mReader = reader;
	}
	
	public void addGroupContents() throws Exception{
		if (mReader == null){
			throw new Exception("not setted reader!");
		}
		
		Vector contents = mReader.getGroupContents();
		for (int i = 0; i < contents.size(); i++) {
			writeln(mIndex + (String)contents.get(i));
		}
		
		mIndex ++;
		flush();
	}
	
	public long getCurrentGroup(){
		return mIndex;
	}
}
