/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SamReader {
	
	List mFiles;
	int mfileIndex;
	private Map mProperties;
	private BufferedReader mBufferedReader;
	private long mIndex;
	Map mValues;
	
	private String readed;
	private String msDelimiter;

	public String m_current_filename;
	
	private String[] splitString;
	
	private Logger log;
	
	private Vector mvGroup;
	
	SamReader(Map properties, List list, Logger log) throws Exception{
		this.log = log;
		this.mFiles = list;
		this.mProperties = properties;
		
		mfileIndex = 0;
		mIndex = 0;
		mvGroup = new Vector();
		
		msDelimiter = (String)mProperties.get("delimiter");
	}
	
	// ������ ù������ �о���δ�.
	private int openNext() throws Exception{
		if (mBufferedReader != null){
			mBufferedReader.close();
			mBufferedReader = null;
		}

		if (mfileIndex < mFiles.size()){
			String filename = (String)mFiles.get(mfileIndex++);
			m_current_filename = filename;
			
			mBufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "KSC5601"));
			readed = mBufferedReader.readLine();
			if (readed == null){
				return -1;
			}
			log.info("SAM file opened:"+filename);
			return 1;
		}
		// ������ ���̻� �������� ����.
		return 0;
	}

	
	public void init(long startindex) throws Exception{
		// ù° ���� ����
		int ret = openNext();
		if (ret <= 0){
			throw new Exception("SAM���� �ʱ�ȭ ����");
		}
	}
	
	// �׷���� �о���δ�.
	public long next() throws Exception{
		mValues = new HashMap();
		mvGroup.clear();
		long readedindex = 0;

		int smallindex = 1;
		
		do{
			if (splitString == null)
				splitString = readed.split(msDelimiter);
			
			try{
				if (readed == null)
					break;
				if (readed.equals(""))
					continue;

				// index check
				readedindex = Long.parseLong(splitString[0]);
				
				if ( (mIndex+1) < readedindex){	// ���� �ε����� ���� ����. �ٷ� ����. 
					// �ӽ÷�, ���� �׷���̵���� �н��Ѵ�.
//					mIndex ++;
					while((mIndex+1) < readedindex){
						mIndex ++;
					}
					return mIndex;
				}
				
				// ���� �ε����� ��쿡 �Ʒ��� �����.
				
				// group ���� Map�� ����..
				String[] values = new String[splitString.length-3];
				System.arraycopy(splitString, 3, values, 0, values.length);
				List list = (ArrayList)mValues.get(splitString[2]);	// �����׷캰�� ����.
				if (list == null){
					list = new ArrayList();
					mValues.put(splitString[2], list);
				}
				list.add(values);
				
				// SAM���� ���� �״�� ����. ����SAMFile ����� ����
				String sDelimiter = msDelimiter.replaceAll("\\\\", "");
				int pos = readed.indexOf(sDelimiter);
				mvGroup.add(readed.substring(pos));
				
				// �����..
			}catch(Exception e){
				e.printStackTrace();
				throw e;
			}
			
			splitString = null;
			readed = mBufferedReader.readLine();
		}while(readed != null);

		// ���� ���� ���������Ƿ�, ��������..
		int ret = openNext();
		
		// 0 �̸� ��� �б� �۾� �Ϸ�. -1�̸� �б����
		if (ret <= 0){
			return ret;
		}
		
		mIndex++;
		return mIndex;
	}
	
	public void pathIndex(long index){
		
	}
	
	
	public void shutdown(){
		try {
			if (mBufferedReader != null)
				mBufferedReader.close();
		} catch (Exception e) {
		}
	}
	
	public int getGroupCount(){
		return mValues.size();
	}
	
	public Vector getGroupContents(){
		return mvGroup;
	}
	
	//
	public List getGroupValues(String group){
		return (List) mValues.get(group);
	}
	
	public String[] getValues(String group, int row){
		List list = (List)mValues.get(group);
		return (String[])list.get(row);
	}
	
	public int getSize(String group){
		List list = (List)mValues.get(group);
		if (list == null)
			return -1;
		return list.size();
	}

	public String getValue(String group, int row, int position){
		List list = (List)mValues.get(group);
		String[] strings = (String[])list.get(row);
		
		return strings[position+1];
	}
	public String getValue(SourceField sourceField, int row) throws Exception{
		List list = (List)mValues.get(sourceField.mGroup);
		String[] strings = (String[])list.get(row);
		
		return strings[sourceField.mPosition+1];
	}
}
