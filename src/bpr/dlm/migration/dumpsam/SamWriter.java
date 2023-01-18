/*
 * Created on 2006. 1. 5.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.dumpsam;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.log4j.Logger;

import bpr.dlm.migration.util.CommonFileWriter;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SamWriter extends CommonFileWriter {
	private String msHead;
	private String msExt;
	private String msFolder;
	private int mErrCode = 0;
	private int mIndex = 0;
	private Logger log;
	public long mWritedCount = 0;
	
	SamWriter(String folder, String head, String ext, Logger log) throws Exception{
		msHead = head;
		msExt = ext;
		mIndex = 0;
		msFolder = folder;
		
		this.log = log;
		
		open(makeFilename(), false);
	}
	
	private String makeFilename(){
		NumberFormat formatter = new DecimalFormat("0000");
	    String sFilename = msFolder + "/" + msHead + formatter.format(mIndex) + "."+msExt;
	    log.info("processing file name : " + sFilename);
	    return sFilename;
	}
	
	public void writeRow(long index, int group_count, String header, int idx, String[] content, String delimiter){
		StringBuffer buff = new StringBuffer("");
		buff.append(index+delimiter);
		buff.append(group_count+delimiter);
		buff.append(header+delimiter);
		buff.append(idx);
		for (int i = 0; i < content.length; i++) {
			if (content[i] == null)
				content[i] = "NULL";
			buff.append(delimiter+content[i]);
		}
		buff.append(delimiter+" ");
		
//		if (content[0] == null)
//			content[0] = "NULL";
//		buff.append(content[0]);
//
//		for (int i = 1; i < content.length; i++) {
//			if (content[i] == null)
//				content[i] = "NULL";
//			buff.append(delimiter+content[i]);
//		}
//		
		
		writeln(buff.toString());
		
		mWritedCount ++;
	}

	public int next(){
		int ret = 0;
		
		if (m_bufferedWriter == null){
			mErrCode = 1;
			return 1;
		}
		ret = flush();
		if (ret != 0)
			return ret;
		
		// 기존에 열린 파일 닫기
		ret = close();
		if (ret != 0)
			return ret;


		// 다음 파일을 연다
		mIndex ++;

	    try {
			open(makeFilename(), false);
		} catch (Exception e) {
			mErrCode = 1;
			return 1;
		}
		
		return 0;
	}
	
	public String getLastError(){
		switch (mErrCode) {
		case 0:
			return "";
		case 1:
			return "파일이 열려 있지 않습니다";

		default:
			return "IO 에러:" + lastErrorMessage;
		}
	}
}
