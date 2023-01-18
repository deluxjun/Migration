package bpr.dlm.migration.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * @author deluxjun
 *
 */
public class CommonFileWriter {
	private String m_filename;
	protected BufferedWriter m_bufferedWriter;
	protected String lastErrorMessage;
	
	public CommonFileWriter(){}

	/**
	 * @param filename
	 */
	public CommonFileWriter(String filename, boolean append) throws Exception{
		m_filename = filename;

		m_bufferedWriter = new BufferedWriter(new FileWriter(filename, append));
	}

	/**
	 * @param filename
	 * @return
	 */
	public void open(String filename, boolean append) throws Exception{
		m_filename = filename;
		
//		mBufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "KSC5601"));
		m_bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename, append), "KSC5601"));
	}

	/**
	 * @param str
	 * @return
	 */
	public int write(String str){
		if (m_bufferedWriter == null)
			return 1;

		try {
			m_bufferedWriter.write(str);
		} catch (IOException e) {
			// TODO: handle exception
			lastErrorMessage = e.getMessage();
			return 2;
		}

		return 0;
	}
	
	/**
	 * @param line
	 * @return
	 */
	public int writeln(String line){
		if (m_bufferedWriter == null)
			return 1;

		try {
			m_bufferedWriter.write(line);
			m_bufferedWriter.newLine();
		} catch (IOException e) {
			// TODO: handle exception
			lastErrorMessage = e.getMessage();
			return 2;
		}

		return 0;
	}
	
	/**
	 * @return
	 */
	public int close(){
		if (m_bufferedWriter == null)
			return 1;
		
		try {
			m_bufferedWriter.close();
		} catch (IOException e) {
			// TODO: handle exception
			lastErrorMessage = e.getMessage();
			return 2;
		}
		
		return 0;
	}
	
	public int flush(){
		try {
			m_bufferedWriter.flush();
		} catch (IOException e) {
			lastErrorMessage = e.getMessage();
			return 2;
		}
		return 0;
	}
}
