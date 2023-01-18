package bpr.dlm.migration.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * @author 배준수
 * ini 파일을 읽어들인다.
 * 
 */
public class IniFile {

	Hashtable sections;

	public IniFile() {
		sections = new Hashtable();
	}

	/**
	 * @param filename					파일 명
	 * @exception FileNotFoundException
	 */
	public IniFile(String filename) throws FileNotFoundException {
		this();
		load(filename);
	}

	/**
	 * @param url				파일명(URL)
	 * @exception IOException
	 */
	public IniFile(URL url) throws IOException {
		this();
		load(url.openStream());
	}

	/**
	 * @param input				스트림
	 * @exception IOException
	 */
	public IniFile(InputStream input) throws IOException {
		this();
		load(input);
	}

	/**
	 * 키 값을 셋
	 *
	 * @param section  섹션명
	 * @param key      키명
	 * @param value    새 값
	 */
	public void setKeyValue(String section, String key, String value) throws Exception{
		try {
			getSection(section).put(key.toUpperCase(), value);
		} catch (NullPointerException e) {
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * 섹션들을 가져온다
	 *
	 * @return   섹션 값
	 */
	public Hashtable getSections() {
		return sections;
	}

	/**
	 * 섹션을 가져온다
	 *
	 * @param section  섹션명
	 * @return         섹션값 해쉬
	 */
	public Hashtable getSection(String section) {
		return (Hashtable) (sections.get(section.toUpperCase()));
	}

	/**
	 * 키 값이 존재하는지 체크
	 *
	 * @param section  섹션명
	 * @param key      키명
	 * @return         존재 여부
	 */
	public boolean isNullOrEmpty(String section, String key) {
		String value = getKeyValue(section, key);
		return (value == null || value.length() == 0);
	}

	/**
	 * ini 파일에서 String형 값을 가져온다.
	 *
	 * @param section  섹션명
	 * @param key      키 명
	 * @return         값
	 */
	public String getKeyValue(String section, String key) {
		try {
			return (String) getSection(section).get(key.toUpperCase());
		} catch (NullPointerException e) {
			return null;
		}
	}
	
	/**
	 * ini 파일에서 Vector형 값을 가져온다.
	 *
	 * @param section  섹션명
	 * @param key      키 명
	 * @return         값
	 */
	public Vector getKeyVectorValue(String section, String key) {
		try {
			Object obj = getSection(section).get(key.toUpperCase());
			if (obj instanceof String){
				Vector vec = new Vector(1);
				vec.addElement((String)obj);
				return vec;
			} else if (obj instanceof Vector){
				return (Vector)obj;
			} else {
				return null;
			}
		} catch (NullPointerException e) {
			return null;
		}
	}
	/**
	 * ini 파일에서 int형 값을 가져온다.
	 *
	 * @param section  섹션명
	 * @param key      키명
	 * @return         The KeyIntValue value
	 */
	public int getKeyIntValue(String section, String key) {
		return getKeyIntValue(section, key, 0);
	}

	/**
	 * ini 파일에서 int형 값을 가져온다.
	 *
	 * @param section       섹션명
	 * @param key           키명
	 * @param defaultValue  디폴트 값
	 * @return              The KeyIntValue value
	 */
	public int getKeyIntValue(String section, String key, int defaultValue) {
		String value = getKeyValue(section, key.toUpperCase());
		if (value == null) {
			return defaultValue;
		} else {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException e) {
				return 0;
			}
		}
	}

	/**
	 * Key,Value를 얻어온다
	 *
	 * @param aSection	섹션명
	 * @return			key value 값. 이중배열
	 */
	public String[][] getKeysAndValues(String aSection) {
		Hashtable section = getSection(aSection);
		if (section == null) {
			return null;
		}
		String[][] results = new String[section.size()][2];
		int i = 0;
		for (Enumeration f = section.keys(), g = section.elements(); f
				.hasMoreElements(); i++) {
			results[i][0] = (String) f.nextElement();
			results[i][1] = (String) g.nextElement();
		}
		return results;
	}

	/**
	 * ini 파일 로드
	 *
	 * @param filename					파일명
	 * @exception FileNotFoundException
	 */
	public void load(String filename) throws FileNotFoundException {
		load(new FileInputStream(filename));
	}

	/**
	 * ini파일 저장
	 *
	 * @param filename			파일명
	 * @exception IOException	
	 */
	public void save(String filename) throws IOException {
		save(new FileOutputStream(filename));
	}

	/**
	 * ini파일 로드
	 *
	 * @param in  Description of Parameter
	 */
	public void load(InputStream in) {
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(in));
			String read;
			Hashtable section = null;
			String section_name;
			while ((read = input.readLine()) != null) {
				if (read.startsWith(";") || read.startsWith("#")) {
					continue;
				} else if (read.startsWith("[")) {
					// new section
					section_name = read.substring(1, read.indexOf("]"))
							.toUpperCase();
					section = (Hashtable) sections.get(section_name);
					if (section == null) {
						section = new Hashtable();
						sections.put(section_name, section);
					}
				} else if (read.indexOf("=") != -1 && section != null) {
					// new key
					String key = read.substring(0, read.indexOf("=")).trim().toUpperCase();
					String value = read.substring(read.indexOf("=") + 1).trim();
					
					Object obj = section.get(key);
					if ( obj != null) {
						if (obj instanceof String){
							Vector values = new Vector(2);
							values.addElement((String)obj);
							values.addElement(value);
							section.put(key, values);
						} else if (obj instanceof Vector){
							Vector values = (Vector)obj;
							values.addElement(value);
							section.put(key, values);
						} else {
							throw new Exception("unknownerror");
						}
					} else {
						section.put(key, value);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ini 파일 저장
	 *
	 * @param out  저장할 스트림
	 */
	public void save(OutputStream out) {
		try {
			PrintWriter output = new PrintWriter(out);
			String section;
			for (Enumeration e = sections.keys(); e.hasMoreElements();) {
				section = (String) e.nextElement();
				output.println("[" + section + "]");
				for (Enumeration f = getSection(section).keys(), g = getSection(
						section).elements(); f.hasMoreElements();) {
					output.println(f.nextElement() + "=" + g.nextElement());
				}
			}
			output.flush();
			output.close();
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void save(String filename, String[] keys) throws IOException {
		OutputStream out = new FileOutputStream(filename);
		try {
			PrintWriter output = new PrintWriter(out);
			String section;
			for (Enumeration e = sections.keys(); e.hasMoreElements();) {
				section = (String) e.nextElement();
				output.println("[" + section + "]");
				for (int i = 0; i < keys.length; i++) {
					Object obj = getSection(section).get(keys[i]);
					if (obj != null){
						output.println(keys[i] + "=" + obj);
					}
				}
			}
			output.flush();
			output.close();
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 섹션을 추가
	 *
	 * @param section  추가할 섹션명
	 */
	public void addSection(String section) {
		sections.put(section.toUpperCase(), new Hashtable());
	}

	public void removeSection(String section) {
	}

	/**
	 * 테스트 메인
	 *
	 * @param args           The command line arguments
	 * @exception Exception  Description of Exception
	 */
	public static void main(String[] args) throws Exception {
		(new IniFile()).load(new FileInputStream(args[0]));
	}
}
