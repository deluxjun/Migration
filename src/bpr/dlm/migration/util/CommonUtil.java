/*
 * CommonUtil.java
 *
 * Created on January 30, 2003, 11:13 AM
 */

package bpr.dlm.migration.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author  administrator
 */
public final class CommonUtil {
	public static final String LS = System.getProperty("line.separator");
    
    private CommonUtil() {}

    private static java.net.URL loadResource(Object obj, String path) {
    	ClassLoader resourceLoader = obj.getClass().getClassLoader();
        return resourceLoader.getResource(path);
    }
    
    public static boolean createEmptyFile(String filename){
    	try{
		    File file = new File(filename);
		    file.delete();
	    	file.createNewFile();
    	}catch(IOException e){
    		return false;
    	}
    	return true;
    }
    
    public static String getNowTime(String format){
    	SimpleDateFormat dateformat = new SimpleDateFormat(format);
    	return dateformat.format(new Date());
    }
	
    public static String createDateDirectory(String parent){
    	StringBuffer directory = new StringBuffer(parent+File.separator);

    	SimpleDateFormat dateformat = new SimpleDateFormat("MMddhhmmss");
    	directory.append(dateformat.format(new Date()));
		
		boolean success = false;
		int i = 0;
		String dirname = directory.toString();
    	String strDir = dirname;
		for (i = 0; i < 1000; i++) {
			strDir = dirname + i;
		    if ((new File(strDir)).mkdirs())
		    	break;
		}
		if (i >= 1000)
			return null;	// directory create error
		
		return strDir;
    }
    
    public static boolean deleteDir(String dir){
        boolean success = (new File(dir)).delete();
        return success;
    }

    // Deletes all files and subdirectories under dir.
    // Returns true if all deletions were successful.
    // If a deletion fails, the method stops attempting to delete and returns false.
    public static boolean deleteDirAll(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDirAll(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
    
        // The directory is now empty so delete it
        return dir.delete();
    }
    
    public static boolean renameFile(String from, String to){
        // File (or directory) with old name
        File file = new File(from);
        
        // File (or directory) with new name
        File file2 = new File(to);
        
        // Rename file (or directory)
        return file.renameTo(file2);
    }

    
    public static String readFile(URL url) {
        try {
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(url.openStream()));
            StringBuffer xmlText = new StringBuffer();
            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
                xmlText.append( line );
            }
            reader.close();
            return xmlText.toString();
        }
        catch (Exception exp) {
            exp.printStackTrace();
            return "";
        }
    }

    /**
     * 주어진 길이 n 안에서 string s를 좌로 또는 우로 위치시킨다.
     * @param s
     * @param n
     * @param c
     * @param paddingLeft
     * @return
     */
    public synchronized static String paddingString( String s, int n, char c , boolean paddingLeft  ) {
		StringBuffer str = new StringBuffer(s);
		int strLength  = str.length();
		if ( n > 0 && n > strLength ) {
	      for ( int i = 0; i <= n ; i ++ ) {
	            if ( paddingLeft ) {
	              if ( i < n - strLength ) str.insert( 0, c );
	            }
	            else {
	              if ( i > strLength ) str.append( c );
	            }
	      }
		}
		return str.toString();
	}

    public static String readFile(String filename) {
        try {
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(new FileInputStream(filename)));
            StringBuffer xmlText = new StringBuffer();
            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
                xmlText.append( line );
            }
            reader.close();
            return xmlText.toString();
        }
        catch (Exception exp) {
            exp.printStackTrace();
            return "";
        }
    }
    
    public static Set readFileToSet(String filename){
    	Set set = new HashSet();
    	set.clear();
    	
        try {
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(new FileInputStream(filename)));
            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
            	set.add(line);
            }
            reader.close();
            return set;
        }
        catch (Exception exp) {
            return null;
        }    	
    }

    public static Vector readFileToVector(String filename){
    	Vector vec = new Vector();
        try {
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(new FileInputStream(filename)));
            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
            	vec.addElement(line);
            }
            reader.close();
            return vec;
        }
        catch (Exception exp) {
            return null;
        }
    }
    public static Vector readFileToVector(File file){
    	Vector vec = new Vector();
        try {
            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(new FileInputStream(file)));
            String line = null;
            while ( ( line = reader.readLine() ) != null ) {
            	vec.addElement(line);
            }
            reader.close();
            return vec;
        }
        catch (Exception exp) {
            return null;
        }
    }
    
	public static String parseTextWithParam(String sql, String[] base_values){
	    String patternStr = "\\$[0-9]+";
	    StringBuffer buf = new StringBuffer();
	    
	    // Compile regular expression
	    Pattern pattern = Pattern.compile(patternStr);
	    Matcher matcher = pattern.matcher(sql);
	    
	    boolean found = false;
	    while ((found = matcher.find())) {
	        // Get the match result
	        String replaceStr = matcher.group();
	    
	        // Convert to uppercase
	        try{
	        	int index = Integer.parseInt(replaceStr.substring(1));
	        	if (base_values[index] == null){
	        		base_values[index] = "NULL";
	        	}
	        	matcher.appendReplacement(buf, base_values[index]);
	        }catch(Exception e){
	        	e.printStackTrace();
	        	return null;
	        }
	    }
	    matcher.appendTail(buf);
	    
	    return buf.toString();
	}
	
	public static Object[] listtoArray(List list){
		Iterator i = list.iterator();
		Object[] ret = new Object[list.size()];
		int count = 0;
		while(i.hasNext()){
			ret[count++] = i.next();
		}
		return ret;
	}
}
