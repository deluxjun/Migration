/*
 * Created on 2006. 3. 17.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.image;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.log4j.Logger;

import bpr.dlm.migration.util.CommonFileWriter;
import bpr.dlm.migration.util.CommonUtil;

import com.enterprisedt.net.ftp.FTPClient;
import com.enterprisedt.net.ftp.FTPTransferType;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class ImageDownloader {
	private String m_host;
	private String m_user;
	private String m_password;
	private String m_path;
	
	private Logger log;

	public ImageDownloader(String host, String user, String password, String path, Logger log){
		m_host = host;
		m_user = user;
		m_password = password;
		m_path = path;
		
		this.log = log;
	}
	
	public boolean getImageFTP(String s_info, Object[] filepath, String downfolder) throws Exception{
		if (!downfolder.endsWith("/"))
			downfolder += "/";
		File downpath = new File(downfolder);
		if (!downpath.exists()){
			downpath.mkdirs();
		}
		FTPClient ftp = null;
		boolean bOk = false;

        try {
            ftp = new FTPClient();
            ftp.setRemoteHost(m_host);
            
            // connect
            ftp.connect();
            
             // login
            ftp.login(m_user, m_password);

            // set up passive ASCII transfers
//            ftp.setConnectMode(FTPConnectMode.PASV);
            ftp.setType(FTPTransferType.BINARY);
            
            ftp.chdir(m_path);
            
    		NumberFormat formatter = new DecimalFormat("00000000");
			//작업일자_블럭번호_블럭내일련번호
			String[] arrFilePath = s_info.split("_");
			int i_Temp = Integer.parseInt(arrFilePath[2]);
			String s_OcrFilePath = arrFilePath[0] + "/" + arrFilePath[1] + "/" + "JIB" + formatter.format(i_Temp) + ".TIF";
			
            log.debug("receive file..");
            
            // OCR 은 이미지가 무조건 하나겠지만, 공통화를 위해..
			for (int j = 0; j < filepath.length; j++) {
                ftp.get(downfolder+filepath[j]+".tif", s_OcrFilePath);
    			bOk = true;
                log.debug("downloaded images from ips:"+downfolder+filepath[j]+".tif");
			}
				
            if(!bOk){
            	throw new Exception("cannot get images from IPS image server:"+s_info);
            }

        	
/*            // set up client    
            ftp = new FTPClient();
//            FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);
            
            ftp.connect(m_host);
            ftp.setSoTimeout(6000);  
            if( !ftp.login(m_user, m_password) ){
            	throw new Exception("ftp login error");
            }

            // set up passive ASCII transfers
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
//            ftp.enterLocalPassiveMode();
//            ftp.enterRemotePassiveMode();

            if(!ftp.changeWorkingDirectory(m_path))
            	throw new Exception("ftp change folder error");

            
    		NumberFormat formatter = new DecimalFormat("00000000");
			//작업일자_블럭번호_블럭내일련번호
			String[] arrFilePath = s_info.split("_");
			int i_Temp = Integer.parseInt(arrFilePath[2]);
			String s_OcrFilePath = arrFilePath[0] + "/" + arrFilePath[1] + "/" + "JIB" + formatter.format(i_Temp) + ".TIF";
			
            log.debug("receive file..");
            
            // OCR 은 이미지가 무조건 하나겠지만, 공통화를 위해..
			for (int j = 0; j < filepath.length; j++) {
                OutputStream output;
                output = new FileOutputStream(downfolder+filepath[j]+".tif");
                if (!ftp.retrieveFile(s_OcrFilePath, output)){
                	throw new Exception("ftp retrieve error:"+s_OcrFilePath);
                }
                output.close();
    			bOk = true;
                log.debug("downloaded images from ips:"+downfolder+filepath[j]+".tif");
			}
				
            if(!bOk){
            	throw new Exception("cannot get images from IPS image server:"+s_info);
            }*/
            
        } catch (Exception e) {
            throw new Exception("image download error:"+e.getMessage());
        } finally{
            if (ftp != null && ftp.connected()){
                try {
                    ftp.quit();
                } catch (Exception f){
                }
            }
            
/*            if (ftp != null && ftp.isConnected()){
                try {
                	ftp.logout();
                    ftp.disconnect();
                } catch (IOException f){
                }
            }*/
        	

        }
        
        return true;
	}
	
	public boolean getImage(String gaejung_code, String imgid, Object[] filepath, String downfolder) throws Exception{
		
        log.debug("ips getImage called:" + imgid);

		if (!downfolder.endsWith("/"))
			downfolder += "/";
		File downpath = new File(downfolder);
		if (!downpath.exists()){
			downpath.mkdirs();
		}
		String unique_path = imgid+CommonUtil.getNowTime("yyMMddHHmmss");
		String req_file = makeRequestIndexFile(downfolder+unique_path, gaejung_code, imgid, filepath);
		com.enterprisedt.net.ftp.FTPClient ftp = null;
		boolean bOk = false;

        try {
        	
            ftp = new com.enterprisedt.net.ftp.FTPClient();
            ftp.setRemoteHost(m_host);
            
            // connect
            ftp.connect();
            
             // login
            ftp.login(m_user, m_password);

            // set up passive ASCII transfers
//            ftp.setConnectMode(FTPConnectMode.PASV);
            ftp.setType(FTPTransferType.BINARY);
            
            ftp.chdir(m_path);

            // copy file to server 
            ftp.mkdir(unique_path);
            
            ftp.chdir(unique_path);
            
            ftp.put(req_file, "index.dat");
            
            // delete index file
            try {
				File file = new File(req_file);
				file.delete();
			} catch (Exception e) {
				log.error("delete error", e);
			}
            
            ftp.put("status.ready", "status.flg");
 
            for (int i = 0; i < 10; i++) {
				Thread.sleep(500);
				
				// get images
				try {
					String[] files = ftp.dir("status.done");
					if (files == null || files.length < 1){
						continue;
					}
				} catch (Exception e) {
					continue;
				}
				
	            log.debug("receive file..");
				for (int j = 0; j < filepath.length; j++) {
	                ftp.get(downfolder+filepath[j]+".tif", filepath[j]+".tif");
	                log.debug("downloaded images from ips:" + unique_path + "->" + filepath[j]+".tif");
				}
				bOk = true;
				break;
			}
            
            if(!bOk){
            	throw new Exception("cannot get images from IPS image server:"+req_file);
            }

        	
        	
        	
/*            // set up client    
            ftp = new FTPClient();
//            FTPClientConfig conf = new FTPClientConfig(FTPClientConfig.SYST_NT);
            
            ftp.connect(m_host);
            ftp.setSoTimeout(6000);  
            if( !ftp.login(m_user, m_password) ){
            	throw new Exception("ftp login error");
            }

            // set up passive ASCII transfers
//            ftp.setConnectMode(FTPConnectMode.PASV);
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
//            ftp.enterLocalPassiveMode();
//            ftp.enterRemotePassiveMode();

            if(!ftp.changeWorkingDirectory(m_path))
            	throw new Exception("ftp change folder error");

            // copy file to server 
            if (!ftp.makeDirectory(unique_path)){
            	throw new Exception("ftp create folder error: "+unique_path);
            }
            if(!ftp.changeWorkingDirectory(unique_path)){
            	throw new Exception("ftp change folder error");
            }
            
            InputStream input = new FileInputStream(req_file);
            if (!ftp.storeFile("index.dat", input)){
            	throw new Exception("ftp store index.dat error");
            }
            input.close();
            input = null;

            // delete index file
            try {
				File file = new File(req_file);
				file.delete();
			} catch (Exception e) {
				log.error("delete error", e);
			}
            
            input = new FileInputStream("status.ready");
            if (!ftp.storeFile("status.flg", input)){
            	throw new Exception("ftp store status.flg error");
            }
 
//            if (!ftp.rename("status.ready", "status.flg")){
//            	throw new Exception("ftp change status file error");
//            }

            for (int i = 0; i < 10; i++) {
				Thread.sleep(500);
				
				// get images
				String[] files = ftp.listNames("status.done");
				if (files == null || files.length < 1){
					continue;
				}
				
	            log.debug("receive file..");
				for (int j = 0; j < filepath.length; j++) {
	                OutputStream output;
	                output = new FileOutputStream(downfolder+filepath[j]+".tif");
	                if (!ftp.retrieveFile(filepath[j]+".tif", output)){
	                	throw new Exception("retrieve error: "+filepath[j]+".tif");
	                }
	                output.close();
	                log.debug("downloaded images from ips:" + unique_path + "->" + filepath[j]+".tif");
				}
				bOk = true;
				break;
			}
            
            if(!bOk){
            	throw new Exception("cannot get images from IPS image server:"+req_file);
            }*/
            
//            if (!ftp.rename("status.done", "status.end")){
//                ftp.logout();
//            }
            
        } catch (Exception e) {
            throw new Exception("IPS download error:"+e.getMessage());
        } finally{
            if (ftp != null && ftp.connected()){
                try {
                	ftp.quit();
                } catch (Exception f){
                }
            }

        }
        
        return true;
	}
	
	private String makeRequestIndexFile(String filename, String gaejung_code, String imgid, Object[] filepath) throws Exception{
		CommonFileWriter writer = new CommonFileWriter(filename, false);
		
		if (filepath.length < 1){
			throw new Exception("No image file to import exists");
		}
		
		String files = (String)filepath[0]+ ".tif";
		for (int i = 1; i < filepath.length; i++) {
			files += "," + filepath[i] + ".tif";
		}
		writer.writeln(gaejung_code + "^" + imgid + "^" + "C" + "^" + filepath.length + "^" + files);
		writer.flush();
		writer.close();
		
		return filename;
	}


}
