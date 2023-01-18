/*
 * Created on 2006. 3. 20.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.newwork;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import bpr.dlm.migration.util.CommonUtil;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class PINewImageAgent {
	private Logger log;
	private String m_shfile;
	private int m_idletime;
	private static boolean m_flag;
	
	public static final String AGENT_END = "agent.end";
	public static final String AGENT_RUNNING = "agent.running";
	
	
	private int m_schedule_type = 0;
	private String m_static_time = "";
	private int m_repeat_time = 5; // minute
	private int m_idle_count = 2*60; // minute

	PINewImageAgent(String idletime, String shfile) throws Exception{
    	PropertyConfigurator.configure("log_agent.properties");
        log = Logger.getLogger("main");		// 로그 기록
        
        m_shfile = shfile;

		setIdleTime(idletime);
        m_flag = false;
	}
	
	private void setIdleTime(String idletime) throws Exception{
		if (idletime.startsWith("day")){
			m_schedule_type = 0;
			int pos = idletime.indexOf("=");
			m_static_time = idletime.substring(pos+1);
		} else {
			m_schedule_type = 1;
			m_repeat_time = Integer.parseInt(idletime);
			
			if (m_repeat_time < 3)	// 2분 이하로는 안되게한다..
				m_repeat_time = 3;
			
			m_repeat_time *= 60;
		}
	}
	
	private boolean checkExecuteTime(long count){
		if (m_schedule_type == 0){	// 매일 반복
			String now = CommonUtil.getNowTime("HHmm");
			if (m_static_time.equals(now)){
				if (m_idle_count > count)
					return false;
				return true;
			}
		}else if(m_schedule_type == 1){ // 분당 반복
			if ( m_repeat_time < count ){
				return true;
			}
		}
		
		return false;
	}
	
	public boolean checkTerminate(){
		File term = new File(AGENT_END);
		if (term.exists())
			return true;
		
		return false;
	}
	
	public static boolean setStatus(String status){
		File file = new File(AGENT_END);
		if (file.exists())
			file.delete();
		file = new File(AGENT_RUNNING);
		if (file.exists())
			file.delete();

		return CommonUtil.createEmptyFile(status);
	}
	
	public boolean run(){
    	BufferedReader bufferedReader;
    	BufferedReader bufferedErrorReader;
    	String readed;

    	log.info("<<<<<<<<<<<<<<<<< Started IPS NewImage Agent >>>>>>>>>>>>>>>>>");

TERM:  	while(true){
		    try {
		    	String nowtime = CommonUtil.getNowTime("yy/MM/dd HH:mm:ss");
		    	
		    	log.info("[ " + nowtime + " : started ]");
		    	// Execute a command without arguments
		        String command = "bash " + m_shfile;
		        Process child = Runtime.getRuntime().exec(command);

		        // Get the input stream and read from it
//		        bufferedErrorReader = new BufferedReader(new InputStreamReader(child.getErrorStream()));
//		    	while((readed = bufferedErrorReader.readLine()) != null){
//		    		log.error(readed);
//		    	}
		    	
		    	bufferedReader = new BufferedReader(new InputStreamReader(child.getInputStream()));
		    	while((readed = bufferedReader.readLine()) != null){
		    		log.info(readed);
		    	}

		    	bufferedReader.close();
		    	log.info("[ " + nowtime + " : ended ]");
		    	child.destroy();
		    	child = null;
		    	
		    	long count = 0;
		    	while(!checkExecuteTime(count++)){
			    	Thread.sleep(1000);
			    	if (checkTerminate())
			    		break TERM;
		    	}
		    } catch (IOException e) {
		    } catch (Exception e){
		    	
		    }
	    }

    	log.info("<<<<<<<<<<<<<<<<< Terminated IPS NewImage Agent >>>>>>>>>>>>>>>>>");
    	
    	return true;
	}
	
    /**
     * 실행 옵션을 프린트
     */
    private static void usageInfo() {
        System.out.println("Usage:");
        System.out.println("java bpr.dlm.migration.newwork.IPSNewImageAgent [idle time] [sh file]");
    }
    
	public static void main(String[] args) {
		if (args.length < 2){
			usageInfo();
			System.exit(1);
		}

		try {
			
			PINewImageAgent agent = new PINewImageAgent(args[0],args[1]);
			PINewImageAgent.setStatus(AGENT_RUNNING);

			if (agent.run()){
			}
			
			PINewImageAgent.setStatus(AGENT_END);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.exit(0);
	}
}
