/*
 * IPS 이미지 파일 이관 프로그램
 */
package bpr.dlm.migration.imagemove;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;



public class AgentStartup {
	private AgentControl controller;
	
	private static final int COMMAND_NOTHING = 0;
	private static final int COMMAND_STATUS = 1;
	private static final int COMMAND_TERMINATE = 2;
	
	public String m_downpath;
	
	public Logger log;
	
	public AgentStartup(String ini) throws Exception{
    	PropertyConfigurator.configure("log_agent.properties");
        log = Logger.getLogger("main");		// 로그 기록

        init(ini);
	}
	
	private void init(String ini) throws Exception{
		log.info("[IPSImageMoveAgent Started]");
		controller = new AgentControl();
		controller.init(this, ini, log);
		
		controller.printThreadInfo();
	}
	
	private boolean run() throws Exception{
		
		int count = 0;
		
		while(true){
			count ++;
			Thread.sleep(2000);
			// check command
			int checkpoint = checkCommand();
			if (checkpoint == COMMAND_STATUS){
				log.info("command : status!");
				controller.printThreadInfo();
			} else if (checkpoint == COMMAND_TERMINATE){
				log.info("command : terminate!");
				controller.terminate();
				break;
			}

			// 삭제
			try {
				if (count > 1){ // 10분에 한번
			        String command = "del " + m_downpath+"*.tif_OK";
			        Process child = Runtime.getRuntime().exec(command);
				}
			} catch (Exception e) {}
			
			if (!controller.checkAliveThreads()){
				break;
			}
		}
		log.info("[IPSImageMoveAgent Terminated]");
		
		return true;
	}
	
	private int checkCommand(){
		File command = null;
		try {
			command = new File("command.status");
			if (command.exists())
				return COMMAND_STATUS;

			command = new File("command.end");
			if (command.exists())
				return COMMAND_TERMINATE;
		} catch (Exception e) {
		} finally{
			if (command != null){
				command.delete();
			}
		}
		
		return COMMAND_NOTHING;
	}
	
    private static void usageInfo() {
        System.out.println("Usage:");
        System.out.println("APP [ini file]");
    }
    
	public static void main(String args[]) {
		
		if (args.length < 1){
			usageInfo();
			System.exit(1);
		}
		
		AgentStartup agent = null;
		try {
			
			agent = new AgentStartup(args[0]);

			if (agent.run()){
			}
		} catch (Exception e) {
			agent.log.error("agent error:",e);
		}

		System.exit(0);
	}
}
