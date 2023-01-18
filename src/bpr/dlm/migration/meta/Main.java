/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.XMLReader;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class Main {
	private Logger log;
	
	Main(String logfilename){
    	PropertyConfigurator.configure(logfilename);
        log = Logger.getLogger("main");		// 로그 기록		
	}

	public Logger getLogger(){
		return log;
	}	
	
	public boolean run(String file) {
		Migration m;
		
		try {
			SAXParserFactory spf = SAXParserFactory.newInstance();
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();

			m = new Migration(log);

			xmlReader.setContentHandler(new MigrationHandler(m, log));
			log.info("Parsing file: " + file);
			xmlReader.parse(file);
			
			m.init();
		} catch (Exception e) {
			log.error("초기화 오류",e);
			return false;
		}

		log.info("Migration started:" + file);
		try{
			if (m != null){
				m.run();
				
				if (m.mbErrorSam){
					System.out.println("ErrorSam");
				}
				log.info("Migration Successfully ended: " + file);
			}
		}catch(Exception e){
			log.error("Internal error",e);
			return false;
		}
		
		return true;
	}

    /**
     * 실행 옵션을 프린트
     */
    private static void usageInfo() {
        System.out.println("Usage:");
        System.out.println("java bpr.dlm.migration.dumpsam.Main [config file] [logfile name]");
    }
    public static void main(String[] args) {
		if (args.length < 2){
			usageInfo();
			System.exit(1);
		}

		Main prg = new Main(args[1]);

		prg.getLogger().info("[Started : " + args[0] + "]");
		if (prg.run(args[0]))
			System.out.println("Success");
		else
			System.out.println("Fail");
		prg.getLogger().info("[Terminated: " + args[0] + "]");
		
		System.exit(0);
	}
}
