/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import bpr.dlm.migration.db.DBConnectionPool;
import bpr.dlm.migration.image.ImageDownloader;
import bpr.dlm.migration.image.ImageUploader;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class MigrationHandler extends DefaultHandler {

	private String value;

	private Migration migration;

	private Rule currentStep;
	
	private List currentSamFiles;
	private Map currentCodeSet;
	private Rule currentRule;
	private TargetFieldSet currentFieldSet;
	
	private Logger log;

	MigrationHandler(Migration migration, Logger log) {
		this.migration = migration;
		this.log = log;
	}

	public void startElement(String namespaceURI, String localName, String qName, Attributes attr) throws SAXException {
		if (qName.equals("common")) {
			migration.getProperties().put("delimiter", attr.getValue("delimiter"));
			migration.getProperties().put("elapsed-time", attr.getValue("elapsed-time"));
			migration.getProperties().put("commit-count", attr.getValue("commit-count"));
			migration.getProperties().put("error-count", attr.getValue("error-count"));
			migration.getProperties().put("status-file", attr.getValue("status-file"));
		}
		else if (qName.equals("database")) {
			try {
				DBConnectionPool pool = new DBConnectionPool(attr.getValue("url"), attr.getValue("userid"), attr.getValue("password"), attr.getValue("driver"), 5, 5, log);
				migration.getDBList().put(attr.getValue("name"), pool);
				migration.getConnectionList().put(attr.getValue("name"), pool.getConnection());
			} catch (Exception e) {
				log.error("DB 초기화 에러",e);
				throw (SAXException) e;
			}
			log.debug("database:" + attr.getValue("name"));
		}
		else if (qName.equals("ftp")) {
			try {
				migration.setImageDownloader(new ImageDownloader(attr.getValue("host"), attr.getValue("user"), attr.getValue("password"), attr.getValue("path"), log));
			} catch (Exception e) {
				log.error("Image Downloader error",e);
				throw (SAXException) e;
			}
			log.debug("ftp:" + attr.getValue("name"));
		}
		else if (qName.equals("xtorm")) {
			try {
				int port = Integer.parseInt(attr.getValue("port"));
				migration.setImageUploader(new ImageUploader(
						attr.getValue("host"), port, attr.getValue("gateway"), attr.getValue("indexid"), attr.getValue("sclassid"),
						attr.getValue("cclassid"), attr.getValue("user"), attr.getValue("password"), attr.getValue("fields"), log));
			} catch (Exception e) {
				log.error("Image Uploader initialize error",e);
				throw (SAXException) e;
			}
			log.debug("xtorm:" + attr.getValue("name"));
		}
		// sam 파일 타이틀
		else if (qName.equals("source-files")) {
			currentSamFiles = new ArrayList();
			try {
				migration.getSamList().put(attr.getValue("name"), new SamReader(migration.getProperties(), currentSamFiles, log));
			} catch (Exception e) {
				log.error("SAM파일 초기화 에러",e);
				throw (SAXException)e;
			}
			log.debug("source-files:" + attr.getValue("name"));
		}
		// sam 파일 리스트
		else if (qName.equals("source-file")) {
			if (currentSamFiles != null){
				currentSamFiles.add(attr.getValue("url"));
			}
			log.debug("source-file:" + attr.getValue("url"));
		}
		else if (qName.equals("function")) {
			String sql = attr.getValue("sql");
			if (sql != null){
				String type = attr.getValue("type");
				if (type != null && type.equalsIgnoreCase("update")){
					migration.getFunctionList().put(attr.getValue("name"), new Function(Function.UPDATE_SQL, attr.getValue("db"), sql, attr.getValue("ignore-quotation"), migration, log));
				}else {
					migration.getFunctionList().put(attr.getValue("name"), new Function(Function.NORMAL_SQL, attr.getValue("db"), sql, attr.getValue("ignore-quotation"), migration, log));
				}
			}
			else
				migration.getFunctionList().put(attr.getValue("name"), new Function(Function.INTERNAL, attr.getValue("db"), attr.getValue("internal"), attr.getValue("ignore-quotation"), migration, log));

			log.debug("function:" + attr.getValue("name"));
		}
		// 코드 매핑값
		else if (qName.equals("codes")) {
			currentCodeSet = new HashMap();
			migration.getCodeList().put(attr.getValue("name"), currentCodeSet);
			log.debug("codes:" + attr.getValue("name"));
		}
		else if (qName.equals("code")) {
			if (currentCodeSet != null){
				currentCodeSet.put(attr.getValue("from"), attr.getValue("to"));
			}
//			log.debug("code:" + attr.getValue("from"));
		}
		// 룰
		else if (qName.equals("rule")) {
		    currentRule = new Rule(attr.getValue("name"), attr.getValue("source"), attr.getValue("report-file"), log);
		    migration.addRule(currentRule);
		}
		else if (qName.equals("source-field")) {
			try{
				currentRule.addSourceField(attr.getValue("name"), attr.getValue("group"), attr.getValue("position"));
//				log.debug("source-field:" + attr.getValue("name"));
			} catch(Exception e){
				log.error("SourceField allocation error!",e);
				throw (SAXException) e;
			}
		}
		else if (qName.equals("variable")) {
			try{
				currentRule.addVariable(attr.getValue("name"), Variable.STRING, attr.getValue("value"));
			} catch(Exception e){
				log.error("variable initialize error",e);
				throw (SAXException) e;
			}
		}
		else if (qName.equals("variables")) {
			try{
				currentRule.addVariables(attr.getValue("db"), attr.getValue("name"), attr.getValue("value"), attr.getValue("groupid"));
			} catch(Exception e){
				log.error("variables  initialize error",e);
				throw (SAXException) e;
			}
		}
		else if (qName.equals("target-fields")) {
			try{
				currentFieldSet = new TargetFieldSet(attr.getValue("db"), attr.getValue("table"), attr.getValue("groupid"));
				log.debug("target-fields:" + attr.getValue("table"));
				currentRule.addTargetFieldSet(currentFieldSet);
			} catch(Exception e){
				log.error("target-fields initialize error",e);
				throw (SAXException) e;
			}
		}
		else if (qName.equals("target-field")) {
			currentFieldSet.addField(attr.getValue("name"), attr.getValue("type"), attr.getValue("value"));
			log.debug("target-field:" + attr.getValue("name"));
		}
//		<image-down ftp="IPSFTP" opcode="=M.gaejung_code" imgid="M.img_id" files="!filepath" path="./download"/>
//		<image-up xtorm="BPRIMG1P" files="!filepath" path="./download"/>
		else if (qName.equals("action")) {
			Vector vec = new Vector();
			try{
				for (int i = 0; i < 10; i++) {
					String value = attr.getValue("param"+(i+1));
					if (value == null)
						break;
					vec.add(value);
				}
			}catch(Exception e){
			}
			currentRule.addAction(new Action(migration, attr.getValue("type"), vec, log));
			log.debug("action:" + attr.getValue("type"));
		}
	}


	public void endElement(String namespaceURI, String localName, String rawName) throws SAXException {
		value = null;
	}

	public void characters(char[] ch, int start, int end) throws SAXException {
		value = new String(ch, start, end);
		value = value.trim();
	}
}
