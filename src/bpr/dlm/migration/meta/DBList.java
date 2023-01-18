/*
 * Created on 2006. 1. 16.
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package bpr.dlm.migration.meta;

import java.util.HashMap;
import java.util.Map;

import bpr.dlm.migration.db.DBConnectionPool;

/**
 * @author deluxjun
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class DBList {
	private Map DBs;

	private static DBList instance = null;

	private DBList() {
		DBs = new HashMap();
	}

	public void addDBPool(String name, DBConnectionPool dbpool) {
		DBs.put(name, dbpool);
	}

	public static DBList getInstance() {
		if (instance == null) {
			instance = new DBList();
		}
		return instance;
	}

	public DBConnectionPool getDBPool(String name) throws Exception {
		Object o = DBs.get(name);
		if (o == null) {
			throw new Exception("정의된 DB가 없습니다: " + name);
		}
		return (DBConnectionPool) o;
	}
}
