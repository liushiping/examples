package com.lsp.hbase.demo;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.lsp.hbase.demo.annotation.Column;
import com.lsp.hbase.demo.annotation.HTable;
import com.lsp.hbase.demo.annotation.Rowkey;
import com.lsp.hbase.demo.exceptions.HbaseException;
import com.lsp.hbase.demo.fields.FieldCache;

/**
 * @description: hbase工具类
 * @version 1.0
 * @Date 2018-11-5 下午2:46:27
 */
public class HbaseHelper {

	private final static Logger logger = Logger.getLogger(HbaseHelper.class);

	private static Configuration conf;

	private static int treadPoolSize = 200;

	private static ExecutorService executorPool = Executors.newFixedThreadPool(treadPoolSize);

	private static Connection connection = null;

	private static Admin admin = null;

	private static Object obj = new Object();
	static {
		try {
			conf = HBaseConfiguration.create();
			// 2014-08-15 修改为从hbase-size.xml里获取配置信息
			// conf.set("hbase.zookeeper.quorum",PlatFormConstants.getHbaseZooQuoRum());
			// conf.set("hbase.rootdir", PlatFormConstants.getHbaseRootDir());
			// getHBaseConnection();
			// pool = new HTablePool(conf, 100, new
			// HTableFactory(),PoolType.RoundRobin);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * 取得HBase连接
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Connection getHBaseConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				return connection;
			}
			synchronized (obj) {
				connection = ConnectionFactory.createConnection(conf, executorPool);
				admin = connection.getAdmin();
			}
		} catch (Exception e) {
			throw new HbaseException(e.getMessage());
		}
		return connection;
	}

	/**
	 * 取得表名
	 * 
	 * @param quoteRelation
	 * @return
	 */
	public static String getTableName(Class<?> cls) {
		return getTableAnnotation(cls).name();
	}

	public static Object getRowKey(Object obj) {
		if (obj == null) {
			return null;
		}
		Field[] fields = obj.getClass().getDeclaredFields();
		if (fields != null) {
			for (Field field : fields) {
				Rowkey rowkey = field.getAnnotation(Rowkey.class);
				if (rowkey != null) {
					field.setAccessible(true);
					try {
						return field.get(obj);
					} catch (Exception e) {
					}
				}
			}
		}
		return null;
	}

	/**
	 * 获取model中的表名
	 *
	 * @param cls
	 * @return
	 */
	public static HTable getTableAnnotation(Class<?> cls) {
		HTable htable = cls.getAnnotation(HTable.class);
		if (htable == null) {
			throw new HbaseException(
					cls.getName() + "does not contain cn.sxw.hadoop.core.hbase.annotation.HTable annotation!");
		}
		return htable;
	}

	/**
	 * 关闭连接
	 * 
	 */
	public static void closeConnection() {
		if (connection != null && !connection.isClosed()) {
			try {
				connection.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * 创建表
	 *
	 * @param tableName
	 */
	public static void createTable(String tableName) {
		logger.debug("start create table ......");
		try {
			if (StringUtils.isEmpty(tableName)) {
				return;
			}
			TableName tableNameObj = TableName.valueOf(tableName);
			if (admin.tableExists(tableNameObj)) {
				throw new HbaseException("this table " + tableName + "already exist!");
			}
			TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
			ColumnFamilyDescriptorBuilder cfdBuider = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data"));// 默认就一个列族
			tdBuilder.setColumnFamily(cfdBuider.build());
			admin.createTable(tdBuilder.build());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.debug("end create table ......");
	}

	/**
	 * 检查表是否存在
	 *
	 * @param tableName
	 * @return
	 */
	public static boolean existsTable(String tableName) {
		TableName tableNameObj = TableName.valueOf(tableName);
		try {
			return admin.tableExists(tableNameObj);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * 根据注释检查表是否存在
	 *
	 * @param modelClass
	 * @return
	 */
	public static boolean existsTable(Class<?> modelClass) {
		if (modelClass == null) {
			throw new HbaseException("modelClass can't null!");
		}
		HTable htable = modelClass.getAnnotation(HTable.class);
		if (htable == null) {
			throw new HbaseException(
					modelClass.getName() + "can't contain cn.sxw.hadoop.core.hbase.annotation.HTable annotation!");
		}
		return existsTable(htable.name());
	}

	/**
	 * @description 根据类开创建表
	 * @param cls         类
	 * @param isExistSkip 如果表存存是否跳过创建
	 */
	public static void createTable(Class<?> cls, boolean isExistSkip) {
		logger.debug("start create table ......");
		try {
			HTableInfo info = FieldCache.getInstance.getTableNameByClass(cls);
			if (existsTable(info.getTableName())) {
				if (!isExistSkip) {
					logger.info("create table :" + cls.getName());
					throw new HbaseException("The table " + info.getTableName() + "already exist!");
				} else {
					return;
				}
			}
			TableName tableNameObj = TableName.valueOf(info.getTableName());
			TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
			String families[] = info.getFamilies();
			for (String name : families) {
				ColumnFamilyDescriptorBuilder cfdBuider = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(name));
				tdBuilder.setColumnFamily(cfdBuider.build());
			}
			logger.info("create table :" + cls.getName());
			admin.createTable(tdBuilder.build());
			admin.flush(tableNameObj);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.debug("end create table ......");
	}

	/**
	 * @description 插入数据
	 * @param tableName 表名
	 * @param lm        插入对象
	 */
	public static void insertData(String tableName, Object lm) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		try {
			table = getHBaseConnection().getTable(tableNameObj);
			if (StringUtils.isEmpty(tableName) || lm == null) {
				return;
			}
//			table.setWriteBufferSize(1024 * 1024 * 1);
			Object[] rowkeys = FieldCache.getInstance.getRowkeyNameAndValue(lm);
			if (StringUtils.isEmpty(tableName) || rowkeys == null || rowkeys.length != 2) {
				return;
			}
			Collection<Field> lists = FieldCache.getInstance.get(lm.getClass());
			Put put = new Put(rowkeys[1].toString().getBytes());
			for (Field field : lists) {
				Column c = field.getAnnotation(Column.class);
				if (c != null) {
					String family = c.family();
					String fieldName = field.getName();
					Object obj = field.get(lm);
					if (obj != null) {
						put.addColumn(family.getBytes(), fieldName.getBytes(), String.valueOf(obj).getBytes());
					}
				}
			}
			table.put(put);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
	}

	/**
	 * @description 批量插入
	 * @param tableName 表名
	 * @param lists     <rowkey,数据对象>
	 */
	public static void insertData(String tableName, List<?> lists) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		try {
			if (StringUtils.isEmpty(tableName) || lists == null || lists.isEmpty()) {
				return;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			List<Put> listPuts = new ArrayList<Put>();
			for (Object obj : lists) {
				Object[] rowkeys = FieldCache.getInstance.getRowkeyNameAndValue(obj);
				if (rowkeys == null || rowkeys.length != 2) {
					continue;
				}
				Collection<Field> listFields = FieldCache.getInstance.get(obj.getClass());
				Put put = new Put(rowkeys[1].toString().getBytes());
				for (Field field : listFields) {
					Column c = field.getAnnotation(Column.class);
					if (c != null) {
						String family = c.family();
						String fieldName = field.getName();
						Object res = field.get(obj);
						if (res != null) {
							put.addColumn(family.getBytes(), fieldName.getBytes(), String.valueOf(res).getBytes());
						}
					}
				}
				listPuts.add(put);
			}
			table.put(listPuts);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
	}

	/**
	 * @description 批量执行CURD
	 * @param tableName
	 * @param lists
	 * @return
	 */
	public static Object[] batchExecute(String tableName, List<Row> lists) {
		if (StringUtils.isEmpty(tableName) || lists == null || lists.size() == 0) {
			return null;
		}
		Object[] results = new Object[lists.size()];
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		try {
			table = getHBaseConnection().getTable(tableNameObj);
			table.batch(lists, results);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
		return results;
	}

	/**
	 * @description 下线表
	 * @param tableName
	 * @throws IOException
	 */
	public void disableTable(String tableName) throws IOException {
		TableName tableNameObj = TableName.valueOf(tableName);
		admin.disableTable(tableNameObj);
	}

	/**
	 * @description 删除表
	 * @param tableName
	 * @throws IOException
	 */
	public void dropTable(String tableName) throws IOException {
		TableName tableNameObj = TableName.valueOf(tableName);
		if (existsTable(tableName)) {
			disableTable(tableName);
			admin.deleteTable(tableNameObj);
		}
	}

	/**
	 * @description 根据rowkey删除
	 * @param tableName
	 * @param rowKey
	 */
	public static void deleteRow(String tableName, String rowKey) {
		deleteRow(tableName, new String[] { rowKey });
	}

	/**
	 * @description 根据rowkeys 删除数据
	 * @param tableName 表名
	 * @param rowKeys
	 */
	public static void deleteRow(String tableName, String[] rowKeys) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		try {
			if (StringUtils.isEmpty(tableName) || rowKeys == null || rowKeys.length == 0) {
				return;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			List<Delete> list = new ArrayList<Delete>();
			for (String rowKey : rowKeys) {
				Delete d1 = new Delete(rowKey.getBytes());
				list.add(d1);
			}
			table.delete(list);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
	}

	/**
	 * @description 删除列数据
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 * @param timestamp
	 */
	public static void deleteColumnData(String tableName, String rowKey, String family, String column, Long timestamp) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		try {
			if (StringUtils.isEmpty(family) || StringUtils.isEmpty(tableName) || StringUtils.isEmpty(rowKey)) {
				return;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Delete del = new Delete(Bytes.toBytes(rowKey));
			if (StringUtils.isNotEmpty(column) && timestamp == null) {
				del.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
			} else if (StringUtils.isNotEmpty(column) && timestamp != null) {
				del.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), timestamp);
			}
			table.delete(del);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
	}

	/**
	 * @description 列出所有表信息
	 * @param tableName
	 * @return
	 */
	public static List<TableDescriptor> listTables(String tableName) {
		try {
			return admin.listTableDescriptors();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	/**
	 * @description 全表扫描
	 * @param tableName 表名
	 * @param resCls    类名
	 * @param limit     限制查询条数
	 * @return
	 */
	public static <T> List<T> findAllLimit(String tableName, Class<T> resCls, int limit) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			scan.setCaching(100);
			Filter filter = new PageFilter(limit);
			scan.setFilter(filter);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * @description 根据rowkey查询
	 * @param tableName
	 * @param resCls
	 * @param rowKey
	 * @return
	 */
	public static <T> T findByRowKey(String tableName, Class<T> resCls, String rowKey) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		Result result = null;
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null) {
				return null;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Get scan = new Get(rowKey.getBytes());// 根据rowkey查询
			result = table.get(scan);
			Object obj = convertResultToObject(result, resCls);
			if (obj != null) {
				return (T) resCls.cast(obj);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
		return null;
	}

	/**
	 * 根据多列查询
	 * 
	 * @param tableName   表名
	 * @param famliy      列族
	 * @param resCls      返回类
	 * @param cols        列与值reg Map
	 * @param limit       限制查询条数
	 * @param isLikeQuery 是否模糊查询
	 * @return
	 */
	public static <T> List<T> finaByManyColumn(String tableName, String famliy, Class<T> resCls,
			Map<String, String> cols, long limit, boolean isLikeQuery) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			scan.setCaching(1);
			List<Filter> filters = new ArrayList<Filter>();
			for (Iterator<Map.Entry<String, String>> it = cols.entrySet().iterator(); it.hasNext();) {
				Map.Entry<String, String> entry = it.next();
				String key = entry.getKey();
				String value = entry.getValue();
				Filter filter = null;
				if (isLikeQuery) {
					if (StringUtils.isEmpty(famliy)) {
						filter = new SingleColumnValueFilter(null, Bytes.toBytes(key), CompareOperator.EQUAL,
								new RegexStringComparator(value));
					} else {
						filter = new SingleColumnValueFilter(Bytes.toBytes(famliy), Bytes.toBytes(key),
								CompareOperator.EQUAL, new RegexStringComparator(value));
					}
				} else {

					if (StringUtils.isEmpty(famliy)) {

						filter = new SingleColumnValueFilter(null, Bytes.toBytes(key), CompareOperator.EQUAL,
								Bytes.toBytes(value.toString()));
					} else {
						filter = new SingleColumnValueFilter(Bytes.toBytes(famliy), Bytes.toBytes(key),
								CompareOperator.EQUAL, Bytes.toBytes(value.toString()));
					}
				}
				filters.add(filter);
			}

			Filter filter = new PageFilter(limit);
			filters.add(filter);
			FilterList filterList1 = new FilterList(filters);
			scan.setFilter(filterList1);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 根据rowkey正则查询
	 *
	 * @param tableName   表名
	 * @param rowkeyReg   查询正则表达式
	 * @param resCls      类模板
	 * @param startRowKey rowkey 开始值.可以为空
	 * @param limit       限制返回记录数
	 * @return
	 */
	public static <T> List<T> queryLikeRowkey(String tableName, String rowkeyReg, Class<T> resCls, String startRowKey,
			int limit) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			List<Filter> listFilters = new ArrayList<Filter>(2);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(startRowKey)) {
				scan.withStartRow(Bytes.toBytes(startRowKey));
			}
			scan.setCaching(200);
			scan.setCacheBlocks(false);

			RegexStringComparator comp = new RegexStringComparator(rowkeyReg,
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

			RowFilter filter = new RowFilter(CompareOperator.EQUAL, comp);
			listFilters.add(filter);

			PageFilter pageFilter = new PageFilter(limit);
			listFilters.add(pageFilter);
			FilterList fl = new FilterList(listFilters);
			scan.setFilter(fl);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 按列模糊查询
	 * 
	 * @param tableName   表名
	 * @param famliy      列族
	 * @param columnName  列名
	 * @param queryReg    查询正则
	 * @param resCls      类模板
	 * @param startRowKey 开始rowkey值
	 * @param limit       限制返回记录数
	 * @return
	 */
	public static <T> List<T> queryLikeColumn(String tableName, String famliy, String columnName, String queryReg,
			Class<T> resCls, String startRowKey, int limit) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			List<Filter> listFilters = new ArrayList<Filter>(2);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(startRowKey)) {
				scan.withStartRow(Bytes.toBytes(startRowKey));
			}
			scan.setCaching(200);
			scan.setCacheBlocks(false);

			RegexStringComparator comp = new RegexStringComparator(queryReg);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(famliy), Bytes.toBytes(columnName),
					CompareOperator.EQUAL, comp);
			listFilters.add(filter);
			PageFilter pageFilter = new PageFilter(limit);
			listFilters.add(pageFilter);
			FilterList fl = new FilterList(listFilters);
			scan.setFilter(fl);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 关闭表
	 * 
	 * @param table
	 */
	public static void closeTable(Table table) {
		if (table != null) {
			try {
				table.close();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * @description 将结果集封装对象返回
	 * @param scanner
	 * @param resCls
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public static <T> List<T> convertResultToList(ResultScanner scanner, Class<T> resCls)
			throws InstantiationException, IllegalAccessException {
		List<T> lists = new ArrayList<T>();
		for (Result result : scanner) {
			Object obj = convertResultToObject(result, resCls);
			if (obj != null) {
				lists.add(resCls.cast(obj));
			}
		}
		return lists;
	}

	/**
	 * 结果集转换
	 *
	 * @author : <a href="mailto:dejianliu@sxw.cn">dejianliu</a> 2016-2-16
	 *         上午11:33:48
	 * @param result
	 * @param resCls
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public static Object convertResultToObject(Result result, Class<?> resCls)
			throws InstantiationException, IllegalAccessException {
		if (result == null || result.rawCells() == null || result.isEmpty()) {
			return null;
		}
		Map<String, Field> fieldMap = FieldCache.getInstance.getFieldMapByClass(resCls);
		String rowKeyName = FieldCache.getInstance.getRowKeyFieldName(resCls);
		Field rowKeyField = fieldMap.get(rowKeyName);

		Object resObj = resCls.newInstance();
		Cell[] cells = result.rawCells();

		for (Cell cell : cells) {
			String rowKey = new String(cell.getRowArray());
			rowKeyField.set(resObj, rowKey);
//			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
			byte[] value = CellUtil.cloneValue(cell);
			Field field = fieldMap.get(colName);
			if (field != null && field.getAnnotation(Column.class) != null) {
				field.set(resObj, convertObject(field.getType(), value));
			}
		}
		return resObj;
	}

	private static Object convertObject(Class<?> type, byte[] value) {
		String objStr = new String(value);
		try {
			if (type == String.class) {
				return objStr;
			} else if (type == Integer.class || type == int.class) {
				return Integer.valueOf(objStr);
			} else if (type == Long.class || type == long.class) {
				return Long.valueOf(objStr);
			} else if (type == Double.class || type == double.class) {
				return Double.valueOf(objStr);
			} else if (type == Float.class || type == float.class) {
				return Double.valueOf(objStr);
			} else if (type == Date.class) {
				return DateUtils.parseDate(objStr,
						new String[] { "yyyy-MM-dd", "yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss" });
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return null;

	}

	/**
	 * @Description : 向已存在的表中添加协处理器，加快表统计速度，注意此方法调用时会让表暂时不可用!!!
	 * @param tableName 表名
	 */
	public static void addCoprocessor(String tableName) {
		TableName tableNameObj = TableName.valueOf(tableName);
		try {
			String coprocessor = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
			if (StringUtils.isEmpty(tableName)) {
				return;
			}
			if (!admin.tableExists(tableNameObj)) {
				throw new HbaseException("this table " + tableName + " does not exist!");
			}
			admin.disableTable(tableNameObj);
			TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
			tdBuilder.setCoprocessor(coprocessor);
			admin.modifyTable(tdBuilder.build());
			admin.enableTable(tableNameObj);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * @Description : 统计Hbase表中数据总行数。注意：当HBase库中数据里很大时，此方法会导致长时间的阻塞，因此请慎用!!!
	 * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static long countRows(String tableName) throws IOException {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		long count = 0;
		try {
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			ResultScanner scanner = table.getScanner(scan);
			Result result = scanner.next();

			while (result != null) {
				count++;
				result = scanner.next();
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			closeTable(table);
		}
		return count;
	}

	/**
	 * 根据多列查询
	 *
	 * @param tableName   表名
	 * @param famliy      列族
	 * @param resCls      返回类
	 * @param cols        列与值reg Map
	 * @param limit       限制查询条数
	 * @param isLikeQuery 是否模糊查询
	 * @return
	 */
	public static <T> List<T> findByManyColumn(String tableName, String famliy, Class<T> resCls,
			Map<String, String> cols, long limit, boolean isLikeQuery) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			FilterList filterList1 = createFilterList(cols, famliy, isLikeQuery);

			Filter filter = new PageFilter(limit);
			filterList1.addFilter(filter);
			scan.setFilter(filterList1);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 根据多列查询
	 * 
	 * @param tableName   表名
	 * @param famliy      列族
	 * @param resCls      返回类
	 * @param cols        列与值reg Map
	 * @param limit       限制查询条数
	 * @param isLikeQuery 是否模糊查询
	 * @param startRowKey 起始键值
	 * @return
	 */
	public static <T> List<T> findByManyColumn(String tableName, String famliy, Class<T> resCls,
			Map<String, String> cols, long limit, boolean isLikeQuery, String startRowKey) {
		if (StringUtils.isBlank(startRowKey)) {
			return findByManyColumn(tableName, famliy, resCls, cols, limit, isLikeQuery);
		}

		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			scan.withStartRow(Bytes.toBytes(startRowKey));
			FilterList filterList1 = createFilterList(cols, famliy, isLikeQuery);
			Filter filter = new PageFilter(limit);
			filterList1.addFilter(filter);
			scan.setFilter(filterList1);
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 条件查询
	 * 
	 * @param tableName
	 * @param resCls
	 * @param filterList
	 * @param startRowKey
	 * @param limit
	 * @return
	 */
	public static <T> List<T> findByCondition(String tableName, Class<T> resCls, FilterList filterList,
			String startRowKey, Long limit) {
		TableName tableNameObj = TableName.valueOf(tableName);
		Table table = null;
		ResultScanner scanner = null;
		List<T> lists = new ArrayList<T>();
		try {
			if (limit == null) {
				limit = Long.valueOf(100);
			}
			if (StringUtils.isEmpty(tableName) || resCls == null || limit < 1) {
				return lists;
			}
			table = getHBaseConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(startRowKey)) {
				scan.withStartRow(Bytes.toBytes(startRowKey));
			}
			if (filterList == null) {
				filterList = new FilterList();
			}
			if (limit > 10000) {
				limit = Long.valueOf(10000l);
			}
			filterList.addFilter(new PageFilter(limit));
			if (filterList != null) {
				scan.setFilter(filterList);
			}
			scanner = table.getScanner(scan);
			lists = convertResultToList(scanner, resCls);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			closeTable(table);
		}
		return lists;
	}

	/**
	 * 组装查询条件
	 * 
	 * @param cols
	 * @param famliy
	 * @param isLikeQuery
	 * @return
	 */
	public static FilterList createFilterList(Map<String, String> cols, String famliy, boolean isLikeQuery) {
		FilterList filterList1 = new FilterList();
		for (Iterator<Map.Entry<String, String>> it = cols.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, String> entry = it.next();
			String key = entry.getKey();
			String value = entry.getValue();
			SingleColumnValueFilter filter = null;

			if (StringUtils.isBlank(value)) {
				continue;
			}

			if (isLikeQuery) {
				if (StringUtils.isEmpty(famliy)) {
					filter = new SingleColumnValueFilter(null, Bytes.toBytes(key), CompareOperator.EQUAL,
							new RegexStringComparator(value));
				} else {
					filter = new SingleColumnValueFilter(Bytes.toBytes(famliy), Bytes.toBytes(key),
							CompareOperator.EQUAL, new RegexStringComparator(value));
				}
			} else {
				if (StringUtils.isEmpty(famliy)) {
					filter = new SingleColumnValueFilter(null, Bytes.toBytes(key), CompareOperator.EQUAL,
							Bytes.toBytes(value.toString()));
				} else {
					filter = new SingleColumnValueFilter(Bytes.toBytes(famliy), Bytes.toBytes(key),
							CompareOperator.EQUAL, Bytes.toBytes(value.toString()));
				}
			}
			filter.setFilterIfMissing(true);
			filterList1.addFilter(filter);
		}
		return filterList1;
	}

}
