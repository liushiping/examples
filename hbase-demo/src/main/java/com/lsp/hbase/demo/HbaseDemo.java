package com.lsp.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

	private static Configuration conf = HBaseConfiguration.create();
	private static Admin admin;

	static {
//        conf.set("hbase.rootdir", "hdfs://192.168.209.129:9000/hbase");
		conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
		// 设置Zookeeper,直接设置IP地址
//        conf.set("hbase.zookeeper.quorum", "192.168.209.129,192.168.209.130,192.168.209.131");
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建表，可以同时创建多个列簇
	 *
	 * @param tableName
	 * @param columnFamily
	 */
	public void createTable(String tableName, String... columnFamily) {
		TableName tableNameObj = TableName.valueOf(tableName);
		try {
			if (this.admin.tableExists(tableNameObj)) {
				System.out.println("Table : " + tableName + " already exists !");
			} else {
				HTableDescriptor td = new HTableDescriptor(tableNameObj);
				int len = columnFamily.length;
				for (int i = 0; i < len; i++) {
					HColumnDescriptor family = new HColumnDescriptor(columnFamily[i]);
					td.addFamily(family);
				}
				admin.createTable(td);
				System.out.println(tableName + " 表创建成功！");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表创建失败！");
		}
	}

	@Test
	public void testCreateTable() {
		createTable("cross_history", "carinfo", "parkInfo", "deviceInfo");
	}

	public void delTable(String tableName) {
		TableName tableNameObj = TableName.valueOf(tableName);
		try {
			if (this.admin.tableExists(tableNameObj)) {
				admin.disableTable(tableNameObj);
				admin.deleteTable(tableNameObj);
				System.out.println(tableName + " 表删除成功！");
			} else {
				System.out.println(tableName + " 表不存在！");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表删除失败！");
		}
	}

	@Test
	public void testDelTable() {
		delTable("cross_history");
	}

	public void insertRecord(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(rowKey.getBytes());
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			table.close();
			connection.close();
			System.out.println(tableName + " 表插入数据成功！");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表插入数据失败！");
		}
	}

	@Test
	public void testInsertRecord() {
		insertRecord("cross_history", "001", "carinfo", "plateNo", "浙A12345");
		insertRecord("cross_history", "002", "carinfo", "plateNo", "浙A12345");
		insertRecord("cross_history", "003", "carinfo", "plateNo", "浙A12345");
		insertRecord("cross_history", "001", "parkInfo", "parkName", "中兴花园");
		insertRecord("cross_history", "002", "parkInfo", "parkName", "中兴花园");
		insertRecord("cross_history", "003", "parkInfo", "parkName", "中兴花园");
		insertRecord("cross_history", "001", "deviceInfo", "deviceInfo", "道闸");
		insertRecord("cross_history", "002", "deviceInfo", "deviceInfo", "道闸");
		insertRecord("cross_history", "003", "deviceInfo", "deviceInfo", "道闸");
	}

	public void deleteRecord(String tableName, String rowKey) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete del = new Delete(rowKey.getBytes());
			table.delete(del);
			System.out.println(tableName + " 表删除数据成功！");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(tableName + " 表删除数据失败！");
		}
	}

	@Test
	public void testDeleteRecord() {
		deleteRecord("cross_history", "001");
	}

	public Result getOneRecord(String tableName, String rowKey) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey.getBytes());
			Result rs = table.get(get);
			System.out.println(tableName + " 表获取数据成功！");
			System.out.println("rowkey为:" + rowKey);
			List<Cell> cells = rs.listCells();
			if (cells != null) {
				for (Cell cell : cells) {
					System.out.println(new String(cell.getFamily()) + " : " + new String(cell.getQualifier()) + " : "
							+ new String(cell.getValue()));
				}
			}

			return rs;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	@Test
	public void testGetOneRecord() {
		getOneRecord("cross_history", "002");
	}

	public List<Result> getAll(String tableName) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			List<Result> list = new ArrayList<Result>();
			for (Result r : scanner) {
				list.add(r);
			}
			scanner.close();
			System.out.println(tableName + " 表获取所有记录成功！");
			return list;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Test
	public void testGetAll() {
		System.out.println(getAll("cross_history"));
	}

	// 创建表
	public static void createTable(String tablename, String columnFamily) throws Exception {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		TableName tableNameObj = TableName.valueOf(tablename);

		if (admin.tableExists(tableNameObj)) {
			System.out.println("Table exists!");
			System.exit(0);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
		admin.close();
		connection.close();
	}

	// 删除表
	public static void deleteTable(String tableName) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Admin admin = connection.getAdmin();
			TableName table = TableName.valueOf(tableName);
			admin.disableTable(table);
			admin.deleteTable(table);
			System.out.println("delete table " + tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 插入一行记录
	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			table.close();
			connection.close();
			System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HbaseDemo.createTable("testTb", "info");
		HbaseDemo.addRecord("testTb", "001", "info", "name", "zhangsan");
		HbaseDemo.addRecord("testTb", "001", "info", "age", "20");
		// HbaseDao.deleteTable("testTb");
	}

}
