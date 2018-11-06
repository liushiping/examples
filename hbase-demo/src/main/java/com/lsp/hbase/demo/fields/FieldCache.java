package com.lsp.hbase.demo.fields;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.log4j.Logger;

import com.lsp.hbase.demo.HTableInfo;
import com.lsp.hbase.demo.annotation.Column;
import com.lsp.hbase.demo.annotation.HTable;
import com.lsp.hbase.demo.annotation.Rowkey;

/**
 * @description: 属性缓存
 * @Date 2018-11-5 下午1:41:41
 */
public class FieldCache {

	private static Logger logger = Logger.getLogger(FieldCache.class);

	public final static FieldCache getInstance = new FieldCache();

	/**
	 * key = 表名 value=类名
	 */
	private ConcurrentHashMap<String, HTableInfo> htableMaps = null;

	/**
	 * key = className value = [key=fieldName,value=Field]
	 */
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Field>> cacheAnnoMap = null;

	private FieldCache() {
		cacheAnnoMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Field>>();
		htableMaps = new ConcurrentHashMap<String, HTableInfo>();
	}

	/**
	 * @param clazz
	 * @return
	 */
	private Field[] getAllFields(Class<?> clazz) {
		List<Field> allFields = new ArrayList<Field>();
		allFields.addAll(filterFields(clazz.getDeclaredFields()));
		Class<?> parent = clazz.getSuperclass();
		while ((parent != null) && (parent != Object.class)) {
			allFields.addAll(filterFields(parent.getDeclaredFields()));
			parent = parent.getSuperclass();
		}
		return allFields.toArray(new Field[allFields.size()]);
	}

	/**
	 * 过滤static字段
	 * 
	 * @param fields
	 * @return
	 */
	private List<Field> filterFields(Field[] fields) {
		List<Field> result = new ArrayList<Field>();
		for (Field field : fields) {
			if (!Modifier.isStatic(field.getModifiers())) {
				field.setAccessible(true);
				result.add(field);
			}
		}
		return result;
	}

	/**
	 * 取得类中所有属性
	 * 
	 * @param clazz
	 * @return
	 */
	public Collection<Field> get(Class<?> clazz) {
		HTable htable = clazz.getAnnotation(HTable.class);
		if (htable == null) {
			return null;
		}
		ConcurrentHashMap<String, Field> map = cacheAnnoMap.get(clazz.getName());
		if (map != null && map.size() > 0) {
			return map.values();
		}
		Field[] fields = getAllFields(clazz);
		ConcurrentHashMap<String, Field> fieldMap = new ConcurrentHashMap<String, Field>();
		for (Field field : fields) {
			Column c = field.getAnnotation(Column.class);
			if (c != null) {
				fieldMap.put(field.getName(), field);
			}
			Rowkey id = field.getAnnotation(Rowkey.class);
			if (id != null) {
				fieldMap.put(field.getName(), field);
			}
		}

		cacheAnnoMap.put(clazz.getName(), fieldMap);
		return fieldMap.values();
	}

	/**
	 * @description 根据类名取得该类下面所有 字段与属性之间的关系
	 * @param clazz
	 * @return
	 */
	public ConcurrentHashMap<String, Field> getFieldMapByClass(Class<?> clazz) {
		ConcurrentHashMap<String, Field> fieldMap = cacheAnnoMap.get(clazz.getName());
		if (fieldMap != null) {
			return fieldMap;
		}
		Field[] fields = getAllFields(clazz);
		fieldMap = new ConcurrentHashMap<String, Field>();
		for (Field field : fields) {
			Column c = field.getAnnotation(Column.class);
			if (c != null) {
				fieldMap.put(field.getName(), field);
			}
			Rowkey id = field.getAnnotation(Rowkey.class);
			if (id != null) {
				fieldMap.put(field.getName(), field);
			}
		}
		cacheAnnoMap.put(clazz.getName(), fieldMap);
		return fieldMap;
	}

	/**
	 * @description 根据类名取得表信息
	 * @param clazz
	 * @return
	 */
	public HTableInfo getTableNameByClass(Class<?> clazz) {
		HTable htable = clazz.getAnnotation(HTable.class);
		if (htable == null) {
			return null;
		}
		if (htableMaps.get(htable.name()) != null) {
			return htableMaps.get(htable.name());
		}
		if (htableMaps.get(clazz.getSimpleName()) != null) {
			return htableMaps.get(clazz.getName());
		}
		HTableInfo ti = new HTableInfo();
		ti.setRefCls(clazz);
		ti.setTableName(htable.name());
		ti.setFamilies(htable.families());
		htableMaps.put(htable.name(), ti);
		return ti;

	}

	/**
	 * 取得ID
	 * 
	 * @param clazz
	 * @return
	 */
	public String getRowKeyFieldName(Class<?> clazz) {
		String name = null;
		Collection<Field> cols = get(clazz);
		for (Field f : cols) {
			Rowkey rowkey = f.getAnnotation(Rowkey.class);
			if (rowkey != null) {
				name = f.getName();
				break;
			}
		}
		return name;
	}

	/**
	 * 
	 * @param obj
	 * @return
	 */
	public Object[] getRowkeyNameAndValue(Object obj) {
		try {
			Collection<Field> cols = get(obj.getClass());
			for (Field f : cols) {
				Rowkey rowkey = f.getAnnotation(Rowkey.class);
				if (rowkey != null) {
					String name = f.getName();
					String getMethod = "get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
					if (f.getType() == String.class) {
						String resRowKey = (String) MethodUtils.invokeMethod(obj, getMethod, null);
						return new Object[] { name, resRowKey };
					} else if (f.getType() == Long.class || f.getType() == long.class) {
						Long resRowKey = (Long) MethodUtils.invokeMethod(obj, getMethod, null);
						return new Object[] { name, resRowKey };
					} else if (f.getType() == Integer.class || f.getType() == int.class) {
						Integer resRowKey = (Integer) MethodUtils.invokeMethod(obj, getMethod, null);
						return new Object[] { name, resRowKey };
					}
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	/**
	 * 取得属性字段
	 * 
	 * @param clazz
	 * @param fieldName 属性名称
	 * @return
	 */
	public Field getField(Class<?> clazz, String fieldName) {
		Field field = null;
		Collection<Field> cols = get(clazz);
		for (Field f : cols) {
			if (f.getName().equals(fieldName)) {
				field = f;
				break;
			}
		}
		return field;
	}

}
