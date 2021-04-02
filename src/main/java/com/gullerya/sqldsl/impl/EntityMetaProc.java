package com.gullerya.sqldsl.impl;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

public class EntityMetaProc<T> {
	final Class<T> type;
	final String fqSchemaTableName;
	final Map<String, FieldMetaProc> byFName;
	final Map<String, FieldMetaProc> byColumn;

	EntityMetaProc(Class<T> type) throws ReflectiveOperationException {
		this.type = type;

		Entity e = type.getDeclaredAnnotation(Entity.class);
		if (e == null) {
			throw new IllegalArgumentException("type " + type + " MUST be annotated with " + Entity.class);
		}
		String tmpName = !e.name().isEmpty() ? e.name() : type.getSimpleName();
		Table t = type.getDeclaredAnnotation(Table.class);
		if (t != null) {
			tmpName = !t.name().isEmpty() ? t.name() : tmpName;
			fqSchemaTableName = "\"" + (!t.schema().isEmpty() ? (t.schema() + "\".\"") : "") + tmpName + "\"";
		} else {
			fqSchemaTableName = "\"" + tmpName + "\"";
		}

		Map<String, FieldMetaProc> tmpByField = new HashMap<>();
		Map<String, FieldMetaProc> tmpByColumn = new HashMap<>();
		for (Field f : type.getDeclaredFields()) {
			Column ef = f.getDeclaredAnnotation(Column.class);
			if (ef != null) {
				FieldMetaProc fm = new FieldMetaProc(f, ef);
				tmpByField.put(f.getName(), fm);
				tmpByColumn.put(fm.columnName, fm);
			}
		}
		if (tmpByField.isEmpty()) {
			throw new IllegalArgumentException(
					"type " + type + " MUST have at least 1 fields annotated with " + Column.class);
		}
		byFName = tmpByField.entrySet().stream().sorted(Map.Entry.comparingByKey())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
		byColumn = tmpByColumn.entrySet().stream().sorted(Map.Entry.comparingByKey())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	}
}
