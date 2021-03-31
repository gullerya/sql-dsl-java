package com.gullerya.sqldsl.impl;

import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.statements.Insert;

public class InsertImpl<T> implements Insert<T> {
	private final EntityDALImpl.ESConfig<T> config;

	InsertImpl(EntityDALImpl.ESConfig<T> config) {
		this.config = config;
	}

	@Override
	public boolean insert(T entity, Literal... literals) {
		if (entity == null) {
			throw new IllegalArgumentException("inserted entity MUST NOT be NULL");
		}
		Map<String, String> literalsMap = validateCollect(literals);
		List<Map.Entry<EntityFieldMetadata, Object>> params = new ArrayList<>();
		String sql = buildInsertSet(entity, literalsMap, params);
		return config.preparedStatementAndDo(sql, s -> {
			for (int i = 0; i < params.size(); i++) {
				Map.Entry<EntityFieldMetadata, Object> paramValue = params.get(i);
				EntityFieldMetadata fieldMetadata = paramValue.getKey();
				Object pv = paramValue.getValue();
				if (fieldMetadata.converter != null) {
					s.setObject(i + 1, fieldMetadata.converter.convertToDatabaseColumn(pv));
				} else {
					if (pv instanceof InputStream) {
						s.setBinaryStream(i + 1, (InputStream) pv);
					} else if (pv instanceof byte[]) {
						s.setBytes(i + 1, (byte[]) pv);
					} else {
						s.setObject(i + 1, pv);
					}
				}
			}
			return s.executeUpdate() == 1;
		});
	}

	@Override
	public int[] insert(Collection<T> entities, Literal... literals) {
		if (entities == null || entities.isEmpty()) {
			throw new IllegalArgumentException("entities list MUST NOT be NULL nor EMPTY");
		}
		Map<String, String> literalsMap = validateCollect(literals);
		List<Map.Entry<EntityFieldMetadata, Object[]>> paramsSets = new ArrayList<>();
		String sql = buildInsertSet(entities, literalsMap, paramsSets);
		return config.preparedStatementAndDo(sql, s -> {
			for (int ec = 0; ec < entities.size(); ec++) {
				for (int fc = 0; fc < paramsSets.size(); fc++) {
					Map.Entry<EntityFieldMetadata, Object[]> paramValue = paramsSets.get(fc);
					EntityFieldMetadata fieldMetadata = paramValue.getKey();
					Object pv = paramValue.getValue()[ec];
					if (fieldMetadata.converter != null) {
						s.setObject(fc + 1, fieldMetadata.converter.convertToDatabaseColumn(pv));
					} else {
						if (pv instanceof InputStream) {
							s.setBinaryStream(fc + 1, (InputStream) pv);
						} else if (pv instanceof byte[]) {
							s.setBytes(fc + 1, (byte[]) pv);
						} else {
							s.setObject(fc + 1, pv);
						}
					}
				}
				s.addBatch();
			}
			return s.executeBatch();
		});
	}

	private Map<String, String> validateCollect(Literal... literals) {
		Map<String, String> result = new TreeMap<>();
		if (literals != null && literals.length > 0) {
			for (Literal literal : literals) {
				if (!config.em.byColumn.containsKey(literal.field)) {
					throw new IllegalArgumentException("field '" + literal.field + "' is not found in entity " + config.em.type);
				}
				result.put(literal.field, literal.value);
			}
		}
		return result;
	}

	private String buildInsertSet(T entity, Map<String, String> literals, List<Map.Entry<EntityFieldMetadata, Object>> params) {
		Set<String> nonNullSet = new LinkedHashSet<>();
		try {
			for (EntityFieldMetadata fm : config.em.byColumn.values()) {
				if (literals.containsKey(fm.column.name())) {
					continue;
				}
				Object fv = fm.field.get(entity);
				if (fm.converter != null) {
					fv = fm.converter.convertToDatabaseColumn(fv);
				}
				if (fv != null) {
					nonNullSet.add(fm.column.name());
					params.add(new AbstractMap.SimpleEntry<>(fm, fv));
				}
			}
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to prepare insert data set", roe);
		}

		String fields = String.join(",", nonNullSet);
		String values = String.join(",", Collections.nCopies(nonNullSet.size(), "?"));
		if (!literals.isEmpty()) {
			fields += (nonNullSet.isEmpty() ? "" : ",") + String.join(",", literals.keySet());
			values += (nonNullSet.isEmpty() ? "" : ",") + String.join(",", literals.values());
		}
		return "INSERT INTO " + config.em.fqSchemaTableName + " (" + fields + ")" + " VALUES (" + values + ")";
	}

	private String buildInsertSet(Collection<T> entities, Map<String, String> literals, List<Map.Entry<EntityFieldMetadata, Object[]>> params) {
		Map<String, Map.Entry<EntityFieldMetadata, Object[]>> nonNullSet = new LinkedHashMap<>();
		try {
			for (EntityFieldMetadata fm : config.em.byColumn.values()) {
				if (literals.containsKey(fm.column.name())) {
					continue;
				}
				int i = 0;
				for (T entity : entities) {
					Object fv = fm.field.get(entity);
					if (fm.converter != null) {
						fv = fm.converter.convertToDatabaseColumn(fv);
					}
					if (fv != null) {
						nonNullSet
								.computeIfAbsent(fm.column.name(), c -> new AbstractMap.SimpleEntry<>(fm, new Object[entities.size()]))
								.getValue()[i] = fv;
					}
					i++;
				}
			}
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to prepare insert data set", roe);
		}
		String fields = String.join(",", nonNullSet.keySet());
		String values = String.join(",", Collections.nCopies(nonNullSet.size(), "?"));
		params.addAll(nonNullSet.values());
		if (!literals.isEmpty()) {
			fields += "," + String.join(",", literals.keySet());
			values += "," + String.join(",", literals.values());
		}
		return "INSERT INTO " + config.em.fqSchemaTableName + " (" + fields + ")" + " VALUES (" + values + ")";
	}
}
