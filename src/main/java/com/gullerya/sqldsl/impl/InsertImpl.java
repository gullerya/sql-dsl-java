package com.gullerya.sql.impl;

import java.io.InputStream;
import java.util.*;

class InsertImpl<T> implements Insert<T> {
	private final EntityService.ESConfig<T> config;

	InsertImpl(EntityService.ESConfig<T> config) {
		this.config = config;
	}

	@Override
	public boolean insert(T entity, Literal... literals) {
		if (entity == null) {
			throw new IllegalArgumentException("inserted entity MUST NOT be NULL");
		}
		Map<String, String> literalsMap = validateCollect(literals);
		List<Map.Entry<EntityService.FieldMetadata, Object>> params = new ArrayList<>();
		String sql = buildInsertSet(entity, literalsMap, params);
		return config.preparedStatementAndDo(sql, s -> {
			for (int i = 0; i < params.size(); i++) {
				Map.Entry<EntityService.FieldMetadata, Object> paramValue = params.get(i);
				EntityService.FieldMetadata fieldMetadata = paramValue.getKey();
				Object value = paramValue.getValue();
				if (fieldMetadata.jdbcConverter != null) {
					fieldMetadata.jdbcConverter.toDB(s, i + 1, value);
				} else {
					if (value instanceof InputStream) {
						s.setBinaryStream(i + 1, (InputStream) value);
					} else if (value instanceof byte[]) {
						s.setBytes(i + 1, (byte[]) value);
					} else {
						s.setObject(i + 1, value);
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
		List<Map.Entry<EntityService.FieldMetadata, Object[]>> paramsSets = new ArrayList<>();
		String sql = buildInsertSet(entities, literalsMap, paramsSets);
		return config.preparedStatementAndDo(sql, s -> {
			for (int ec = 0; ec < entities.size(); ec++) {
				for (int fc = 0; fc < paramsSets.size(); fc++) {
					Map.Entry<EntityService.FieldMetadata, Object[]> paramValue = paramsSets.get(fc);
					EntityService.FieldMetadata fieldMetadata = paramValue.getKey();
					Object value = paramValue.getValue()[ec];
					if (fieldMetadata.jdbcConverter != null) {
						fieldMetadata.jdbcConverter.toDB(s, fc + 1, value);
					} else {
						if (value instanceof InputStream) {
							s.setBinaryStream(fc + 1, (InputStream) value);
						} else if (value instanceof byte[]) {
							s.setBytes(fc + 1, (byte[]) value);
						} else {
							s.setObject(fc + 1, value);
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

	private String buildInsertSet(T entity, Map<String, String> literals, List<Map.Entry<EntityService.FieldMetadata, Object>> params) {
		Set<String> nonNullSet = new LinkedHashSet<>();
		try {
			for (EntityService.FieldMetadata fm : config.em.byColumn.values()) {
				if (literals.containsKey(fm.fieldMetadata.value())) {
					continue;
				}
				Object fv = fm.field.get(entity);
				Object cfv = fm.typeConverter.toDB(fv);
				if (cfv != null) {
					nonNullSet.add(fm.fieldMetadata.value());
					params.add(new AbstractMap.SimpleEntry<>(fm, cfv));
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

	private String buildInsertSet(Collection<T> entities, Map<String, String> literals, List<Map.Entry<EntityService.FieldMetadata, Object[]>> params) {
		Map<String, Map.Entry<EntityService.FieldMetadata, Object[]>> nonNullSet = new LinkedHashMap<>();
		try {
			for (EntityService.FieldMetadata fm : config.em.byColumn.values()) {
				if (literals.containsKey(fm.fieldMetadata.value())) {
					continue;
				}
				int i = 0;
				for (T entity : entities) {
					Object fv = fm.field.get(entity);
					Object cfv = fm.typeConverter.toDB(fv);
					if (cfv != null) {
						nonNullSet
								.computeIfAbsent(fm.fieldMetadata.value(), c -> new AbstractMap.SimpleEntry<>(fm, new Object[entities.size()]))
								.getValue()[i] = cfv;
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
