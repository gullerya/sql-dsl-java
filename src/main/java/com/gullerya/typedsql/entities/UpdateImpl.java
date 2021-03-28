package com.gullerya.typedsql.entities;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

class UpdateImpl<T> implements Update<T>, Update.UpdateDownstream {
	private final EntityService.ESConfig<T> config;
	private final Map<String, Map.Entry<EntityService.FieldMetadata, Object>> updateSet = new LinkedHashMap<>();
	private final Map<String, String> literalsSet = new TreeMap<>();

	UpdateImpl(EntityService.ESConfig<T> config) {
		this.config = config;
	}

	@Override
	public UpdateDownstream update(T updateSet, Literal... literals) {
		if (updateSet == null) {
			throw new IllegalArgumentException("update set MUST NOT be NULL");
		}
		validateCollect(literals);
		buildUpdateSet(updateSet);
		if (this.updateSet.isEmpty() && this.literalsSet.isEmpty()) {
			throw new IllegalArgumentException("update set MUST HAVE at least one updatable field which value is not NULL");
		}
		return this;
	}

	@Override
	public int all() {
		return internalUpdate(null);
	}

	@Override
	public Integer where(WhereClause where) {
		Where.validate(config.em, where);
		return internalUpdate(where);
	}

	private void validateCollect(Literal... literals) {
		if (literals != null && literals.length > 0) {
			for (Literal literal : literals) {
				if (!config.em.byColumn.containsKey(literal.field)) {
					throw new IllegalArgumentException("field '" + literal.field + "' is not found in entity " + config.em.type);
				}
				literalsSet.put(literal.field, literal.value);
			}
		}
	}

	private void buildUpdateSet(T input) {
		try {
			for (EntityService.FieldMetadata fm : config.em.byColumn.values()) {
				if (literalsSet.containsKey(fm.fieldMetadata.value())) {
					continue;
				}
				if (fm.fieldMetadata.readonly()) {
					continue;
				}

				Object fv = fm.field.get(input);
				Object cfv = fm.typeConverter.toDB(fv);
				if (cfv != null) {
					updateSet.put(fm.fieldMetadata.value(), new AbstractMap.SimpleEntry<>(fm, cfv));
				}
			}
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to prepare insert data set", roe);
		}
	}

	private int internalUpdate(WhereClause where) {
		String sql = "UPDATE " + config.em.fqSchemaTableName + " SET " + updateSet.keySet().stream().map(k -> k + "=?").collect(Collectors.joining(","));
		if (!literalsSet.isEmpty()) {
			sql += (updateSet.isEmpty() ? "" : ",") + literalsSet.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(","));
		}

		Collection<WhereFieldValuePair> parametersCollector = new ArrayList<>();
		if (where != null) {
			sql += " WHERE " + where.stringify(parametersCollector);
		}

		return config.preparedStatementAndDo(sql, s -> {
			int i = 0;
			for (Map.Entry<EntityService.FieldMetadata, Object> valueParam : updateSet.values()) {
				EntityService.FieldMetadata fieldMetadata = valueParam.getKey();
				Object value = valueParam.getValue();
				i++;
				if (fieldMetadata.jdbcConverter != null) {
					fieldMetadata.jdbcConverter.toDB(s, i, value);
				} else {
					if (value instanceof InputStream) {
						s.setBinaryStream(i, (InputStream) value);
					} else if (value instanceof byte[]) {
						s.setBytes(i, (byte[]) value);
					} else {
						s.setObject(i, value);
					}
				}
			}
			if (where != null) {
				for (Where.WhereFieldValuePair parameter : parametersCollector) {
					i++;
					EntityService.FieldMetadata fm = config.em.byColumn.get(parameter.column);
					if (fm.jdbcConverter != null) {
						fm.jdbcConverter.toDB(s, i, parameter.value);
					} else {
						s.setObject(i, fm.typeConverter.toDB(parameter.value));
					}
				}
			}
			return s.executeUpdate();
		});
	}
}
