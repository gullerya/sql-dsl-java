package com.gullerya.sqldsl.impl;

import java.util.*;
import java.util.stream.Collectors;

import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.clauses.Where;
import com.gullerya.sqldsl.api.statements.Update;

class StatementUpdateImpl<T> implements Update<T>, Update.UpdateDownstream {
	private final EntityDALImpl.ESConfig<T> config;
	private final Map<String, Map.Entry<FieldMetaProc, Object>> updateSet = new LinkedHashMap<>();
	private final Map<String, String> literalsSet = new TreeMap<>();

	StatementUpdateImpl(EntityDALImpl.ESConfig<T> config) {
		this.config = config;
	}

	@Override
	public UpdateDownstream update(T updateSet, Literal... literals) {
		if (updateSet == null) {
			throw new IllegalArgumentException("update set MUST NOT be NULL");
		}
		validateCollect(literalsSet, literals);
		buildUpdateSet(updateSet);
		if (this.updateSet.isEmpty() && literalsSet.isEmpty()) {
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
		validateWhereClause(config.em, where);
		return internalUpdate(where);
	}

	private void validateCollect(Map<String, String> accumulator, Literal... literals) {
		if (literals != null && literals.length > 0) {
			for (Literal literal : literals) {
				FieldMetaProc fm = config.em.byColumn.get((literal.column));
				if (fm == null) {
					throw new IllegalArgumentException("column '" + literal.column + "' is not defined for entity " + config.em.type);
				}
				if (!fm.column.insertable()) {
					continue;
				}
				accumulator.put(literal.column, literal.value);
			}
		}
	}

	private void validateWhereClause(EntityMetaProc<T> em, Where.WhereClause where) {
		if (where == null) {
			throw new IllegalArgumentException("where clause MUST NOT be NULL");
		}
		Collection<String> fields = where.collectFields();
		for (String f : fields) {
			if (!em.byColumn.containsKey(f)) {
				throw new IllegalArgumentException("field '" + f + "' not found in entity " + em.type + " definition");
			}
		}
	}

	private void buildUpdateSet(T input) {
		for (FieldMetaProc fm : config.em.byColumn.values()) {
			if (literalsSet.containsKey(fm.columnName)) {
				continue;
			}
			if (!fm.column.updatable()) {
				continue;
			}

			Object fv = fm.getFieldValue(input);
			Object cfv = fm.translateFieldToColumn(fv);
			if (cfv != null) {
				updateSet.put(fm.columnName, new AbstractMap.SimpleEntry<>(fm, cfv));
			}
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

		return config.prepareStatementAndDo(sql, s -> {
			int i = 0;
			for (Map.Entry<FieldMetaProc, Object> valueParam : updateSet.values()) {
				FieldMetaProc fm = valueParam.getKey();
				fm.setColumnValue(s, ++i, valueParam.getValue());
			}
			if (where != null) {
				for (Where.WhereFieldValuePair parameter : parametersCollector) {
					FieldMetaProc fm = config.em.byColumn.get(parameter.column);
					fm.setColumnValue(s, ++i, parameter.value);
				}
			}
			return s.executeUpdate();
		});
	}
}
