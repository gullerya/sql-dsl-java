package com.gullerya.sqldsl.impl;

import java.util.*;
import java.util.stream.Collectors;

import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.clauses.Where;
import com.gullerya.sqldsl.api.statements.Update;

class UpdateImpl<ET> implements Update<ET>, Update.UpdateDownstream {
	private final EntityDALImpl.ESConfig<ET> config;
	private final Map<String, Map.Entry<FieldMetaProc, Object>> updateSet = new LinkedHashMap<>();
	private final Map<String, String> literalsSet = new TreeMap<>();

	UpdateImpl(EntityDALImpl.ESConfig<ET> config) {
		this.config = config;
	}

	@Override
	public UpdateDownstream update(ET updateSet, Literal... literals) {
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
		validateWhereClause(config.em, where);
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

	private void validateWhereClause(EntityMetaProc<ET> em, Where.WhereClause where) {
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

	private void buildUpdateSet(ET input) {
		for (FieldMetaProc fm : config.em.byColumn.values()) {
			if (literalsSet.containsKey(fm.columnName)) {
				continue;
			}
//				if (fm.fieldMetadata.readonly()) {
//					continue;
//				}

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
