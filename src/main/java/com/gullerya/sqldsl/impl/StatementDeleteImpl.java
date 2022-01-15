package com.gullerya.sqldsl.impl;

import java.util.ArrayList;
import java.util.Collection;

import com.gullerya.sqldsl.api.clauses.Where;
import com.gullerya.sqldsl.api.statements.Delete;

public record StatementDeleteImpl<T>(EntityDALImpl.ESConfig<T> config) implements Delete<T> {

	@Override
	public int deleteAll() {
		return internalDelete(null);
	}

	@Override
	public int deleteAll(Where.WhereClause where) {
		validateWhereClause(config.em(), where);
		return internalDelete(where);
	}

	private int internalDelete(Where.WhereClause where) {
		String sql = "DELETE FROM " + config.em().fqSchemaTableName;
		Collection<Where.WhereFieldValuePair> parametersCollector = new ArrayList<>();
		if (where != null) {
			sql += " WHERE " + where.stringify(parametersCollector);
		}
		return config.prepareStatementAndDo(sql, s -> {
			if (where != null) {
				int i = 0;
				for (Where.WhereFieldValuePair parameter : parametersCollector) {
					i++;
					FieldMetaProc fm = config.em().byColumn.get(parameter.column());
					Object pv = fm.translateFieldToColumn(parameter.value());
					s.setObject(i, pv);
				}
			}
			return s.executeUpdate();
		});
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
}
