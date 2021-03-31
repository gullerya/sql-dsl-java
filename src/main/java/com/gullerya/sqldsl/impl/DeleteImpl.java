package com.gullerya.sqldsl.impl;

import java.util.ArrayList;
import java.util.Collection;

import com.gullerya.sqldsl.api.clauses.Where;
import com.gullerya.sqldsl.api.statements.Delete;

public class DeleteImpl<ET> implements Delete<ET> {
	private final EntityDALImpl.ESConfig<ET> config;

	DeleteImpl(EntityDALImpl.ESConfig<ET> config) {
		this.config = config;
	}

	@Override
	public int delete() {
		return internalDelete(null);
	}

	@Override
	public int delete(Where.WhereClause where) {
		Where.validate(config.em, where);
		return internalDelete(where);
	}

	private int internalDelete(Where.WhereClause where) {
		String sql = "DELETE FROM " + config.em.fqSchemaTableName;
		Collection<Where.WhereFieldValuePair> parametersCollector = new ArrayList<>();
		if (where != null) {
			sql += " WHERE " + where.stringify(parametersCollector);
		}
		return config.preparedStatementAndDo(sql, s -> {
			if (where != null) {
				int i = 0;
				for (Where.WhereFieldValuePair parameter : parametersCollector) {
					i++;
					EntityFieldMetadata fm = config.em.byColumn.get(parameter.column);
					Object pv = parameter.value;
					if (fm.converter != null) {
						pv = fm.converter.convertToDatabaseColumn(pv);
					}
					s.setObject(i, pv);
				}
			}
			return s.executeUpdate();
		});
	}
}
