package com.gullerya.restdsl;

import com.gullerya.restdsl.filter.Filter;
import com.gullerya.restdsl.pagination.Pagination;
import com.gullerya.restdsl.sorter.Sorter;

public class Query {
	public final Filter filter;
	public final Sorter sorter;
	public final Pagination pagination;

	public Query(String input) {
		this.filter = null;
		this.sorter = null;
		this.pagination = null;
	}
}
