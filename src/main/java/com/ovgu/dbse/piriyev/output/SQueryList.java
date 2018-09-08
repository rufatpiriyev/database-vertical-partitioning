package com.ovgu.dbse.piriyev.output;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class SQueryList {

	List<SQuery> queries;

	public SQueryList(String constructor) {
		//Here our homework is to parse this string into a list of attributes
		JsonParser parser = new JsonParser();
		Gson gson = new Gson();
		JsonArray array = parser.parse(constructor).getAsJsonObject().get("queries").getAsJsonArray();
		queries = new ArrayList<>();
		for (JsonElement jsonobject:array) {
			queries.add(gson.fromJson(jsonobject, SQuery.class));
		}
	}
	@JsonProperty
	public List<SQuery> getQueries() {
		return queries;
	}

	public void setQueries(List<SQuery> queries) {
		this.queries = queries;
	}
}
