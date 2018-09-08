package com.ovgu.dbse.piriyev.output;

import java.util.ArrayList;
import java.util.List;


import com.google.gson.JsonParser;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SAttributeList {

	List<SAttribute> attributes;

	public SAttributeList(String constructor) {
		//Here our homework is to parse this string into a list of attributes
		JsonParser parser = new JsonParser();
		Gson gson = new Gson();
		JsonArray array = parser.parse(constructor).getAsJsonObject().get("attributes").getAsJsonArray();
		attributes = new ArrayList<>();
		for (JsonElement jsonobject:array) {
			attributes.add(gson.fromJson(jsonobject, SAttribute.class));
		}
	}
	@JsonProperty
	public List<SAttribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<SAttribute> attributes) {
		this.attributes = attributes;
	}
	
}
