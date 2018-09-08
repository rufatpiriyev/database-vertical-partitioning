package com.ovgu.dbse.piriyev.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class STable {
 
	String name;
	
	String type;

	public STable(String constructorString) {
		JsonParser parser = new JsonParser();
		Gson gson = new Gson();
		JsonObject asObject = parser.parse(constructorString).getAsJsonObject();
		this.name=asObject.get("name").getAsString();
		this.type=asObject.get("type").getAsString();
		
	}
	
	@JsonProperty
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@JsonProperty
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
