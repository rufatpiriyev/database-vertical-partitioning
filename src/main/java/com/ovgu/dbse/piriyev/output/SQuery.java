package com.ovgu.dbse.piriyev.output;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SQuery {

	String name;
	
	String type;
	
	int weight;
	
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

	@JsonProperty
	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	@JsonProperty
	public double getSelectivity() {
		return selectivity;
	}

	public void setSelectivity(double selectivity) {
		this.selectivity = selectivity;
	}

	@JsonProperty
	public List<Integer> getProjections() {
		return projections;
	}

	public void setProjections(List<Integer> projections) {
		this.projections = projections;
	}

	@JsonProperty
	public List<Integer> getFilteredColumns() {
		return filteredColumns;
	}

	public void setFilteredColumns(List<Integer> filteredColumns) {
		this.filteredColumns = filteredColumns;
	}

	double selectivity;
	
	List<Integer> projections;
	
	List<Integer> filteredColumns; 
    
    public SQuery(String constructorString) {
    	JsonParser parser = new JsonParser();
		Gson gson = new Gson();
		JsonObject asObject = parser.parse(constructorString).getAsJsonObject();
		this.name=asObject.get("name").getAsString();
		this.type=asObject.get("type").getAsString();
		this.weight=asObject.get("weight").getAsInt();
		this.selectivity=asObject.get("selectivity").getAsDouble();
		
		String csvProjections =asObject.get("projections").getAsString();
		String csvFilteredColumns =asObject.get("filteredColumns").getAsString();
		
		filteredColumns = Arrays.asList(csvFilteredColumns.split(",")).stream().map(it->Integer.parseInt(it)).collect(Collectors.toList());
		projections = Arrays.asList(csvProjections.split(",")).stream().map(it->Integer.parseInt(it)).collect(Collectors.toList());
		
	}
}
