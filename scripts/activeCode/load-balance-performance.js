function run(value, field, querier) {
	
	if(field =="LOAD" || field=="COST" || field=="REALCOST"){
		return value;
	}
	
	var lat = value.get(field);
	var last_assigned = lat.get(4);
	
	var host = HOSTS;
	var region_latency = PERFORMANCE;
	var resource_threshold = 4;
	var replica_performance = [0,1,1,2,5,24,10000];
	var replica_thres = 1250;
	var interval = 250;
	var numRequestPerQuery = 10;
	
	var loadMap = querier.readGuid(null, "LOAD");
	var load = loadMap.get("LOAD");
	var selectedMap = querier.readGuid(null, "SELECTED");
	var selected = selectedMap.get("SELECTED");
	
	if(last_assigned != -1){
		load = load.put(last_assigned, Number(load.get(last_assigned))-numRequestPerQuery);
	}
	
	var estimates = [];
	for(i=0; i<host.length; i=i+2){
		var section = (Number(load.get(i))+numRequestPerQuery)/interval;
		
		var process = replica_performance[Math.floor(section)]+(replica_performance[Math.floor(section)+1]-replica_performance[Math.floor(section)])*(section-Math.floor(section));
		var estimate = process + lat.get(Math.floor(i/2));
		estimates.push(estimate);
	}
	
	var toUpdate = false;
	var index = -1;
	var region = -1;
	
	if(selected.length() == resource_threshold){
		var small = Number.MAX_VALUE;
		var region = -1;
		for(i=0; i<selected.length(); i++){
			if(estimates[selected.get(i)] < small){
				region = selected.get(i);
				small = estimates[selected.get(i)];
			}
		}
		index = region*2;
	}else{
		var newRegion = region_latency.indexOf(region_latency.concat().sort()[selected.length()]);
		var region = -1;
		
		var small = Number.MAX_VALUE;
		var smaller = true;
		for(i=0; i<selected.length(); i++){
			if(estimates[newRegion] > estimates[selected.get(i)]){
				smaller = false;
				if(estimates[selected.get(i)] < small){
					region = selected.get(i);
					small = estimates[selected.get(i)];
				}
			}else{
				if(estimates[newRegion] < small){
					region = newRegion;
					small = estimates[newRegion];
				}
			}		
		}
		if(smaller){
			region = newRegion;
			selected.put(region);
			toUpdate = true;
		}
		index = region*2;		
	}
		
	
	
	var newLoad = load.put(index, Number(load.get(index))+numRequestPerQuery);	
	querier.writeGuid(null, "LOAD", loadMap.put("LOAD",newLoad) );
	querier.writeGuid(null, field, value.put(field, lat.put(4, index)) );
	if(toUpdate){
		querier.writeGuid(null, "SELECTED", selectedMap.put("SELECTED",selected));
	}
	
	return value.put(field, host[index]);
}