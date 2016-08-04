/**
 * Policy has been debugged by Zhaoyu Gao
 * on Aug 3, 2016
 */
 
function run(value, field, querier) {
	
	if(field =="LOAD" || field=="COST"){
		return value;
	}
	
	var lat = value.get(field);
	 
	var host = HOSTS;
	var region_latency = PERFORMANCE;
	var performance_thres = 200;
	var replica_performance = [0,1,1,2,5,24,10000];
	var replica_thres = 1250;
	var interval = 250;
	
	var costMap = querier.readGuid(null, "COST");
	var cost = costMap.get("COST");
	var loadMap = querier.readGuid(null, "LOAD");
	var load = loadMap.get("LOAD");
	var numRequestPerQuery = 10;
	var candidate = [];
	
	var sections = [];
	var estimates = [];
	
	for(i=0;i<host.length;i++){
		var section = (load.get(i)+numRequestPerQuery)/interval;
		sections.push(section);
		
		var process = replica_performance[Math.floor(section)]+(replica_performance[Math.floor(section)+1]-replica_performance[Math.floor(section)])*(section-Math.floor(section));
		var estimate = process + lat.get(Math.floor(i/2));
		estimates.push(estimate);
		if(estimate<performance_thres){
			candidate.push(i);
		}
	}
		
	var index = -1;
	
	if(candidate.length == 0){
		var ind = region_latency.indexOf(Math.min(region_latency));
		for(i=0;i<2;i++){
			if((load[i+ind*2]+numRequestPerQuery) < replica_thres){
				index = i+ind*2;
				break;
			}
		}
	}else{
		var max_cost = -Number.MAX_VALUE;
		
		for(i=0; i<candidate.length; i++){
			if(cost.get(candidate[i]) > max_cost){
				index = candidate[i];
				max_cost = cost.get(candidate[i]);
			}
		}
	}	
	
	if(index==-1){
		index = Math.floor(Math.random()*host.length);
	}
	
	var newLoad = load.put(index, load.get(index)+numRequestPerQuery);
	var newCost = cost.put(index, cost.get(index)+numRequestPerQuery*60);
	querier.writeGuid(null, "LOAD", loadMap.put("LOAD",newLoad) );
	querier.writeGuid(null, "COST", costMap.put("COST",newCost) );

	
	return value.put(field, host[index]);
}