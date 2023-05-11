// CREATING UNIQUE CONSTRAINTS
CREATE CONSTRAINT FOR (call:Call) REQUIRE call.RowID IS UNIQUE;
CREATE CONSTRAINT FOR (unit:Unit) REQUIRE unit.UnitId IS UNIQUE;
CREATE CONSTRAINT FOR (als:ALS) REQUIRE als.ALSUnit IS UNIQUE;
CREATE CONSTRAINT FOR (reactC:RC) REQUIRE reactC.ReactionCategory IS UNIQUE;
CREATE CONSTRAINT FOR (address:Address) REQUIRE address.Address IS UNIQUE;
CREATE CONSTRAINT FOR (city:CallCity) REQUIRE city.City IS UNIQUE;
CREATE CONSTRAINT FOR (neighborhood:CallNN) REQUIRE neighborhood.NeighborhoodName IS UNIQUE;
CREATE CONSTRAINT FOR (supervisor:CallSD) REQUIRE supervisor.SupervisorDistrict IS UNIQUE;
CREATE CONSTRAINT FOR (station:CallSA) REQUIRE station.StationArea IS UNIQUE;


// CREATING NODES
//------------------------------ CALL NODE ----------------------------------------
//
// 1. Create Call node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (call:Call {RowId: row.RowID,
			callYear:row.Year,
			callMonth:row.Month,
			callTime:row.Time,
			callPrio:row.Priority,
			callRisk:row.CallTypeGroup,
			callCause:row.CallType,
			callDisp:row.CallFinalDisposition
});

// Loading Call Node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.RowID is not null
CALL {
	With row
	MERGE
		(call:Call {RowId: row.RowID})
		ON CREATE SET call.callYear=row.Year,
			   call.callMonth=row.Month,
			   call.callTime=row.Time,
			   call.callPrio=row.Priority,
			   call.callRisk=row.CallTypeGroup,
			   call.callCause=row.CallType,
			   call.callDisp=row.CallFinalDisposition
}
IN TRANSACTIONS OF 500 ROWS;

//------------------------------- UNIT NODE ------------------------------------------
//      
// // 2. Create Unit node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (unit:Unit {unitId: row.UnitId});

// //
// // Loading data into Unit node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.UnitId is not null
CALL {
	With row
	MERGE
		(unit:Unit {unitId: row.UnitId})
}
IN TRANSACTIONS OF 500 ROWS;


//----------------------------- REACTION CATEGORY NODE -----------------------------------
// 3. Create Reaction node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (reactC:RC {reactCategory:row.ReactionCategory});

//
// Loading Data into Reaction Node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.ReactionCategory is not null
CALL {
	With row
	MERGE
		(reactC:RC {reactCategory:row.ReactionCategory})
    }
IN TRANSACTIONS OF 500 ROWS;


//------------------------------ ALS NODE --------------------------------------------

//
// 3. Create ALS node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (als:ALS {alsUnit: row.ALSUnit});

//
// Loading Data into ALS Node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.ALSUnit is not null
CALL {
	With row
	MERGE
		(als:ALS {alsUnit: row.ALSUnit})
    }
IN TRANSACTIONS OF 500 ROWS;

//----------------------------- AREA NODE ---------------------------------------------
//
// 4. Create Area node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (address:Address {callAddress:row.Address});

// Loading Area Node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.Address is not null
CALL {
	With row
	MERGE
		(address:Address {callAddress: row.Address})
    }
IN TRANSACTIONS OF 500 ROWS;

//---------------------------- CITY NODE----------------------------------------------

//
// 5. Create City node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (city:CallCity {callCity: row.City});


//
// Loading data into city node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.City is not null
CALL {
	WITH row
		MERGE (city:CallCity {callCity: row.City})
} 
IN TRANSACTIONS OF 500 ROWS;

//--------------------------- SUPERVISOR DISTRICT NODE --------------------------------

//
// 6. Create Supervisor District node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (supervisor:CallSD {addressSD:row.SupervisorDistrict});

//
// Loading data into supervisor district node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.SupervisorDistrict is not null
CALL {
	WITH row
		MERGE (supervisor:CallSD {addressSD:row.SupervisorDistrict})
} 
IN TRANSACTIONS OF 500 ROWS;

//--------------------------- NEIGHBORHOOD NAME NODE ----------------------------------

//
// 7. Create Neighborhood name node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (neighborhood:CallNN {addressNN:row.NeighborhoodName});

//
// Loading data into neighborhood name node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.NeighborhoodName is not null
CALL {
	WITH row
		MERGE (neighborhood:CallNN {addressNN:row.NeighborhoodName})
} 
IN TRANSACTIONS OF 500 ROWS;


//--------------------------- STATION AREA NODE --------------------------------------

//
// 8. Create Station Area node
:auto LOAD CSV WITH HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
With row limit 1
CREATE (station:CallSA {stationArea:row.StationArea});

//
// Loading data into station area node
:auto LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row with row where 
row.StationArea is not null
CALL {
	WITH row
		MERGE (station:CallSA {stationArea:row.StationArea})
} 
IN TRANSACTIONS OF 500 ROWS;


// CREATING RELATIONSHIPS
-------------------------------------------------------------------------------------------
//
// 1. Relationship between Call node and Unit node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (unit:Unit)
WHERE call.RowId = row.RowID AND unit.unitId = row.UnitId
CREATE (call)-[:ASSIGNED_TO]->(unit); 

//
// 2. Relationship Between Call node and Reaction node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (reactC:RC)
WHERE call.RowId = row.RowID AND reactC.reactCategory=row.ReactionCategory
CREATE (call)-[:REACTION_TIME_IN]->(reactC);

//
// 3. Relationship Between Call node and ALS node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (als:ALS)
WHERE call.RowId = row.RowID AND als.alsUnit = row.ALSUnit
CREATE (call)-[:IS_ALS]->(als);

//
// 4. Relationship Between Call node and Address node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (address:Address)
WHERE call.RowId = row.RowID AND address.callAddress = row.Address
CREATE (call)-[:FIRE_AT]->(address);

//
// 5. Relationship Between Call node and City node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (city:CallCity)
WHERE call.RowId = row.RowID AND city.callCity = row.City
CREATE (call)-[:ADDRESS_IN]->(city);

//
// 6. Relationship Between Call node and Supervisor District node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (supervisor:CallSD)
WHERE call.RowId = row.RowID AND supervisor.addressSD = row.SupervisorDistrict
CREATE (call)-[:SD_ASSOCIATED_WITH]->(supervisor);

//
// 7. Relationship Between Call node and Neighborhood node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (neighborhood:CallNN)
WHERE call.RowId = row.RowID AND neighborhood.addressNN = row.NeighborhoodName
CREATE (call)-[:ND_ASSOCIATED_WITH]->(neighborhood);

//
// 8. Relationship Between Call node and Station Area node
LOAD CSV With HEADERS FROM 'file:///Cleansed_data_FINAL.csv' AS row
MATCH (call:Call), (station:CallSA)
WHERE call.RowId = row.RowID AND station.stationArea = row.StationArea
CREATE (call)-[:SA_ASSOCIATED_WITH]->(station);
