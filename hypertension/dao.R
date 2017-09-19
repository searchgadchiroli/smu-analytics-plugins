age <- function(from, to) {
  from_lt = as.POSIXlt(from)
  to_lt = as.POSIXlt(to)
  
  age = to_lt$year - from_lt$year
  
  ifelse(to_lt$mon < from_lt$mon |
           (to_lt$mon == from_lt$mon & to_lt$mday < from_lt$mday),
         age - 1, age)
}

getConceptId <- function(mysqlPool, conceptName){
	mysqlPool %>%
	    tbl("concept_name") %>%
	    filter(voided == 0, name==conceptName,
	    	concept_name_type=="FULLY_SPECIFIED") %>%
	    select(concept_id) %>%
	    pull(concept_id)
}

fetchData <- function(mysqlPool, psqlPool, shouldFetchAll, startDate, endDate) {
	variablesToFetch <- list("Systolic",
                           "Diastolic")
	
	htnInitialFormConceptId <- getConceptId(mysqlPool, "HTN Initial")
	patientsWithHTNInitialFormFilled <- mysqlPool %>%
	 	tbl("obs") %>%
	 	filter(voided == 0, concept_id == htnInitialFormConceptId,
	 		obs_datetime < startDate) %>%
	 	select(person_id) %>%
	 	collect(n=Inf)

	patientIdsForInitialFormFilled <- pull(patientsWithHTNInitialFormFilled, person_id)

	patientPresentConceptId <- getConceptId(mysqlPool, "HTN Follow, Is the patient present?")
	obsForPatientPresentConcept <- mysqlPool %>%
	 	tbl("obs") %>%
	 	filter(voided == 0, concept_id == patientPresentConceptId,
	 		person_id %in% patientIdsForInitialFormFilled,
	 		obs_datetime > startDate, obs_datetime<endDate) %>%
	 	select(person_id, concept_id, value_coded, obs_datetime) %>%
	 	collect(n=Inf)


	personIds <- pull(obsForPatientPresentConcept, person_id)
	
  	patients <- mysqlPool %>%
	    tbl("person") %>%
	    filter(voided==0, person_id %in% personIds) %>%
	    select(person_id, gender, birthdate) %>% 
	    collect(n=Inf) %>% 
	    mutate(birthdate = ymd(birthdate)) %>% 
	    rename(Gender = gender) %>% 
	    mutate(Age = age(from=birthdate, to=Sys.Date())) %>% 
	    select(-birthdate)

	patientIdentifiers <- mysqlPool %>% 
		tbl("patient_identifier") %>% 
		filter(voided==0,identifier_type==3, patient_id %in% personIds) %>% 
		select(patient_id, identifier) %>% 
		collect(n=Inf)

	htnFollowUpFormConceptId <- getConceptId(mysqlPool, "HTN Follow-Up")
	followUpFormObsIds <- mysqlPool %>%
		tbl("obs") %>%
		filter(voided == 0, person_id %in% personIds, concept_id==htnFollowUpFormConceptId) %>%
		collect(n=Inf) %>%
		pull(obs_id)
		
	bloodPressureConceptId <- getConceptId(mysqlPool, "Blood Pressure")
	bloodPressureObsIds <- mysqlPool %>%
		tbl("obs") %>%
		filter(voided == 0, obs_group_id %in% followUpFormObsIds, concept_id==bloodPressureConceptId) %>%
		collect(n=Inf) %>%
		pull(obs_id)

	systolicDataConceptId <- getConceptId(mysqlPool, "Systolic Data")
	systolicDataObsId <- mysqlPool %>%
		tbl("obs") %>%
		filter(voided == 0, obs_group_id %in% bloodPressureObsIds, concept_id==systolicDataConceptId) %>%
		collect(n=Inf) %>%
		pull(obs_id)
	
	systolicConceptId <- getConceptId(mysqlPool, "Systolic")
	systolicObsValues <- mysqlPool %>%
		tbl("obs") %>%
		filter(voided == 0, obs_group_id %in% systolicDataObsId,
		 concept_id==systolicConceptId,
		  obs_datetime > startDate, obs_datetime<endDate) %>%
		collect(n=Inf) %>%
		rename(Systolic = value_numeric) %>%
		select(person_id, Systolic) %>%
		collect(n=Inf)

	conceptNames <- mysqlPool %>%
	    tbl("concept_name") %>%
	    filter(voided == 0, concept_name_type=="FULLY_SPECIFIED") %>%
	    select(concept_id,name) %>%
	    collect(n=Inf) 


	patients <- patients %>%
		inner_join(patientIdentifiers, by = c("person_id"="patient_id")) %>%
		inner_join(obsForPatientPresentConcept, by = c("person_id"="person_id")) %>%
		inner_join(conceptNames, by=c("value_coded"="concept_id")) %>%
		left_join(systolicObsValues, by=c("person_id"="person_id")) %>%
		rename(`Patient Present`=name) %>%
		rename(ID=identifier) %>%
		rename(`Visit Date`=obs_datetime) %>%
		select(ID, Gender, Age, `Patient Present`, `Visit Date`, Systolic) %>%
		collect(n=Inf)

	patients
}