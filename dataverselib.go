package dataverselib

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

// GetVersionOfDataset get a specific version of a dataverse dataset.
// It uses Dataverse API https://guides.dataverse.org/en/latest/api/native-api.html#get-version-of-a-dataset
// "$SERVER_URL/api/datasets/:persistentId/versions/{version}"
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - parameters: map[string]interface{} - request parameters (e.g., persistentId, excludeFiles, etc)
//   - version: string - version identifier (e.g., ":latest", ":draft", "1", etc)
//
// Returns:
//   - DatasetVersion struct
//   - error if the request fail
func GetVersionOfDataset(apiClient *ApiClient, parameters map[string]interface{}, version string) (DatasetVersion, error) {
	//curl "https://borealisdata.ca/api/datasets/:persistentId/versions/:latest?excludeFiles=true&persistentId=doi:10.5683/SP3/IXWUWU"
	dv := DatasetVersion{}
	r := RequestResponse{}
	client := apiClient.HttpClient

	u := apiClient.BaseUrl + "/api/datasets/:persistentId/versions/" + version
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(parameters, u, headers, client)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("Error making request: %s %s\n", parameters["persistentId"], err)
		return dv, err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error non-OK HTTP status: %s %s\n", parameters["persistentId"], resp.Status)
		return dv, fmt.Errorf("failed to get dataset version: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		log.Printf("Error decoding response for dataset version: %s %s\n", parameters["persistentId"], err)
		return dv, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &dv)
	} else {
		return dv, fmt.Errorf("Error from server getting dataset version: %s %s", parameters["persistentId"], r.Message)
	}
	return dv, nil
}

// GetContentOfDataverse get content of specific dataverse collection.
// It uses Dataverse API https://guides.dataverse.org/en/latest/api/native-api.html#show-contents-of-a-dataverse-collection
// "$SERVER_URL/api/dataverses/$ID/contents"
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//
// Returns:
//   - array that contains ItemInDataverse elements
//   - error if the request fail
func GetContentOfDataverse(apiClient *ApiClient, dataverseAlias string) ([]ItemInDataverse, error) {
	//curl -H "X-Dataverse-key:$API_TOKEN" "$SERVER_URL/api/dataverses/$ID/contents"
	r := RequestResponse{}
	c := []ItemInDataverse{}
	u := apiClient.BaseUrl + "/api/dataverses/" + dataverseAlias + "/contents"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(nil, u, headers, apiClient.HttpClient)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("Error making request: %s %s\n", dataverseAlias, err)
		return c, err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error non-OK HTTP status: %s %s\n", dataverseAlias, resp.Status)
		return c, fmt.Errorf("Error to get dataverse contents: %s", resp.Status)
	}

	// Process response as needed
	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		log.Printf("Error decoding response for dataverse content: %s %s\n", dataverseAlias, err)
		return c, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &c)
	} else {
		return c, fmt.Errorf(r.Message)
	}

	return c, nil
}

// GetAllDatasetsInDataverseAndSubdataverses get pids and ids of all datasets in specific dataverse collection and its subdataverses.
// It is a recursive function that calls itself for each dataverse collection in the content of the dataverse collection until it reaches the dataset level.
// It uses GetContentOfDataverse(apiClient *ApiClient) ([]ItemInDataverse, error) function
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//   - datasets: *[]MinimalDataset - pointer to an array that contains MinimalDataset elements (id and pid of dataset)
//
// Returns:
//   - error if the request fail
func GetAllDatasetsInDataverseAndSubdataverses(apiClient *ApiClient, dataverseAlias string, datasets *[]MinimalDataset) error {

	c, err := GetContentOfDataverse(apiClient, dataverseAlias)
	if err != nil {
		return err
	}

	for _, item := range c {
		if item.Type == "dataverse" {
			err := GetAllDatasetsInDataverseAndSubdataverses(apiClient, strconv.Itoa(item.Id), datasets)
			if err != nil {
				return err
			}

		} else if item.Type == "dataset" {
			pid := item.Protocol + ":" + item.Authority + item.Separator + item.Identifier

			(*datasets) = append((*datasets), MinimalDataset{Id: item.Id, Pid: pid})
		}
	}
	return nil

}

// GetTotalCount get total count of a search dataverse API.
// It uses dataverse search API, documentation https://guides.dataverse.org/en/latest/api/search.html#
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - parameters: map[string]interface{} - request parameters for search (e.g., q, type, metadata_fields, subtree, etc)
//
// Returns:
//   - total count of the search result
//   - error if the request fail
func GetTotalCount(apiClient *ApiClient, parameters map[string]interface{}) (int, error) {
	parameters["start"] = "0"

	u := apiClient.BaseUrl + "/api/search"

	r := RequestResponse{}
	s := SearchResult{}
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(parameters, u, headers, apiClient.HttpClient)
	defer resp.Body.Close()
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Error to get search: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return 0, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &s)
	} else {
		return 0, fmt.Errorf("Error from server getting search: %s ", r.Message)
	}

	return s.TotalCount, nil
}

func getAllMetadataStartEndSearch(apiClient *ApiClient, parameters map[string]interface{}, jobs <-chan int, results chan<- []SearchItem) {

	for start := range jobs {

		r := RequestResponse{}
		s := SearchResult{}
		fmt.Println(start)
		parameters["start"] = strconv.Itoa(start)

		u := apiClient.BaseUrl + "/api/search"
		headers := map[string]interface{}{
			"X-Dataverse-key": apiClient.ApiToken,
		}
		resp, err := GetRequest(parameters, u, headers, apiClient.HttpClient)
		defer resp.Body.Close()
		if err != nil {
			log.Printf("Error getting request for start:%d, %s\n", start, err)
			return
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error in request status for start:%d, %d\n", start, resp.StatusCode)
			return
		}

		err = json.NewDecoder(resp.Body).Decode(&r)
		if err != nil {
			log.Printf("Error decoding request for start:%d, %s\n", start, err)
			return
		}
		if r.Status == "OK" {
			json.Unmarshal(r.Data, &s)
		} else {
			log.Printf("Error status decoder for start:%d, %s\n", start, r.Status)
			return
		}

		results <- s.Items
	}

}

// GetAllMetadataOfDatasetsInDataverseSearchParallel get datasets metadata from specific dataverse.
// It uses dataverse search API, documentation https://guides.dataverse.org/en/latest/api/search.html#
// It is a parallel version of GetAllMetadataOfDatasetsInDataverseSearch(apiClient *ApiClient, mbList []string) ([]SearchItem, error) function that uses goroutines to get metadata of datasets in parallel. It divides the total count of the search result into batches (numInBatch) and assigns each batch to a goroutine to get the metadata. The results are collected in a channel and combined at the end. The number of goroutines can be controlled by the numOfWorkers parameter.
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//   - mbList: []string - list of metadata blocks to be included in the search
//   - numOfWorkers: int - limit the number of goroutines for parallel processing
//   - numInBatch: int - number of per page in search (e.g., 300)
//
// Returns:
//   - Search result is an array that contains SearchItem elements, which include global_id, identifier_of_dataverse, and metadata_blocks of each dataset in the search result
//   - error if the request fail
func GetAllMetadataOfDatasetsInDataverseSearchParallel(apiClient *ApiClient, dataverseAlias string, mbList []string, numOfWorkers int, numInBatch int) ([]SearchItem, error) {

	allItems := make([]SearchItem, 0)
	mbListPar := make([]string, 0)
	for _, mb := range mbList {
		mbField := mb + ":*"
		mbListPar = append(mbListPar, mbField)
	}
	parameters := map[string]interface{}{
		"q":               "*",
		"type":            "dataset",
		"metadata_fields": mbListPar,
		"subtree":         dataverseAlias,
		"per_page":        "1",
	}

	totalCount, err := GetTotalCount(apiClient, parameters)
	if err != nil {
		return nil, err
	}

	numbOfRoutines := totalCount / numInBatch
	if numInBatch*numbOfRoutines < totalCount {
		numbOfRoutines = numbOfRoutines + 1
	}

	numOfJobs := numbOfRoutines
	jobs := make(chan int, numOfJobs)
	results := make(chan []SearchItem, numOfJobs)
	//limiter := time.Tick(20 * time.Second)

	for batch := 0; batch < numOfWorkers; batch++ {

		start := batch * numInBatch
		if start > totalCount-numInBatch {
			break
		}

		parameters = map[string]interface{}{
			"q":               "*",
			"type":            "dataset",
			"metadata_fields": mbListPar,
			"subtree":         dataverseAlias,
			"per_page":        strconv.Itoa(numInBatch),
			"start":           strconv.Itoa(start),
		}

		go getAllMetadataStartEndSearch(apiClient, parameters, jobs, results)
	}

	// send jobs
	for j := 0; j < numOfJobs; j++ {
		jobs <- j * numInBatch
	}
	close(jobs)

	// collect results
	for a := 0; a < numOfJobs; a++ {
		items := <-results
		allItems = append(allItems, items...)
	}

	log.Println("Total length:", len(allItems))

	return allItems, nil

}

// GetSpecificMetadataOfDatasetsInDataverseSearchParallel get datasets metadata from specific dataverse with search string.
// It uses dataverse search API, documentation https://guides.dataverse.org/en/latest/api/search.html#
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//   - mbList: []string - list of metadata blocks to be included in the search
//   - numOfWorkers: int - limit the number of goroutines for parallel processing
//   - numInBatch: int - number of per page in search (e.g., 300)
//   - searchStr: string - search string for the search query (e.g., "title:water")
//
// Returns:
//   - Search result is an array that contains SearchItem elements, which include global_id, identifier_of_dataverse, and metadata_blocks of each dataset in the search result
//   - error if the request fail
func GetSpecificMetadataOfDatasetsInDataverseSearchParallel(apiClient *ApiClient, dataverseAlias string, mbListPar []string, numOfWorkers int, numInBatch int, searchStr string) ([]SearchItem, error) {

	allItems := make([]SearchItem, 0)

	parameters := map[string]interface{}{
		"q":               searchStr,
		"type":            "dataset",
		"metadata_fields": mbListPar,
		"subtree":         dataverseAlias,
		"per_page":        "1",
	}

	totalCount, err := GetTotalCount(apiClient, parameters)

	if err != nil {
		return nil, err
	}

	numbOfRoutines := totalCount / numInBatch
	if numInBatch*numbOfRoutines < totalCount {
		numbOfRoutines = numbOfRoutines + 1
	}

	numOfJobs := numbOfRoutines
	jobs := make(chan int, numOfJobs)
	results := make(chan []SearchItem, numOfJobs)
	//limiter := time.Tick(20 * time.Second)

	for batch := 0; batch < numOfWorkers; batch++ {

		start := batch * numInBatch
		if start > totalCount-numInBatch {
			break
		}

		parameters = map[string]interface{}{
			"q":               searchStr,
			"type":            "dataset",
			"metadata_fields": mbListPar,
			"subtree":         dataverseAlias,
			"per_page":        strconv.Itoa(numInBatch),
			"start":           strconv.Itoa(start),
		}

		go getAllMetadataStartEndSearch(apiClient, parameters, jobs, results)
	}

	// send jobs
	for j := 0; j < numOfJobs; j++ {
		jobs <- j * 300
	}
	close(jobs)
	// collect results
	for a := 0; a < numOfJobs; a++ {
		items := <-results
		allItems = append(allItems, items...)

	}

	log.Println("Total length:", len(allItems))

	return allItems, nil

}

// GetAllMetadataOfDatasetsInDataverseSearch get datasets metadata from specific dataverse.
// It uses dataverse search API, documentation https://guides.dataverse.org/en/latest/api/search.html#
// It is a not parallel version of GetAllMetadataOfDatasetsInDataverseSearchParallel(apiClient *ApiClient, mbList []string, numOfWorkers int, numInBatch int) ([]SearchItem, error) function that gets metadata of datasets sequentially by iterating through the search result with start and numInBatch parameters until it reaches the end of the search result. It is simpler than the parallel version but may take longer time to get the metadata if the search result is large.
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//   - mbList: []string - list of metadata blocks to be included in the search
//   - numInBatch: int - number of per page in search (e.g., 300). maximum is 1000 according to dataverse API documentation.
//
// Returns:
//   - Search result is an array that contains SearchItem elements, which include global_id, identifier_of_dataverse, and metadata_blocks of each dataset in the search result
//   - error if the request fail
func GetAllMetadataOfDatasetsInDataverseSearch(apiClient *ApiClient, dataverseAlias string, mbList []string, numInBatch int) ([]SearchItem, error) {
	// curl "https://borealisdata.ca/api/search?q=*&type=dataset&metadata_fields=geospatial:*&metadata_fields=citation:*&subtree=international"
	r := RequestResponse{}
	s := SearchResult{}
	allItems := make([]SearchItem, 0)

	mbListPar := make([]string, 0)
	for _, mb := range mbList {
		mbField := mb + ":*"
		mbListPar = append(mbListPar, mbField)
	}

	start := 0
	for {

		parameters := map[string]interface{}{
			"q":               "*",
			"type":            "dataset",
			"metadata_fields": mbListPar,
			"subtree":         dataverseAlias,
			"per_page":        strconv.Itoa(numInBatch),
			"start":           strconv.Itoa(start),
		}

		u := apiClient.ApiToken + "/api/search"
		headers := map[string]interface{}{
			"X-Dataverse-key": apiClient.ApiToken,
		}
		resp, err := GetRequest(parameters, u, headers, apiClient.HttpClient)
		defer resp.Body.Close()
		if err != nil {
			return allItems, err
		}

		if resp.StatusCode != http.StatusOK {
			return allItems, fmt.Errorf("Error to get search: %s", resp.Status)
		}

		err = json.NewDecoder(resp.Body).Decode(&r)
		if err != nil {
			return allItems, err
		}
		if r.Status == "OK" {
			json.Unmarshal(r.Data, &s)
		} else {
			return allItems, fmt.Errorf("Error from server getting search: %s ", r.Message)
		}
		for _, v := range s.Items {
			allItems = append(allItems, v)
		}
		start = start + len(s.Items)
		if start >= s.TotalCount {
			break
		}
	}

	return allItems, nil
}

// GetListOfMetadataBlocksOfDataverse get list of all metadatablocks for specific dataverse.
// It uses dataverse native API, documentation https://guides.dataverse.org/en/latest/api/native-api.html#list-metadata-blocks-defined-on-a-dataverse-collection
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//   - parameters: map[string]interface{} - request parameters for the API (e.g., returnDatasetFieldTypes, onlyDisplayedOnCreate, datasetType, etc)
//
// Returns:
//   - list of metadata blocks defined on the dataverse collection, which can be used in the search API to get specific metadata of datasets in the dataverse collection
//   - a dictionary that maps display name of metadata block to its name, which can be used to get the name of metadata block from its display name in the dataverse collection
//   - error if the request fail
func GetListOfMetadataBlocksOfDataverse(apiClient *ApiClient, dataverseAlias string, parameters map[string]interface{}) ([]string, map[string]string, error) {
	//curl -H "X-Dataverse-key:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" "https://demo.dataverse.org/api/dataverses/root/metadatablocks?returnDatasetFieldTypes=true&onlyDisplayedOnCreate=true&datasetType=software"
	client := apiClient.HttpClient
	r := RequestResponse{}
	metadatablocks := []string{}
	metaBlocsDict := make(map[string]string)
	u := apiClient.BaseUrl + "/api/dataverses/" + dataverseAlias + "/metadatablocks"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(parameters, u, headers, client)
	if err != nil {
		return metadatablocks, metaBlocsDict, err
	}

	defer resp.Body.Close()
	if err != nil {
		return metadatablocks, metaBlocsDict, err
	}

	if resp.StatusCode != http.StatusOK {
		return metadatablocks, metaBlocsDict, fmt.Errorf("Error to get search: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return metadatablocks, metaBlocsDict, err
	}
	mbList := make([]MetadataBlockItem, 0)
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &mbList)
	} else {
		return metadatablocks, metaBlocsDict, fmt.Errorf("Error from server getting list of metadatablocks: %s ", r.Message)
	}
	for _, v := range mbList {
		metadatablocks = append(metadatablocks, v.Name)
		metaBlocsDict[v.DisplayName] = v.Name
	}
	return metadatablocks, metaBlocsDict, nil
}

// GetExportMetadataOfDataset exports metadata in provided format.
// It uses dataverse Native API, documentation https://guides.dataverse.org/en/latest/api/native-api.html#export-metadata-of-a-dataset-in-various-formats
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - persistentId: string - persistent identifier of the dataset (e.g., "doi:10.5072/FK2/J8SJZB")
//   - exporterFormat: string - format to export metadata (e.g., "ddi", "json", etc)
//   - published: bool - whether to export published version or draft version of the dataset. If true, it will export published version; if false, it will export draft version.
//
// Returns:
//   - exported metadata in bytes, which can be saved as a file or processed further
//   - error if the request fail
func GetExportMetadataOfDataset(apiClient *ApiClient, persistentId string, exporterFormat string, published bool) ([]byte, error) {
	//curl -H "X-Dataverse-key: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" "https://demo.dataverse.org/api/datasets/export?exporter=ddi&persistentId=doi:10.5072/FK2/J8SJZB&version=:draft"
	var requestParameters map[string]interface{}
	url := apiClient.BaseUrl + "/api/datasets/export?exporter=" + exporterFormat + "&persistentId=" + persistentId
	if !published {
		url = url + "&version=:draft"
	}
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(requestParameters, url, headers, apiClient.HttpClient)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error to export metadata: %s", resp.Status)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return bodyBytes, nil
}
