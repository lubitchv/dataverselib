package dataverselib

import (
	"archive/zip"
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
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
	headers := map[string]interface{}{}
	if apiClient.ApiToken != "" {
		headers = map[string]interface{}{
			"X-Dataverse-key": apiClient.ApiToken,
		}
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
	log.Println("Starting getAllMetadataStartEndSearch")
	for start := range jobs {

		r := RequestResponse{}
		s := SearchResult{}
		fmt.Println(start)
		parameters["start"] = strconv.Itoa(start)

		u := apiClient.BaseUrl + "/api/search"
		headers := map[string]interface{}{}
		if apiClient.ApiToken != "" {
			headers = map[string]interface{}{
				"X-Dataverse-key": apiClient.ApiToken,
			}
		}

		resp, err := GetRequest(parameters, u, headers, apiClient.HttpClient)
		defer resp.Body.Close()
		if err != nil {
			log.Printf("Error getting request for start:%d, %s\n", start, err)
			return
		}
		log.Println("good")

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error in request status for start:%d, %d\n", start, resp.StatusCode)
			return
		}

		err = json.NewDecoder(resp.Body).Decode(&r)
		if err != nil {
			log.Printf("Error decoding request for start:%d, %s\n", start, err)
			return
		}
		log.Println(r.Status)
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
	log.Println(totalCount)
	if err != nil {
		return nil, err
	}

	numbOfRoutines := totalCount / numInBatch
	log.Println("number of routines:", numbOfRoutines)
	if numInBatch*numbOfRoutines < totalCount {
		numbOfRoutines = numbOfRoutines + 1
	}
	log.Println("number of routines:", numbOfRoutines)
	numOfJobs := numbOfRoutines
	jobs := make(chan int, numOfJobs)
	results := make(chan []SearchItem, numOfJobs)
	//limiter := time.Tick(20 * time.Second)
	log.Println("Number of workers:", numOfWorkers)
	for batch := 0; batch < numOfWorkers; batch++ {

		start := batch * numInBatch
		log.Println(totalCount - numInBatch)
		if start > totalCount-numInBatch && batch != 0 {
			log.Println("finish break ", start)
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
	log.Println("number of Jobs:", numOfJobs)
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
	log.Printf("Start getting metadata with search string: %s\n", searchStr)
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
	if totalCount <= numInBatch {
		numInBatch = totalCount
	}
	numbOfRoutines := totalCount / numInBatch
	if numInBatch*numbOfRoutines < totalCount {
		numbOfRoutines = numbOfRoutines + 1
	}

	log.Println("number of routines", numbOfRoutines)
	log.Println("number of workers", numOfWorkers)

	n := math.Min(float64(numOfWorkers), float64(numbOfRoutines))
	numOfWorkers = int(n)

	log.Println("New number of workers", numOfWorkers)

	numOfJobs := numbOfRoutines
	jobs := make(chan int, numOfJobs)
	results := make(chan []SearchItem, numOfJobs)
	//limiter := time.Tick(20 * time.Second)

	for batch := 0; batch < numOfWorkers; batch++ {

		start := batch * numInBatch
		log.Println("Start:", start)
		if start > totalCount-numInBatch {
			log.Println("finish break")
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

		u := apiClient.BaseUrl + "/api/search"
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

func GetDatasetFromSearch(apiClient *ApiClient, q string, dvType string) ([]SearchItem, error) {
	r := RequestResponse{}
	s := SearchResult{}

	parameters := map[string]interface{}{
		"q":    q,
		"type": dvType,
	}

	u := apiClient.BaseUrl + "/api/search"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(parameters, u, headers, apiClient.HttpClient)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Errorf("Error to get search: %s", resp.Status)
		return nil, fmt.Errorf("Error to get search: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return nil, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &s)
	}
	log.Println(s)
	if len(s.Items) > 1 {
		return s.Items, fmt.Errorf("Error more than 1 result found for query: %s", q)
	}
	if len(s.Items) == 0 {
		return s.Items, fmt.Errorf("Error no result found for query: %s", q)
	}

	return s.Items, nil

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
	headers := map[string]interface{}{}
	if apiClient.ApiToken != "" {
		headers = map[string]interface{}{
			"X-Dataverse-key": apiClient.ApiToken,
		}
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

func flatCompoundClass(c map[string]interface{}, column string, columnsMap *map[string]int, headers *[]string) string {
	var flatValues string = ""
	var currentFieldInCompound int = 0
	for _, raw := range c {

		if inner, ok := raw.(map[string]interface{}); ok {
			if typeName, ok := inner["typeName"]; ok {
				columnIndx, ok := (*columnsMap)[column]
				if !ok {
					columnIndx = len(*columnsMap)
					(*columnsMap)[column] = columnIndx
					*headers = append(*headers, column)
				}

				if currentFieldInCompound == 0 {
					flatValues += fmt.Sprintf("{")
				}
				flatValues += fmt.Sprintf("'%s':'%s'", typeName, inner["value"].(string))

				if currentFieldInCompound < len(c)-1 {
					flatValues += fmt.Sprintf(",")
				}

				if currentFieldInCompound == len(c)-1 {
					flatValues += fmt.Sprintf("}")
				}
				currentFieldInCompound++
			}
		}
	}
	return flatValues
}

// ConvertMetadataToCSVFormat converts and flattens search metadata into csv, one dataset - one line.
//
// Parameters:
//   - datasets: []SearchItem - array that contains SearchItem elements with metadata blocks of each dataset
//   - mbDict: map[string]string - dictionary that maps display name of metadata block to its name, which can be used to get the name of metadata block from its display name in the dataverse collection
//
// Returns:
//   - Search result is an array that contains map of column name and value for each dataset, which can be used to create a csv file with columns of metadata fields and rows of datasets
//   - array of column names for the csv file, which can be used as header of the csv file
func ConvertMetadataToCSVFormat(datasets []SearchItem, mbDict map[string]string) ([]map[string]string, []string) {
	var columnsMap map[string]int = make(map[string]int) //columns of csv file
	var records []map[string]string = make([]map[string]string, 0)
	var headers []string = make([]string, 0)

	headers = append(headers, "persistentId") // first column is always persistentId

	for _, ds := range datasets {

		var record map[string]string = make(map[string]string)

		//fmt.Println(ds.GlobalId)

		record["persistentId"] = ds.GlobalId

		metadata := ds.MetadataBlocks
		for _, mb := range metadata { // for each metadata block
			fields := mb.Fields
			for _, field := range fields {
				//fmt.Printf("Field TypeName: %s, Value: %+v, Multiple: %t, TypeClass: %s\n", field.TypeName, field.Value, field.Multiple, field.TypeClass)
				if mbDict != nil {
					mb.Name = mbDict[mb.DisplayName]
				}
				column := mb.Name + ":" + field.TypeName
				if field.TypeClass == "primitive" || field.TypeClass == "controlledVocabulary" {
					columnIndx, ok := columnsMap[column]
					flatValues := ""
					if field.Multiple {
						for indx, v := range field.Value.([]interface{}) {
							if indx > 0 {
								flatValues += "|"
							}
							flatValues += fmt.Sprintf("%s", v)
						}
					} else {
						flatValues = fmt.Sprintf("%s", field.Value)
					}

					if ok {
						record[column] = flatValues
					} else {
						columnIndx = len(columnsMap)
						columnsMap[column] = columnIndx
						headers = append(headers, column)
						record[column] = flatValues
					}

				} else if field.TypeClass == "compound" {
					flatValues := ""
					if field.Multiple {
						entry, ok := field.Value.([]interface{}) // it is an array of values
						if ok {
							for indx, v := range entry { // for each value in the array
								c, _ := v.(map[string]interface{}) // value is string: class
								if indx > 0 {
									flatValues += "|"
								}
								flatValues += flatCompoundClass(c, column, &columnsMap, &headers)
							}
						} else {
							log.Printf("  Unexpected type for single compound field: %T\n", field.Value)
						}

					} else {
						c, ok := field.Value.(map[string]interface{}) // it is a single compound class
						if ok {
							flatValues += flatCompoundClass(c, column, &columnsMap, &headers)
						}
					}
					record[column] = flatValues
				}
			}

		}
		records = append(records, record)

	}
	return records, headers
}

// GetMetadataBlockField gets metadatablock fields for specific metadatablock.
// It uses dataverse native API, documentation https://guides.dataverse.org/en/latest/api/native-api.html#show-info-about-single-metadata-block
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - metadataBlockName: string - name of the metadata block (e.g., "geospatial", "citation", etc)
//
// Returns:
//   - MetadataBlockInfo struct, which contains name of the metadata block and its fields, which can be used to get the metadata fields of the metadata block for further processing
//   - error if the request fail
func GetMetadataBlockFields(apiClient *ApiClient, metadataBlockName string) (MetadataBlockInfo, error) {
	//$SERVER/api/metadatablocks/geospatial
	log.Println("Start getting metadata block fields for:", metadataBlockName)
	client := apiClient.HttpClient
	r := RequestResponse{}
	mb := MetadataBlockInfo{}

	u := apiClient.BaseUrl + "/api/metadatablocks/" + metadataBlockName
	log.Println(u)
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}

	resp, err := GetRequest(nil, u, headers, client)
	if err != nil {
		log.Printf("Error making request: %s\n", err)
		return mb, err
	}
	defer resp.Body.Close()
	if err != nil {
		return mb, err
	}
	log.Println(resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return mb, fmt.Errorf("Error to get search: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return mb, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &mb)
	} else {
		return mb, fmt.Errorf("Error from server getting search: %s ", r.Message)
	}
	log.Println(mb.Fields)
	return mb, nil

}

// CreateHeadersCSVMetadata creates an array of column names for the csv file, which can be used as header of the csv file, based on the metadata blocks and their fields defined in the dataverse collection.
//
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//
// Returns:
//   - MetadataBlockInfo struct, which contains name of the metadata block and its fields, which can be used to get the metadata fields of the metadata block for further processing
//   - error if the request fail
func CreateHeadersCSVMetadata(apiClient *ApiClient, dataverseAlias string) ([]string, error) {
	headers := []string{}
	mbList, _, err := GetListOfMetadataBlocksOfDataverse(apiClient, dataverseAlias, nil)
	if err != nil {
		log.Printf("Error getting metadata blocks for dataverse %s: %s\n", dataverseAlias, err)
		return headers, err
	}
	for _, mb := range mbList {
		fmt.Println(mb)
		mbInfo, err := GetMetadataBlockFields(apiClient, mb)
		if err != nil {
			log.Printf("Error getting metadata block fields for metadata block %s: %s\n", mb, err)
			return headers, err
		}
		for _, field := range mbInfo.Fields {
			column := mb + ":" + field.Name
			headers = append(headers, column)
		}
	}

	return headers, nil
}

func CreateDatasetWithJson(apiClient *ApiClient, dataverseAlias string, parameters map[string]interface{}, jsonData []byte) (MinimalDataset, error) {
	// curl -H "X-Dataverse-key:$API_TOKEN" -X POST "$SERVER_URL/api/dataverses/$PARENT/datasets?doNotValidate=true" --upload-file dataset-incomplete.json -H 'Content-type:application/json'
	r := RequestResponse{}
	dataset := MinimalDataset{}
	url := apiClient.BaseUrl + "/api/dataverses/" + dataverseAlias + "/datasets"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
		"Content-type":    "application/json",
	}
	fmt.Printf("%s\n", string(jsonData))

	resp, err := PostRequest(parameters, url, headers, apiClient.HttpClient, jsonData)
	if err != nil {
		log.Println(err)
		return dataset, err
	}

	defer resp.Body.Close()
	if err != nil {
		return dataset, err
	}
	fmt.Println(resp.StatusCode)
	if resp.StatusCode != 201 {
		bodyBytes, err := io.ReadAll(resp.Body)

		fmt.Println("Status:", resp.Status)
		fmt.Println("Response:", string(bodyBytes))
		if err != nil {
			return dataset, err
		}
		return dataset, fmt.Errorf("Error CreateDatasetWithJson: %s, %d\n", string(bodyBytes), resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return dataset, err
	}

	if r.Status == "OK" {
		json.Unmarshal(r.Data, &dataset)
	} else {
		return dataset, fmt.Errorf("Error from server getting versions of dataset: %s ", r.Message)
	}
	log.Println(r)
	log.Println("++++++++++++")
	log.Println(dataset)
	return dataset, nil
}
func FindRequiredFields(metaFields []MetadataBlockInfo) (map[string][]string, error) {
	requiredFields := make(map[string][]string)
	for _, mb := range metaFields {
		for _, field := range mb.Fields {
			if field.IsRequired {
				requiredFields[mb.Name+":"+field.Name] = make([]string, 0)
				if field.ChildFields != nil {
					for _, child := range field.ChildFields {
						if child.IsRequired {
							requiredFields[mb.Name+":"+field.Name] = append(requiredFields[mb.Name+":"+field.Name], child.Name)
						}
					}
				}
			}
		}
	}
	return requiredFields, nil
}

func CheckMandatoryFieldsInHeaders(metaFields []MetadataBlockInfo, headers []string) ([]string, error) {
	requiredFieldsMissing := make([]string, 0)
	requiredFields, err := FindRequiredFields(metaFields)
	if err != nil {
		return requiredFieldsMissing, err
	}

	set := make(map[string]struct{})

	// make set from headers
	for _, element := range headers {
		set[element] = struct{}{}
	}
	for field, _ := range requiredFields {
		if _, exists := set[field]; !exists {
			requiredFieldsMissing = append(requiredFieldsMissing, field)
		}
	}
	return requiredFieldsMissing, nil

}
func CreateMapBlockFields(metaFields []MetadataBlockInfo) (map[string]map[string]MetadataFieldItem, error) {
	blockFields := make(map[string]map[string]MetadataFieldItem)
	for _, mb := range metaFields {
		blockFields[mb.Name] = mb.Fields
	}
	return blockFields, nil
}

func CreateDataverseDatasetVersionStruct(metaFields []MetadataBlockInfo, headers []string, row []string) (DatasetVersion, error) {
	datasetVersion := DatasetVersion{}
	datasetVersion.MetadataBlocks = make(map[string]MetadataBlock)
	mainMissingMandatoryFields, err := CheckMandatoryFieldsInHeaders(metaFields, headers)
	if err != nil {
		return datasetVersion, err
	}
	if len(mainMissingMandatoryFields) > 0 {
		return datasetVersion, fmt.Errorf("Missing mandatory fields in headers: %v", mainMissingMandatoryFields)
	}

	blockFields, _ := CreateMapBlockFields(metaFields)
	metadataBlocksNames := make(map[string]struct{})
	var metadatablock MetadataBlock
	for i, column := range headers {
		value := row[i]
		split := strings.Split(column, ":")
		if len(split) != 2 {
			return datasetVersion, fmt.Errorf("Invalid column name: %s", column)
		}

		if _, ok := metadataBlocksNames[split[0]]; !ok {
			metadataBlocksNames[split[0]] = struct{}{}
			metadatablock = MetadataBlock{
				Name:   split[0],
				Fields: make([]MetadataField, 0),
			}
			//datasetVersion.MetadataBlocks[split[0]] = metadatablock
		}
		if fields, ok := blockFields[split[0]]; ok {

			if field, ok := fields[split[1]]; ok {
				mf := MetadataField{
					TypeName:  field.Name,
					TypeClass: field.TypeClass,
					Multiple:  field.Multiple,
					Value:     nil,
				}
				if field.TypeClass == "primitive" || field.TypeClass == "controlledVocabulary" {
					if field.Multiple {
						finalValueStringArray := strings.Split(value, "|")
						mf.Value = finalValueStringArray

						//metadatablock.Fields = append(metadatablock.Fields, mf)

					} else {
						mf.Value = value
						//metadatablock.Fields = append(metadatablock.Fields, mf)

					}

				} else if field.TypeClass == "compound" {

					if field.Multiple {
						valueClassArray := make([]map[string]interface{}, 0)
						elements := strings.Split(value, "|")
						for _, element := range elements {
							m := make(map[string]string)
							err := json.Unmarshal([]byte(element), &m)
							if err != nil {
								return datasetVersion, fmt.Errorf("Error unmarshalling compound field value: %s", err)
							}
							var valueClassMult map[string]interface{} = make(map[string]interface{})
							for key, _ := range m {
								if _, ok := field.ChildFields[key]; !ok {
									return datasetVersion, fmt.Errorf("Child field %s not found in metadata block %s for compound field %s", key, split[0], split[1])
								} else {
									childField := MetadataField{
										TypeName:  key,
										TypeClass: field.ChildFields[key].TypeClass,
										Multiple:  field.ChildFields[key].Multiple,
										Value:     m[key],
									}
									valueClassMult[key] = childField

								}
							}
							valueClassArray = append(valueClassArray, valueClassMult)

						}
						mf.Value = valueClassArray
						//metadatablock.Fields = append(metadatablock.Fields, mf)
					} else {
						element := value
						m := make(map[string]string)
						err := json.Unmarshal([]byte(element), &m)
						if err != nil {
							return datasetVersion, fmt.Errorf("Error unmarshalling compound field value: %s", err)
						}
						var valueClassMult map[string]interface{} = make(map[string]interface{})
						for key, _ := range m {
							if _, ok := field.ChildFields[key]; !ok {
								return datasetVersion, fmt.Errorf("Child field %s not found in metadata block %s for compound field %s", key, split[0], split[1])
							} else {
								childField := MetadataField{
									TypeName:  key,
									TypeClass: field.ChildFields[key].TypeClass,
									Multiple:  field.ChildFields[key].Multiple,
									Value:     m[key],
								}
								valueClassMult[key] = childField
							}
						}
						mf.Value = valueClassMult
					}

				} else {
					return datasetVersion, fmt.Errorf("Field %s not found in metadata block %s", split[1], split[0])
				}
				metadatablock.Fields = append(metadatablock.Fields, mf)
			}
		}
		datasetVersion.MetadataBlocks[split[0]] = metadatablock

	}

	return datasetVersion, nil
}

func CreateDataset(apiClient *ApiClient, dataverseAlias string, datasetVersion DatasetVersion) (MinimalDataset, error) {
	minDataset := MinimalDataset{}
	//default license CC0 1.0
	log.Println(datasetVersion.TermsOfUse)
	if datasetVersion.License.Name == "" && datasetVersion.License.Uri == "" {
		datasetVersion.License.Name = "CC0 1.0"
		datasetVersion.License.Uri = "http://creativecommons.org/publicdomain/zero/1.0"
	}

	dataset := CreateDatasetItem{
		DatasetVersionField: datasetVersion,
	}

	jsonData, err := json.Marshal(dataset)
	if err != nil {
		return minDataset, fmt.Errorf("Error marshalling dataset version: %s", err)
	}
	minDataset, err = CreateDatasetWithJson(apiClient, dataverseAlias, nil, jsonData)
	if err != nil {
		return minDataset, fmt.Errorf("Error creating dataset with json: %s", err)
	}

	return minDataset, nil
}

// GetListOfMandatoryFieldsOfDataverse returns a map of blocks with mandatory fields for specific dataverse collection.
// Parameters:
//   - apiClient: *ApiClient - Dataverse API client (BaseUrl, ApiToken, HttpClient)
//   - dataverseAlias: string - alias of the dataverse collection (e.g., "root", "international", etc)
//
// Returns:
//   - a map of blocks, each block has an array of mandatory fields.
//     Each such field is represented as a map where the key is the field name and the value is an array of its mandatory child fields (if any).
//     If field is mandatory but there are no mandatory child fields, the array of child fields is empty.
//     For example, citation block can have an array
//
// [map[datasetContact:[datasetContactEmail]] map[title:[]] map[dsDescription:[dsDescriptionValue]] map[subject:[]] map[author:[authorName]]]
//   - error if the request fail
func GetListOfMandatoryFieldsOfDataverse(apiClient *ApiClient, dataverseAlias string) (map[string][]map[string][]string, error) {
	requiredFields := make(map[string][]map[string][]string)
	mb, _, err := GetListOfMetadataBlocksOfDataverse(apiClient, dataverseAlias, nil)
	if err != nil {
		return nil, err
	}
	log.Println("Starting treating mb for dataverse:", dataverseAlias)
	for _, mbName := range mb {

		mbInfo, err := GetMetadataBlockFields(apiClient, mbName)
		blockFields := make([]map[string][]string, 0)
		if err != nil {
			return nil, err
		}
		for _, field := range mbInfo.Fields {

			if field.IsRequired {
				childArray := make([]string, 0)

				if field.ChildFields != nil {

					for _, child := range field.ChildFields {
						if child.IsRequired {

							childArray = append(childArray, child.Name)
						}
					}
				}
				fields := make(map[string][]string)
				fields[field.Name] = childArray
				blockFields = append(blockFields, fields)
			}
		}
		requiredFields[mbName] = blockFields
	}
	return requiredFields, nil
}

func GetVersionsOfDataset(apiClient *ApiClient, parameters map[string]interface{}) ([]DatasetVersion, error) {
	versions := make([]DatasetVersion, 0)
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/versions"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	r := RequestResponse{}
	resp, err := GetRequest(parameters, url, headers, apiClient.HttpClient)
	if err != nil {
		return versions, err
	}
	defer resp.Body.Close()
	if err != nil {
		return versions, err
	}
	log.Println(resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return versions, fmt.Errorf("Error to get versions of dataset: %s", resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return versions, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &versions)
	} else {
		return versions, fmt.Errorf("Error from server getting versions of dataset: %s ", r.Message)
	}
	return versions, nil
}
func AddFileToDataset(apiClient *ApiClient, parameters map[string]interface{}, filePath string, jsonData AddFileMetadata) error {
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/add"
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return err
	}

	resp, err := PostRequestMultiPartJsonAndFile(parameters, url, headers, apiClient.HttpClient, filePath, string(jsonBytes), "POST")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Println("Status:", resp.Status)
	fmt.Println("Response:", string(respBody))
	return nil
}

func UpdateFileMetadata(apiClient *ApiClient, parameters map[string]interface{}, jsonData UpdateFileMetadataStruct, fileId int) error {
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
		"Content-Type":    "application/json",
	}
	fileIdString := strconv.Itoa(fileId)
	url := apiClient.BaseUrl + "/api/files/" + fileIdString + "/metadata"
	log.Println(url)
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return err
	}

	resp, err := PostRequestMultiPartJsonAndFile(parameters, url, headers, apiClient.HttpClient, "", string(jsonBytes), "POST")

	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Println("Status:", resp.Status)
	fmt.Println("Response:", string(respBody))
	return nil
}
func GetFileFromDataset(apiClient *ApiClient, parameters map[string]interface{}, fileId int) ([]byte, error) {
	// /api/access/datafile/$id
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	url := apiClient.BaseUrl + "/api/access/datafile/" + strconv.Itoa(fileId)
	resp, err := GetRequest(parameters, url, headers, apiClient.HttpClient)
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}

	log.Println(resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error to get file: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)

	return body, nil
}

func zipBytes(filename string, data []byte) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	w, err := zw.Create(filename)
	if err != nil {
		return nil, err
	}

	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}

	err = zw.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func AddFileToDatasetFromMemory(apiClient *ApiClient, parameters map[string]interface{}, fileBytes []byte, jsonData AddFileMetadata) (int, error) {
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/add"

	contentType := http.DetectContentType(fileBytes)
	log.Println(contentType)

	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return 0, err
	}
	var z []byte = nil
	if contentType == "application/zip" {
		log.Println("This is zip file")
		z, err = zipBytes("filename", fileBytes)
		if err != nil {
			return 0, err
		}

	} else {
		z = fileBytes
	}
	log.Println("ADDING FILE")
	resp, err := PostRequestMultiPartJsonAndFileFromMemory(parameters, url, headers, apiClient.HttpClient, z, string(jsonBytes), "POST", contentType)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	r := RequestResponse{}
	f := Files{}

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return 0, err
	}
	if r.Status == "OK" {
		json.Unmarshal(r.Data, &f)
	} else {
		return 0, fmt.Errorf("Error for AddFileToDatasetFromMemory: %s ", r.Message)
	}

	return f.Files[0].DataFile.Id, nil
}
func deleteFileFromDataset(apiClient *ApiClient, fileId int) error {

}

func DeleteFilesFromDataset(apiClient *ApiClient, pid string, files []File) error {
	for _, file := range files {

	}
	return nil
}

func AddFilesToDataset(apiClientOrigin *ApiClient, apiClientTarget *ApiClient, pid string, files []File) (map[int]int, error) {
	parametersOrigin := map[string]interface{}{
		"format": "original",
	}
	parametersTarget := map[string]interface{}{
		"persistentId": pid,
	}
	oldNewFilesIds := make(map[int]int)
	log.Println("Length of files to add: ", len(files))
	for _, file := range files {
		log.Println("Hi")
		log.Println(file)
		var jsonData AddFileMetadata
		jsonData.Description = file.Description
		jsonData.Label = file.Label
		jsonData.DirectoryLabel = file.DirectoryLabel
		jsonData.Restrict = strconv.FormatBool(file.Restricted)

		f, err := GetFileFromDataset(apiClientOrigin, parametersOrigin, file.DataFile.Id)
		if err != nil {
			return oldNewFilesIds, err
		}
		var newFileId int
		newFileId, err = AddFileToDatasetFromMemory(apiClientTarget, parametersTarget, f, jsonData)
		if err != nil {
			return oldNewFilesIds, err
		}
		oldNewFilesIds[file.DataFile.Id] = newFileId

	}
	return oldNewFilesIds, nil
}

func UpdateCustomTermsOfUse(apiClient *ApiClient, id int, customTerms CustomTerms) error {
	idStr := strconv.Itoa(id)
	url := apiClient.BaseUrl + "/api/datasets/" + idStr + "/license"

	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
		"Content-Type":    "application/json",
	}
	parameters := map[string]interface{}{}
	ct := CreateCustomTerms{
		CustomTerms: customTerms,
	}

	jsonData, err := json.Marshal(ct)
	log.Println(string(jsonData))
	if err != nil {
		return err
	}
	resp, err := PutRequest(parameters, url, headers, apiClient.HttpClient, jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Println("Status:", resp.Status)
	fmt.Println("Response:", string(respBody))
	return nil
}

func PublishDataset(apiClient *ApiClient, parameters map[string]interface{}) error {
	//curl -H "X-Dataverse-key: $API_TOKEN" -X POST "$SERVER_URL/api/datasets/:persistentId/actions/:publish?persistentId=$PERSISTENT_ID&type=$MAJOR_OR_MINOR"
	//Superusers can pass type=updatecurrent to update metadata without changing the version number
	//assureIsIndexed. If true, the call will fail with a 409 (“CONFLICT”) response if the dataset is awaiting re-indexing
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/actions/:publish"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := PostRequest(parameters, url, headers, apiClient.HttpClient, nil)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		log.Println("Status:", resp.Status)
		log.Println("Response:", string(respBody))
		return fmt.Errorf("Error to publish dataset: %s", resp.Status)
	}
	return nil
}

func UpdateCustomTermsOfAccess(apiClient *ApiClient, id int, customTermsOfAccess CustomTermsOfAccess) error {
	idStr := strconv.Itoa(id)
	url := apiClient.BaseUrl + "/api/datasets/" + idStr + "/access"

	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
		"Content-Type":    "application/json",
	}
	parameters := map[string]interface{}{}
	ct := CreateCustomTermsOfAccess{
		CustomTermsOfAccess: customTermsOfAccess,
	}

	jsonData, err := json.Marshal(ct)
	log.Println(string(jsonData))
	if err != nil {
		return err
	}
	resp, err := PutRequest(parameters, url, headers, apiClient.HttpClient, jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	log.Println("Status:", resp.Status)
	log.Println("Response:", string(respBody))
	return nil
}

func isLocked(apiClient *ApiClient, pid string) (bool, error) {
	locks, err := getLocks(apiClient, pid)
	if err != nil {
		return true, err
	}
	if len(locks) > 0 {
		return true, nil
	}
	return false, nil
}

func getLocks(apiClient *ApiClient, pid string) (LockResults, error) {
	//curl "$SERVER_URL/api/datasets/$ID/locks"
	lockResults := make([]LockResult, 0)
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/locks"
	parameters := map[string]interface{}{
		"persistentId": pid,
	}
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	resp, err := GetRequest(parameters, url, headers, apiClient.HttpClient)
	if err != nil {
		return lockResults, err
	}
	if resp.StatusCode != http.StatusOK {
		return lockResults, fmt.Errorf("Error to check lock: %s", resp.Status)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	r := RequestResponse{}
	err = json.Unmarshal(respBody, &r)
	if err != nil {
		return lockResults, err
	}

	if r.Status == "OK" {
		json.Unmarshal(r.Data, &lockResults)
	} else {
		return lockResults, fmt.Errorf("Error getting locks: %s %s", parameters["persistentId"], r.Message)
	}

	log.Println("Status:", resp.Status)
	log.Println("Response:", string(respBody))
	return lockResults, nil
}
func (u DatasetVersion) MarshalJSON() ([]byte, error) {
	type Alias DatasetVersion

	aux := struct {
		Alias
		License *License `json:"license,omitempty"`
	}{
		Alias: (Alias)(u),
	}

	if u.License.Name != "" {

		aux.License = &u.License
	}

	return json.Marshal(aux)
}

func UpdateDatasetMetadata(apiClient *ApiClient, pid string, datasetVersion DatasetVersion) error {
	//curl -H "X-Dataverse-key: $API_TOKEN" -X PUT "$SERVER_URL/api/datasets/:persistentId/versions/:draft?persistentId=$PERSISTENT_IDENTIFIER" --upload-file dataset-update-metadata.json
	url := apiClient.BaseUrl + "/api/datasets/:persistentId/versions/:draft"
	headers := map[string]interface{}{
		"X-Dataverse-key": apiClient.ApiToken,
	}
	parameters := map[string]interface{}{
		"persistentId": pid,
	}
	//if datasetVersion.License.Name == "" {
	//	datasetVersion.License.Name = "CC0 1.0"
	//	datasetVersion.License.Uri = "http://creativecommons.org/publicdomain/zero/1.0"
	//}
	jsonData, err := json.Marshal(datasetVersion)

	if err != nil {
		return err
	}
	locked, err := isLocked(apiClient, pid)
	if err != nil {
		return err
	}
	if locked {
		time.Sleep(30 * time.Second)
	}
	resp, err := PutRequest(parameters, url, headers, apiClient.HttpClient, jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	log.Println("Status:", resp.Status)
	log.Println("Response:", string(respBody))
	return nil
}

func deletedAndAddedFilesList(filesPrevVer []File, filesCurVer []File) ([]File, []File, error) {
	delFileList := make([]File, 0)
	addFileList := make([]File, 0)
	filesPrevVerMap := make(map[int]File)
	for _, filePrevVer := range filesPrevVer {
		filesPrevVerMap[filePrevVer.DataFile.Id] = filePrevVer
	}
	filesCurVerMap := make(map[int]File)
	for _, fileCurVer := range filesCurVer {
		filesCurVerMap[fileCurVer.DataFile.Id] = fileCurVer
	}

	for _, fileCurVer := range filesCurVerMap {
		found := false
		_, found = filesPrevVerMap[fileCurVer.DataFile.Id]
		if !found {
			addFileList = append(addFileList, fileCurVer)
		}
	}

	for _, filePrevVer := range filesPrevVerMap {
		found := false
		_, found = filesCurVerMap[filePrevVer.DataFile.Id]
		if !found {
			delFileList = append(delFileList, filePrevVer)
		}
	}
	return delFileList, addFileList, nil
}

func CreateAllVersionsOfDataset(apiClientOrigin *ApiClient, apiClientTarget *ApiClient, dataverseAlias string, datasetVersions []DatasetVersion) (MinimalDataset, error) {
	dataset := MinimalDataset{}
	slices.SortStableFunc(datasetVersions, func(dv1, dv2 DatasetVersion) int {

		if dv1.VersionState == "DRAFT" && dv2.VersionState == "RELEASED" {

			return 1
		}
		if dv1.VersionState == "RELEASED" && dv2.VersionState == "DRAFT" {
			return -1
		}
		if dv1.VersionNumber == dv2.VersionNumber {
			return cmp.Compare(dv1.VersionMinorNumber, dv2.VersionMinorNumber)
		} else {
			return cmp.Compare(dv1.VersionNumber, dv2.VersionNumber)
		}
	})

	var oldNewFileIds map[int]int
	if len(datasetVersions) > 0 {
		//create the first version, then add files, then create the rest of versions based on the first version
		files := datasetVersions[0].Files
		log.Println(files)
		dv := datasetVersions[0]
		dv.Files = nil

		var err error
		dataset, err = CreateDataset(apiClientTarget, dataverseAlias, dv)
		log.Println(dataset)
		if err != nil {
			return dataset, fmt.Errorf("Error creating dataset version ", err)
		}
		log.Printf("Created dataset version with pid: %s\n", dataset.Pid)
		//return dataset, nil

		oldNewFileIds, err = AddFilesToDataset(apiClientOrigin, apiClientTarget, dataset.Pid, files)
		if err != nil {
			return dataset, err
		}
		customTerms := CustomTerms{}
		customTerms.TermsOfUse = dv.TermsOfUse
		customTerms.CitationRequirements = dv.CitationRequirements
		customTerms.Disclaimer = dv.Disclaimer
		customTerms.Conditions = dv.Conditions
		customTerms.ConfidentialityDeclaration = dv.ConfidentialityDeclaration
		customTerms.DepositorRequirements = dv.DepositorRequirements
		customTerms.SpecialPermissions = dv.SpecialPermissions
		customTerms.Restrictions = dv.Restrictions

		err = UpdateCustomTermsOfUse(apiClientTarget, dataset.Id, customTerms)
		if err != nil {
			return dataset, err
		}
		customTermsOfAccess := CustomTermsOfAccess{}
		customTermsOfAccess.OriginalArchive = dv.OriginalArchive
		customTermsOfAccess.FileAccessRequest = dv.FileAccessRequest
		customTermsOfAccess.TermsOfAccess = dv.TermsOfAccess
		customTermsOfAccess.ContactForAccess = dv.ContactForAccess
		customTermsOfAccess.DataAccessPlace = dv.DataAccessPlace
		customTermsOfAccess.AvailabilityStatus = dv.AvailabilityStatus
		customTermsOfAccess.SizeOfCollection = dv.SizeOfCollection
		customTermsOfAccess.StudyCompletion = dv.StudyCompletion

		err = UpdateCustomTermsOfAccess(apiClientTarget, dataset.Id, customTermsOfAccess)
		if err != nil {
			return dataset, err
		}
		if dv.VersionState == "RELEASED" {
			parametersPublish := map[string]interface{}{
				"persistentId": dataset.Pid,
				"type":         "major",
			}
			err = PublishDataset(apiClientTarget, parametersPublish)
			if err != nil {
				return dataset, err
			}
		}

		for i := 1; i < len(datasetVersions); i++ {
			//Update dataset metadata
			dv = datasetVersions[i]
			dv.Files = nil
			log.Println(dataset.Pid)
			err = UpdateDatasetMetadata(apiClientTarget, dataset.Pid, dv)
			if err != nil {
				return dataset, err
			}
			var delFilesList []File
			var addFilesList []File
			delFilesList, addFilesList, err = deletedAndAddedFilesList(datasetVersions[i-1].Files, datasetVersions[i].Files)
			if err != nil {
				return dataset, err
			}
			for j := range delFilesList {
				if newId, ok := oldNewFileIds[delFilesList[j].DataFile.Id]; ok {
					delFilesList[j].DataFile.Id = newId
				} else {
					return dataset, fmt.Errorf("Error getting new file id for file id %d", delFilesList[j].DataFile.Id)
				}
			}
			DeleteFilesFromDataset(apiClientTarget, dataset.Pid, delFilesList)
			AddFilesToDataset(apiClientOrigin, apiClientTarget, dataset.Pid, addFilesList)

		}

	} else {
		return dataset, fmt.Errorf("No dataset versions to create")
	}
	return dataset, nil

}

func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
