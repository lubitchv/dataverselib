package dataverselib

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type MetadataBlock struct {
	DisplayName string          `json:"displayName"`
	Name        string          `json:"name"`
	Fields      []MetadataField `json:"fields"`
}

type MetadataField struct {
	TypeName  string      `json:"typeName"`
	Multiple  bool        `json:"multiple"`
	TypeClass string      `json:"typeClass"`
	Value     interface{} `json:"value"` // Can be string, array, or object
}

type License struct {
	Name                   string `json:"name"`
	Uri                    string `json:"uri"`
	IconUri                string `json:"iconUri,omitempty"`
	RightsIdentifier       string `json:"rightsIdentifier,omitempty"`
	RightsIdentifierScheme string `json:"rightsIdentifierScheme,omitempty"`
	SchemeUri              string `json:"schemeUri,omitempty"`
	LanguageCode           string `json:"languageCode,omitempty"`
}

type DatasetVersion struct {
	ID                           int       `json:"id"`
	DatasetId                    int       `json:"datasetId"`
	DatasetPersistentId          string    `json:"datasetPersistentId"`
	DatasetType                  string    `json:"datasetType"`
	StorageIdentifier            string    `json:"storageIdentifier"`
	VersionNumber                int       `json:"versionNumber"`
	InternalVersionNumber        int       `json:"internalVersionNumber"`
	VersionMinorNumber           int       `json:"versionMinorNumber"`
	VersionState                 string    `json:"versionState"`
	LatestVersionPublishingState string    `json:"latestVersionPublishingState"`
	DeaccessionLink              string    `json:"deaccessionLink"`
	ProductionDate               string    `json:"productionDate"`
	LastUpdateTime               time.Time `json:"lastUpdateTime"`
	ReleaseTime                  time.Time `json:"releaseTime"`
	CreateTime                   time.Time `json:"createTime"`
	PublicationDate              string    `json:"publicationDate"`
	CitationDate                 string    `json:"citationDate"`
	License                      License   `json:"license,omitempty"`
	FileAccessRequest            bool      `json:"fileAccessRequest"`

	MetadataBlocks map[string]MetadataBlock `json:"metadataBlocks"`
}

type ItemInDataverse struct {
	Type              string `json:"type"`
	Id                int    `json:"id"`
	Identifier        string `json:"identifier,omitempty"`
	PersistentUrl     string `json:"persistentUrl,omitempty"`
	Protocol          string `json:"protocol,omitempty"`
	Authority         string `json:"authority,omitempty"`
	Separator         string `json:"separator,omitempty"`
	Publisher         string `json:"publisher,omitempty"`
	PublicationDate   string `json:"publicationDate,omitempty"`
	StorageIdentifier string `json:"storageIdentifier,omitempty"`
	Title             string `json:"title,omitempty"`
}

type MinimalDataset struct {
	Id  int
	Pid string
}

type RequestResponse struct {
	Status  string          `json:"status"`
	Data    json.RawMessage `json:"data,omitempty"`
	Message string          `json:"message,omitempty"`
}

type SearchResult struct {
	Q          string       `json:"q"`
	TotalCount int          `json:"total_count"`
	Start      int          `json:"start,omitempty"`
	Items      []SearchItem `json:"items"`
}

type SearchItem struct {
	GlobalId              string                   `json:"global_id"`
	IdentifierOfDataverse string                   `json:"identifier_of_dataverse"`
	MetadataBlocks        map[string]MetadataBlock `json:"metadataBlocks,omitempty"`
}

type MetadataBlockItem struct {
	Id              int    `json:"id"`
	DisplayName     string `json:"displayName"`
	DisplayOnCreate bool   `json:"displayOnCreate"`
	Name            string `json:"name"`
}

type SafeSearchItems struct {
	mu       sync.Mutex
	allItems []SearchItem
}

type ApiClient struct {
	BaseUrl    string
	ApiToken   string
	HttpClient *http.Client
}
