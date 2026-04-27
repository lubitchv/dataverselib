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

type MetadataCompound map[string]MetadataField

type MetadataFieldItem struct {
	Name                       string                       `json:"name"`
	DisplayName                string                       `json:"displayName"`
	DisplayOnCreate            bool                         `json:"displayOnCreate"`
	Title                      string                       `json:"title"`
	Type                       string                       `json:"type"`
	TypeClass                  string                       `json:"typeClass"`
	Watermark                  string                       `json:"watermark"`
	Description                string                       `json:"description"`
	Multiple                   bool                         `json:"multiple"`
	IsControlledVocabulary     bool                         `json:"isControlledVocabulary"`
	IsAdvancedSearchFieldType  bool                         `json:"isAdvancedSearchFieldType"`
	DisplayFormat              string                       `json:"displayFormat"`
	DisplayOrder               int                          `json:"displayOrder"`
	IsRequired                 bool                         `json:"isRequired"`
	ControlledVocabularyValues []string                     `json:"controlledVocabularyValues,omitempty"`
	ChildFields                map[string]MetadataFieldItem `json:"childFields,omitempty"`
}

type MetadataBlockInfo struct {
	Id              int                          `json:"id"`
	DisplayName     string                       `json:"displayName"`
	DisplayOnCreate bool                         `json:"displayOnCreate"`
	Name            string                       `json:"name"`
	Fields          map[string]MetadataFieldItem `json:"fields"`
}

type License struct {
	Name                   string `json:"name,omitempty"`
	Uri                    string `json:"uri,omitempty"`
	IconUri                string `json:"iconUri,omitempty"`
	RightsIdentifier       string `json:"rightsIdentifier,omitempty"`
	RightsIdentifierScheme string `json:"rightsIdentifierScheme,omitempty"`
	SchemeUri              string `json:"schemeUri,omitempty"`
	LanguageCode           string `json:"languageCode,omitempty"`
}

type Checksum struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type AddFileMetadata struct {
	Description    string   `json:"description,omitempty"`
	Label          string   `json:"label,omitempty"`
	DirectoryLabel string   `json:"directoryLabel,omitempty"`
	Categories     []string `json:"categories,omitempty"`
	Restrict       string   `json:"restrict,omitempty"`
	TabIngest      string   `json:"tabIngest,omitempty"`
}

type DataFile struct {
	Id                int       `json:"id,omitempty"`
	PersistentId      string    `json:"persistentId,omitempty"`
	Filename          string    `json:"filename,omitempty"`
	ContentType       string    `json:"contentType,omitempty"`
	FriendlyType      string    `json:"friendlyType,omitempty"`
	Filesize          int64     `json:"filesize,omitempty"`
	Description       string    `json:"description,omitempty"`
	StorageIdentifier string    `json:"storageIdentifier,omitempty"`
	RootDataFileId    int       `json:"rootDataFileId,omitempty"`
	Md5               string    `json:"md5,omitempty"`
	Checksum          Checksum  `json:"checksum,omitempty"`
	TabularData       bool      `json:"tabularData,omitempty"`
	CreationDate      string    `json:"creationDate,omitempty"`
	PublicationDate   string    `json:"publicationDate,omitempty"`
	DirectoryLabel    string    `json:"directoryLabel,omitempty"`
	LastUpdateTime    time.Time `json:"lastUpdateTime,omitempty"`
	fileAceessRequest bool      `json:"fileAccessRequest,omitempty"`
}
type File struct {
	Description      string   `json:"description,omitempty"`
	Label            string   `json:"label,omitempty"`
	Restricted       bool     `json:"restricted,omitempty"`
	DirectoryLabel   string   `json:"directoryLabel,omitempty"`
	Version          int      `json:"version,omitempty"`
	DatasetVersionId int      `json:"datasetVersionId,omitempty"`
	DataFile         DataFile `json:"dataFile,omitempty"`
}

type Files struct {
	Files []File `json:"files,omitempty"`
}

type UpdateFileMetadataStruct struct {
	Description    string   `json:"description,omitempty"`
	Label          string   `json:"label,omitempty"`
	DirectoryLabel string   `json:"directoryLabel,omitempty"`
	Categories     []string `json:"categories,omitempty"`
	Restrict       string   `json:"restrict,omitempty"`
	ProvFreeform   string   `json:"provFreeform,omitempty"`
	DataFileTags   []string `json:"dataFileTags,omitempty"`
	ContentType    string   `json:"contentType,omitempty"`
}

type DatasetVersion struct {
	ID                           int       `json:"id,omitempty"`
	DatasetId                    int       `json:"datasetId,omitempty"`
	DatasetPersistentId          string    `json:"datasetPersistentId,omitempty"`
	DatasetType                  string    `json:"datasetType,omitempty"`
	StorageIdentifier            string    `json:"storageIdentifier,omitempty"`
	VersionNumber                int       `json:"versionNumber,omitempty"`
	InternalVersionNumber        int       `json:"internalVersionNumber,omitempty"`
	VersionMinorNumber           int       `json:"versionMinorNumber,omitempty"`
	VersionState                 string    `json:"versionState,omitempty"`
	LatestVersionPublishingState string    `json:"latestVersionPublishingState,omitempty"`
	DeaccessionLink              string    `json:"deaccessionLink,omitempty"`
	ProductionDate               string    `json:"productionDate,omitempty"`
	LastUpdateTime               time.Time `json:"lastUpdateTime,omitempty"`
	ReleaseTime                  time.Time `json:"releaseTime,omitempty"`
	CreateTime                   time.Time `json:"createTime,omitempty"`
	PublicationDate              string    `json:"publicationDate,omitempty"`
	CitationDate                 string    `json:"citationDate,omitempty"`
	License                      License   `json:"license,omitempty"`
	TermsOfUse                   string    `json:"termsOfUse,omitempty"`
	ConfidentialityDeclaration   string    `json:"confidentialityDeclaration,omitempty"`
	SpecialPermissions           string    `json:"specialPermissions,omitempty"`
	Restrictions                 string    `json:"restrictions,omitempty"`
	CitationRequirements         string    `json:"citationRequirements,omitempty"`
	DepositorRequirements        string    `json:"depositorRequirements,omitempty"`
	Conditions                   string    `json:"conditions,omitempty"`
	Disclaimer                   string    `json:"disclaimer,omitempty"`
	TermsOfAccess                string    `json:"termsOfAccess,omitempty"`
	DataAccessPlace              string    `json:"dataAccessPlace,omitempty"`
	OriginalArchive              string    `json:"originalArchive,omitempty"`
	AvailabilityStatus           string    `json:"availabilityStatus,omitempty"`
	ContactForAccess             string    `json:"contactForAccess,omitempty"`
	SizeOfCollection             string    `json:"sizeOfCollection,omitempty"`
	StudyCompletion              string    `json:"studyCompletion,omitempty"`

	FileAccessRequest bool `json:"fileAccessRequest,omitempty"`

	MetadataBlocks map[string]MetadataBlock `json:"metadataBlocks,omitempty"`
	Files          []File                   `json:"files,omitempty"`
}

type CustomTermsOfAccess struct {
	FileAccessRequest  bool   `json:"fileAccessRequest"`
	TermsOfAccess      string `json:"termsOfAccess"`
	DataAccessPlace    string `json:"dataAccessPlace"`
	OriginalArchive    string `json:"originalArchive"`
	AvailabilityStatus string `json:"availabilityStatus"`
	ContactForAccess   string `json:"contactForAccess"`
	SizeOfCollection   string `json:"sizeOfCollection"`
	StudyCompletion    string `json:"studyCompletion"`
}

type CreateCustomTermsOfAccess struct {
	CustomTermsOfAccess CustomTermsOfAccess `json:"customTermsOfAccess"`
}

type CustomTerms struct {
	TermsOfUse                 string `json:"termsOfUse"`
	ConfidentialityDeclaration string `json:"confidentialityDeclaration"`
	SpecialPermissions         string `json:"specialPermissions,omitempty"`
	Restrictions               string `json:"restrictions,omitempty"`
	CitationRequirements       string `json:"citationRequirements,omitempty"`
	DepositorRequirements      string `json:"depositorRequirements,omitempty"`
	Conditions                 string `json:"conditions,omitempty"`
	Disclaimer                 string `json:"disclaimer,omitempty"`
}

type CreateCustomTerms struct {
	CustomTerms CustomTerms `json:"customTerms"`
}
type CreateDatasetItem struct {
	DatasetVersionField DatasetVersion `json:"datasetVersion"`
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
	Id  int    `json:"id"`
	Pid string `json:"persistentId"`
}

type RequestResponse struct {
	Status  string          `json:"status"`
	Data    json.RawMessage `json:"data,omitempty"`
	Message string          `json:"message,omitempty"`
}

type LockResult struct {
	LockType string `json:"lockType"`
	Date     string `json:"date"`
	User     string `json:"user"`
	Dataset  string `json:"dataset"`
}
type LockResults []LockResult

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

type Config struct {
	UrlBase        string `json:"url_base,omitempty"`
	ApiToken       string `json:"api_token,omitempty"`
	DataverseAlias string `json:"dataverse_alias,omitempty"`
	NumOfWorkers   int    `json:"num_of_workers,omitempty"`
	NumInSearch    int    `json:"num_in_search,omitempty"`

	UrlBaseOrigin  string `json:"url_base_origin,omitempty"`
	ApiTokenOrigin string `json:"api_token_origin,omitempty"`
	UrlBaseTarget  string `json:"url_base_target,omitempty"`
	ApiTokenTarget string `json:"api_token_target,omitempty"`
}

func primitiveOneField(typeName string, value string) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  false,
		TypeClass: "primitive",
		Value:     value,
	}
}

func primitiveArrayField(typeName string, value []string) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  true,
		TypeClass: "primitive",
		Value:     value,
	}
}

func compoundOneField(typeName string, value MetadataCompound) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  false,
		TypeClass: "compound",
		Value:     value,
	}
}

func controlledVocabArrayField(typeName string, multiple bool, value []string) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  true,
		TypeClass: "controlledVocabulary",
		Value:     value,
	}
}

func controlledVocabOneField(typeName string, multiple bool, value string) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  false,
		TypeClass: "controlledVocabulary",
		Value:     value,
	}
}

func compoundArrayField(typeName string, multiple bool, value []MetadataCompound) MetadataField {
	return MetadataField{
		TypeName:  typeName,
		Multiple:  true,
		TypeClass: "compound",
		Value:     value,
	}
}
