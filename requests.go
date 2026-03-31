package dataverselib

import (
	"bytes"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
)

func Request(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client, jsonData []byte, method string) (*http.Response, error) {
	resp := &http.Response{}

	u, err := url.Parse(urlString)
	if err != nil {
		return resp, err
	}

	// Add query parameters
	q := u.Query()

	if requestParameters != nil {
		for k, v := range requestParameters {
			if vStr, ok := v.(string); ok {
				q.Set(k, vStr)
			} else if vSlice, ok := v.([]string); ok {
				for _, item := range vSlice {
					q.Add(k, item)
				}
			}
		}
	}

	u.RawQuery = q.Encode() // encode parameters into URL
	log.Println(u.String())
	var body io.Reader
	if jsonData != nil {
		log.Println(jsonData)
		body = bytes.NewReader(jsonData)
	} else {
		log.Println("Json data is nil")
	}
	log.Println(method)
	if body == nil {
		log.Println("Body is nil")
	}
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return resp, err
	}
	if headers != nil {
		for k, v := range headers {
			if vStr, ok := v.(string); ok {
				req.Header.Set(k, vStr)
			} else if vSlice, ok := v.([]string); ok {
				for _, item := range vSlice {
					req.Header.Add(k, item)
				}
			}
		}
	}

	resp, err = client.Do(req)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func GetRequest(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client) (*http.Response, error) {
	resp, err := Request(requestParameters, urlString, headers, client, nil, "GET")
	return resp, err
}

func PostRequest(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client, jsonData []byte) (*http.Response, error) {
	resp, err := Request(requestParameters, urlString, headers, client, jsonData, "POST")
	return resp, err
}

func PostRequestFile(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client, filePath string) (*http.Response, error) {
	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	resp, err := PostRequest(requestParameters, urlString, headers, client, data)
	return resp, err

}

func PostRequestMultiPartJsonAndFile(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client, filePath string, jsonData string, method string) (*http.Response, error) {
	resp := &http.Response{}
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return resp, err
	}
	defer file.Close()

	// Prepare body
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Add file field (equivalent to -F "file=@...")
	part, err := writer.CreateFormFile("file", filePath)
	if err != nil {
		return resp, err
	}
	//Copy file content to part
	_, err = io.Copy(part, file)
	if err != nil {
		return resp, err
	}

	//Add json file metadata
	err = writer.WriteField("jsonData", jsonData)
	if err != nil {
		return resp, err
	}
	log.Println("Added json")

	writer.Close()

	// Build request URL
	u, err := url.Parse(urlString)
	if err != nil {
		return resp, err
	}

	// Add query parameters
	q := u.Query()

	if requestParameters != nil {
		for k, v := range requestParameters {
			if vStr, ok := v.(string); ok {
				q.Set(k, vStr)
			} else if vSlice, ok := v.([]string); ok {
				for _, item := range vSlice {
					q.Add(k, item)
				}
			}
		}
	}

	u.RawQuery = q.Encode() // encode parameters into URL
	log.Println(u.String())

	req, err := http.NewRequest(method, u.String(), &body)
	if err != nil {
		return resp, err
	}
	if headers != nil {
		for k, v := range headers {
			if vStr, ok := v.(string); ok {
				req.Header.Set(k, vStr)
			} else if vSlice, ok := v.([]string); ok {
				for _, item := range vSlice {
					req.Header.Add(k, item)
				}
			}
		}
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err = client.Do(req)
	if err != nil {
		return resp, err
	}

	return resp, nil

	//respBody, _ := io.ReadAll(resp.Body)
	//fmt.Println("Status:", resp.Status)
	//fmt.Println("Response:", string(respBody))
}
