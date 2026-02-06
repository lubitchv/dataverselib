package dataverselib

import (
	"net/http"
	"net/url"
)

func GetRequest(requestParameters map[string]interface{}, urlString string, headers map[string]interface{}, client *http.Client) (*http.Response, error) {
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
	req, err := http.NewRequest("GET", u.String(), nil)
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
