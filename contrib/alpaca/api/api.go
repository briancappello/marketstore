package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"net/http"
	"net/url"
)

const (
	baseURL   = "https://api.alpaca.markets"
	assetsURL = "%v/v2/assets"
)

var (
	apiKey    string
	apiSecret string
)

type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return e.Message
}

type Asset struct {
	ID           string `json:"id"`
	Class        string `json:"class"`
	Exchange     string `json:"exchange"`
	Symbol       string `json:"symbol"`
	Status       string `json:"status"`
	Tradable     bool   `json:"tradable"`
	Marginable   bool   `json:"marginable"`
	Shortable    bool   `json:"shortable"`
	EasyToBorrow bool   `json:"easy_to_borrow"`
}

func SetCredentials(key, secret string) {
	apiKey = key
	apiSecret = secret
}

func get(u *url.URL) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("APCA-API-KEY-ID", apiKey)
	req.Header.Set("APCA-API-SECRET-KEY", apiSecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err = verify(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func verify(resp *http.Response) (err error) {
	if resp.StatusCode >= http.StatusMultipleChoices {
		var body []byte
		defer resp.Body.Close()

		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		apiErr := APIError{}

		err = json.Unmarshal(body, &apiErr)
		if err == nil {
			err = &apiErr
		}
	}

	return
}

func unmarshal(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}

func ListAssets() ([]Asset, error) {
	assets := []Asset{}

	u, err := url.Parse(fmt.Sprintf(assetsURL, baseURL))
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("status", "active")
	q.Set("asset_class", "us_equity")

	u.RawQuery = q.Encode()

	resp, err := get(u)
	if err != nil {
		return nil, err
	}

	r := []Asset{}
	if err = unmarshal(resp, &r); err != nil {
		return nil, err
	}
	for _, asset := range r {
		if asset.Tradable {
			assets = append(assets, asset)
		}
	}

	return assets, nil
}
