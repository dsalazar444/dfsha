package http_client

type Client struct { /* baseURL, token, *http.Client */
}

func New(baseURL, token string) *Client
func (c *Client) Get(path string, out any) error
func (c *Client) Post(path string, body, out any) error
func (c *Client) PostMultipart(path string, fields map[string]string, fileField string, data []byte, out any) error
func (c *Client) Delete(path string, out any) error
