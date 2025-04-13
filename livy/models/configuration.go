package models

type Configuration struct {
	Id string `json:"id"`
	ConfigName string `json:"configname"`
	Value string `json:"value"`
}

func (c *Configuration) Tablename() string{
	return "configuration"
}