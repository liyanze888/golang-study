package main

type Character struct {
	Id           string   `json:"-" es:"keywords" comment:"不需要分割 进行查询"`
	Tags         []string `json:"Tags" es:"keywords" comment:"不需要分割 进行查询"`
	Name         string   `json:"Name" es:"text" comment:"需要分词"`
	Introduction string   `json:"Introduction" es:"text" comment:"需要分词"`
	Greeting     string   `json:"Greeting" es:"text" comment:"需要分词"`
}
