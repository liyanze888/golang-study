package main

import "time"

type Character struct {
	Id            string    `json:"-" es:"keywords" comment:"不需要分割 进行查询"`
	Tags          []string  `json:"Tags" es:"keywords" comment:"不需要分割 进行查询"`
	Name          string    `json:"Name" es:"text" comment:"需要分词"`
	Introduction  string    `json:"Introduction" es:"text" comment:"需要分词"`
	Greeting      string    `json:"Greeting" es:"text" comment:"需要分词"`
	CreatedAt     time.Time `json:"CreatedAt" es:"date" comment:""`
	UpdatedAt     time.Time `json:"UpdatedAt" es:"date" comment:""`
	TagPopularity int64     `json:"TagPopularity" es:"long" comment:"Tag 获取  排序使用"`
	Gender        int64     `json:"Gender" es:"long" comment:"Tag 获取  排序使用"`
}
