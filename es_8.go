package main

import (
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

var es8 *elasticsearch.Client

func Es8() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://43.156.247.144:9200",
		},
	}

	var err error
	es8, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}
	//es8DeleteIndex(Es8IndexName)
	//es8CreateIndex(Es8IndexName)
	es8CreateDocument(Es8IndexName)
	//es8CheckIndex(Es8IndexName)
	//es8SimpleSearch(Es8IndexName)
	//es8SearchByTags(Es8IndexName)
	searchAfter := es8SearchSortable(Es8IndexName, nil)
	es8SearchSortable(Es8IndexName, searchAfter)
}

const (
	Es8IndexName = "character"
)

func es8CheckIndexExists(indexName string) bool {
	exists, err := es8.Indices.Exists([]string{indexName}, es8.Indices.Exists.WithContext(context.Background()))
	if err != nil {
		log.Fatalf("Error Delete Exists request: %s", err)
	}
	if exists.IsError() {
		// 索引不存在
		if exists.StatusCode == http.StatusNotFound {
			log.Printf("index %s not exists\n", indexName)
			return false
		}
		log.Fatalf("Error: %s", exists.Status())
	}
	return true
}

func es8DeleteIndex(indexName string) {
	if !es8CheckIndexExists(indexName) {
		return
	}

	response, err := es8.Indices.Delete([]string{indexName}, es8.Indices.Delete.WithContext(context.Background()))
	if err != nil {
		log.Fatalf("Error Delete index request: %s", err)
	}
	defer response.Body.Close()

	if response.IsError() {
		all, err := io.ReadAll(response.Body)
		if err == nil {
			log.Println(string(all))
		}
		log.Fatalf("Error: %s", response.Status())
	}

	log.Println("Index delete successfully:", indexName)
}

func es8CreateIndex(indexName string) {
	createIndexRequest := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"Tags": map[string]interface{}{
					"type": "keyword",
				},
				"Name": map[string]interface{}{
					"type": "text",
				},
				"Introduction": map[string]interface{}{
					"type": "text",
				},
				"Greeting": map[string]interface{}{
					"type": "text",
				},
				"CreatedAt": map[string]interface{}{
					"type": "date",
					//"format": "yyyy-MM-dd HH:mm:ss", //default : "strict_date_optional_time||epoch_millis"
				},
				"UpdatedAt": map[string]interface{}{
					"type": "date",
					//"format": "yyyy-MM-dd HH:mm:ss",
				},
				"TagPopularity": map[string]interface{}{
					"type": "long",
					//"format": "yyyy-MM-dd HH:mm:ss",
				},
			},
		},
	}

	createIndexJSON, err := json.Marshal(createIndexRequest)
	if err != nil {
		log.Fatalf("Error marshalling create index request: %s", err)
	}

	// 发送创建索引的请求
	response, err := es8.Indices.Create(indexName, es8.Indices.Create.WithBody(strings.NewReader(string(createIndexJSON))))
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}
	defer response.Body.Close()

	all, err := io.ReadAll(response.Body)
	var respBody *CreateIndexResponse
	if err == nil {
		err := json.Unmarshal(all, &respBody)
		if err != nil {
			panic(err)
		}
	}

	if response.IsError() {
		if strings.EqualFold(respBody.Err.Type, ResourceAlreadyExistsException) {
			log.Printf("index %s already exists", indexName)
			return
		}
		log.Fatalf("Error: %+v", respBody)
	}

	log.Printf("create Index: %+v", respBody)
	log.Println("Index created successfully:", indexName)
}

// _id 一样支持更新
func es8CreateDocument(indexName string) {
	data := []Character{
		{
			Id:            "character_1",
			Tags:          []string{"game", "anime", "hero"},
			Name:          "hello world",
			Introduction:  "this is a test work11111",
			Greeting:      "nothing",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			TagPopularity: 100,
		},
		{
			Id:            "character_2",
			Tags:          []string{"elf", "gay", "game"},
			Name:          "hello work",
			Introduction:  "this is a test world22222",
			Greeting:      "nothing",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			TagPopularity: 500,
		},
	}

	//data := []map[string]interface{}{
	//	{
	//		"Tags":         []string{"game", "anime", "hero"},
	//		"Name":         "hello world",
	//		"Introduction": "this is a test work",
	//		"Greeting":     "nothing",
	//	},
	//	{
	//		"Tags":         []string{"elf", "gay", "game"},
	//		"Name":         "hello work",
	//		"Introduction": "this is a test world",
	//		"Greeting":     "nothing",
	//	},
	//}

	var (
		bulkRequestBody strings.Builder
	)

	for _, entry := range data {
		// 每个操作包括一个index操作和一个文档
		opType := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    entry.Id,
			},
		}

		if err := json.NewEncoder(&bulkRequestBody).Encode(opType); err != nil {
			log.Fatalf("Error encoding index operation: %s", err)
		}

		if err := json.NewEncoder(&bulkRequestBody).Encode(entry); err != nil {
			log.Fatalf("Error encoding entry: %s", err)
		}
	}

	// 发送批量请求
	response, err := es8.Bulk(
		strings.NewReader(bulkRequestBody.String()),
		es8.Bulk.WithContext(context.Background()),
	)
	if err != nil {
		log.Fatalf("Error indexing data: %s", err)
	}
	defer response.Body.Close()
	all, err := io.ReadAll(response.Body)
	var respBody *CreateIndexResponse
	if err == nil {
		err := json.Unmarshal(all, &respBody)
		if err != nil {
			panic(err)
		}
	}
	log.Printf("create document: %+v", respBody)
	if response.IsError() {
		log.Fatalf("Error: %s", response.Status())
	}

	log.Println("Data document successfully")
}

func es8SimpleSearch(indexName string) {
	// 准备搜索请求
	searchRequest := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	searchJSON, err := json.Marshal(searchRequest)
	if err != nil {
		log.Fatalf("Error marshalling search request: %s", err)
	}

	// 发送搜索请求
	res, err := es8.Search(
		es8.Search.WithContext(context.Background()),
		es8.Search.WithIndex(indexName),
		es8.Search.WithBody(strings.NewReader(string(searchJSON))),
		es8.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		log.Fatalf("Error searching data: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error: %s", res.Status())
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		log.Println("Hit:", source)
	}
}

// 搜索by tags
func es8SearchByTags(indexName string) {
	//searchRequest := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match": map[string]interface{}{
	//			"Tags": "game",
	//		},
	//	},
	//}
	searchRequest := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"Tags": "game",
						},
					},
					{
						"term": map[string]interface{}{
							"Tags": "gay",
						},
					},
				},
			},
		},
	}
	searchJSON, err := json.Marshal(searchRequest)
	if err != nil {
		log.Fatalf("Error marshalling search request: %s", err)
	}

	// 发送搜索请求
	res, err := es8.Search(
		es8.Search.WithContext(context.Background()),
		es8.Search.WithIndex(indexName),
		es8.Search.WithBody(strings.NewReader(string(searchJSON))),
		es8.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		log.Fatalf("Error searching data: %s", err)
	}
	defer res.Body.Close()

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	if res.IsError() {
		log.Fatalf("Error: %s", res.Status())
	}

	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		log.Println("Hit:", source)
	}
}

// 搜索by tags
func es8SearchSortable(indexName string, searchAfter interface{}) interface{} {
	searchRequest := map[string]interface{}{
		"size": 1,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"Tags": "game",
						},
					},
					//{
					//	"term": map[string]interface{}{
					//		"Tags": "gay",
					//	},
					//},
				},
			},
		},
		"sort": []interface{}{
			map[string]interface{}{
				"TagPopularity": map[string]interface{}{
					"order": "asc",
				},
			},
		},
	}

	if searchAfter != nil {
		searchRequest["search_after"] = searchAfter
	}

	searchJSON, err := json.Marshal(searchRequest)
	if err != nil {
		log.Fatalf("Error marshalling search request: %s", err)
	}
	log.Println(string(searchJSON))
	// 发送搜索请求
	res, err := es8.Search(
		es8.Search.WithContext(context.Background()),
		es8.Search.WithIndex(indexName),
		es8.Search.WithBody(strings.NewReader(string(searchJSON))),
		es8.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		log.Fatalf("Error searching data: %s", err)
	}
	defer res.Body.Close()

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	if res.IsError() {
		log.Fatalf("Error: %s", res.Status())
	}

	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		log.Println("Hit:", source)
		searchAfter = hit.(map[string]interface{})["sort"]
	}
	return searchAfter
}

func es8CheckIndex(indexName string) {
	response, err := es8.Indices.Get([]string{indexName}, es8.Indices.Get.WithContext(context.Background()))
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}
	defer response.Body.Close()

	all, err := io.ReadAll(response.Body)
	var respBody map[string]interface{}
	if err == nil {
		err := json.Unmarshal(all, &respBody)
		if err != nil {
			log.Fatalf("Error creating index: %s", err)
		}
	}

	//searchRequest := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match_all": map[string]interface{}{},
	//	},
	//}
	//searchJSON, err := json.Marshal(searchRequest)
	//if err != nil {
	//	log.Fatalf("Error marshalling search request: %s", err)
	//}
	count, err := es8.Count(es8.Count.WithIndex(indexName), es8.Count.WithContext(context.Background()))
	defer count.Body.Close()
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}
	all1, err := io.ReadAll(count.Body)
	var respBody1 map[string]interface{}
	if err == nil {
		err := json.Unmarshal(all1, &respBody1)
		if err != nil {
			log.Fatalf("Error creating index: %s", err)
		}
	}
	log.Printf("create Index: %+v", respBody1)
}
