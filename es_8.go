package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	info, err := es8.Indices.Get([]string{"_all"})
	if err != nil {
		panic(err)
	}
	data, err := ReadDataParseJson[map[string]interface{}](info.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%+v", data)
	es8DeleteIndex(Es8IndexName)
	//es8CreateIndex(Es8IndexName)
	//es8CreateDocument(Es8IndexName)
	//es8CheckIndex(Es8IndexName)
	//es8SimpleSearch(Es8IndexName)
	//es8SearchByTags(Es8IndexName)
	//searchAfter := es8SearchSortable(Es8IndexName, nil)
	//es8SearchSortable(Es8IndexName, searchAfter)
	//es8SearchDeleteDocument(Es8IndexName)
	//es8SearchFuzzy(Es8IndexName)

}

const (
	//Es8IndexName = "character_1"
	Es8IndexName = "user_1"
)

func es8CheckIndexExists(indexName string) bool {
	exists, err := es8.Indices.Exists([]string{indexName}, es8.Indices.Exists.WithContext(context.Background()))
	if err != nil {
		log.Fatalf("Error Delete Exists request: %s", err)
	}
	data, err := ReadDataParseJson[CreateIndexResponse](exists.Body)
	if err != nil {
		log.Printf("index %s not exists\n", indexName)
	}
	log.Printf("index %+v not exists\n", data)

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

	all, err := io.ReadAll(response.Body)
	if err == nil {
		log.Println(string(all))
	}
	if response.IsError() {
		log.Fatalf("Error: %s", response.Status())
	}

	log.Println("Index delete successfully:", indexName)
}

func es8CreateIndex(indexName string) {
	createIndexRequest := map[string]interface{}{
		"settings": map[string]interface{}{
			"index": map[string]interface{}{
				"sort.field": []string{
					"TagPopularity", "CreatedAt", "UpdatedAt",
				},
				"sort.order": []string{
					"desc", "asc", "desc",
				},
			},
		},
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
				"Gender": map[string]interface{}{
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
			Gender:        1,
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
			Gender:        2,
		},
		{
			Id:            "character_3",
			Tags:          []string{"elf", "feboy", "game"},
			Name:          "hello work",
			Introduction:  "this is a test world22222",
			Greeting:      "nothing",
			CreatedAt:     time.Now().AddDate(0, 0, 1),
			UpdatedAt:     time.Now().AddDate(0, 0, 1),
			TagPopularity: 100,
			Gender:        3,
		},
		{
			Id:            "character_4",
			Tags:          []string{"elf", "oc", "game"},
			Name:          "hello work",
			Introduction:  "this is a test world22222",
			Greeting:      "nothing",
			CreatedAt:     time.Now().AddDate(0, 0, 1),
			UpdatedAt:     time.Now().AddDate(0, 0, 1),
			TagPopularity: 500,
			Gender:        4,
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
	var respBody *BulkDataResponse
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

func es8SearchFuzzy(indexName string) {
	now := time.Now()
	defer func() {
		log.Printf("es8SearchFuzzy use time: %v", time.Since(now))
	}()
	// 创建搜索请求
	//searchRequest := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match_bool_prefix": map[string]interface{}{
	//			"Name": "hello ow",
	//		},
	//	},
	//}
	//searchRequest := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match_phrase_prefix": map[string]interface{}{
	//			"Introduction": "hel",
	//		},
	//	},
	//	//"_source": []string{""},
	//}
	searchRequest := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		//"_source": []string{""},
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
		log.Println("Hit:", source, "id", hit.(map[string]interface{})["_id"])
	}
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

func es8SearchDeleteDocument(indexName string) {
	res, err := es8.Delete(indexName, fmt.Sprintf("3"), es8.Delete.WithContext(context.Background()))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	if res.IsError() {
		log.Fatalf("Error: %s", res.Status())
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
					//{
					//	"term": map[string]interface{}{
					//		"Tags": "gay",
					//	},
					//},
				},
				"filter": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"CreatedAt": map[string]interface{}{
								"gte": "2023-12-12T00:00:00Z",
								"lt":  "2024-02-01T00:00:00Z",
							},
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
		"size": 2,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				//"must": []map[string]interface{}{
				//	{
				//		"match": map[string]interface{}{
				//			"Name": "hell",
				//		},
				//	},
				//	{
				//		"term": map[string]interface{}{
				//			"Tags": "game",
				//		},
				//	},
				//	//{
				//	//	"term": map[string]interface{}{
				//	//		"Tags": "gay",
				//	//	},
				//	//},
				//},
				"filter": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"Tags": "game",
						},
					},
				},
			},
		},
		"sort": []interface{}{
			map[string]interface{}{
				"TagPopularity": map[string]interface{}{
					"order": "asc",
				},
				"UpdatedAt": map[string]interface{}{
					"order": "asc",
				},
			},
		},
		"_source": []string{"Name", "Tags"}, //
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

	//all, err := io.ReadAll(res.Body)
	//log.Printf("%s", string(all))
	var resp SearchResponse[Character]
	all, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error ReadAll the response body: %s", err)
	}
	if err := json.NewDecoder(bytes.NewReader(all)).Decode(&resp); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	if res.IsError() {
		log.Fatalf("Error: %s", res.Status())
	}
	hits := resp.Hits.Hits
	for _, hit := range hits {
		source := hit.Source
		log.Println("Hit:", source)
		searchAfter = hit.Sort
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

func ReadDataParseJson[M any](reader io.Reader) (data *M, err error) {
	all, err := io.ReadAll(reader)
	if err != nil {
		return
	}
	err = json.Unmarshal(all, &data)
	if err != nil {
		return
	}
	return
}
