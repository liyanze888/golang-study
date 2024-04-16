package main

type CreateIndexResponse struct {
	Err    CreateIndexError `json:"error"`
	Status int              `json:"status"`
}

type CreateIndexError struct {
	RootCause CreateIndexRootCause `json:"root_cause"`
	Type      string               `json:"type"`
	Reason    string               `json:"reason"`
	IndexUuid string               `json:"index_uuid"`
	Index     string               `json:"index"`
}

type CreateIndexRootCause []struct {
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	IndexUuid string `json:"index_uuid"`
	Index     string `json:"index"`
}

type BulkDataResponse struct {
	Took   int            `json:"took"`
	Errors bool           `json:"errors"`
	Items  []BulkDataItem `json:"items"`
}

type BulkDataItem struct {
	Index BulkDataIndex `json:"index"`
}

type BulkDataIndex struct {
	Index       string         `json:"_index"`
	Id          string         `json:"_id"`
	Version     int            `json:"_version"`
	Result      string         `json:"result"`
	Shards      BulkDataShards `json:"_shards"`
	SeqNo       int            `json:"_seq_no"`
	PrimaryTerm int            `json:"_primary_term"`
	Status      int            `json:"status"`
}

type BulkDataShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type SearchShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type SearchTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchHitsPayload[M any] struct {
	Index  string        `json:"_index"`
	Id     string        `json:"_id"`
	Score  interface{}   `json:"_score"`
	Source M             `json:"_source"`
	Sort   []interface{} `json:"sort"`
}

type SearchHitsDto[M any] struct {
	Total    SearchTotal            `json:"total"`
	MaxScore interface{}            `json:"max_score"`
	Hits     []SearchHitsPayload[M] `json:"hits"`
}

type SearchResponse[M any] struct {
	Took     int              `json:"took"`
	TimedOut bool             `json:"timed_out"`
	Shards   SearchShards     `json:"_shards"`
	Hits     SearchHitsDto[M] `json:"hits"`
}
