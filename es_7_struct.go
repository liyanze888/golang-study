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
