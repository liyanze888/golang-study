package peekaboogames

import (
	"cmp"
	"encoding/csv"
	jsoniter "github.com/json-iterator/go"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type (
	CharacterTextReviewDetail struct {
		PornInfo     *TextReviewPornInfo     `json:"porn_info,omitempty"`
		UnderageInfo *TextReviewUnderageInfo `json:"underage_info,omitempty"`
	}

	TextReviewPornInfo struct {
		Score    int64  `json:"score"`
		KeyWords string `json:"key_words,omitempty"`
	}

	// TextReviewUnderageInfo 命中运营给的词库就是100 未命中就是0
	TextReviewUnderageInfo struct {
		Score    int64  `json:"score"`
		KeyWords string `json:"key_words,omitempty"`
	}

	OrderPayload struct {
		Keyword string
		Num     int
	}
)

func CalcSensitiveWords() {
	slog.Info("CalcSensitiveWords")
	fileNames := []string{
		"peekaboogames/sqllab_untitled_query_5_20240516T040212.csv",
		"peekaboogames/sqllab_untitled_query_5_20240516T040441.csv",
		"peekaboogames/sqllab_untitled_query_5_20240516T040543.csv",
	}
	container := make(map[string]map[string]int)
	for _, fileName := range fileNames {
		calcSensitiveWordsWorker(container, fileName)
	}
	openFile, err := os.Create("porn_outfile.csv")
	if err != nil {
		panic(err)
	}
	pornWriter := csv.NewWriter(openFile)
	orderSensitiveWordsWorker(container, "porn", pornWriter)
	pornWriter.Flush()

	underageFile, err := os.Create("underage_outfile.csv")
	if err != nil {
		panic(err)
	}
	underageWriter := csv.NewWriter(underageFile)
	orderSensitiveWordsWorker(container, "underage", underageWriter)
	pornWriter.Flush()
}

func orderSensitiveWordsWorker(container map[string]map[string]int, name string, writer *csv.Writer) {
	m := container[name]
	var pds []OrderPayload
	for k, v := range m {
		pds = append(pds, OrderPayload{
			Keyword: k,
			Num:     v,
		})
	}
	slices.SortFunc(pds, func(a, b OrderPayload) int {
		return cmp.Compare(b.Num, a.Num)
	})
	var outdata = [][]string{}
	for _, pd := range pds {
		outdata = append(outdata, []string{
			pd.Keyword,
			strconv.Itoa(pd.Num),
		})
	}
	writer.WriteAll(outdata)
}

func calcSensitiveWordsWorker(container map[string]map[string]int, fileName string) {
	openFile, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(openFile)
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record[5]) == 0 {
			continue
		}
		detail := new(CharacterTextReviewDetail)
		all := strings.ReplaceAll(record[5], "'", "\"")
		err := json.Unmarshal([]byte(all), &detail)
		if err != nil {
			slog.Error("parse json failed", all)
			continue
		}
		if detail.PornInfo != nil {
			if _, ok := container["porn"]; !ok {
				container["porn"] = make(map[string]int)
			}
			if len(detail.PornInfo.KeyWords) > 0 {
				keywords := strings.Split(detail.PornInfo.KeyWords, ",")
				for _, keyword := range keywords {
					if len(keyword) > 0 {
						container["porn"][keyword] = container["porn"][keyword] + 1
					}
				}
			}

		}
		if detail.UnderageInfo != nil {
			if _, ok := container["underage"]; !ok {
				container["underage"] = make(map[string]int)
			}
			if len(detail.UnderageInfo.KeyWords) > 0 {
				keywords := strings.Split(detail.UnderageInfo.KeyWords, ",")
				for _, keyword := range keywords {
					if len(keyword) > 0 {
						container["underage"][keyword] = container["underage"][keyword] + 1
					}
				}
			}
		}
	}
}

func CalcCharacterTextReviewDetail() {
	slog.Info("CalcCharacterTextReviewDetail")
}
