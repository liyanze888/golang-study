# golang-study


## match_phrase_prefix
比如
```json

{
    "query": {
    "match_phrase_prefix" : {
      "message" : "quick brown f"
     }
    }
}
```

结果返回包含 quick brown f 以这个短语开始的数据

## match_bool_prefix
比如
```json

{
    "query": {
    "match_bool_prefix" : {
      "message" : "quick brown f"
     }
    }
}
```

结果返回包含 quick 或者 brown 或者以f 开始的数据

