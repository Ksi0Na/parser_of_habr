GET _search
{
  "query": {
    "match_all": {}
  }
}

GET /my_index/_search
{
  "query": {
    "match": {
      "author": "Bright_Translate"
    }
  }
}

GET /my_index/_search
{
  "query": {
    "multi_match": {
      "query": "Bright_Translate",
      "fields": ["author", "header"]
    }
  }
}

GET /my_index/_search
{
  "query": {
    "multi_match": {
      "query": "Firemoon",
      "fields": ["author"],
      "operator": "and"
    }
  }
}

GET /my_index/_search
{
  "aggs": {
    "publications_by_date": {
      "date_histogram": {
        "field": "time",
        "calendar_interval": "day"
      },
      "aggs": {
        "authors": {
          "terms": {
            "field": "author"
          }
        }
      }
    }
  }
}