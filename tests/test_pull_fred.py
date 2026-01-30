from pull_fred import search_keywords

def test_search_keywords():
    keywords = "monetary service index"
    result = search_keywords(keywords)
    assert result
    assert type(result) == dict
    assert "error_code" not in result