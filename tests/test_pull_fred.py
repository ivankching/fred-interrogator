from pull_fred import search_keywords, pull_observations

def test_search_keywords():
    """Test that the search_keywords function returns a dict with no error code"""
    keywords = "monetary service index"
    result = search_keywords(keywords)
    assert result
    assert type(result) == dict
    assert "error_code" not in result

def test_pull_observations():
    """Test that the pull_observations function returns a dict with no error code"""
    series_id = "MSIM2"
    result = pull_observations(series_id)
    assert result
    assert result["success"]