from tiktok_ads_mcp.config import config

ENV = ("TIKTOK_ACCESS_TOKEN", "TIKTOK_ACCESS_TOKEN_XINCHENG", "TIKTOK_ACCESS_TOKEN_2", "TIKTOK_ACCESS_TOKEN_ZECHENG")


def _clear(monkeypatch):
    for k in ENV:
        monkeypatch.delenv(k, raising=False)


def test_generic_token_names(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "a")
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN_2", "b")
    assert (config.ACCESS_TOKEN, config.ACCESS_TOKEN_2) == ("a", "b")


def test_legacy_token_names_still_work(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN_XINCHENG", "x")
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN_ZECHENG", "z")
    assert (config.ACCESS_TOKEN, config.ACCESS_TOKEN_2) == ("x", "z")


def test_generic_name_wins_over_legacy(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "a")
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN_XINCHENG", "x")
    assert config.ACCESS_TOKEN == "a"


def test_unset_is_empty(monkeypatch):
    _clear(monkeypatch)
    assert (config.ACCESS_TOKEN, config.ACCESS_TOKEN_2) == ("", "")
    assert "TIKTOK_ACCESS_TOKEN" in " ".join(config.get_missing_credentials())
