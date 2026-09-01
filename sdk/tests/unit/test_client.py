import httpx
import pytest
from lakehouse.client import Lakehouse, LakehouseError, _normalize_schema, _split
from lakehouse.context import Process


def make_client(handler, **kwargs):
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://lakehouse")
    return Lakehouse(
        http=http,
        actor=kwargs.pop("actor", "alice"),
        process=kwargs.pop("process", Process("adhoc")),
        **kwargs,
    )


def test_split():
    assert _split("sales.orders") == ("sales", "orders")


@pytest.mark.parametrize("bad", ["orders", "sales.", ".orders"])
def test_split_rejects_unqualified(bad):
    with pytest.raises(ValueError, match="namespace.name"):
        _split(bad)


def test_normalize_schema_dict_and_list():
    assert _normalize_schema({"a": "long", "b": "string"}) == [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"},
    ]
    fields = [{"name": "a", "type": "long", "required": True}]
    assert _normalize_schema(fields) == fields


def test_error_responses_raise_with_detail():
    def handler(request):
        return httpx.Response(409, json={"detail": "already exists"})

    lake = make_client(handler)
    with pytest.raises(LakehouseError, match="409: already exists"):
        lake.warehouses()


def test_actor_header_sent_on_every_request():
    seen = {}

    def handler(request):
        seen["actor"] = request.headers.get("X-Lakehouse-Actor")
        return httpx.Response(200, json=[])

    make_client(handler, actor="svc-ingest").warehouses()
    assert seen["actor"] == "svc-ingest"
