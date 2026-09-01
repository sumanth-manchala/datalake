"""Entry point: connect() -> Lakehouse, the control-plane client and Dataset factory."""

from dataclasses import asdict
from typing import Any

import httpx

from lakehouse.context import Process, resolve_actor, resolve_platform, resolve_process
from lakehouse.dataset import Dataset


class LakehouseError(RuntimeError):
    pass


def _split(qualified_name: str) -> tuple[str, str]:
    namespace, _, name = qualified_name.partition(".")
    if not namespace or not name:
        raise ValueError(f"dataset names are 'namespace.name', got {qualified_name!r}")
    return namespace, name


def _normalize_schema(schema) -> list[dict[str, Any]]:
    """Accept {'col': 'type', ...} or [{'name':..., 'type':..., 'required':...}, ...]."""
    if isinstance(schema, dict):
        return [{"name": n, "type": t} for n, t in schema.items()]
    return [dict(f) for f in schema]


class Lakehouse:
    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        *,
        platform: str | None = None,
        actor: str | None = None,
        process: Process | None = None,
        http: httpx.Client | None = None,
    ):
        self._http = http or httpx.Client(base_url=base_url, timeout=30.0)
        self.platform = platform or resolve_platform()
        self.actor = actor or resolve_actor()
        self.process = process or resolve_process()

    def _request(self, method: str, path: str, **kwargs) -> Any:
        headers = kwargs.pop("headers", {}) | {"X-Lakehouse-Actor": self.actor}
        response = self._http.request(method, path, headers=headers, **kwargs)
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise LakehouseError(f"{response.status_code}: {detail}")
        return response.json()

    # ── datasets ────────────────────────────────────────────────────────────
    def create_dataset(
        self,
        qualified_name: str,
        *,
        schema,
        partition_by: list[str],
        warehouse_name: str | None = None,
        description: str | None = None,
        new_version: bool = False,
    ) -> Dataset:
        namespace, name = _split(qualified_name)
        self._request(
            "POST",
            "/v1/datasets",
            json={
                "namespace": namespace,
                "name": name,
                "schema": _normalize_schema(schema),
                "partition_by": partition_by,
                "warehouse_name": warehouse_name,
                "platform": self.platform,
                "description": description,
                "new_version": new_version,
                "process": asdict(self.process),
            },
        )
        return self.dataset(qualified_name)

    def dataset(
        self, qualified_name: str, version: int | None = None, warehouse_name: str | None = None
    ) -> Dataset:
        namespace, name = _split(qualified_name)
        params: dict[str, Any] = {"platform": self.platform}
        if version is not None:
            params["version"] = version
        if warehouse_name is not None:
            params["warehouse_name"] = warehouse_name
        doc = self._request("GET", f"/v1/datasets/{namespace}/{name}", params=params)
        return Dataset(self, doc)

    def datasets(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/datasets")

    def drop_dataset(self, qualified_name: str, version: int | None = None) -> dict[str, Any]:
        namespace, name = _split(qualified_name)
        params = {"version": version} if version is not None else {}
        return self._request("DELETE", f"/v1/datasets/{namespace}/{name}", params=params)

    def warehouses(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/warehouses")

    # ── ledger plumbing (used by Dataset) ───────────────────────────────────
    def get_op(self, op_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/ops/{op_id}")

    def report_op(
        self,
        doc: dict[str, Any],
        *,
        op_id: str,
        op_kind: str,
        snapshot_id: int | None,
        details: dict[str, Any],
    ) -> None:
        self._request(
            "POST",
            f"/v1/datasets/{doc['namespace']}/{doc['name']}/ops",
            json={
                "op_id": op_id,
                "op_kind": op_kind,
                "warehouse_name": doc["warehouse_name"],
                "version": doc["version"],
                "snapshot_id": snapshot_id,
                "process": asdict(self.process),
                "details": details,
            },
        )

    def alter_dataset(self, doc: dict[str, Any], **changes) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/datasets/{doc['namespace']}/{doc['name']}/alter",
            json={
                **changes,
                "version": doc["version"],
                "platform": self.platform,
                "process": asdict(self.process),
            },
        )

    def rollback_dataset(self, doc: dict[str, Any], *, to: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/datasets/{doc['namespace']}/{doc['name']}/rollback",
            json={
                "to": to,
                "version": doc["version"],
                "platform": self.platform,
                "process": asdict(self.process),
            },
        )

    def dataset_history(self, doc: dict[str, Any], *, kind: str) -> list[dict[str, Any]]:
        return self._request(
            "GET",
            f"/v1/datasets/{doc['namespace']}/{doc['name']}/history",
            params={"kind": kind, "version": doc["version"]},
        )

    def dataset_schema(self, doc: dict[str, Any], *, as_of=None) -> dict[str, Any]:
        params: dict[str, Any] = {"platform": self.platform, "version": doc["version"]}
        if as_of is not None:
            params["as_of"] = str(as_of)
        return self._request(
            "GET", f"/v1/datasets/{doc['namespace']}/{doc['name']}/schema", params=params
        )

    def dataset_partitions(self, doc: dict[str, Any], *, as_of=None) -> dict[str, Any]:
        params: dict[str, Any] = {"platform": self.platform, "version": doc["version"]}
        if as_of is not None:
            params["as_of"] = str(as_of)
        return self._request(
            "GET", f"/v1/datasets/{doc['namespace']}/{doc['name']}/partitions", params=params
        )


def connect(base_url: str = "http://localhost:8000", **kwargs) -> Lakehouse:
    return Lakehouse(base_url, **kwargs)
