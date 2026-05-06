import asyncio
import logging
import os
import time
from typing import Any

from zabbix_utils import AsyncZabbixAPI

from zabbix_mcp.models import TransportConfig
from zabbix_mcp.models import ZabbixConfig
from zabbix_mcp.utils import parse_bool

logger = logging.getLogger(__name__)


class ZabbixClient:
    """Async client wrapper for Zabbix API using zabbix_utils AsyncZabbixAPI."""

    _api: AsyncZabbixAPI | None = None
    _task_apis: dict

    def __init__(self, config: ZabbixConfig):
        self.config = config
        self._task_apis = {}

    async def __aenter__(self) -> Any:
        api = await self._create_fresh_api()
        task = asyncio.current_task()
        key = id(task) if task is not None else 0
        self._task_apis[key] = api
        return api

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        task = asyncio.current_task()
        key = id(task) if task is not None else 0
        api = self._task_apis.pop(key, None)
        if api is not None:
            try:
                await api.logout()
            except Exception:
                logger.debug("Ignoring exception while closing Zabbix API session")
        return False

    async def _create_fresh_api(self) -> Any:
        logger.debug(
            f"Creating fresh Zabbix API connection to {self.config.zabbix_url}"
        )
        api: Any = AsyncZabbixAPI(
            url=self.config.zabbix_url,
            token=self.config.token,
            user=self.config.user,
            password=self.config.password,
            validate_certs=self.config.verify_ssl,
            timeout=self.config.timeout,
            skip_version_check=self.config.skip_version_check,
        )
        await api.login()
        logger.debug(f"Connected to Zabbix API version {api.version}")
        return api

    async def get_api(self) -> Any:
        return await self._create_fresh_api()

    async def close(self):
        for api in list(self._task_apis.values()):
            try:
                await api.logout()
            except Exception:
                logger.debug("Ignoring exception while closing Zabbix API session")
        self._task_apis.clear()
        self._api = None

    @property
    def api(self) -> AsyncZabbixAPI | None:
        task = asyncio.current_task()
        key = id(task) if task is not None else 0
        return self._task_apis.get(key, self._api)


class PassthroughClientCache:
    """LRU cache for Zabbix API sessions keyed by (url, token/user)."""

    def __init__(self, max_size: int = 50, ttl: int = 300):
        self._cache: dict[tuple[str, ...], tuple[float, Any]] = {}
        self._max_size = max_size
        self._ttl = ttl
        self._lock = asyncio.Lock()

    def _cache_key(self, config: ZabbixConfig) -> tuple[str, ...]:
        return (
            config.zabbix_url,
            config.token or "",
            config.user or "",
            config.password or "",
        )

    async def get_or_create(self, config: ZabbixConfig) -> Any:
        key = self._cache_key(config)
        now = time.monotonic()

        async with self._lock:
            if key in self._cache:
                ts, api = self._cache[key]
                if now - ts < self._ttl:
                    logger.debug(
                        "Reusing cached Zabbix API session for %s", config.zabbix_url
                    )
                    return api
                else:
                    await self._evict(key)

            if len(self._cache) >= self._max_size:
                await self._evict_oldest()

        api: Any = AsyncZabbixAPI(
            url=config.zabbix_url,
            token=config.token,
            user=config.user,
            password=config.password,
            validate_certs=config.verify_ssl,
            timeout=config.timeout,
            skip_version_check=config.skip_version_check,
        )
        await api.login()

        async with self._lock:
            self._cache[key] = (now, api)

        return api

    async def invalidate(self, config: ZabbixConfig) -> None:
        key = self._cache_key(config)
        async with self._lock:
            if key in self._cache:
                logger.info(
                    "Invalidating cached Zabbix API session for %s",
                    config.zabbix_url,
                )
                await self._evict(key)

    async def _evict(self, key: tuple[str, ...]) -> None:
        if key in self._cache:
            _, api = self._cache.pop(key)
            try:
                await api.logout()
            except Exception:
                logger.debug("Ignoring exception closing evicted session")

    async def _evict_oldest(self) -> None:
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k][0])
        await self._evict(oldest_key)

    async def close_all(self) -> None:
        async with self._lock:
            for key in list(self._cache):
                _, api = self._cache[key]
                try:
                    await api.logout()
                except Exception:
                    logger.debug("Ignoring exception closing cached session")
            self._cache.clear()


_passthrough_cache: PassthroughClientCache | None = None


def get_passthrough_cache(config: ZabbixConfig) -> PassthroughClientCache:
    global _passthrough_cache
    if _passthrough_cache is None:
        _passthrough_cache = PassthroughClientCache(
            max_size=config.passthrough_cache_size,
            ttl=config.passthrough_cache_ttl,
        )
    return _passthrough_cache


def extract_zabbix_config_from_request(
    request: Any, default_config: ZabbixConfig | None = None
) -> ZabbixConfig:
    url = request.headers.get("x-zabbix-url")
    token = request.headers.get("x-zabbix-token")
    user = request.headers.get("x-zabbix-user")
    password = request.headers.get("x-zabbix-password")

    if not url and default_config:
        return default_config
    if not url:
        raise ValueError(
            "X-Zabbix-URL header required when no default ZABBIX_URL is configured"
        )

    base = default_config
    return ZabbixConfig(
        zabbix_url=url,
        token=token or None,
        user=user or None,
        password=password or None,
        verify_ssl=base.verify_ssl if base else True,
        timeout=base.timeout if base else 30,
        skip_version_check=base.skip_version_check if base else False,
        read_only_mode=base.read_only_mode if base else False,
        disabled_tags=base.disabled_tags if base else set(),
        rate_limit_enabled=base.rate_limit_enabled if base else False,
        rate_limit_max_requests=base.rate_limit_max_requests if base else 60,
        rate_limit_window_minutes=base.rate_limit_window_minutes if base else 1,
        passthrough_enabled=True,
        passthrough_cache_size=base.passthrough_cache_size if base else 50,
        passthrough_cache_ttl=base.passthrough_cache_ttl if base else 300,
        tool_search_enabled=base.tool_search_enabled if base else False,
        tool_search_strategy=base.tool_search_strategy if base else "bm25",
        tool_search_max_results=base.tool_search_max_results if base else 5,
    )


def get_zabbix_config_from_env() -> ZabbixConfig:
    disabled_tags_str = os.getenv("DISABLED_TAGS", "")
    disabled_tags = set()
    if disabled_tags_str.strip():
        disabled_tags = {
            tag.strip() for tag in disabled_tags_str.split(",") if tag.strip()
        }

    return ZabbixConfig(
        zabbix_url=os.getenv("ZABBIX_URL", ""),
        token=os.getenv("ZABBIX_TOKEN"),
        user=os.getenv("ZABBIX_USER"),
        password=os.getenv("ZABBIX_PASSWORD"),
        verify_ssl=parse_bool(os.getenv("ZABBIX_VERIFY_SSL"), default=True),
        timeout=int(os.getenv("ZABBIX_TIMEOUT", "30")),
        skip_version_check=parse_bool(
            os.getenv("ZABBIX_SKIP_VERSION_CHECK"), default=False
        ),
        read_only_mode=parse_bool(os.getenv("READ_ONLY_MODE"), default=False),
        disabled_tags=disabled_tags,
        rate_limit_enabled=parse_bool(os.getenv("RATE_LIMIT_ENABLED"), default=False),
        rate_limit_max_requests=int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "60")),
        rate_limit_window_minutes=int(os.getenv("RATE_LIMIT_WINDOW_MINUTES", "1")),
        passthrough_enabled=parse_bool(os.getenv("ZABBIX_PASSTHROUGH"), default=False),
        passthrough_cache_size=int(os.getenv("ZABBIX_PASSTHROUGH_CACHE_SIZE", "50")),
        passthrough_cache_ttl=int(os.getenv("ZABBIX_PASSTHROUGH_CACHE_TTL", "300")),
        tool_search_enabled=parse_bool(os.getenv("TOOL_SEARCH_ENABLED"), default=False),
        tool_search_strategy=(
            "regex"
            if os.getenv("TOOL_SEARCH_STRATEGY", "bm25").lower() == "regex"
            else "bm25"
        ),
        tool_search_max_results=int(os.getenv("TOOL_SEARCH_MAX_RESULTS", "5")),
    )


def get_transport_config_from_env() -> TransportConfig:
    return TransportConfig(
        transport_type=os.getenv("MCP_TRANSPORT", "stdio").lower(),
        http_host=os.getenv("MCP_HTTP_HOST", "0.0.0.0"),
        http_port=int(os.getenv("MCP_HTTP_PORT", "8000")),
        http_bearer_token=os.getenv("MCP_HTTP_BEARER_TOKEN"),
    )
