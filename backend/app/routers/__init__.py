"""业务路由汇总：场景化幂等接口先注册，随后是所有模块共用的资源路由。"""
from __future__ import annotations

from app.routers import ingest_api, resource

ROUTERS = [ingest_api, resource]
