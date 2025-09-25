import os
import time
import random
import json
import requests
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional
from kafka import KafkaProducer
from pydantic import BaseModel, Field, ValidationError
import re

categories = {
    "番剧": 13, "国创": 167, "音乐": 3, "舞蹈": 129, "游戏": 4,
    "知识": 36, "美食": 211, "生活": 160, "鬼畜": 119, "时尚": 155,
    "娱乐": 5, "影视": 181, "科技": 188, "体育": 234, "汽车": 223,
    "动物圈": 217, "纪录片": 177, "电影": 23, "TV剧集": 11, "综艺": 71
}

# 兼容多种环境变量名
KAFKA_SERVERS = (
    os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    or os.getenv("KAFKA_BOOTSTRAP")
    or os.getenv("KAFKA_SERVERS")
    or "kafka:9092"
)
TOPIC = os.getenv("TOPIC", "bilibili_videos")
CONTROL_CATEGORY = "__MARKER__"
MARKER_NAME = "ROUND_END"
FETCH_MODE = os.getenv("FETCH_MODE", "hot").lower()
HOT_DAYS = os.getenv("HOT_DAYS", "3")
INTERVAL = int(os.getenv("FETCH_INTERVAL", "300"))
LIMIT = int(os.getenv("LIMIT", "60"))
DEDUP = os.getenv("DEDUP", "false").lower() == "true"

DEBUG_FETCH = os.getenv("DEBUG_FETCH", "false").lower() == "true"
ENABLE_DETAIL_STAT = os.getenv("ENABLE_DETAIL_STAT", "false").lower() == "true"
ENABLE_VIEW_DETAIL = os.getenv("ENABLE_VIEW_DETAIL", "false").lower() == "true"
DETAIL_CALL_LIMIT = int(os.getenv("DETAIL_CALL_LIMIT", "20"))
DETAIL_CACHE_TTL = int(os.getenv("DETAIL_CACHE_TTL", "600"))
BILI_COOKIE = os.getenv("BILI_COOKIE")
ENABLE_FALLBACK_KEYS = os.getenv("ENABLE_FALLBACK_KEYS", "true").lower() == "true"
DROP_ZERO_VIDEOS = os.getenv("DROP_ZERO_VIDEOS", "true").lower() == "true"

zero_video_counter: Dict[str, int] = {}
nonzero_video_counter: Dict[str, int] = {}

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
RETRY = int(os.getenv("RETRY", "2"))
PARSE_CHINESE_NUM = os.getenv("PARSE_CHINESE_NUM", "true").lower() == "true"

FALLBACK_KEY_MAP = {
    "view": ["view", "play", "plays"],
    "like": ["like", "likes"],
    "coin": ["coin", "coins"],
    "favorite": ["favorite", "favorites", "fav"],
    "danmaku": ["danmaku", "danmu"],
    "reply": ["reply", "replies", "comment", "comments"],
    "share": ["share", "shares"],
}

class Video(BaseModel):
    bvid: str
    title: str = Field(..., min_length=1)
    pubdate: datetime
    category: str
    duration: int
    view: int = Field(ge=0)
    like: int = Field(ge=0)
    coin: int = Field(ge=0)
    favorite: int = Field(ge=0)
    danmaku: int = 0
    reply: int = 0
    share: int = 0
    engagement: int = Field(ge=0)
    collect_time: datetime

_num_pattern = re.compile(r"^[0-9,.]+(万|亿)?$")

def _parse_number(val) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if not isinstance(val, str):
        return 0
    s = val.strip()
    if not s:
        return 0
    try:
        if PARSE_CHINESE_NUM and _num_pattern.match(s):
            unit = 1
            if s.endswith('万'):
                unit = 10000
                s = s[:-1]
            elif s.endswith('亿'):
                unit = 100000000
                s = s[:-1]
            s = s.replace(',', '')
            if s == '':
                return 0
            return int(float(s) * unit)
        s2 = s.replace(',', '')
        if s2.isdigit():
            return int(s2)
        return int(float(s2))
    except Exception:
        return 0

session = requests.Session()
base_headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bilibili.com/",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
}
if BILI_COOKIE:
    base_headers["Cookie"] = BILI_COOKIE
session.headers.update(base_headers)

def _safe_get_json(url: str) -> Optional[dict]:
    for i in range(RETRY + 1):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            return r.json()
        except Exception as e:
            if i == RETRY and DEBUG_FETCH:
                print(f"[请求失败] url={url} err={e}")
            time.sleep(0.3)
    return None

_detail_cache: Dict[str, Tuple[float, dict]] = {}

def fetch_detail_stat(bvid: str) -> dict:
    if not ENABLE_DETAIL_STAT:
        return {}
    url = f"https://api.bilibili.com/x/web-interface/archive/stat?bvid={bvid}"
    data = _safe_get_json(url) or {}
    if data.get("code") == 0:
        return data.get("data", {}) or {}
    return {}

def fetch_detail_view(bvid: str) -> dict:
    if not ENABLE_VIEW_DETAIL:
        return {}
    now_ts = time.time()
    cached = _detail_cache.get(bvid)
    if cached and now_ts - cached[0] < DETAIL_CACHE_TTL:
        return cached[1]
    url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
    data = _safe_get_json(url) or {}
    if data.get("code") == 0:
        stat = (data.get("data", {}) or {}).get("stat", {}) or {}
        _detail_cache[bvid] = (now_ts, stat)
        return stat
    return {}

def parse_duration(d) -> int:
    if isinstance(d, int):
        return d
    if isinstance(d, str):
        parts = d.split(':')
        try:
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            return int(d)
        except Exception:
            return 0
    return 0

# ============ 抓取逻辑 ============
def fetch_newlist(category_name: str, rid: int, limit: int) -> List[dict]:
    url = f"https://api.bilibili.com/x/web-interface/newlist?rid={rid}&ps={limit}&pn=1"
    resp = _safe_get_json(url)
    if not resp or resp.get("code") != 0:
        return []
    archives = resp.get("data", {}).get("archives", []) or []
    out = []
    budget = DETAIL_CALL_LIMIT
    for item in archives[:limit]:
        stat = item.get("stat", {}) or {}
        cleaned_stat = {k: _parse_number(v) for k, v in stat.items()}
        stat = cleaned_stat
        built, budget = enrich_and_build(item, category_name, stat, budget)
        if built:
            out.append(built)
    return out

def fetch_hot_ranking(category_name: str, rid: int, limit: int) -> List[dict]:
    url = f"https://api.bilibili.com/x/web-interface/ranking/region?rid={rid}&day={HOT_DAYS}&original=0"
    resp = _safe_get_json(url)
    if not resp or resp.get("code") != 0:
        return []
    lst = resp.get("data", []) or []
    out = []
    budget = DETAIL_CALL_LIMIT
    for item in lst[:limit]:
        stat = item.get("stat", {}) or {}
        cleaned_stat = {k: _parse_number(v) for k, v in stat.items()}
        stat = cleaned_stat
        built, budget = enrich_and_build(item, category_name, stat, budget)
        if built:
            out.append(built)
    return out

def _stat_zero(stat: dict) -> bool:
    return (stat.get("view", 0) + stat.get("like", 0) + stat.get("coin", 0) + stat.get("favorite", 0) +
            stat.get("danmaku", 0) + stat.get("reply", 0) + stat.get("share", 0)) == 0

def enrich_and_build(item: dict, category_name: str, stat: dict, detail_budget: int) -> Tuple[Optional[dict], int]:
    try:
        zero_sum = _stat_zero(stat)
        if zero_sum and detail_budget > 0:
            detail = fetch_detail_stat(item.get("bvid"))
            if detail:
                for k in ["view", "like", "coin", "favorite", "danmaku", "reply", "share"]:
                    if k in detail and isinstance(detail.get(k), (int, float)):
                        stat[k] = int(detail.get(k, 0))
                zero_sum = _stat_zero(stat)
                detail_budget -= 1
        if zero_sum and detail_budget > 0:
            vstat = fetch_detail_view(item.get("bvid"))
            if vstat:
                for k in ["view", "like", "coin", "favorite", "danmaku", "reply", "share"]:
                    if k in vstat and isinstance(vstat.get(k), (int, float)):
                        stat[k] = int(vstat.get(k, 0))
                zero_sum = _stat_zero(stat)
                detail_budget -= 1
        if ENABLE_FALLBACK_KEYS and zero_sum:
            changed = False
            for target, keys in FALLBACK_KEY_MAP.items():
                if stat.get(target, 0) > 0:
                    continue
                for src_key in keys:
                    raw_val = item.get(src_key)
                    parsed = _parse_number(raw_val)
                    if parsed > 0:
                        stat[target] = parsed
                        changed = True
                        break
            if changed:
                zero_sum = _stat_zero(stat)
                if DEBUG_FETCH:
                    short = {k: stat.get(k) for k in ["view", "like", "coin", "favorite", "danmaku", "reply", "share"]}
                    print(f"[回退字段] cat={category_name} bvid={item.get('bvid')} stat={short}")
        if zero_sum and DROP_ZERO_VIDEOS:
            zero_video_counter[category_name] = zero_video_counter.get(category_name, 0) + 1
            return None, detail_budget
        if zero_sum:
            zero_video_counter[category_name] = zero_video_counter.get(category_name, 0) + 1
        else:
            nonzero_video_counter[category_name] = nonzero_video_counter.get(category_name, 0) + 1
        v = Video(
            bvid=item.get("bvid"),
            title=item.get("title") or "无题",
            pubdate=datetime.fromtimestamp(item.get("pubdate", int(time.time()))),
            category=category_name,
            duration=parse_duration(item.get("duration", 0)),
            view=int(stat.get("view", 0) or 0),
            like=int(stat.get("like", 0) or 0),
            coin=int(stat.get("coin", 0) or 0),
            favorite=int(stat.get("favorite", 0) or 0),
            danmaku=int(stat.get("danmaku", 0) or 0),
            reply=int(stat.get("reply", 0) or 0),
            share=int(stat.get("share", 0) or 0),
            engagement=0,
            collect_time=datetime.now(timezone.utc)
        )
        if hasattr(v, 'model_dump'):
            return v.model_dump(), detail_budget
        else:
            return v.dict(), detail_budget
    except ValidationError as ve:
        if DEBUG_FETCH:
            print(f"[验证失败] bvid={item.get('bvid')} err={ve}")
    except Exception as e:
        if DEBUG_FETCH:
            print(f"[构造异常] bvid={item.get('bvid')} err={e}")
    return None, detail_budget

# ============ 发送与主循环 ============
_producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global _producer
    retry = 0
    servers = [s.strip() for s in (KAFKA_SERVERS or "").split(",") if s.strip()]
    if not servers:
        servers = ["kafka:9092"]
    print(f"[启动] Kafka bootstrap servers = {','.join(servers)}")
    while _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                linger_ms=200,
                retries=3,
            )
        except Exception as e:
            retry += 1
            if retry % 5 == 0:
                print(f"[Kafka连接重试] 已重试{retry}次，仍未连接成功，err={e}")
            time.sleep(2)
    return _producer

def send(batch: List[dict], dedup_cache: set, round_id: int) -> set:
    if not batch:
        return dedup_cache
    prod = get_producer()
    sent = 0
    for v in batch:
        v['round_id'] = int(round_id)
        if DEDUP:
            if v['bvid'] in dedup_cache:
                continue
            dedup_cache.add(v['bvid'])
        try:
            prod.send(TOPIC, value=v)
            sent += 1
        except Exception as e:
            if DEBUG_FETCH:
                print(f"[发送失败] bvid={v.get('bvid')} err={e}")
    prod.flush()
    print(f"✅ 发送 {sent} 条（模式={FETCH_MODE} round={round_id}）")
    return dedup_cache

def send_round_end(round_id: int):
    prod = get_producer()
    now = datetime.utcnow()
    marker = {
        "bvid": f"{CONTROL_CATEGORY}-{round_id}",
        "title": f"{MARKER_NAME} {round_id}",
        "pubdate": now.strftime('%Y-%m-%d %H:%M:%S'),
        "category": CONTROL_CATEGORY,
        "duration": 0,
        "view": 0,
        "like": 0,
        "coin": 0,
        "favorite": 0,
        "danmaku": 0,
        "reply": 0,
        "share": 0,
        "engagement": 0,
        "collect_time": now.strftime('%Y-%m-%d %H:%M:%S'),
        "round_id": int(round_id),
        "marker": MARKER_NAME
    }
    try:
        prod.send(TOPIC, value=marker)
        prod.flush()
        print(f"[MARKER] 发送轮次结束标记 round={round_id}")
    except Exception as e:
        print(f"[MARKER失败] round={round_id} err={e}")

_round_id = 0

def one_round(dedup_cache: set) -> Tuple[set, int]:
    global _round_id
    _round_id += 1
    rid_now = _round_id
    total_raw = 0
    for name, rid in categories.items():
        try:
            if FETCH_MODE == 'hot':
                data = fetch_hot_ranking(name, rid, LIMIT)
            else:
                data = fetch_newlist(name, rid, LIMIT)
            dedup_cache = send(data, dedup_cache, rid_now)
            total_raw += len(data)
        except Exception as e:
            print(f"[抓取或发送异常] cat={name} err={e}")
        time.sleep(random.uniform(0.6, 1.1))
    if zero_video_counter or nonzero_video_counter:
        summary_parts = []
        for cat in categories.keys():
            z = zero_video_counter.get(cat, 0)
            nz = nonzero_video_counter.get(cat, 0)
            if (z + nz) == 0:
                continue
            summary_parts.append(f"{cat}:{nz}/{z}")
        if summary_parts:
            print("[本轮分类有效/零视频 统计] ", ' '.join(summary_parts))
    print(f"⏰ 本轮完成 total_raw={total_raw} (round={rid_now} drop_zero={DROP_ZERO_VIDEOS})")
    send_round_end(rid_now)
    return dedup_cache, rid_now

def main():
    print(f"启动采集: 模式={FETCH_MODE} interval={INTERVAL}s limit={LIMIT} dedup={DEDUP}")
    if DEBUG_FETCH:
        print(f"detail_stat={ENABLE_DETAIL_STAT} view_detail={ENABLE_VIEW_DETAIL} fallback={ENABLE_FALLBACK_KEYS}")
    dedup_cache: set = set()
    print("[启动] 检查Kafka可用性...")
    get_producer()
    print("[启动] Kafka连接成功，开始采集...")
    while True:
        try:
            start = datetime.now()
            dedup_cache, rid_now = one_round(dedup_cache)
            used = (datetime.now() - start).total_seconds()
            sleep_sec = max(0, INTERVAL - used)
            print(f"等待 {int(sleep_sec)}s (round_used={int(used)}s, next_round={rid_now+1})")
            time.sleep(sleep_sec)
        except Exception as e:
            print(f"[主循环异常] {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()