# -*- coding: utf-8 -*-
"""
Shopify CSV 自动生成 + 上传 + 日志统计
功能：24小时无限循环，每3分钟处理一个产品
日志目录：C:\ShopifyAutoLog\
"""

import csv
import json
import os
import re
import time
import threading
import uuid
import requests
import pymysql
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from urllib import parse
from pathlib import Path


# ============================================================
# 全局配置
# ============================================================

# 禁用系统代理
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
for _k in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    os.environ.pop(_k, None)

# 数据库配置
DB_CONFIG = {
    "host": "47.95.157.46",
    "user": "root",
    "password": "root@kunkun",
    "port": 3306,
    "database": "quote_iw",
    "charset": "utf8mb4"
}

# API基础地址
API_BASE_URL     = "http://47.95.157.46:8520"
LOG_API_BASE_URL = "http://47.104.72.198:2580"   # 日志 & Cookie 状态 API

# Shopify配置
STORE_ID = "893848-2"
COOKIE_URL = "https://ceshi-1300392622.cos.ap-beijing.myqcloud.com/shopify-cookies/893848-2.json"

# 日志目录
LOG_DIR = r"C:\ShopifyAutoLog"

# 执行间隔（秒）
TASK_INTERVAL_SECONDS = 180   # 每3分钟一个产品
NO_TASK_WAIT_SECONDS  = 30    # 无任务时等待30秒


# ============================================================
# 日志函数
# ============================================================

def log_info(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: {msg}")

def log_warning(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: {msg}")

def log_error(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {msg}")


# ============================================================
# 每日统计日志
# ============================================================

def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

def _today_log_path() -> str:
    _ensure_log_dir()
    date_str = datetime.now().strftime('%Y-%m-%d')
    return os.path.join(LOG_DIR, f"shopify_{date_str}.log")

def write_daily_log(keer_product_id: str, result: str, detail: str = ""):
    """
    向当天日志文件追加一条记录，并同步写入数据库
    result: 'success' / 'failed' / 'skipped'
    """
    # 写本地文件（原有逻辑保留）
    log_path = _today_log_path()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{now_str}] [{result.upper():8s}] ID={str(keer_product_id or '-'):30s} {detail}\n"
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(line)

    # 同步写入数据库
    _write_db_log(keer_product_id, result, detail)


def _write_db_log(keer_product_id: str, result: str, detail: str = ""):
    """将任务执行结果写入 shopify_task_log 表"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO shopify_task_log
                        (task_date, keer_product_id, result, detail, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """
                now = datetime.now()
                cursor.execute(sql, (
                    now.strftime('%Y-%m-%d'),
                    keer_product_id or '',
                    result,
                    detail[:500] if detail else '',
                    now.strftime('%Y-%m-%d %H:%M:%S')
                ))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log_error(f"DB日志写入失败（不影响主流程）: {e}")

def write_daily_summary():
    """
    在日志文件末尾追加当天的汇总统计（每次循环都更新末尾汇总行）
    改为每次任务结束后调用，统计当天文件内容。
    """
    log_path = _today_log_path()
    if not os.path.exists(log_path):
        return

    total = success = failed = skipped = 0
    lines = []
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 过滤掉旧的汇总行，重新统计
    data_lines = [l for l in lines if not l.startswith('===')]
    for line in data_lines:
        if '[SUCCESS' in line:
            total += 1; success += 1
        elif '[FAILED' in line:
            total += 1; failed += 1
        elif '[SKIPPED' in line:
            skipped += 1

    summary = (
        f"{'=' * 70}\n"
        f"  当日汇总 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 更新)\n"
        f"  执行任务: {total}  成功: {success}  失败: {failed}  跳过(无任务): {skipped}\n"
        f"{'=' * 70}\n"
    )

    with open(log_path, 'w', encoding='utf-8') as f:
        f.writelines(data_lines)
        f.write(summary)


# ============================================================
# 数据类
# ============================================================

@dataclass
class ProductVariant:
    id: int = 0
    title: str = ""
    price: str = "0"
    compare_at_price: Optional[str] = None
    sku: str = ""
    available: bool = True
    option1: Optional[str] = None
    option2: Optional[str] = None
    option3: Optional[str] = None
    grams: int = 0

@dataclass
class ProductImage:
    id: int = 0
    src: str = ""
    alt: Optional[str] = None
    position: int = 0

@dataclass
class ProductDetail:
    id: int = 0
    title: str = ""
    handle: str = ""
    description: str = ""
    vendor: str = ""
    product_type: str = ""
    tags: List[str] = field(default_factory=list)
    variants: List[ProductVariant] = field(default_factory=list)
    images: List[ProductImage] = field(default_factory=list)
    options: List[Dict] = field(default_factory=list)


# ============================================================
# Shopify商品抓取
# ============================================================

class ShopifyScraper:
    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        self.session.trust_env = False

    def fetch(self, product_url: str) -> Optional[ProductDetail]:
        is_preview = 'preview_key=' in product_url or 'products_preview' in product_url
        if is_preview:
            if '?' in product_url:
                base_url = product_url.split('?')[0]
                params = product_url.split('?')[1]
                if not base_url.endswith('.json'):
                    base_url = base_url.rstrip('/') + '.json'
                json_url = f"{base_url}?{params}"
            else:
                json_url = product_url.rstrip('/') + '.json'
        else:
            json_url = product_url.split('?')[0].rstrip('/')
            if not json_url.endswith('.json'):
                json_url += '.json'

        try:
            log_info(f"正在访问: {json_url}")
            response = self.session.get(json_url, timeout=self.timeout)
            if response.status_code == 404:
                log_error(f"商品不存在(404): {product_url}")
                return None
            response.raise_for_status()
            data = response.json()
            if 'product' not in data:
                log_error("响应中没有product字段")
                return None
            return self._parse(data['product'])
        except Exception as e:
            log_error(f"抓取错误: {e}")
            return None

    def _parse(self, data: Dict) -> ProductDetail:
        variants = []
        for v in data.get('variants', []):
            variants.append(ProductVariant(
                id=v.get('id', 0), title=v.get('title', ''),
                price=v.get('price', '0'), compare_at_price=v.get('compare_at_price'),
                sku=v.get('sku', ''), available=v.get('available', True),
                option1=v.get('option1'), option2=v.get('option2'),
                option3=v.get('option3'), grams=v.get('grams', 0)
            ))
        images = []
        for img in data.get('images', []):
            images.append(ProductImage(
                id=img.get('id', 0), src=img.get('src', ''),
                alt=img.get('alt'), position=img.get('position', 0)
            ))
        tags = data.get('tags', '')
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(',') if t.strip()]
        return ProductDetail(
            id=data.get('id', 0), title=data.get('title', ''),
            handle=data.get('handle', ''), description=data.get('body_html', ''),
            vendor=data.get('vendor', ''), product_type=data.get('product_type', ''),
            tags=tags, variants=variants, images=images,
            options=data.get('options', [])
        )


# ============================================================
# ZhipuAI API密钥管理
# ============================================================

_global_zhipuai_keys = []
_keys_cache_lock = threading.Lock()


def _fetch_api_keys_with_retry(api_url: str, max_retries: int = 3) -> list:
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = parse.urlencode({}, True)
    proxies = {'http': None, 'https': None}
    for attempt in range(1, max_retries + 1):
        try:
            log_info(f"📡 正在获取ZhipuAI密钥 (第{attempt}/{max_retries}次)...")
            response = requests.post(api_url, headers=headers, data=data, timeout=10, proxies=proxies)
            if response.status_code == 200:
                result = response.json()
                if result.get("success") and "data" in result:
                    keys = [item["key"].strip() for item in result["data"]]
                    log_info(f"✅ 成功获取 {len(keys)} 个ZhipuAI API密钥")
                    return keys
        except requests.exceptions.Timeout:
            log_warning(f"⚠️ 密钥获取超时 (第{attempt}/{max_retries}次)")
            if attempt < max_retries: time.sleep(2 ** attempt)
        except Exception as e:
            log_error(f"❌ 密钥获取异常: {e}")
            if attempt < max_retries: time.sleep(2 ** attempt)
    return []


def init_global_api_keys() -> bool:
    global _global_zhipuai_keys
    _global_zhipuai_keys = _fetch_api_keys_with_retry(f'{API_BASE_URL}/api/zhipuai_key', max_retries=3)
    log_info(f"🔑 ZhipuAI密钥缓存初始化完成 ({len(_global_zhipuai_keys)} 个)")
    return len(_global_zhipuai_keys) > 0


def get_cached_zhipuai_keys() -> list:
    with _keys_cache_lock:
        return _global_zhipuai_keys.copy()


def refresh_api_keys() -> bool:
    global _global_zhipuai_keys
    with _keys_cache_lock:
        keys = _fetch_api_keys_with_retry(f'{API_BASE_URL}/api/zhipuai_key', max_retries=3)
        if keys:
            _global_zhipuai_keys = keys
            return True
        return False


class APIKeyManager:
    def __init__(self, blacklist_duration=180):
        self.blacklist_duration = blacklist_duration
        self.blacklisted_keys = {}
        self.lock = threading.RLock()
        self.last_used_index = -1

    def add_to_blacklist(self, api_key: str, reason: str = "调用失败"):
        with self.lock:
            self.blacklisted_keys[api_key] = time.time()
            log_warning(f"⚠️ 密钥加入黑名单({reason}): ...{api_key[-8:]} ({self.blacklist_duration}秒)")

    def is_blacklisted(self, api_key: str) -> bool:
        with self.lock:
            if api_key not in self.blacklisted_keys:
                return False
            if time.time() - self.blacklisted_keys[api_key] >= self.blacklist_duration:
                del self.blacklisted_keys[api_key]
                return False
            return True

    def get_next_available_key(self, all_keys: list) -> Optional[str]:
        with self.lock:
            if not all_keys:
                return None
            expired = [k for k, t in self.blacklisted_keys.items()
                       if time.time() - t >= self.blacklist_duration]
            for k in expired:
                del self.blacklisted_keys[k]
            available = [k for k in all_keys if not self.is_blacklisted(k)]
            if not available:
                log_error("❌ 所有密钥都在黑名单中！")
                return None
            self.last_used_index = (self.last_used_index + 1) % len(available)
            selected = available[self.last_used_index]
            log_info(f"🔑 轮换密钥[{self.last_used_index + 1}/{len(available)}]: ...{selected[-8:]}")
            return selected

    def record_success(self, api_key: str):
        pass

    def record_failure(self, api_key: str, error_msg: str = ""):
        if any(kw in error_msg.lower() for kw in ['rate limit', 'too many', '429', '并发', '限流']):
            self.add_to_blacklist(api_key, "限流")


api_key_manager = APIKeyManager(blacklist_duration=180)


def zhipu_single_image_analyze_sync(image_url: str, prompt: str, max_retries: int = 10) -> str:
    wait_all_blocked = 30
    for attempt in range(max_retries):
        selected_key = None
        try:
            api_keys = get_cached_zhipuai_keys()
            if not api_keys:
                if refresh_api_keys():
                    api_keys = get_cached_zhipuai_keys()
            if not api_keys:
                raise Exception("无法获取ZhipuAI API密钥")

            selected_key = api_key_manager.get_next_available_key(api_keys)
            if not selected_key:
                log_warning(f"⚠️ 所有密钥在黑名单，等待{wait_all_blocked}秒...")
                time.sleep(wait_all_blocked)
                selected_key = api_key_manager.get_next_available_key(api_keys)
                if not selected_key:
                    continue

            url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
            headers = {"Authorization": f"Bearer {selected_key}", "Content-Type": "application/json"}
            payload = {
                "model": "glm-4.1v-thinking-flash",
                "max_tokens": 16384,
                "top_p": 0.1,
                "messages": [{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": image_url}},
                    {"type": "text", "text": prompt}
                ]}]
            }
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            if response.status_code == 200:
                result = response.json()
                if result and 'choices' in result and result['choices']:
                    content = result['choices'][0].get('message', {}).get('content', '')
                    if content:
                        api_key_manager.record_success(selected_key)
                        return content
                raise Exception(f"API响应格式错误: {result}")
            else:
                raise Exception(f"API调用失败: {response.status_code}, {response.text}")
        except Exception as e:
            error_msg = str(e)
            log_error(f"❌ ZhipuAI分析失败(第{attempt+1}次): {error_msg}")
            if selected_key:
                api_key_manager.record_failure(selected_key, error_msg)
            sleep_t = 5 if '429' in error_msg else (2 if 'timeout' in error_msg.lower() else 0.5)
            time.sleep(sleep_t)
    return "分析失败：达到最大重试次数"


class ZhipuImageAnalyzer:
    def __init__(self):
        self._initialized = False

    def _ensure_initialized(self):
        if not self._initialized:
            if not get_cached_zhipuai_keys():
                init_global_api_keys()
            self._initialized = True

    def analyze(self, image_url: str, prompt: str) -> str:
        self._ensure_initialized()
        return zhipu_single_image_analyze_sync(image_url, prompt)


# ============================================================
# AI分类
# ============================================================

def get_product_category(analyzer: ZhipuImageAnalyzer, image_url: str) -> Optional[str]:
    try:
        prompt = """Identify the product in the image and return the most appropriate category from this list:

1. Apparel & Accessories (clothing, shoes, jewelry, watches, hats, scarves)
2. Luggage & Bags (handbags, backpacks, wallets, suitcases)
3. Animals & Pet Supplies (pet food, pet toys, pet accessories)
4. Home & Garden (kitchen, bedding, lighting, garden tools, home decor)
5. Furniture (tables, chairs, sofas, beds, desks)
6. Electronics (phones, computers, audio, TV, smart devices)
7. Cameras & Optics (cameras, lenses, binoculars)
8. Health & Beauty (skincare, makeup, hair care, personal care)
9. Sporting Goods (fitness, outdoor, cycling, sports equipment)
10. Toys & Games (toys, games, puzzles)
11. Baby & Toddler (baby products, strollers, baby clothing)
12. Office Supplies (stationery, office equipment)
13. Vehicles & Parts (car accessories, auto parts)
14. Food, Beverages & Tobacco (food, drinks, snacks)
15. Hardware (tools, building materials)
16. Arts & Entertainment (art supplies, musical instruments)
17. Media (books, music, movies)

Rules:
1. Return ONLY the category name exactly as shown (e.g., "Apparel & Accessories")
2. Choose the single best matching category
3. No explanation, no punctuation, just the category name"""

        result = analyzer.analyze(image_url, prompt)
        if result and '分析失败' not in result:
            category = result.strip()
            category = re.sub(r'<\|[^|]+\|>', '', category)
            category = re.sub(r'<think>.*?</think>', '', category, flags=re.IGNORECASE | re.DOTALL)
            category = re.sub(r'<[^>]+>', '', category)
            category = category.strip().split('\n')[0].strip()
            if len(category) > 200:
                category = category[:200]
            log_info(f"🏷️ AI分类结果: {category}")
            return category if category else None
        return None
    except Exception as e:
        log_error(f"❌ AI分类异常: {e}")
        return None


# ============================================================
# 数据库操作
# ============================================================

def fetch_one_task() -> Optional[Dict]:
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT keer_product_id, client_product_url, client_product_image,
                       quotation_result, created_at
                FROM quotation_task_detail
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 3 DAY)
                  AND task_status = '报价单创建完毕'
                  AND client_product_url LIKE 'http%'
                  AND (shopfiy_task IS NULL OR shopfiy_task = '')
                  AND client_product_image IS NOT NULL
                  AND client_product_image != ''
                ORDER BY created_at DESC
                LIMIT 1
            """
            cursor.execute(sql)
            return cursor.fetchone()
    finally:
        conn.close()


def parse_price_from_quotation(quotation_result: str) -> Optional[float]:
    try:
        if not quotation_result:
            return None
        data = json.loads(quotation_result)
        if not isinstance(data, list) or not data:
            return None
        qty1_items = [item for item in data if item.get('quantity') == 1]
        if not qty1_items:
            return None
        for item in qty1_items:
            if item.get('nation') == 'US':
                return float(item.get('price', 0))
        return float(qty1_items[0].get('price', 0))
    except Exception as e:
        log_error(f"价格解析错误: {e}")
        return None


def feedback_task_status(keer_product_id: str, shopfiy_task: int) -> bool:
    try:
        url = f'{API_BASE_URL}/api/task-data/save'
        payload = {"keer_product_id": keer_product_id, "shopfiy_task": shopfiy_task}
        response = requests.post(url, headers={'Content-Type': 'application/json'},
                                 json=payload, timeout=10)
        if response.status_code == 200:
            log_info(f"✅ 状态反馈: {keer_product_id} -> {'成功' if shopfiy_task == 1 else '失败'}")
            return True
        log_error(f"状态反馈失败: {response.status_code}")
        return False
    except Exception as e:
        log_error(f"状态反馈异常: {e}")
        return False


# ============================================================
# CSV生成（Shopify 2024格式）
# ============================================================

def generate_shopify_csv(product: ProductDetail, price: float, category: str,
                         output_path: str) -> bool:
    headers = [
        'Title', 'URL handle', 'Description', 'Vendor', 'Product category', 'Type', 'Tags',
        'Published on online store', 'Status',
        'SKU', 'Barcode',
        'Option1 name', 'Option1 value', 'Option1 Linked To',
        'Option2 name', 'Option2 value', 'Option2 Linked To',
        'Option3 name', 'Option3 value', 'Option3 Linked To',
        'Price', 'Compare-at price', 'Cost per item',
        'Charge tax', 'Tax code',
        'Inventory tracker', 'Inventory quantity', 'Continue selling when out of stock',
        'Weight value (grams)', 'Weight unit for display', 'Requires shipping', 'Fulfillment service',
        'Product image URL', 'Image position', 'Image alt text', 'Variant image URL',
        'Gift card', 'SEO title', 'SEO description'
    ]
    rows = []
    handle = product.handle or re.sub(r'[^a-z0-9]+', '-', product.title.lower()).strip('-')
    safe_category = (category or '')[:200]
    option1_name = product.options[0].get('name', 'Title') if product.options else 'Title'
    option2_name = product.options[1].get('name', '') if len(product.options) > 1 else ''
    option3_name = product.options[2].get('name', '') if len(product.options) > 2 else ''

    for i, variant in enumerate(product.variants):
        row = {
            'Title': product.title if i == 0 else '',
            'URL handle': handle,
            'Description': product.description if i == 0 else '',
            'Vendor': product.vendor if i == 0 else '',
            'Product category': safe_category if i == 0 else '',
            'Type': safe_category if i == 0 else '',
            'Tags': ', '.join(product.tags) if i == 0 else '',
            'Published on online store': 'TRUE' if i == 0 else '',
            'Status': 'active' if i == 0 else '',
            'SKU': variant.sku or '',
            'Barcode': '',
            'Option1 name': option1_name if i == 0 else '',
            'Option1 value': variant.option1 or 'Default Title',
            'Option1 Linked To': '',
            'Option2 name': option2_name if i == 0 else '',
            'Option2 value': variant.option2 or '',
            'Option2 Linked To': '',
            'Option3 name': option3_name if i == 0 else '',
            'Option3 value': variant.option3 or '',
            'Option3 Linked To': '',
            'Price': str(price),
            'Compare-at price': variant.compare_at_price or '',
            'Cost per item': '',
            'Charge tax': 'TRUE',
            'Tax code': '',
            'Inventory tracker': 'shopify',
            'Inventory quantity': '100',
            'Continue selling when out of stock': 'DENY',
            'Weight value (grams)': str(variant.grams) if variant.grams else '0',
            'Weight unit for display': 'g',
            'Requires shipping': 'TRUE',
            'Fulfillment service': 'manual',
            'Product image URL': product.images[0].src if i == 0 and product.images else '',
            'Image position': '1' if i == 0 and product.images else '',
            'Image alt text': product.images[0].alt or '' if i == 0 and product.images else '',
            'Variant image URL': '',
            'Gift card': 'FALSE' if i == 0 else '',
            'SEO title': product.title[:70] if i == 0 else '',
            'SEO description': ''
        }
        rows.append(row)

    for img_idx, img in enumerate(product.images[1:], start=2):
        row = {h: '' for h in headers}
        row['URL handle'] = handle
        row['Product image URL'] = img.src
        row['Image position'] = str(img_idx)
        row['Image alt text'] = img.alt or ''
        rows.append(row)

    try:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        log_info(f"CSV文件已生成: {output_path}")
        return True
    except Exception as e:
        log_error(f"CSV写入失败: {e}")
        return False


# ============================================================
# Cookie 状态上报
# ============================================================

def _report_cookie_status_worker(is_valid: bool, detail: str):
    """后台线程：执行上报，失败静默处理"""
    try:
        url = f"{LOG_API_BASE_URL}/api/shopify/cookie-status/report"
        payload = {
            "store_id": STORE_ID,
            "is_valid": is_valid,
            "checker":  "auto_loop",
            "detail":   detail[:500] if detail else "",
        }
        resp = requests.post(url, json=payload, timeout=2)
        if resp.status_code == 200:
            status_str = "有效" if is_valid else "失效"
            log_info(f"Cookie状态已上报: {status_str} | {detail}")
        else:
            log_warning(f"Cookie状态上报失败: HTTP {resp.status_code}")
    except Exception as e:
        log_warning(f"Cookie状态上报异常（不影响主流程）: {e}")


def report_cookie_status(is_valid: bool, detail: str = ""):
    """
    向 API 服务上报当前 Cookie 有效性。
    后台线程发送，不阻塞主流程。
    """
    t = threading.Thread(target=_report_cookie_status_worker, args=(is_valid, detail), daemon=True)
    t.start()


# ============================================================
# Cookie下载（从腾讯云COS）
# ============================================================

def download_cookies() -> Optional[list]:
    """从COS下载Cookie JSON，返回完整cookie列表（含domain/path信息）"""
    try:
        log_info(f"正在下载Cookie: {COOKIE_URL}")
        resp = requests.get(COOKIE_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        cookie_list = []
        if isinstance(data, dict) and 'cookies' in data:
            for c in data['cookies']:
                if 'name' in c and 'value' in c:
                    cookie_list.append(c)
        elif isinstance(data, list):
            for c in data:
                if 'name' in c and 'value' in c:
                    cookie_list.append(c)

        log_info(f"✅ Cookie加载成功，共 {len(cookie_list)} 个")
        return cookie_list
    except Exception as e:
        log_error(f"❌ Cookie下载失败: {e}")
        return None


# ============================================================
# Shopify CSV上传
# ============================================================

def _get_csrf_token_selenium(cookie_list: list) -> Optional[str]:
    """
    使用 Selenium（真实 Chrome）获取 CSRF Token。
    requests 直接访问会被 Shopify Bot 检测返回 403；
    Selenium 携带完整浏览器指纹，可绕过检测。
    """
    url = f"https://admin.shopify.com/store/{STORE_ID}/products?selectedView=all"
    driver = None
    try:
        chrome_options = ChromeOptions()
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1280,800')
        chrome_options.add_argument('--lang=zh-CN')
        chrome_options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        )
        # 隐藏自动化特征
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })

        # Selenium 注入 cookie 前必须先打开同域页面
        log_info("🌐 Selenium 正在加载 Shopify 后台...")
        driver.get("https://admin.shopify.com/")
        time.sleep(1)

        # 注入 cookie
        for c in cookie_list:
            cookie_entry = {
                'name':   c['name'],
                'value':  c['value'],
                'domain': c.get('domain', '.shopify.com'),
                'path':   c.get('path', '/'),
            }
            if c.get('secure'):
                cookie_entry['secure'] = True
            if c.get('httpOnly'):
                cookie_entry['httpOnly'] = True
            try:
                driver.add_cookie(cookie_entry)
            except Exception:
                pass  # 跳过个别不兼容的 cookie

        # 访问目标页面
        driver.get(url)
        # 等待页面加载
        time.sleep(5)

        content = driver.page_source

        pattern = r'<script type="text/json" data-serialized-id="server-data">\s*(\{.*?\})\s*</script>'
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            current_url = driver.current_url
            log_error(f"未找到 server-data，当前URL: {current_url}")
            if 'login' in current_url or 'accounts.shopify.com' in current_url:
                report_cookie_status(False, "被重定向至登录页，Cookie 已失效")
            else:
                report_cookie_status(False, "Selenium 未找到 server-data，Cookie 可能已失效")
            return None

        server_data = json.loads(match.group(1))
        token = server_data.get('csrfToken')
        if token:
            log_info(f"✅ Selenium 获取 CSRF token 成功: {token[:30]}...")
            report_cookie_status(True, "Selenium CSRF token 获取成功，Cookie 有效")
            return token

        log_error("server-data 中无 csrfToken 字段")
        report_cookie_status(False, "server-data 中无 csrfToken 字段，Cookie 可能已失效")
        return None

    except Exception as e:
        log_error(f"Selenium 获取 CSRF token 异常: {e}")
        report_cookie_status(False, f"Selenium 异常: {str(e)[:200]}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def upload_csv_to_shopify(csv_file: str) -> bool:
    """
    将CSV上传到Shopify后台（失败自动重试1次）
    """
    for attempt in range(1, 3):  # 最多2次（原始 + 重试1次）
        log_info(f"📤 上传CSV（第{attempt}次尝试）: {os.path.basename(csv_file)}")
        if _do_upload(csv_file):
            return True
        if attempt < 2:
            log_warning("上传失败，5秒后重试...")
            time.sleep(5)
    return False


def _do_upload(csv_file: str) -> bool:
    """执行一次上传"""
    # 1. 下载Cookie
    cookie_list = download_cookies()
    if not cookie_list:
        return False

    from requests.cookies import RequestsCookieJar
    jar = RequestsCookieJar()
    for c in cookie_list:
        domain = c.get('domain', '')
        path   = c.get('path', '/')
        jar.set(c['name'], c['value'], domain=domain, path=path)

    session = requests.Session()
    session.cookies = jar

    cookies_dict     = {c['name']: c['value'] for c in cookie_list}
    session_token    = cookies_dict.get('_shopify_s', '')
    multitrack_token = cookies_dict.get('_shopify_y', '')

    # 2. 获取文件信息
    file_path = Path(csv_file)
    file_size = file_path.stat().st_size
    filename  = file_path.name
    log_info(f"文件: {filename}，大小: {file_size} bytes")

    # 3. 获取CSRF Token（Selenium 真实浏览器，绕过 Shopify Bot 检测）
    csrf_token = _get_csrf_token_selenium(cookie_list)
    if not csrf_token:
        return False

    # 4. 获取上传凭证
    log_info("获取GCS上传凭证...")
    api_url = (f"https://admin.shopify.com/api/operations/"
               f"a2199f150c46ccdff0a4ea14b2362f7b6c06412eee6d360d8f0e128486e39cf4/"
               f"ProductCSVStageUploads/shopify/{STORE_ID}")

    req_headers = {
        'accept': 'application/json',
        'accept-language': 'zh-CN,zh;q=0.9',
        'apollographql-client-name': 'core',
        'cache-control': 'no-cache,no-store,must-revalidate,max-age=0',
        'content-type': 'application/json',
        'origin': 'https://admin.shopify.com',
        'referer': f'https://admin.shopify.com/store/{STORE_ID}/products?selectedView=all',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'shopify-proxy-api-enable': 'true',
        'target-manifest-route-id': 'products:list',
        'target-pathname': '/store/:storeHandle/products',
        'target-slice': 'products-section',
        'x-csrf-token': csrf_token,
    }

    page_view_token = str(uuid.uuid4())
    payload = {
        "operationName": "ProductCSVStageUploads",
        "variables": {
            "input": [{
                "filename": filename,
                "mimeType": "text/csv",
                "httpMethod": "POST",
                "fileSize": str(file_size),
                "resource": "PRODUCT_IMPORT"
            }]
        },
        "extensions": {
            "client_context": {
                "page_view_token": page_view_token,
                "client_route_handle": "products:list",
                "client_pathname": f"/store/{STORE_ID}/products",
                "client_normalized_pathname": "/store/:storeHandle/products",
                "shopify_session_token": session_token,
                "shopify_multitrack_token": multitrack_token
            }
        }
    }

    try:
        resp = session.post(api_url, headers=req_headers, json=payload, timeout=30)
        if resp.status_code != 200:
            log_error(f"获取凭证失败: {resp.status_code} {resp.text[:300]}")
            return False

        result = resp.json()
        if 'errors' in result:
            for err in result['errors']:
                log_error(f"GraphQL错误: {err.get('message', '')}")
            return False

        staged = result['data']['stagedUploadsCreate']['stagedTargets'][0]
        upload_url = staged['url']
        parameters  = staged['parameters']
        log_info(f"✅ 获取上传凭证成功")
    except Exception as e:
        log_error(f"获取凭证异常: {e}")
        return False

    # 5. 上传到GCS
    log_info("上传文件到Google Cloud Storage...")
    try:
        files_data = {}
        for param in parameters:
            files_data[param['name']] = (None, param['value'])

        with open(csv_file, 'rb') as f:
            files_data['file'] = (filename, f, 'text/csv')
            upload_headers = {
                'accept': '*/*',
                'origin': 'https://admin.shopify.com',
                'referer': 'https://admin.shopify.com/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            up_resp = requests.post(upload_url, headers=upload_headers,
                                    files=files_data, timeout=60)

        if up_resp.status_code in [200, 201, 204]:
            log_info("✅ CSV上传到GCS成功！")
            return True
        else:
            log_error(f"GCS上传失败: {up_resp.status_code} {up_resp.text[:300]}")
            return False
    except Exception as e:
        log_error(f"GCS上传异常: {e}")
        return False


# ============================================================
# 主循环
# ============================================================

def process_one_task(analyzer: ZhipuImageAnalyzer) -> str:
    """
    处理单条任务
    返回值: 'success' / 'failed' / 'skipped'
    """
    # 1. 获取任务
    task = fetch_one_task()
    if not task:
        log_info("暂无待处理任务，等待下一轮...")
        return 'skipped'

    keer_product_id  = task.get('keer_product_id')
    client_product_url   = task.get('client_product_url')
    client_product_image = task.get('client_product_image')
    quotation_result     = task.get('quotation_result')

    log_info(f"--- 开始处理任务: {keer_product_id} ---")
    log_info(f"商品URL: {client_product_url}")

    # 2. 解析价格
    price = parse_price_from_quotation(quotation_result)
    if price is None:
        log_warning("价格解析失败，使用默认价格 0.0")
        price = 0.0
    log_info(f"解析价格: ${price}")

    # 3. 抓取商品
    scraper = ShopifyScraper()
    product = scraper.fetch(client_product_url)
    if not product:
        log_error("商品抓取失败")
        feedback_task_status(keer_product_id, 2)
        return 'failed'

    log_info(f"商品标题: {product.title} | 变体: {len(product.variants)} | 图片: {len(product.images)}")

    # 4. AI分类
    category = None
    if client_product_image:
        log_info("正在识别商品分类...")
        category = get_product_category(analyzer, client_product_image)
    log_info(f"商品分类: {category or '未设置'}")

    # 5. 生成CSV（保存到C:\ShopifyAutoLog\csv\）
    csv_dir = os.path.join(LOG_DIR, 'csv')
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, f"shopify_import_{keer_product_id}.csv")

    if not generate_shopify_csv(product, price, category, csv_path):
        log_error("CSV生成失败")
        feedback_task_status(keer_product_id, 2)
        return 'failed'

    # 6. 上传CSV
    upload_ok = upload_csv_to_shopify(csv_path)

    if upload_ok:
        feedback_task_status(keer_product_id, 1)
        log_info(f"✅ 任务完成: {keer_product_id}")
        return 'success'
    else:
        feedback_task_status(keer_product_id, 2)
        log_error(f"❌ 任务失败: {keer_product_id}")
        return 'failed'


def main_loop():
    """24小时无限循环主入口"""
    print("\n" + "=" * 60)
    print("🚀 Shopify 自动化工具 — 无限循环模式")
    print(f"   每 {TASK_INTERVAL_SECONDS // 60} 分钟处理一个产品")
    print(f"   日志目录: {LOG_DIR}")
    print("=" * 60 + "\n")

    _ensure_log_dir()
    analyzer = ZhipuImageAnalyzer()
    init_global_api_keys()

    while True:
        loop_start = time.time()
        keer_product_id = None

        try:
            # 先偷看一下任务ID，用于日志
            task_preview = fetch_one_task()
            keer_product_id = task_preview.get('keer_product_id') if task_preview else None

            result = process_one_task(analyzer)

            if result == 'skipped':
                write_daily_log('-', 'skipped', '无待处理任务')
                write_daily_summary()
                log_info(f"等待 {NO_TASK_WAIT_SECONDS} 秒后重试...")
                time.sleep(NO_TASK_WAIT_SECONDS)
                continue  # 跳过3分钟等待，直接再轮询

            elif result == 'success':
                write_daily_log(keer_product_id or '-', 'success', '生成+上传均成功')

            elif result == 'failed':
                write_daily_log(keer_product_id or '-', 'failed', '生成或上传失败')

            write_daily_summary()

        except Exception as e:
            log_error(f"🔴 主循环异常（不中断程序）: {e}")
            import traceback
            traceback.print_exc()
            write_daily_log(keer_product_id or '-', 'failed', f'主循环异常: {str(e)[:100]}')
            write_daily_summary()

        # 等待至满3分钟
        elapsed = time.time() - loop_start
        remaining = TASK_INTERVAL_SECONDS - elapsed
        if remaining > 0:
            log_info(f"⏱️ 本次耗时 {elapsed:.1f}s，等待 {remaining:.1f}s 后处理下一个...")
            time.sleep(remaining)


# ============================================================
# 影刀 RPA 入口（供影刀直接调用）
# ============================================================

def shopify_run(args=None):
    """
    影刀 RPA 统一入口函数。
    在影刀中配置「执行Python函数」，函数名填 shopify_run，即可全自动运行。
    args 参数由影刀平台传入，可忽略。
    """
    main_loop()


# ============================================================
# 程序入口
# ============================================================

if __name__ == "__main__":
    main_loop()
