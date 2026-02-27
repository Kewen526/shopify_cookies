# -*- coding: utf-8 -*-
"""
Shopify CSV 自动生成 + 上传
支持单次测试 (process_one_task) 和无限循环模式 (run_forever)。
"""

import csv
import json
import os
import re
import time
import traceback
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
LOG_API_BASE_URL = "http://47.104.72.198:2580"

# Shopify配置
STORE_ID   = "893848-2"
COOKIE_URL = "https://ceshi-1300392622.cos.ap-beijing.myqcloud.com/shopify-cookies/893848-2.json"

# 库存同步配置
INVENTORY_LOCATION_ID   = "83358875936"
INVENTORY_LOCATION_NAME = "牟平区北关大街845"
AUTODS_LOCATION_NAME    = "AutoDS prod-pfhikdgf"   # AutoDS 仓库位置（固定）
INVENTORY_WAIT_SECONDS  = 120              # 产品导入后等待秒数（1-2分钟）
INVENTORY_QUANTITY      = 100              # 固定库存数量

# 日志目录
LOG_DIR = r"C:\ShopifyAutoLog"


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
    log_path = _today_log_path()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{now_str}] [{result.upper():8s}] ID={str(keer_product_id or '-'):30s} {detail}\n"
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(line)
    _write_db_log(keer_product_id, result, detail)


def _write_db_log(keer_product_id: str, result: str, detail: str = ""):
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
    t = threading.Thread(target=_report_cookie_status_worker, args=(is_valid, detail), daemon=True)
    t.start()


# ============================================================
# Cookie下载
# ============================================================

def download_cookies() -> Optional[list]:
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
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })

        log_info("🌐 Selenium 正在加载 Shopify 后台...")
        driver.get("https://admin.shopify.com/")
        time.sleep(1)

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
                pass

        driver.get(url)
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
    for attempt in range(1, 3):
        log_info(f"📤 上传CSV（第{attempt}次尝试）: {os.path.basename(csv_file)}")
        if _do_upload(csv_file):
            return True
        if attempt < 2:
            log_warning("上传失败，5秒后重试...")
            time.sleep(5)
    return False


def _do_upload(csv_file: str) -> bool:
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

    file_path = Path(csv_file)
    file_size = file_path.stat().st_size
    filename  = file_path.name
    log_info(f"文件: {filename}，大小: {file_size} bytes")

    csrf_token = _get_csrf_token_selenium(cookie_list)
    if not csrf_token:
        return False

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
        else:
            log_error(f"GCS上传失败: {up_resp.status_code} {up_resp.text[:300]}")
            return False
    except Exception as e:
        log_error(f"GCS上传异常: {e}")
        return False

    # 步骤3 + 步骤4：触发 Shopify 真正导入
    return _trigger_shopify_import(session, req_headers, parameters,
                                   session_token, multitrack_token, page_view_token)


def _trigger_shopify_import(session: requests.Session, base_headers: dict,
                             gcs_parameters: list,
                             session_token: str, multitrack_token: str,
                             page_view_token: str) -> bool:
    """
    完整的 Shopify 导入流程（抓包确认的真实接口）：
      步骤3: ProductImportCreate  → 用 GCS key 创建导入任务，返回 ProductImport ID
      步骤4: ProductImportSubmit  → 用 ID 提交执行，产品才会真正出现在后台
    """

    # 从 GCS 参数里提取 key（格式如 tmp/xxxxx/filename.csv）
    staged_key = None
    for param in gcs_parameters:
        if param.get('name') == 'key':
            staged_key = param['value']
            break

    if not staged_key:
        log_error("❌ 未找到 GCS staged key，无法触发导入")
        return False

    log_info(f"📥 步骤3: ProductImportCreate，staged_key: {staged_key}")

    # ── 公共 headers ──────────────────────────────────────────
    common_headers = {
        'accept': 'application/json',
        'accept-language': 'zh-CN,zh;q=0.9',
        'apollographql-client-name': 'core',
        'cache-control': 'no-cache,no-store,must-revalidate,max-age=0',
        'content-type': 'application/json',
        'origin': 'https://admin.shopify.com',
        'referer': f'https://admin.shopify.com/store/{STORE_ID}/products?selectedView=all',
        'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'),
        'shopify-proxy-api-enable': 'true',
        'target-manifest-route-id': 'products:list',
        'target-pathname': '/store/:storeHandle/products',
        'target-slice': 'products-section',
        'x-csrf-token': base_headers.get('x-csrf-token', ''),
    }

    client_context = {
        "page_view_token": page_view_token,
        "client_route_handle": "products:list",
        "client_pathname": f"/store/{STORE_ID}/products",
        "client_normalized_pathname": "/store/:storeHandle/products",
        "shopify_session_token": session_token,
        "shopify_multitrack_token": multitrack_token
    }

    # ── 步骤3: ProductImportCreate ────────────────────────────
    create_url = (
        f"https://admin.shopify.com/api/operations/"
        f"68c029f983cbd39de99c30c73518a1f84a1053e06c5b312ed4d994967dc36a3f/"
        f"ProductImportCreate/shopify/{STORE_ID}"
    )
    create_payload = {
        "operationName": "ProductImportCreate",
        "variables": {
            "input": {
                "url": staged_key,
                "overwrite": True,
                "publishToAllChannels": True
            }
        },
        "extensions": {"client_context": client_context}
    }

    try:
        resp = session.post(create_url, headers=common_headers, json=create_payload, timeout=30)
        log_info(f"ProductImportCreate 响应: HTTP {resp.status_code}")
        log_info(f"响应内容: {resp.text[:500]}")

        if resp.status_code != 200:
            log_error(f"ProductImportCreate 失败: {resp.status_code}")
            return False

        result = resp.json()
        if 'errors' in result:
            log_error(f"ProductImportCreate GraphQL 错误: {result['errors']}")
            return False

        # 提取 ProductImport GID，格式: gid://shopify/ProductImport/xxxxxxxx
        try:
            import_gid = result['data']['productImportCreate']['productImport']['id']
        except (KeyError, TypeError) as e:
            log_error(f"无法从响应中提取 ProductImport ID: {e}，响应: {result}")
            return False

        log_info(f"✅ ProductImportCreate 成功，Import ID: {import_gid}")

    except Exception as e:
        log_error(f"ProductImportCreate 异常: {e}")
        return False

    # ── 步骤4: ProductImportSubmit ────────────────────────────
    log_info(f"📤 步骤4: ProductImportSubmit，ID: {import_gid}")

    submit_url = (
        f"https://admin.shopify.com/api/operations/"
        f"0623f4c83b0e6dfe94448cebe8295bb1ae5c3b6406ed1e9acec2d69571d477a4/"
        f"ProductImportSubmit/shopify/{STORE_ID}"
    )
    submit_payload = {
        "operationName": "ProductImportSubmit",
        "variables": {
            "id": import_gid
        },
        "extensions": {"client_context": client_context}
    }

    try:
        resp = session.post(submit_url, headers=common_headers, json=submit_payload, timeout=30)
        log_info(f"ProductImportSubmit 响应: HTTP {resp.status_code}")
        log_info(f"响应内容: {resp.text[:500]}")

        if resp.status_code != 200:
            log_error(f"ProductImportSubmit 失败: {resp.status_code}")
            return False

        result = resp.json()
        if 'errors' in result:
            log_error(f"ProductImportSubmit GraphQL 错误: {result['errors']}")
            return False

        log_info("✅ ProductImportSubmit 成功！产品将在 Shopify 后台异步导入（通常1~2分钟内完成）")
        return True

    except Exception as e:
        log_error(f"ProductImportSubmit 异常: {e}")
        return False


# ============================================================
# 库存同步（产品导入后设置库存数量）
# ============================================================

def generate_inventory_csv(product: ProductDetail, location_name: str,
                            output_path: str, quantity: int = 100) -> bool:
    """
    生成 Shopify 库存导入 CSV。
    格式与 Shopify 导出的库存 CSV 完全一致：
    - 每个变体两行：牟平区北关大街845 行 + AutoDS 行
    - 使用 "On hand (current)" 和 "On hand (new)" 列
    """
    headers = [
        'Handle', 'Title',
        'Option1 Name', 'Option1 Value',
        'Option2 Name', 'Option2 Value',
        'Option3 Name', 'Option3 Value',
        'SKU', 'HS Code', 'COO',
        'Location', 'Bin name',
        'Incoming (not editable)', 'Unavailable (not editable)',
        'Committed (not editable)', 'Available (not editable)',
        'On hand (current)', 'On hand (new)'
    ]

    handle = product.handle or re.sub(r'[^a-z0-9]+', '-', product.title.lower()).strip('-')
    option1_name = product.options[0].get('name', 'Title') if product.options else 'Title'
    option2_name = product.options[1].get('name', '') if len(product.options) > 1 else ''
    option3_name = product.options[2].get('name', '') if len(product.options) > 2 else ''

    rows = []
    for variant in product.variants:
        common = {
            'Handle': handle,
            'Title': product.title,
            'Option1 Name': option1_name,
            'Option1 Value': variant.option1 or 'Default Title',
            'Option2 Name': option2_name,
            'Option2 Value': variant.option2 or '',
            'Option3 Name': option3_name,
            'Option3 Value': variant.option3 or '',
            'SKU': variant.sku or '',
            'HS Code': '',
            'COO': '',
        }
        # 主仓库行：设置库存数量
        main_row = {
            **common,
            'Location': location_name,
            'Bin name': '',
            'Incoming (not editable)': '0',
            'Unavailable (not editable)': '0',
            'Committed (not editable)': '0',
            'Available (not editable)': '0',
            'On hand (current)': '0',
            'On hand (new)': str(quantity),
        }
        rows.append(main_row)
        # AutoDS 行：not stocked
        autods_row = {
            **common,
            'Location': AUTODS_LOCATION_NAME,
            'Bin name': '',
            'Incoming (not editable)': 'not stocked',
            'Unavailable (not editable)': 'not stocked',
            'Committed (not editable)': 'not stocked',
            'Available (not editable)': 'not stocked',
            'On hand (current)': 'not stocked',
            'On hand (new)': '',
        }
        rows.append(autods_row)

    try:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        log_info(f"库存CSV已生成: {output_path} ({len(product.variants)} 个变体, 数量={quantity})")
        return True
    except Exception as e:
        log_error(f"库存CSV写入失败: {e}")
        return False


def sync_inventory(inventory_csv_file: str) -> bool:
    """
    完整的库存同步流程：
    1. 下载Cookie + 获取CSRF Token
    2. InventoryStagedUploads → 获取GCS上传凭证
    3. 上传库存CSV到GCS
    4. InventoryImportCreate → 创建导入任务
    5. InventoryImportSubmit → 提交导入
    6. JobPoller → 轮询等待完成
    """
    for attempt in range(1, 3):
        log_info(f"📦 库存同步（第{attempt}次尝试）: {os.path.basename(inventory_csv_file)}")
        if _do_inventory_sync(inventory_csv_file):
            return True
        if attempt < 2:
            log_warning("库存同步失败，10秒后重试...")
            time.sleep(10)
    return False


def _do_inventory_sync(inventory_csv_file: str) -> bool:
    """执行库存同步的具体逻辑"""
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

    file_path = Path(inventory_csv_file)
    file_size = file_path.stat().st_size
    filename  = file_path.name
    log_info(f"库存文件: {filename}，大小: {file_size} bytes")

    # 获取 CSRF Token
    csrf_token = _get_csrf_token_selenium(cookie_list)
    if not csrf_token:
        return False

    page_view_token = str(uuid.uuid4())

    # 库存操作的公共 headers
    inv_headers = {
        'accept': 'application/json',
        'accept-language': 'zh-CN,zh;q=0.9',
        'apollographql-client-name': 'core',
        'cache-control': 'no-cache,no-store,must-revalidate,max-age=0',
        'content-type': 'application/json',
        'origin': 'https://admin.shopify.com',
        'referer': (f'https://admin.shopify.com/store/{STORE_ID}/products/inventory'
                    f'?location_id={INVENTORY_LOCATION_ID}'),
        'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'),
        'shopify-proxy-api-enable': 'true',
        'target-manifest-route-id': 'products:inventory:list',
        'target-pathname': '/store/:storeHandle/products/inventory',
        'target-slice': 'inventory-section',
        'x-csrf-token': csrf_token,
    }

    client_context = {
        "page_view_token": page_view_token,
        "client_route_handle": "products:inventory:list",
        "client_pathname": f"/store/{STORE_ID}/products/inventory",
        "client_normalized_pathname": "/store/:storeHandle/products/inventory",
        "shopify_session_token": session_token,
        "shopify_multitrack_token": multitrack_token
    }

    # ── 步骤1: InventoryStagedUploads ──────────────────────────
    log_info("📤 库存步骤1: InventoryStagedUploads（获取GCS上传凭证）")
    stage_url = (
        f"https://admin.shopify.com/api/operations/"
        f"dafbde9e8213fb109b67860a344cd72657293731daa8abb55ddc0245a477716c/"
        f"InventoryStagedUploads/shopify/{STORE_ID}"
    )
    stage_payload = {
        "operationName": "InventoryStagedUploads",
        "variables": {
            "input": [{
                "filename": filename,
                "mimeType": "text/csv",
                "fileSize": str(file_size),
                "httpMethod": "POST",
                "resource": "INVENTORY_IMPORT"
            }]
        },
        "extensions": {"client_context": client_context}
    }

    try:
        resp = session.post(stage_url, headers=inv_headers, json=stage_payload, timeout=30)
        if resp.status_code != 200:
            log_error(f"InventoryStagedUploads 失败: {resp.status_code} {resp.text[:300]}")
            return False

        result = resp.json()
        if 'errors' in result:
            log_error(f"InventoryStagedUploads GraphQL 错误: {result['errors']}")
            return False

        staged = result['data']['stagedUploadsCreate']['stagedTargets'][0]
        upload_url  = staged['url']
        parameters  = staged['parameters']
        log_info("✅ 库存GCS凭证获取成功")
    except Exception as e:
        log_error(f"InventoryStagedUploads 异常: {e}")
        return False

    # ── 步骤2: 上传库存CSV到GCS ────────────────────────────────
    log_info("📤 库存步骤2: 上传CSV到Google Cloud Storage")
    try:
        files_data = {}
        for param in parameters:
            files_data[param['name']] = (None, param['value'])

        with open(inventory_csv_file, 'rb') as f:
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
            log_info("✅ 库存CSV上传到GCS成功")
        else:
            log_error(f"库存GCS上传失败: {up_resp.status_code} {up_resp.text[:300]}")
            return False
    except Exception as e:
        log_error(f"库存GCS上传异常: {e}")
        return False

    # 从 GCS 参数中提取 staged key
    staged_key = None
    for param in parameters:
        if param.get('name') == 'key':
            staged_key = param['value']
            break

    if not staged_key:
        log_error("未找到库存 GCS staged key")
        return False

    # ── 步骤3: InventoryImportCreate ──────────────────────────
    log_info(f"📥 库存步骤3: InventoryImportCreate，staged_key: {staged_key}")
    create_url = (
        f"https://admin.shopify.com/api/operations/"
        f"8d2fcb60da9f65b5f03a0f9efed1ae09b64e237405a6aabab8c530247ce79a49/"
        f"InventoryImportCreate/shopify/{STORE_ID}"
    )
    idempotency_key_create = str(uuid.uuid4())
    create_payload = {
        "operationName": "InventoryImportCreate",
        "variables": {
            "url": staged_key,
            "idempotencyKey": idempotency_key_create
        },
        "extensions": {"client_context": client_context}
    }

    try:
        resp = session.post(create_url, headers=inv_headers, json=create_payload, timeout=30)
        log_info(f"InventoryImportCreate 响应: HTTP {resp.status_code}")

        if resp.status_code != 200:
            log_error(f"InventoryImportCreate 失败: {resp.status_code}")
            return False

        result = resp.json()
        if 'errors' in result:
            log_error(f"InventoryImportCreate GraphQL 错误: {result['errors']}")
            return False

        try:
            import_gid = result['data']['inventoryImportCreate']['inventoryImport']['id']
        except (KeyError, TypeError) as e:
            log_error(f"无法提取 InventoryImport ID: {e}，响应: {json.dumps(result)[:500]}")
            return False

        log_info(f"✅ InventoryImportCreate 成功，Import ID: {import_gid}")

    except Exception as e:
        log_error(f"InventoryImportCreate 异常: {e}")
        return False

    # ── 步骤4: InventoryImportSubmit ──────────────────────────
    log_info(f"📤 库存步骤4: InventoryImportSubmit，ID: {import_gid}")
    submit_url = (
        f"https://admin.shopify.com/api/operations/"
        f"e1cbb128d9f0abd1c1b35dc85ab7ae7718944c96e5a4538b945acca1a707bd95/"
        f"InventoryImportSubmit/shopify/{STORE_ID}"
    )
    idempotency_key_submit = str(uuid.uuid4())
    submit_payload = {
        "operationName": "InventoryImportSubmit",
        "variables": {
            "id": import_gid,
            "idempotencyKey": idempotency_key_submit
        },
        "extensions": {"client_context": client_context}
    }

    job_id = None
    try:
        resp = session.post(submit_url, headers=inv_headers, json=submit_payload, timeout=30)
        log_info(f"InventoryImportSubmit 响应: HTTP {resp.status_code}")

        if resp.status_code != 200:
            log_error(f"InventoryImportSubmit 失败: {resp.status_code}")
            return False

        result = resp.json()
        if 'errors' in result:
            log_error(f"InventoryImportSubmit GraphQL 错误: {result['errors']}")
            return False

        # 提取 Job ID 用于轮询
        try:
            job_data = result.get('data', {}).get('inventoryImportSubmit', {})
            job_id = job_data.get('job', {}).get('id')
        except (KeyError, TypeError, AttributeError):
            pass

        log_info("✅ InventoryImportSubmit 成功！库存导入已提交")

    except Exception as e:
        log_error(f"InventoryImportSubmit 异常: {e}")
        return False

    # ── 步骤5: JobPoller（轮询等待完成）─────────────────────────
    if job_id:
        log_info(f"⏳ 库存步骤5: JobPoller 轮询，Job ID: {job_id}")
        _poll_inventory_job(session, inv_headers, job_id, csrf_token)
    else:
        log_info("未获取到 Job ID，跳过轮询（库存导入已提交，将在后台异步完成）")

    return True


def _poll_inventory_job(session: requests.Session, headers: dict,
                         job_id: str, csrf_token: str,
                         max_polls: int = 20, interval: int = 5):
    """
    轮询 Shopify 异步 Job 状态，直到完成或超时。
    """
    poller_base_url = (
        f"https://admin.shopify.com/api/operations/"
        f"e1593abda1eb0795fd588f8374f0f642659c1252872a4117c0ffd5e1db328980/"
        f"JobPoller/shopify/{STORE_ID}"
    )

    variables_json = json.dumps({"id": job_id})
    params = parse.urlencode({
        "operationName": "JobPoller",
        "variables": variables_json
    })
    poll_url = f"{poller_base_url}?{params}"

    for i in range(1, max_polls + 1):
        time.sleep(interval)
        try:
            resp = session.get(poll_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                log_warning(f"JobPoller 第{i}次 HTTP {resp.status_code}")
                continue

            result = resp.json()
            job_data = result.get('data', {}).get('job', {})
            done = job_data.get('done', False)

            if done:
                log_info(f"✅ 库存导入 Job 已完成（第{i}次轮询）")
                return
            else:
                log_info(f"⏳ 库存导入进行中... ({i}/{max_polls})")
        except Exception as e:
            log_warning(f"JobPoller 第{i}次异常: {e}")

    log_warning(f"JobPoller 达到最大轮询次数 ({max_polls})，库存导入可能仍在后台进行")


# ============================================================
# 单任务处理（测试用）
# ============================================================

def process_one_task(analyzer: ZhipuImageAnalyzer) -> str:
    """
    处理单条任务
    返回值: 'success' / 'failed' / 'skipped'
    """
    task = fetch_one_task()
    if not task:
        log_info("暂无待处理任务，退出。")
        return 'skipped'

    keer_product_id      = task.get('keer_product_id')
    client_product_url   = task.get('client_product_url')
    client_product_image = task.get('client_product_image')
    quotation_result     = task.get('quotation_result')

    log_info(f"--- 开始处理任务: {keer_product_id} ---")
    log_info(f"商品URL: {client_product_url}")

    # 解析价格（原始为欧元，×1.2 转为美元）
    price = parse_price_from_quotation(quotation_result)
    if price is None:
        log_warning("价格解析失败，使用默认价格 0.0")
        price = 0.0
    price_eur = price
    price = round(price * 1.2, 2)
    log_info(f"解析价格: €{price_eur} → ${price}（×1.2 EUR→USD）")

    # 抓取商品
    scraper = ShopifyScraper()
    product = scraper.fetch(client_product_url)
    if not product:
        log_error("商品抓取失败")
        feedback_task_status(keer_product_id, 2)
        return 'failed'

    log_info(f"商品标题: {product.title} | 变体: {len(product.variants)} | 图片: {len(product.images)}")

    # AI分类
    category = None
    if client_product_image:
        log_info("正在识别商品分类...")
        category = get_product_category(analyzer, client_product_image)
    log_info(f"商品分类: {category or '未设置'}")

    # 生成CSV
    csv_dir = os.path.join(LOG_DIR, 'csv')
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, f"shopify_import_{keer_product_id}.csv")

    if not generate_shopify_csv(product, price, category, csv_path):
        log_error("CSV生成失败")
        feedback_task_status(keer_product_id, 2)
        return 'failed'

    # 上传CSV
    upload_ok = upload_csv_to_shopify(csv_path)

    if upload_ok:
        # ── 库存同步 ──────────────────────────────────────
        log_info(f"产品导入成功，等待 {INVENTORY_WAIT_SECONDS} 秒后同步库存...")
        time.sleep(INVENTORY_WAIT_SECONDS)

        inventory_csv_path = os.path.join(csv_dir, f"inventory_{keer_product_id}.csv")
        if generate_inventory_csv(product, INVENTORY_LOCATION_NAME, inventory_csv_path,
                                   quantity=INVENTORY_QUANTITY):
            inv_ok = sync_inventory(inventory_csv_path)
            if inv_ok:
                log_info(f"✅ 库存同步成功: {keer_product_id}")
            else:
                log_warning(f"⚠️ 库存同步失败（不影响产品导入状态）: {keer_product_id}")
        else:
            log_warning(f"⚠️ 库存CSV生成失败: {keer_product_id}")

        feedback_task_status(keer_product_id, 1)
        log_info(f"✅ 任务完成: {keer_product_id}")
        return 'success'
    else:
        feedback_task_status(keer_product_id, 2)
        log_error(f"❌ 任务失败: {keer_product_id}")
        return 'failed'


# ============================================================
# 程序入口（单任务测试模式）
# ============================================================

def run_forever(task_interval: int = 10, key_refresh_hours: int = 1):
    """
    无限循环运行 Shopify 自动上架任务。
    24小时不间断从数据库拉取任务并处理。

    参数:
        task_interval:      每次任务之间的等待秒数（默认10秒）
        key_refresh_hours:  ZhipuAI密钥刷新间隔（小时，默认1小时）
    """
    _ensure_log_dir()

    log_info("初始化 ZhipuAI 密钥...")
    if not init_global_api_keys():
        log_error("ZhipuAI 密钥初始化失败，60秒后重试...")
        time.sleep(60)
        if not init_global_api_keys():
            log_error("ZhipuAI 密钥初始化二次失败，退出")
            return

    analyzer = ZhipuImageAnalyzer()

    print("=" * 60)
    print("🚀 Shopify 自动上架 — 无限循环模式已启动")
    print(f"   任务间隔: {task_interval}秒")
    print(f"   密钥刷新: 每{key_refresh_hours}小时")
    print(f"   日志目录: {LOG_DIR}")
    print("=" * 60)
    log_info("无限循环模式已启动")

    task_count = 0
    success_count = 0
    fail_count = 0
    last_key_refresh = time.time()

    while True:
        try:
            # 定时刷新 ZhipuAI 密钥
            if time.time() - last_key_refresh > key_refresh_hours * 3600:
                log_info("⏰ 定时刷新 ZhipuAI 密钥...")
                if refresh_api_keys():
                    log_info("✅ 密钥刷新成功")
                else:
                    log_warning("⚠️ 密钥刷新失败，继续使用旧密钥")
                last_key_refresh = time.time()

            # 处理一条任务
            result = process_one_task(analyzer)

            if result == 'success':
                task_count += 1
                success_count += 1
                log_info(f"📊 累计: 处理{task_count}条, 成功{success_count}, 失败{fail_count}")
            elif result == 'failed':
                task_count += 1
                fail_count += 1
                log_info(f"📊 累计: 处理{task_count}条, 成功{success_count}, 失败{fail_count}")
            else:
                # skipped — 没有新任务
                pass

            time.sleep(task_interval)

        except KeyboardInterrupt:
            log_info("🛑 收到中断信号，正在退出...")
            print(f"\n最终统计: 处理{task_count}条, 成功{success_count}, 失败{fail_count}")
            break
        except Exception as e:
            log_error(f"💥 循环中未预期异常: {e}")
            log_error(traceback.format_exc())
            log_info(f"30秒后继续运行...")
            time.sleep(30)


if __name__ == "__main__":
    run_forever()
