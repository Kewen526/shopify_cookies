# -*- coding: utf-8 -*-
"""
Shopify 日志与 Cookie 状态 API 服务
端口: 5002

接口列表:
  GET  /api/shopify/daily-stats           查询任务执行日志统计（支持按日期/范围筛选）
  POST /api/shopify/cookie-status/report  上报 Cookie 状态（供本地 PyCharm 调用）
  GET  /api/shopify/cookie-status         查询最新 Cookie 状态

运行方式:
  python api_server.py
"""

import pymysql
from datetime import datetime, date
from flask import Flask, request, jsonify

app = Flask(__name__)

# ============================================================
# 数据库配置
# ============================================================

DB_CONFIG = {
    "host": "47.95.157.46",
    "user": "root",
    "password": "root@kunkun",
    "port": 3306,
    "database": "quote_iw",
    "charset": "utf8mb4",
}


def get_conn():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# ============================================================
# 辅助函数
# ============================================================

def ok(data=None, msg="success"):
    return jsonify({"code": 0, "msg": msg, "data": data})


def err(msg="error", code=400):
    return jsonify({"code": code, "msg": msg, "data": None}), code


def parse_date_param(val: str, param_name: str):
    """将字符串解析为 date 对象，格式 YYYY-MM-DD"""
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


# ============================================================
# 接口 1：查询任务执行日志统计
# GET /api/shopify/daily-stats
#
# 参数（三选一）：
#   date=2024-01-15              查单日
#   start_date=...&end_date=...  查日期范围
#   （不传）                      查当天
# ============================================================

@app.route("/api/shopify/daily-stats", methods=["GET"])
def daily_stats():
    today = date.today()

    date_str       = request.args.get("date")
    start_date_str = request.args.get("start_date")
    end_date_str   = request.args.get("end_date")

    if date_str:
        d = parse_date_param(date_str, "date")
        if d is None:
            return err("date 格式错误，请使用 YYYY-MM-DD")
        start_d = end_d = d
    elif start_date_str or end_date_str:
        start_d = parse_date_param(start_date_str, "start_date")
        end_d   = parse_date_param(end_date_str,   "end_date")
        if start_d is None or end_d is None:
            return err("start_date / end_date 格式错误，请使用 YYYY-MM-DD")
        if start_d > end_d:
            return err("start_date 不能晚于 end_date")
    else:
        start_d = end_d = today

    try:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                sql = """
                    SELECT
                        task_date,
                        COUNT(*) AS total,
                        SUM(result = 'success') AS success,
                        SUM(result = 'failed')  AS failed,
                        SUM(result = 'skipped') AS skipped
                    FROM shopify_task_log
                    WHERE task_date BETWEEN %s AND %s
                    GROUP BY task_date
                    ORDER BY task_date DESC
                """
                cursor.execute(sql, (start_d.strftime("%Y-%m-%d"),
                                     end_d.strftime("%Y-%m-%d")))
                rows = cursor.fetchall()
        finally:
            conn.close()

        result = []
        for row in rows:
            result.append({
                "task_date": str(row["task_date"]),
                "total":     int(row["total"]   or 0),
                "success":   int(row["success"] or 0),
                "failed":    int(row["failed"]  or 0),
                "skipped":   int(row["skipped"] or 0),
            })

        return ok({
            "query_range": {
                "start_date": start_d.strftime("%Y-%m-%d"),
                "end_date":   end_d.strftime("%Y-%m-%d"),
            },
            "stats": result,
        })

    except Exception as e:
        return err(f"数据库查询异常: {e}", 500)


# ============================================================
# 接口 2：上报 Cookie 状态
# POST /api/shopify/cookie-status/report
#
# Body (JSON):
#   {
#     "store_id":  "893848-2",   必填
#     "is_valid":  false,        必填，true=有效 false=失效
#     "checker":   "pycharm",    可选，来源标识
#     "detail":    "401错误"     可选，备注说明
#   }
# ============================================================

@app.route("/api/shopify/cookie-status/report", methods=["POST"])
def cookie_report():
    body = request.get_json(silent=True)
    if not body:
        return err("请求体必须是 JSON 格式")

    store_id = body.get("store_id", "").strip()
    if not store_id:
        return err("缺少必填字段: store_id")

    is_valid_raw = body.get("is_valid")
    if is_valid_raw is None:
        return err("缺少必填字段: is_valid")
    is_valid = 1 if is_valid_raw else 0

    checker = str(body.get("checker", "pycharm"))[:50]
    detail  = str(body.get("detail",  ""))[:500]

    try:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO shopify_cookie_status
                        (store_id, is_valid, checked_at, checker, detail)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    store_id,
                    is_valid,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    checker,
                    detail,
                ))
            conn.commit()
        finally:
            conn.close()

        return ok({
            "store_id":   store_id,
            "is_valid":   bool(is_valid),
            "reported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, msg="上报成功")

    except Exception as e:
        return err(f"数据库写入异常: {e}", 500)


# ============================================================
# 接口 3：查询最新 Cookie 状态
# GET /api/shopify/cookie-status?store_id=893848-2
#
# 参数:
#   store_id  可选，不传则返回所有店铺最新状态
# ============================================================

@app.route("/api/shopify/cookie-status", methods=["GET"])
def cookie_status():
    store_id = request.args.get("store_id", "").strip()

    try:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                if store_id:
                    # 查指定店铺的最新一条
                    sql = """
                        SELECT store_id, is_valid, checked_at, checker, detail
                        FROM shopify_cookie_status
                        WHERE store_id = %s
                        ORDER BY checked_at DESC
                        LIMIT 1
                    """
                    cursor.execute(sql, (store_id,))
                    row = cursor.fetchone()
                    if not row:
                        return err(f"未找到 store_id={store_id} 的 Cookie 状态记录", 404)

                    data = {
                        "store_id":   row["store_id"],
                        "is_valid":   bool(row["is_valid"]),
                        "checked_at": str(row["checked_at"]),
                        "checker":    row["checker"],
                        "detail":     row["detail"],
                    }
                else:
                    # 查所有店铺各自最新一条（子查询取最大ID）
                    sql = """
                        SELECT s.store_id, s.is_valid, s.checked_at, s.checker, s.detail
                        FROM shopify_cookie_status s
                        INNER JOIN (
                            SELECT store_id, MAX(id) AS max_id
                            FROM shopify_cookie_status
                            GROUP BY store_id
                        ) t ON s.store_id = t.store_id AND s.id = t.max_id
                        ORDER BY s.checked_at DESC
                    """
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    data = [
                        {
                            "store_id":   r["store_id"],
                            "is_valid":   bool(r["is_valid"]),
                            "checked_at": str(r["checked_at"]),
                            "checker":    r["checker"],
                            "detail":     r["detail"],
                        }
                        for r in rows
                    ]
        finally:
            conn.close()

        return ok(data)

    except Exception as e:
        return err(f"数据库查询异常: {e}", 500)


# ============================================================
# 启动
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=False)
