import sys
import sqlite3
import json
import os
import base64
import time
import re
from base.spider import Spider


class Spider(Spider):
    def getName(self):
        return "Universal_DB_Spider"

    # ==================== 扫描目录列表 ====================
    # 程序会在这些子目录下递归搜索 .db 数据库文件
    SCAN_DIR_LIST = [
        "bh", "tvbox", "bhh", "tvbox/lz", "lz",
        "VodPlus", "peekpili/php-scripts", "纯福利", "江湖"
    ]

    # ==================== 常量配置 ====================
    MIN_DB_SIZE = 3 * 1024 * 1024          # 数据库文件最小尺寸（3MB），小于该值的文件将被跳过
    DB_LOGO = "https://img.icons8.com/color/94/folder-invoices.png"  # 默认封面图标（无图片时使用）
    CHUNK_SIZE = 500                        # 分块大小（预留字段）
    PAGE_LIMIT = 40                         # 每页显示条目数
    COUNT_THRESHOLD = 20                    # 数量阈值（预留字段）
    SCAN_CACHE_PATH = "/data/local/tmp/db_scan_cache.json"  # 扫描结果缓存文件路径
    SCAN_CACHE_TTL = 300                    # 缓存有效期（秒），超过300秒则重新扫描

    # ==================== 初始化方法 ====================
    def init(self, extend=""):
        self.inited = True                  # 标记初始化完成
        self.databases = {}                 # 存储所有扫描到的数据库信息 {base64_key: {name, path, size_str, valid}}
        self.scan_roots = ["/storage/emulated/0"]  # 扫描根目录列表，默认为手机内部存储
        self.auto_mapping_cache = {}        # 自动字段映射缓存，避免重复分析表结构
        self.row_count_cache = {}           # 行数统计缓存，避免重复执行 COUNT 查询
        self._conn_cache = {}               # 数据库连接缓存，复用连接提升性能

        # ---- 扫描外部存储设备（如SD卡、U盘等）----
        try:
            if os.path.exists("/storage"):
                for s in os.listdir("/storage"):
                    # 排除系统保留目录
                    if s not in [
                        "self", "emulated",
                        "knox", "sdcard0", "runtime"
                    ]:
                        full_s = os.path.join("/storage", s)
                        if os.path.isdir(full_s):
                            self.scan_roots.append(full_s)  # 将外部存储加入扫描列表
        except:
            pass

        # ---- 加载缓存或执行全量扫描 ----
        if not self._load_scan_cache():
            # 缓存不存在或已过期，执行差异扫描
            self._differential_scan()
            self._save_scan_cache()

    # ==================== 加载扫描缓存 ====================
    def _load_scan_cache(self):
        """尝试从缓存文件加载数据库列表，缓存有效期为 SCAN_CACHE_TTL 秒"""
        try:
            if not os.path.exists(self.SCAN_CACHE_PATH):
                return False  # 缓存文件不存在
            mtime = os.path.getmtime(self.SCAN_CACHE_PATH)
            if time.time() - mtime > self.SCAN_CACHE_TTL:
                return False  # 缓存已过期，需要重新扫描
            with open(self.SCAN_CACHE_PATH, 'r') as f:
                cached = json.load(f)
            # 验证缓存中的文件是否仍然存在且满足最小尺寸要求
            valid_items = {}
            for key, info in cached.items():
                p = info.get("path", "")
                if os.path.exists(p):
                    sz = os.path.getsize(p)
                    if sz >= self.MIN_DB_SIZE:
                        valid_items[key] = info
            if valid_items:
                self.databases = valid_items  # 使用缓存数据
                return True
        except:
            pass
        return False  # 加载失败，需要重新扫描

    # ==================== 保存扫描缓存 ====================
    def _save_scan_cache(self):
        """将当前扫描到的数据库列表持久化到 JSON 缓存文件"""
        try:
            cache_data = {}
            for key, info in self.databases.items():
                cache_data[key] = {
                    "name": info.get("name", ""),
                    "path": info.get("path", ""),
                    "size_str": info.get("size_str", ""),
                    "valid": info.get("valid", 1)
                }
            # 确保缓存目录存在
            os.makedirs(
                os.path.dirname(self.SCAN_CACHE_PATH),
                exist_ok=True
            )
            with open(self.SCAN_CACHE_PATH, 'w') as f:
                json.dump(cache_data, f)
        except:
            pass

    # ==================== 格式化文件大小 ====================
    def _format_size(self, size_bytes):
        """将字节数转换为可读的文件大小字符串，如 '12.5M' 或 '512K'"""
        if size_bytes < 1048576:  # 小于 1MB
            return f"{int(size_bytes / 1024)}K"
        return f"{size_bytes / 1048576:.1f}M"

    # ==================== 差异扫描 ====================
    def _differential_scan(self):
        """
        遍历所有扫描根目录下的目标子目录，递归查找符合条件的 .db 文件。
        筛选条件：文件后缀为 .db 且大小 >= MIN_DB_SIZE（3MB）。
        结果按（是否外部存储, 文件大小）排序后存入 self.databases。
        """
        temp_list = []
        for root_p in self.scan_roots:
            # 判断是否为外部存储设备（非内置存储路径）
            is_ext = not root_p.startswith("/storage/emulated/0")
            star = "☆" if is_ext else ""  # 外部存储的文件名后加 ☆ 标记

            for target in self.SCAN_DIR_LIST:
                base_p = os.path.join(root_p, target)
                if not os.path.isdir(base_p):
                    continue  # 目标子目录不存在，跳过

                # 递归遍历目录下所有文件
                for root, dirs, files in os.walk(base_p):
                    for file in files:
                        if not file.lower().endswith(".db"):
                            continue  # 非 .db 文件，跳过
                        f_path = os.path.join(root, file)
                        try:
                            sz_raw = os.path.getsize(f_path)
                            if sz_raw < self.MIN_DB_SIZE:
                                continue  # 文件太小，跳过

                            # 计算相对路径用于显示
                            rel_path = (
                                f_path
                                .replace("/storage/emulated/0/", "")
                                .replace(root_p, "")
                            )
                            # 构建显示名称：包含路径、大小和外部存储标记
                            display_name = (
                                f"📁{rel_path} "
                                f"({self._format_size(sz_raw)}){star}"
                            )
                            # 使用文件完整路径的 Base64 编码作为唯一键
                            db_key = base64.b64encode(
                                f_path.encode()
                            ).decode()
                            temp_list.append({
                                "key": db_key,
                                "name": display_name,
                                "path": f_path,
                                "is_ext": is_ext,
                                "size_bytes": sz_raw
                            })
                        except:
                            continue

        # 排序规则：内部存储优先（False < True），同类型按文件大小升序
        temp_list.sort(key=lambda x: (x["is_ext"], x["size_bytes"]))

        # 将排序后的结果写入数据库字典
        for item in temp_list:
            self.databases[item["key"]] = {
                "name": item["name"],
                "path": item["path"],
                "size_str": self._format_size(item["size_bytes"]),
                "valid": 1  # 标记为有效数据库
            }

    # ==================== 获取数据库连接 ====================
    def _get_connection(self, db_path):
        """
        获取指定路径的 SQLite 数据库连接（带缓存复用机制）。
        如果缓存的连接仍然有效则直接复用，否则创建新连接。
        连接参数优化：启用 WAL 日记模式、内存映射、增大缓存、使用内存临时存储。
        """
        # 尝试复用缓存的连接
        if db_path in self._conn_cache:
            try:
                self._conn_cache[db_path].execute("SELECT 1")  # 测试连接是否仍然有效
                return self._conn_cache[db_path]
            except:
                del self._conn_cache[db_path]  # 连接已失效，删除缓存

        if not db_path or not os.path.exists(db_path):
            return None  # 文件不存在，返回空

        try:
            conn = sqlite3.connect(db_path, timeout=5)  # 设置 5 秒超时
            conn.row_factory = sqlite3.Row  # 使查询结果支持列名访问（如 row["name"]）
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=DELETE")       # 启用 WAL 日记模式，提升并发读写性能
            cur.execute("PRAGMA cache_size=-8192")        # 设置缓存大小为 8MB（负数表示 KB 单位）
            cur.execute("PRAGMA mmap_size=268435456")     # 启用内存映射 I/O，最大 256MB
            cur.execute("PRAGMA temp_store=MEMORY")       # 临时数据存储在内存中，避免磁盘 I/O
            cur.execute("PRAGMA synchronous=NORMAL")      # 同步模式设为 NORMAL（性能与安全的平衡）
            cur.execute("PRAGMA page_size=4096")          # 页大小设为 4KB
            self._conn_cache[db_path] = conn  # 缓存连接以便复用
            return conn
        except:
            return None  # 连接失败

    # ==================== 自动字段映射 ====================
    def _get_auto_mapping(self, conn, db_key=None):
        """
        自动分析数据库表结构，建立逻辑字段到实际列名的映射关系。
        核心流程：
        1. 获取所有用户表名，按优先级选择主数据表
        2. 查找分类表（如 categories、type 等）
        3. 对主表的每个逻辑字段，从候选列名列表中通过评分机制匹配最合适的列
        4. 分析分类表的 ID 字段和名称字段
        5. 返回完整的映射信息
        """
        cache_key = db_key or id(conn)
        if cache_key in self.auto_mapping_cache:
            return self.auto_mapping_cache[cache_key]  # 命中缓存，直接返回

        try:
            cursor = conn.cursor()

            # ---- 第一步：获取所有用户表（排除 SQLite 系统表 sqlite_*）----
            cursor.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in cursor.fetchall()]

            # ---- 第二步：按优先级选择主数据表 ----
            # 优先匹配常见的视频数据表名，若都不匹配则取第一个表
            priority_tables = [
                "videos", "vod_unified_data",
                "cj", "vod", "data", "video_detail"
            ]
            target_table = next(
                (t for t in priority_tables if t in tables),
                tables[0] if tables else None
            )
            if not target_table:
                return None  # 没有可用的表

            # ---- 第三步：查找分类表 ----
            cat_tables = [
                "categories", "type", "vod_type", "classes"
            ]
            target_cat_table = next(
                (t for t in cat_tables if t in tables), None
            )

            # ---- 第四步：获取主表的所有列名 ----
            cursor.execute(
                f"PRAGMA table_info(`{target_table}`)"
            )
            cols = [str(r[1]) for r in cursor.fetchall()]

            # ---- 第五步：为每个逻辑字段匹配实际列名（评分机制）----
            mapping = {}
            # 候选列名列表：按优先级从高到低排列
            field_candidates = {
                "vod_id": [           # 视频ID的候选列名
                    "id", "vod_id", "uuid", "aid", "rowid"
                ],
                "vod_name": [         # 视频名称的候选列名
                    "name", "vod_name", "title", "subject"
                ],
                "vod_pic": [          # 封面图片的候选列名
                    "image", "vod_pic", "pic",
                    "thumbnail", "cover"
                ],
                "vod_play_url": [     # 播放链接的候选列名
                    "play_url", "vod_play_url", "url", "link"
                ],
                "vod_remarks": [      # 备注/质量标签的候选列名
                    "remarks", "vod_remarks", "quality", "note"
                ],
                "vod_content": [      # 简介内容的候选列名
                    "content", "vod_content",
                    "description", "summary"
                ],
                "category_field": [   # 分类字段的候选列名
                    "type_id", "category_id", "type_name",
                    "class_id", "actress_id"
                ]
            }

            for k, candidates in field_candidates.items():
                # 找出当前表中与候选列表匹配的实际列名
                matches = [c for c in candidates if c in cols]
                if not matches:
                    mapping[k] = None  # 没有任何匹配，该字段设为空
                    continue

                best_match = matches[0]  # 默认选第一个匹配项
                max_score = -1

                # 对每个匹配列进行评分，选择得分最高的作为最佳匹配
                for match in matches:
                    score = 0
                    # 查询该列的前 10 条非空数据用于分析
                    cursor.execute(
                        f'SELECT `{match}` '
                        f'FROM `{target_table}` '
                        f'WHERE `{match}` IS NOT NULL '
                        f'AND `{match}` != "" LIMIT 10'
                    )
                    results = cursor.fetchall()
                    if not results:
                        continue

                    # 分类字段特殊评分逻辑
                    if k == "category_field":
                        distinct_vals = set(
                            [str(r[0]) for r in results]
                        )
                        # 如果去重后只有 1 个值但有多条记录，说明该字段不适合做分类
                        if (
                            len(distinct_vals) <= 1
                            and len(results) > 1
                        ):
                            score -= 50  # 惩罚
                        # 有独立分类表时加分（分类字段更可能是真正的外键）
                        if target_cat_table:
                            score += 30

                    # 优先选择候选列表中排在前面的列名（越靠前权重越高）
                    score += (
                        20 if match == candidates[0] else 5
                    )
                    if score > max_score:
                        max_score = score
                        best_match = match

                mapping[k] = best_match  # 记录该逻辑字段的最佳匹配列名

            # ---- 第六步：分析分类表的字段结构 ----
            cat_id_field = None     # 分类表中的 ID 字段名
            cat_name_field = None   # 分类表中的名称字段名
            if target_cat_table:
                try:
                    cursor.execute(
                        f"PRAGMA table_info(`{target_cat_table}`)"
                    )
                    cat_cols = [
                        str(r[1]) for r in cursor.fetchall()
                    ]
                    # 按优先级查找分类名称字段
                    for c in [
                        "name", "type_name", "class_name",
                        "category_name", "title"
                    ]:
                        if c in cat_cols:
                            cat_name_field = c
                            break
                    # 查找分类 ID 字段：优先使用主表中映射的分类字段
                    data_cat = mapping.get("category_field")
                    if data_cat and data_cat in cat_cols:
                        cat_id_field = data_cat
                    else:
                        # 按优先级查找 ID 字段
                        for c in [
                            "id", "type_id",
                            "category_id", "class_id"
                        ]:
                            if (
                                c in cat_cols
                                and c != cat_name_field
                            ):
                                cat_id_field = c
                                break
                        # 兜底：查找任何以 _id 结尾的字段
                        if not cat_id_field:
                            for c in cat_cols:
                                if (
                                    c.endswith("_id")
                                    and c != cat_name_field
                                ):
                                    cat_id_field = c
                                    break
                except:
                    pass

            # ---- 组装最终映射结果 ----
            result = {
                "table_name": target_table,          # 主数据表名
                "cat_table_name": target_cat_table,  # 分类表名
                "cat_id_field": cat_id_field,        # 分类表中的 ID 字段名
                "cat_name_field": cat_name_field,    # 分类表中的名称字段名
                "field_mapping": mapping             # 逻辑字段 → 实际列名的映射字典
            }
            if db_key:
                self.auto_mapping_cache[cache_key] = result  # 缓存映射结果避免重复分析
            return result
        except:
            return None

    # ==================== 缓存行数统计 ====================
    def _cached_row_count(self, cursor, table, where_col=None, where_val=None):
        """
        带缓存的行数统计查询。
        支持带 WHERE 条件的 COUNT 查询，结果缓存在 self.row_count_cache 中。
        缓存超过 500 条时自动清空以防止内存泄漏。
        """
        cache_key = f"{table}_{where_col}_{where_val}"
        if cache_key in self.row_count_cache:
            return self.row_count_cache[cache_key]  # 命中缓存，直接返回

        # 防止缓存无限增长
        if len(self.row_count_cache) > 500:
            self.row_count_cache.clear()

        try:
            if where_col and where_val is not None:
                # 带条件的 COUNT 查询（按分类筛选时使用）
                cursor.execute(
                    f"SELECT COUNT(*) FROM `{table}` "
                    f"WHERE CAST(`{where_col}` AS TEXT) = ?",
                    (str(where_val),)
                )
            else:
                # 全表 COUNT 查询
                cursor.execute(
                    f"SELECT COUNT(*) FROM `{table}`"
                )
            count = cursor.fetchone()[0]
            self.row_count_cache[cache_key] = count  # 存入缓存
            return count
        except:
            return 0  # 查询失败返回 0

    # ==================== 获取数据库显示名称 ====================
    def _get_db_display_name(self, db_path):
        """从数据库文件路径中提取显示名称（去掉 .db 扩展名）"""
        try:
            basename = os.path.basename(db_path)
            name_no_ext = os.path.splitext(basename)[0]
            if name_no_ext:
                return name_no_ext
            # 文件名为空时使用父目录名
            parent = os.path.basename(os.path.dirname(db_path))
            if parent:
                return parent
        except:
            pass
        return "全部数据"  # 兜底默认名称

    # ==================== 解析封面图片 ====================
    def _resolve_pic(self, raw_pic):
        """验证封面图片 URL 有效性，无效时返回默认图标地址"""
        if (
            raw_pic
            and str(raw_pic).strip()
            and str(raw_pic).startswith("http")  # 必须是 http 开头的合法 URL
        ):
            return str(raw_pic).strip()
        return self.DB_LOGO  # 返回默认封面图标

    # ==================== 清理名称文本 ====================
    def _clean_name(self, raw_name):
        """
        清理视频名称中的冗余信息：
        1. 去除方括号/圆括号中的内容（如 [VIP]、【独家】等）
        2. 去除开头的"目录"、"第X集"等无意义前缀
        3. 如果名称包含中文，去除开头的纯英文/数字/符号前缀
        4. 去除开头的标点符号和空白字符
        """
        s = str(raw_name).strip()
        if not s:
            return s

        # 去除方括号和圆括号中的内容（如 [VIP]、【独家】、(高清) 等）
        s = re.sub(
            r'[$$\[\【《][^$$\]】》]*[\)\]】》]', '', s
        )
        # 去除开头的"目录"、"第X页/集/章节/回"等前缀
        s = re.sub(
            r'^(目录\s*[:：\-]?\s*|'
            r'第\s*\d+\s*[页集章节回]\s*|'
            r'\d+[\s\-\._/]*\s*)',
            '', s
        )
        # 如果名称包含中文，去除开头的纯英文/数字/符号前缀
        if re.search(r'[\u4e00-\u9fff]', s):
            s = re.sub(
                r'^[a-zA-Z0-9\-_\.~!@#$%^&*()=+${}|;:,<>?/\\ ]+',
                '', s
            )
        # 去除开头的标点符号和空白
        s = re.sub(
            r'^[\s\-_—–·.。、,，:：;；]+', '', s
        )
        return s.strip() if s.strip() else str(raw_name).strip()

    # ==================== 渲染平面数据列表 ====================
    def _render_flat_data(
        self, conn, auto, db_key,
        table, category_val=None,
        pg="1"
    ):
        """
        根据字段映射从数据库查询视频列表并返回分页结果。
        支持按分类筛选和分页查询。
        返回格式符合 TVBox 的标准列表结构。
        """
        m = auto["field_mapping"]
        cursor = conn.cursor()
        vod_list = []
        limit = self.PAGE_LIMIT        # 每页 40 条
        pg_int = int(pg)
        offset = (pg_int - 1) * limit  # 计算偏移量

        # 获取各逻辑字段对应的数据库列名
        f_id = m.get("vod_id") or "rowid"      # ID 列，兜底使用 rowid
        f_name = m.get("vod_name") or "rowid"  # 名称列
        f_pic = m.get("vod_pic") or "''"       # 封面列，兜底为空字符串
        f_rem = m.get("vod_remarks") or "''"   # 备注列
        f_cnt = m.get("vod_content") or "''"   # 简介列
        cat_field = m.get("category_field")     # 分类字段

        total_data = 0  # 总记录数，用于计算页数

        try:
            if category_val is not None and cat_field:
                # ---- 按分类筛选查询 ----
                total_data = self._cached_row_count(
                    cursor, table, cat_field, category_val
                )
                sql = (
                    f"SELECT {f_id}, {f_name}, "
                    f"{f_pic}, {f_rem}, {f_cnt} "
                    f"FROM `{table}` "
                    f"WHERE CAST(`{cat_field}` AS TEXT) "
                    f"= ? LIMIT ? OFFSET ?"
                )
                cursor.execute(
                    sql,
                    (str(category_val), limit, offset)
                )
            else:
                # ---- 全量查询（不分分类）----
                total_data = self._cached_row_count(
                    cursor, table
                )
                sql = (
                    f"SELECT {f_id}, {f_name}, "
                    f"{f_pic}, {f_rem}, {f_cnt} "
                    f"FROM `{table}` "
                    f"LIMIT ? OFFSET ?"
                )
                cursor.execute(sql, (limit, offset))

            rows = cursor.fetchall()
            for row in rows:
                # 获取备注字段，空值时设为空字符串
                raw_rem = (
                    str(row[3])
                    if row[3] is not None
                    and str(row[3]).strip() != ""
                    else ""
                )
                vod_list.append({
                    "vod_id": f"{db_key}#ID#{row[0]}",   # 组合ID：数据库键 + 分隔符 + 记录ID
                    "vod_name": self._clean_name(row[1]), # 清理后的视频名称
                    "vod_pic": self._resolve_pic(row[2]), # 验证后的封面图URL
                    "vod_remarks": raw_rem,
                    "vod_content": (
                        str(row[4])
                        if row[4] is not None else ""
                    )
                })
        except:
            pass

        # 计算总页数（向上取整）
        pagecount = max(
            1, (total_data + limit - 1) // limit
        )
        return {
            "page": pg_int,        # 当前页码
            "pagecount": pagecount, # 总页数
            "limit": limit,         # 每页条数
            "list": vod_list        # 视频列表
        }

    # ==================== 首页内容 ====================
    def homeContent(self, filter):
        """
        返回首页分类列表。每个数据库文件作为一个独立分类展示。
        跳过标记为无效（valid=0）或文件已不存在的数据库。
        """
        classes = []
        for key, info in self.databases.items():
            if info.get("valid") == 0:
                continue  # 跳过无效数据库
            if not os.path.exists(info.get("path", "")):
                continue  # 跳过文件已不存在的数据库
            classes.append({
                "type_id": key,         # 分类ID即数据库的 Base64 编码键
                "type_name": info["name"]  # 分类名称即数据库的显示名
            })
        return {"class": classes}

    # ==================== 分类内容 ====================
    def categoryContent(self, tid, pg, filter, extend):
        """
        返回指定分类（数据库）下的内容列表。
        tid 格式为 "db_key" 或 "db_key$category_val"（带分类筛选）。
        逻辑：
        1. 如果带有分类筛选值，直接返回该分类下的视频列表
        2. 如果数据库有分类表且分类数 > 1，返回分类文件夹列表
        3. 否则直接返回视频列表
        """
        parts = tid.split('$')
        db_key = parts[0]
        category_val = parts[1] if len(parts) > 1 else None  # 分类筛选值

        # 解析数据库路径（可能是 Base64 编码或原始路径）
        try:
            db_path = (
                base64.b64decode(db_key).decode()
                if len(db_key) > 32 else db_key
            )
        except:
            db_path = db_key

        # 如果解码后的路径在 databases 字典中，获取其实际路径
        if db_path in self.databases:
            db_path = self.databases[db_path].get("path", db_path)

        conn = self._get_connection(db_path)
        if not conn:
            return {"list": []}

        auto = self._get_auto_mapping(conn, db_key)
        if not auto:
            return {"list": []}

        table = auto["table_name"]
        cat_table = auto["cat_table_name"]
        cat_id_field = auto.get("cat_id_field")
        cat_name_field = auto.get("cat_name_field")
        m = auto["field_mapping"]

        # ---- 情况1：带有分类筛选值，直接返回该分类下的视频列表 ----
        if category_val is not None:
            return self._render_flat_data(
                conn, auto, db_key,
                table, category_val, pg
            )

        # ---- 情况2：尝试从分类表获取分类列表 ----
        all_cats = []

        if cat_table and cat_name_field:
            try:
                if cat_id_field:
                    # 有 ID 字段，查询 ID + 名称
                    cursor = conn.cursor()
                    cursor.execute(
                        f"SELECT `{cat_id_field}`, "
                        f"`{cat_name_field}` "
                        f"FROM `{cat_table}`"
                    )
                else:
                    # 没有 ID 字段，使用 rowid 代替
                    cursor = conn.cursor()
                    cursor.execute(
                        f"SELECT `rowid`, "
                        f"`{cat_name_field}` "
                        f"FROM `{cat_table}`"
                    )
                all_cats = cursor.fetchall()

                # 验证分类表的数据是否与主表真正关联
                if all_cats and m.get("category_field"):
                    cursor.execute(
                        f"SELECT COUNT(*) FROM `{table}` "
                        f"WHERE CAST("
                        f"`{m['category_field']}` "
                        f"AS TEXT) = ?",
                        (str(all_cats[0][0]),)
                    )
                    if cursor.fetchone()[0] == 0:
                        all_cats = []  # 分类表数据与主表无关联，清空
            except:
                pass

        # ---- 情况3：分类表不可用，从主表的分类字段中提取唯一值 ----
        if not all_cats and m.get("category_field"):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    f"SELECT DISTINCT "
                    f"CAST(`{m['category_field']}` "
                    f"AS TEXT), "
                    f"`{m['category_field']}` "
                    f"FROM `{table}`"
                )
                all_cats = cursor.fetchall()
            except:
                pass

        # ---- 分类数 <= 1，无需分类导航，直接返回视频列表 ----
        if len(all_cats) <= 1:
            return self._render_flat_data(
                conn, auto, db_key,
                table, category_val=None, pg=pg
            )

        # ---- 分类数 > 1，返回分类文件夹列表供用户选择 ----
        vod_list = []
        for row in all_cats:
            label = self._clean_name(row[1])
            vod_list.append({
                "vod_id": f"{db_key}${row[0]}",   # 组合ID：数据库键 + 分类值
                "vod_name": label,
                "vod_pic": self.DB_LOGO,           # 分类统一使用默认图标
                "vod_tag": "folder",               # 标记为文件夹类型（用于UI展示）
                "vod_remarks": ""
            })
        return {
            "page": 1, "pagecount": 1,
            "limit": 999,  # 分类列表一次性全部返回
            "list": vod_list
        }

    # ==================== 详情内容 ====================
    def detailContent(self, ids):
        """
        根据视频 ID 获取详细信息，包括名称、封面、演员、简介和播放链接。
        IDs 格式为 "db_key#ID#record_id"，通过分隔符拆分数据库键和记录ID。
        """
        mid_full = ids[0]
        db_key, _, real_id = mid_full.partition("#ID#")  # 拆分：数据库键 + 记录ID

        # 解析数据库路径（Base64 编码或原始路径）
        try:
            db_path = (
                base64.b64decode(db_key).decode()
                if len(db_key) > 32 else db_key
            )
        except:
            db_path = db_key

        # 获取数据库元信息
        db_info = self.databases.get(db_key, {})
        if not db_info and db_path in self.databases:
            db_info = self.databases[db_path]

        actual_path = db_info.get("path", db_path)
        conn = self._get_connection(actual_path)
        if not conn:
            return {"list": []}

        auto_info = self._get_auto_mapping(conn, db_key)
        if not auto_info:
            return {"list": []}

        table_name = auto_info["table_name"]
        mapping = auto_info["field_mapping"]

        cursor = conn.cursor()
        id_col = mapping.get("vod_id") or "rowid"  # ID 列名

        try:
            # 根据记录 ID 查询完整记录
            cursor.execute(
                f"SELECT * FROM `{table_name}` "
                f"WHERE `{id_col}` = ?",
                (real_id,)
            )
            row = cursor.fetchone()
            if not row:
                return {"list": []}  # 记录不存在

            # 辅助函数：安全获取字段值，列不存在或值为空时返回空字符串
            def get_val(m_key):
                col = mapping.get(m_key)
                if (
                    col and col in row.keys()
                    and row[col] is not None
                ):
                    return str(row[col])
                return ""

            # 获取播放链接，为空时使用记录ID作为占位
            play_url = get_val("vod_play_url")
            if not play_url:
                play_url = f"Play#{real_id}"

            # 组装详情数据结构
            vod = {
                "vod_id": mid_full,                    # 完整ID
                "vod_name": self._clean_name(          # 清理后的视频名称
                    get_val("vod_name")
                ),
                "vod_pic": self._resolve_pic(          # 验证后的封面图
                    get_val("vod_pic")
                ),
                "vod_remarks": get_val("vod_remarks"), # 备注/质量标签
                "vod_actor": (
                    get_val("vod_actor") or "未知"     # 演员信息，为空时显示"未知"
                ),
                "vod_content": (
                    get_val("vod_content") or "暂无简介"  # 简介，为空时显示"暂无简介"
                ),
                "vod_play_from": "DB数据库",            # 播放来源标识
                "vod_play_url": play_url.replace(
                    '$$$高清', '#播放'  # 替换特殊分隔符为标准格式
                )
            }
            return {"list": [vod]}
        except:
            return {"list": []}

    # ==================== 播放器内容 ====================
    def playerContent(self, flag, id, vipFlags):
        """
        返回播放器配置信息。
        parse=0 表示直接播放原始URL，不经过解析器。
        使用 MIbox PRO 的 User-Agent 以兼容多数视频源。
        """
        return {
            "parse": 0,        # 0=不解析，直接播放
            "url": id,          # 播放地址
            "header": {
                "User-Agent": (
                    "Dalvik/2.1.0 (Linux; U; Android 9; "
                    "MIbox PRO Build/PI)"
                )
            }
        }

    # ==================== 搜索内容 ====================
    def searchContent(self, key, quick, pg="1"):
        """
        在所有有效数据库中搜索视频名称。
        使用 SQL LIKE 进行模糊匹配，每个数据库最多返回20条结果。
        搜索结果前会附加数据库名称作为来源标识（如 [数据库名] 视频名）。
        """
        search_list = []
        limit = 20  # 每个数据库最多返回 20 条匹配结果

        for db_key, db_info in self.databases.items():
            if db_info.get("valid") == 0:
                continue  # 跳过无效数据库

            db_path = db_info.get("path", "")
            if not db_path or not os.path.exists(db_path):
                continue  # 文件不存在，跳过

            conn = self._get_connection(db_path)
            if not conn:
                continue  # 无法连接，跳过

            try:
                auto = self._get_auto_mapping(conn, db_key)
                if not auto:
                    continue

                table = auto["table_name"]
                m = auto["field_mapping"]

                title_field = m.get("vod_name")
                if not title_field:
                    continue  # 没有名称字段，无法搜索

                f_id = m.get("vod_id") or "rowid"
                f_pic = m.get("vod_pic") or "''"
                f_rem = m.get("vod_remarks") or "''"

                cursor = conn.cursor()
                # 使用 LIKE 进行模糊搜索（%key% 匹配包含关键词的名称）
                sql = (
                    f"SELECT `{f_id}`, `{title_field}`, "
                    f"{f_pic}, {f_rem} "
                    f"FROM `{table}` "
                    f"WHERE `{title_field}` LIKE ? "
                    f"LIMIT {limit}"
                )
                cursor.execute(sql, (f"%{key}%",))

                for row in cursor.fetchall():
                    name = str(row[1]) if row[1] else ""
                    if not name:
                        continue
                    # 验证封面图URL
                    pic = (
                        str(row[2])
                        if row[2]
                        and str(row[2]).startswith("http")
                        else ""
                    )
                    rem = str(row[3]) if row[3] else ""
                    db_name = db_info.get("name", "")
                    search_list.append({
                        "vod_id": f"{db_key}#ID#{row[0]}",
                        "vod_name": f"[{db_name}] {self._clean_name(name)}",  # 前缀标注来源数据库
                        "vod_pic": pic,
                        "vod_remarks": rem
                    })
            except:
                pass

        return {"list": search_list, "page": pg}
