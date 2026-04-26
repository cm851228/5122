import sys
import sqlite3
import json
import os
from base.spider import Spider

class Spider(Spider):
    def getName(self):
        return "Universal_DB_Spider"

    def init(self, extend=""):
        # 设置配置文件路径
        self.config_path = extend if extend else "/storage/emulated/0/vodplus/wwwroot/lib/DB测试.json"
        self._db_cache = {}
        self.databases = {}
        # 默认扫描目录
        default_scan_dirs = [
            "/storage/emulated/0/ 我的文件/我的收藏/db/",
            "/storage/emulated/0/lz/db/",
            "/storage/emulated/0/VodPlus/duo/纯福利/db/"
        ]
        scan_dirs = default_scan_dirs.copy()  # 初始使用默认目录
        # 如果配置文件存在就加载
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # 加载数据库配置
                    self.databases = config.get("databases", {})
                    # 如果配置文件中有scan_dirs，用配置的替换默认的
                    if "scan_dirs" in config and config["scan_dirs"]:
                        scan_dirs = config["scan_dirs"]
            except Exception as e:
                print(f"读取配置文件失败: {e}")
        # 执行目录扫描
        self._auto_scan_databases(scan_dirs)

    def _auto_scan_databases(self, dirs):
        for d in dirs:
            if not os.path.exists(d): continue
            for file in os.listdir(d):
                if file.endswith(".db"):
                    full_path = os.path.join(d, file)
                    db_key = f"auto_{file}"
                    if db_key not in self.databases:
                        self.databases[db_key] = {"name": f"🔍 {file}", "path": full_path, "valid": 1}

    def _get_connection(self, db_key):
        db_info = self.databases.get(db_key)
        if not db_info: return None
        db_path = db_info.get("path")
        if not db_path or not os.path.exists(db_path): return None
        try:
            return sqlite3.connect(db_path)
        except: return None

    def _find_best_match(self, cols, candidates):
        for cand in candidates:
            if cand in cols: 
                return cand
        return None
    
    def _get_auto_mapping(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            priority_tables = ["videos", "vod_unified_data", "cj", "vod", "data", "video_detail"]
            target_table = next((t for t in priority_tables if t in tables), tables[0] if tables else None)
            if not target_table: return None
            
            cat_tables = ["categories", "type", "vod_type", "classes"]
            target_cat_table = next((t for t in cat_tables if t in tables), None)
            
            cursor.execute(f"PRAGMA table_info(`{target_table}`)")
            cols = [str(r[1]) for r in cursor.fetchall()]
            
            mapping = {}
            field_candidates = {
                "vod_id": ["id", "vod_id", "uuid", "aid"],
                "vod_name": ["name", "vod_name", "title", "subject"],
                "vod_pic": ["image", "vod_pic", "pic", "thumbnail", "cover"],
                "vod_play_url": ["play_url", "vod_play_url", "url", "link"],
                "vod_remarks": ["vod_remarks", "remarks", "content", "note"],
                "category_field": ["type_id", "category_id", "type_name", "class_id", "actress_id"] # 把 ID 放在前面
            }
            
            for target_field, candidates in field_candidates.items():
                matches = [cand for cand in candidates if cand in cols]
                if not matches:
                    mapping[target_field] = "1" if target_field == "category_field" else None
                    continue
                
                best_match = matches[0]
                max_score = -1
                for match in matches:
                    score = 0
                    cursor.execute(f'SELECT `{match}` FROM `{target_table}` WHERE `{match}` IS NOT NULL AND `{match}` != "" LIMIT 10')
                    results = cursor.fetchall()
                    
                    if target_field == "category_field":
                        # 【关键改动】探测数据多样性：如果一个字段所有值都一样（如全是“电影”），大幅降分
                        distinct_vals = set([str(r[0]) for r in results])
                        if len(distinct_vals) <= 1 and len(results) > 1:
                            score -= 100 
                        # 如果是数字类型，且有分类表，加分（因为这通常是外键）
                        if target_cat_table and all(str(r[0]).isdigit() for r in results):
                            score += 50
                    
                    if match in candidates: score += 20
                    if score > max_score:
                        max_score = score
                        best_match = match
                mapping[target_field] = best_match
                
            return {"table_name": target_table, "cat_table_name": target_cat_table, "field_mapping": mapping}
        except: return None

    def categoryContent(self, tid, pg, filter, extend):
        parts = tid.split('$')
        db_key = parts[0]
        category_val = parts[1] if len(parts) > 1 else None
        
        conn = self._get_connection(db_key)
        if not conn: return {"list": []}
        
        auto_info = self._get_auto_mapping(conn)
        if not auto_info: 
            conn.close()
            return {"list": []}

        table_name = auto_info["table_name"]
        cat_table = auto_info.get("cat_table_name")
        mapping = auto_info["field_mapping"]
        filter_field = mapping.get("category_field")

        cursor = conn.cursor()
        limit = 20
        offset = (int(pg) - 1) * limit
        vod_list = []

        # 探测分类表字段
        c_id_col, c_name_col = "id", "name"
        if cat_table:
            try:
                cursor.execute(f"PRAGMA table_info(`{cat_table}`)")
                cat_cols = [str(r[1]) for r in cursor.fetchall()]
                c_id_col = next((c for c in ["id", "type_id", "category_id"] if c in cat_cols), cat_cols[0])
                c_name_col = next((c for c in ["name", "type_name", "category_name", "title"] if c in cat_cols), cat_cols[-1])
            except: pass

        if category_val is None:
            all_categories = []
            # 【逻辑重申】只要有分类表，就必须从分类表拿目录
            if cat_table:
                try:
                    cursor.execute(f"SELECT CAST(`{c_id_col}` AS TEXT), `{c_name_col}` FROM `{cat_table}`")
                    all_categories = cursor.fetchall()
                except: pass

            # 没分类表才从主表拿
            if not all_categories and filter_field != "1":
                try:
                    cursor.execute(f"SELECT DISTINCT CAST(`{filter_field}` AS TEXT), `{filter_field}` FROM `{table_name}`")
                    all_categories = cursor.fetchall()
                except: pass

            # 【你的要求】只有一个分类时，直接显示列表
            if len(all_categories) == 1:
                category_val = str(all_categories[0][0])
                # 不 return，直接往下走列表查询
            elif len(all_categories) == 0:
                category_val = "__DIRECT__"
            else:
                for row in all_categories:
                    vod_list.append({
                        "vod_id": f"{db_key}${row[0]}",
                        "vod_name": str(row[1]),
                        "vod_pic": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ_fiZQFquf1Z6B--uWZd6n7QBaZl5tAWRmNB_0SEkrIA&s=10",
                        "vod_tag": "folder",
                        "vod_remarks": "📁 点击查看列表"
                    })
                return {"page": 1, "pagecount": 1, "limit": 999, "list": vod_list}

        # --- 渲染视频列表 ---
        f_id = mapping.get("vod_id") or "rowid"
        f_name = mapping.get("vod_name") or "rowid"
        f_pic = mapping.get("vod_pic") or "''"
        f_rem = mapping.get("vod_remarks") or "''"

        try:
            # 这里的查询逻辑增加对 filter_field 的 CAST 保护
            if category_val == "__DIRECT__":
                sql = f"SELECT {f_id}, {f_name}, {f_pic}, {f_rem} FROM `{table_name}` LIMIT ? OFFSET ?"
                cursor.execute(sql, (limit, offset))
            else:
                # 优先按 ID 匹配
                sql = f"SELECT {f_id}, {f_name}, {f_pic}, {f_rem} FROM `{table_name}` WHERE CAST(`{filter_field}` AS TEXT) = ? LIMIT ? OFFSET ?"
                cursor.execute(sql, (str(category_val), limit, offset))
            
            rows = cursor.fetchall()
            for row in rows:
                vod_list.append({
                    "vod_id": f"{db_key}#ID#{row[0]}",
                    "vod_name": str(row[1]),
                    "vod_pic": str(row[2]) if str(row[2]).startswith('http') else "",
                    "vod_remarks": str(row[3])
                })
        except: pass
        
        conn.close()
        return {"page": int(pg), "pagecount": int(pg) + 1, "limit": limit, "list": vod_list}

    def homeContent(self, filter):
        classes = []
        for db_key, db_info in self.databases.items():
            if db_info.get("valid") != 0 and not db_info.get("hide"):
                if os.path.exists(db_info.get("path", "")):
                    classes.append({"type_id": db_key, "type_name": db_info.get("name", db_key)})
        return {"class": classes}

    def detailContent(self, ids):
        mid_full = ids[0]
        # 只有包含 #ID# 的才会进这里
        db_key, _, real_id = mid_full.partition("#ID#")
        conn = self._get_connection(db_key)
        if not conn: return {"list": []}
        
        auto_info = self._get_auto_mapping(conn)
        db_info = self.databases.get(db_key, {})
        main_cfg = db_info.get("tables", {}).get("main", {})
        table_name = main_cfg.get("table_name") or auto_info["table_name"]
        mapping = main_cfg.get("field_mapping") or auto_info["field_mapping"]

        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        id_col = mapping.get("vod_id") or "rowid"
        
        cursor.execute(f"SELECT * FROM {table_name} WHERE {id_col} = ?", (real_id,))
        row = cursor.fetchone()
        if not row: 
            conn.close()
            return {"list": []}

        def get_val(m_key):
            real_col = mapping.get(m_key)
            return str(row[real_col]) if (real_col and real_col in row.keys() and row[real_col] is not None) else ""

        vod = {
            "vod_id": mid_full,
            "vod_name": get_val("vod_name"),
            "vod_pic": get_val("vod_pic"),
            "vod_remarks": get_val("vod_remarks"),
            "vod_actor": get_val("vod_actor"),
            "vod_content": get_val("vod_content"),
            "vod_play_from": "自动识别",
            "vod_play_url": get_val("vod_play_url").replace('$$$高清', '#播放')
        }
        conn.close()
        return {"list": [vod]}

    def playerContent(self, flag, id, vipFlags):
        # 最终播放
        return {"parse": 0, "url": id, "header": {"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; MIbox PRO Build/PI)"}}

    def searchContent(self, key, quick, pg="1"):
        search_list = []
        limit = 20
        # 搜索时不进行深度分页，通常取前几十条结果
        
        for db_key, db_info in self.databases.items():
            if db_info.get("valid") == 0:
                continue
                
            conn = self._get_connection(db_key)
            if not conn:
                continue
                
            try:
                auto_info = self._get_auto_mapping(conn)
                if not auto_info:
                    conn.close()
                    continue

                # 确定表名和搜索字段
                main_cfg = db_info.get("tables", {}).get("main", {})
                table_name = main_cfg.get("table_name") or auto_info["table_name"]
                mapping = main_cfg.get("field_mapping") or auto_info["field_mapping"]
                
                # 优先使用配置的搜索字段，没有则使用识别到的标题字段
                search_fields = main_cfg.get("search_fields")
                if not search_fields:
                    title_field = mapping.get("vod_name")
                    search_fields = [title_field] if title_field else []

                if not search_fields:
                    conn.close()
                    continue

                cursor = conn.cursor()
                # 构造 WHERE 子句: (field1 LIKE ? OR field2 LIKE ?)
                where_clauses = [f"`{field}` LIKE ?" for field in search_fields]
                sql_where = " OR ".join(where_clauses)
                
                f_id = mapping.get("vod_id") or "rowid"
                f_name = mapping.get("vod_name")
                f_pic = mapping.get("vod_pic") or "''"
                f_rem = mapping.get("vod_remarks") or "''"

                sql = f"SELECT {f_id}, {f_name}, {f_pic}, {f_rem} FROM {table_name} WHERE {sql_where} LIMIT {limit}"
                
                # 准备模糊查询参数
                params = [f"%{key}%"] * len(search_fields)
                cursor.execute(sql, params)
                
                for row in cursor.fetchall():
                    search_list.append({
                        "vod_id": f"{db_key}#ID#{row[0]}",
                        "vod_name": f"[{db_info.get('name', db_key)}] {row[1]}",
                        "vod_pic": str(row[2]) if row[2] else "",
                        "vod_remarks": str(row[3]) if row[3] else ""
                    })
            except Exception as e:
                # 打印日志方便调试，生产环境可删除
                # print(f"Search Error in {db_key}: {e}")
                pass
            finally:
                conn.close()

        return {"list": search_list, "page": pg}
