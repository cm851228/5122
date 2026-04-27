import sys
import sqlite3
import json
import os
import threading
import base64
from base.spider import Spider

class Spider(Spider):
    def getName(self):
        return "Universal_DB_Spider"

    # ==========================================================================
    # 💎 【1. 配置与物理路径】    db数据库
    # ==========================================================================
    SCAN_DIR_LIST = [
                "bh",            #电视📺专用文件夹，把电视源文件放在这个文件夹里 或者放在u盘
                "tvbox",       #电视📺专用文件夹，把电视源文件放在这个文件夹里
                "bhh",           #搜索专用，老手机不要超过120m文件
                "tvbox/lz",         # 👈 前面加#关闭   这里可以修改任意大佬包名
                "lz",                                       # 👈 同上
                "VodPlus",                           # 👈 同上
                "peekpili/php-scripts",      # 👈 同上
                "纯福利",                               # 👈 同上
                 "江湖"                   # 👈 前面加#关闭   这里可以修改任意大佬包名 

     ]
    MIN_DB_SIZE = 2 * 1024 * 1024   # 门限小于2M不显示
    DB_LOGO = "https://img.fluency.icons8.com/96/database.png"
    FOLDER_LOGO = "https://img.icons8.com/color/96/opened-folder.png"

    def init(self, extend=""):
        self.inited = True
        self.databases = {}
        self._db_lock = threading.Lock()
        self._conn_cache = {} 
        self._mapping_cache = {} 
        
        # 🧪 增强型多挂载点路径扫描（支持外部存储sd卡和u盘，老手机可以）
        self.scan_roots = ["/storage/emulated/0"]
        try:
            if os.path.exists("/storage"):
                for s in os.listdir("/storage"):
                    if s not in ["self", "emulated", "knox", "sdcard0", "runtime"]:
                        full_s = os.path.join("/storage", s)
                        if os.path.isdir(full_s):
                            self.scan_roots.append(full_s)
        except: pass
        
        self._differential_scan()

    def _format_size(self, size_bytes):
        if size_bytes < 1048576: return f"{int(size_bytes/1024)}K"
        return f"{size_bytes/1048576:.1f}M"

    def _differential_scan(self):
        """核心扫描：按大小从小到大排列，支持外部存储☆标识"""
        temp_list = []
        for root_p in self.scan_roots:
            is_ext = not root_p.startswith("/storage/emulated/0")
            star = "☆" if is_ext else ""
            
            for target in self.SCAN_DIR_LIST:
                base_p = os.path.join(root_p, target)
                if not os.path.isdir(base_p): continue
                
                for root, dirs, files in os.walk(base_p):
                    for file in files:
                        if not file.lower().endswith(".db"): continue
                        f_path = os.path.join(root, file)
                        try:
                            sz_raw = os.path.getsize(f_path)
                            if sz_raw < self.MIN_DB_SIZE: continue # 屏蔽小于2M
                            
                            # 格式化路径/文件名/大小
                            rel_path = f_path.replace("/storage/emulated/0/", "").replace(root_p, "")
                            display_name = f"📁{rel_path} ({self._format_size(sz_raw)}){star}"
                            
                            db_key = base64.b64encode(f_path.encode()).decode()
                            temp_list.append({
                                "key": db_key, 
                                "name": display_name, 
                                "path": f_path,
                                "size_bytes": sz_raw,
                                "is_ext": is_ext
                            })
                        except: continue
        
        # 排序：按文件名      #大小从小到大
        temp_list.sort(key=lambda x: (os.path.dirname(x["path"]), x["name"]))
        
        for item in temp_list:
            self.databases[item["key"]] = {
                "name": item["name"], 
                "path": item["path"], 
                "size_str": self._format_size(item["size_bytes"]),
                "valid": 1
            }

    def _get_connection(self, db_key):
        """单例连接模式：优化读取大文件的性能"""
        if db_key in self._conn_cache:
            try:
                self._conn_cache[db_key].execute("SELECT 1")
                return self._conn_cache[db_key]
            except:
                del self._conn_cache[db_key]

        db_info = self.databases.get(db_key)
        if not db_info: return None
        db_path = db_info.get("path")
        if not db_path or not os.path.exists(db_path): return None
        
        try:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = -2000") 
            self._conn_cache[db_key] = conn
            return conn
        except: return None

    def _get_auto_mapping(self, conn):

        conn_id = id(conn)
        if conn_id in self._mapping_cache: return self._mapping_cache[conn_id]

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]

            priority_tables = ["videos", "vod_unified_data", "cj", "vod", "mac_vod", "data", "video_detail", "list"]
            target_table = next((t for t in priority_tables if t in tables), tables[0] if tables else None)
            
            if not target_table: return None
            
            cursor.execute(f"PRAGMA table_info(`{target_table}`)")
            cols = [str(r[1]) for r in cursor.fetchall()]
            
            mapping = {}
            field_candidates = {
                "vod_id": ["id", "vod_id", "uuid", "guid", "vid"],
                "vod_name": ["name", "vod_name", "title", "subject", "display_name"],
                "vod_pic": ["image", "vod_pic", "pic", "thumbnail", "img", "cover"],
                "vod_play_url": ["play_url", "vod_play_url", "url", "link", "m3u8_url", "vod_url"],
                "vod_remarks": ["vod_remarks", "remarks", "content", "desc", "note", "msg"],
                "vod_actor": ["actor", "vod_actor", "starring", "performer"],
                "vod_content": ["content", "vod_content", "summary", "description"],
                "category_field": ["type_name", "category_id", "class_name", "cate_name", "tag"]
            }
            
            for target_field, candidates in field_candidates.items():

                matches = [cand for cand in candidates if cand in cols]

                if not matches:
                    matches = [col for col in cols if any(c in col.lower() for c in candidates)]
                
                if not matches:
                    if target_field == "category_field": mapping[target_field] = "'无分类'"
                    elif target_field == "vod_id": mapping[target_field] = "rowid"
                    else: mapping[target_field] = None
                    continue
                

                best_match = matches[0]
                max_score = -1
                for match in matches:
                    score = 0
                    try:
                        cursor.execute(f'SELECT `{match}` FROM `{target_table}` WHERE `{match}` IS NOT NULL AND `{match}` != "" LIMIT 5')
                        results = cursor.fetchall()
                        score += len(results) * 10
                        if match in candidates: score += 50 # 官方候选词加分
                        if target_field == "vod_play_url" and len(results) > 0:
                            if any(str(r[0]).startswith(('http', 'rtsp', 'magnet', 'ftp')) for r in results):
                                score += 100 # 包含链接地址重磅加分
                        if score > max_score:
                            max_score = score
                            best_match = match
                    except: continue
                mapping[target_field] = best_match
            
            res = {"table_name": target_table, "field_mapping": mapping}
            self._mapping_cache[conn_id] = res
            return res
        except: return None

    def homeContent(self, filter):
        classes = []
        for db_key, db_info in self.databases.items():
            classes.append({"type_id": db_key, "type_name": db_info.get("name")})
        return {"class": classes}

    def categoryContent(self, tid, pg, filter, extend):
        parts = tid.split('$')
        db_key = parts[0]
        category_val = parts[1] if len(parts) > 1 else None
        
        conn = self._get_connection(db_key)
        if not conn: return {"list": []}
        
        auto_info = self._get_auto_mapping(conn)
        if not auto_info: return {"list": []}

        table = auto_info["table_name"]
        m = auto_info["field_mapping"]
        f_filter = m.get("category_field") or "'无分类'"
        cursor = conn.cursor()
        vod_list = []

        # 分类/分片识别逻辑
        if category_val is None:
            all_cats = []
            try:
                cursor.execute(f"SELECT DISTINCT {f_filter} FROM `{table}` WHERE {f_filter} IS NOT NULL")
                all_cats = cursor.fetchall()
            except: pass

            # 🧩 【虚拟分片逻辑】：如果没有分类或分类极少，进行500条一分片
            if len(all_cats) <= 1:
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                total = cursor.fetchone()[0]
                chunk_size = 500
                for i in range(0, total, chunk_size):
                    start, end = i + 1, min(i + chunk_size, total)
                    cur_pg = (i // chunk_size) + 1
                    vod_list.append({
                        "vod_id": f"{db_key}$CHUNK_{i}",
                        "vod_name": f"清单: {start}-{end}",
                        "vod_pic": self.FOLDER_LOGO,
                        "vod_tag": "folder",
                        "vod_remarks": f"第{cur_pg}页/共{total}条"
                    })
                return {"page": 1, "pagecount": 1, "limit": 999, "list": vod_list}

            # 正常分类列表
            for row in all_cats:
                c_id = str(row[0])
                cursor.execute(f"SELECT COUNT(*) FROM `{table}` WHERE {f_filter} = ?", (c_id,))
                cnt = cursor.fetchone()[0]
                vod_list.append({
                    "vod_id": f"{db_key}${c_id}",
                    "vod_name": c_id,
                    "vod_pic": self.FOLDER_LOGO,
                    "vod_tag": "folder",
                    "vod_remarks": f"🎬本集(共{cnt}条)"
                })
            return {"page": 1, "pagecount": 1, "limit": 999, "list": vod_list}

        # 列表内容显示
        limit = 20
        offset = (int(pg) - 1) * limit
        f_id, f_name, f_pic, f_rem = m["vod_id"], m["vod_name"], m["vod_pic"] or "''", m["vod_remarks"] or "''"

        try:
            if "CHUNK_" in category_val:
                c_start = int(category_val.replace("CHUNK_", ""))
                sql = f"SELECT {f_id}, {f_name}, {f_pic}, {f_rem} FROM `{table}` LIMIT ? OFFSET ?"
                cursor.execute(sql, (limit, c_start + offset))
            else:
                sql = f"SELECT {f_id}, {f_name}, {f_pic}, {f_rem} FROM `{table}` WHERE {f_filter} = ? LIMIT ? OFFSET ?"
                cursor.execute(sql, (category_val, limit, offset))
            
            for row in cursor.fetchall():
                vod_list.append({
                    "vod_id": f"{db_key}#ID#{row[0]}",
                    "vod_name": str(row[1]),
                    "vod_pic": str(row[2]) if str(row[2]).startswith('http') else "",
                    "vod_remarks": str(row[3])
                })
        except: pass
        return {"page": int(pg), "pagecount": int(pg) + 1, "limit": limit, "list": vod_list}

    def detailContent(self, ids):
        mid_full = ids[0]
        db_key, _, real_id = mid_full.partition("#ID#")
        conn = self._get_connection(db_key)
        if not conn: return {"list": []}
        
        auto_info = self._get_auto_mapping(conn)
        db_info = self.databases.get(db_key, {})
        table = auto_info["table_name"]
        m = auto_info["field_mapping"]

        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        id_col = m.get("vod_id") or "rowid"
        
        try:
            cursor.execute(f"SELECT * FROM `{table}` WHERE {id_col} = ?", (real_id,))
            row = cursor.fetchone()
            if not row: return {"list": []}

            def get_val(key):
                col = m.get(key)
                if col and col in row.keys() and row[col] is not None:
                    return str(row[col])
                return ""

            # 🛠 缝合统计简介
            raw_content = get_val("vod_content") or "暂无详细描述"
            v_content = f"【原始简介】: {raw_content}\n"
            v_content += f"--------------------------\n"
            v_content += f"📊 文件大小: {db_info.get('size_str')}\n"
            v_content += f"✅ 物理位置: {db_info.get('path')}\n"
            v_content += f"⚡ 索引标识: ID_{real_id}"

            # 播放链接逻辑恢复
            p_url = get_val("vod_play_url")
            if not p_url: p_url = f"Play#{real_id}"

            vod = {
                "vod_id": mid_full,
                "vod_name": get_val("vod_name"),
                "vod_pic": get_val("vod_pic"),
                "vod_remarks": get_val("vod_remarks"),
                "vod_actor": get_val("vod_actor") or "内详",
                "vod_content": v_content,
                "vod_play_from": "DB数据库",
                "vod_play_url": p_url.split('$$$')[-1] # 自动提取最后一节地址
            }
            return {"list": [vod]}
        except: return {"list": []}

    def searchContent(self, key, quick, pg="1"):
        search_list = []
        limit = 50
        for db_key, db_info in self.databases.items():
            conn = self._get_connection(db_key)
            if not conn: continue
            try:
                auto_info = self._get_auto_mapping(conn)
                table = auto_info["table_name"]
                m = auto_info["field_mapping"]
                f_name = m["vod_name"]
                
                cursor = conn.cursor()
                sql = f"SELECT {m['vod_id']}, {f_name}, {m['vod_pic'] or 'rowid'}, {m['vod_remarks'] or 'rowid'} FROM `{table}` WHERE `{f_name}` LIKE ? LIMIT {limit}"
                cursor.execute(sql, (f"%{key}%",))
                
                for row in cursor.fetchall():
                    search_list.append({
                        "vod_id": f"{db_key}#ID#{row[0]}",
                        "vod_name": f"[{db_info.get('name')}] {row[1]}",
                        "vod_pic": str(row[2]) if str(row[2]).startswith('http') else "",
                        "vod_remarks": str(row[3])
                    })
            except: pass
        return {"list": search_list, "page": pg}

    def playerContent(self, flag, id, vipFlags):
        return {"parse": 0, "url": id, "header": {"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; MIbox PRO Build/PI)"}}

    def __del__(self):
        """内存释放与临时连接清理"""
        try:
            for conn in self._conn_cache.values():
                conn.close()
            self._conn_cache.clear()
            self._mapping_cache.clear()
        except: pass