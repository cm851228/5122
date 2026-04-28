# coding=utf-8
"""
目标站: 华视影视  首页: https://huavod.com
"""
import re
import sys
import json
import urllib.parse
from bs4 import BeautifulSoup

sys.path.append('..')
from base.spider import Spider

class Spider(Spider):

    def init(self, extend=""):
        self.site_url = "https://huavod.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': self.site_url,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }

    def homeContent(self, filter):
        categories = [
            {"type_id": "1", "type_name": "电影"},
            {"type_id": "2", "type_name": "电视剧"},
            {"type_id": "3", "type_name": "综艺"},
            {"type_id": "4", "type_name": "动漫"},
            {"type_id": "5", "type_name": "短剧"},
            {"type_id": "6", "type_name": "纪录片"}
        ]
        
        url = f"{self.site_url}/"
        resp = self.fetch(url, headers=self.headers)
        if not resp:
            return {"class": categories, "list": [], "filters": {}}
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        video_list = self._parse_list(soup)
        
        filters = {}
        # 类型
        type_ul = soup.select_one('ul#swiper-wrapper-da4ece909eb2653a')
        if type_ul:
            options = [{"n": "全部", "v": ""}]
            for li in type_ul.select('li.swiper-slide'):
                text = li.get_text(strip=True)
                if text and text != '全部':
                    options.append({"n": text, "v": text})
            if len(options) > 1:
                filters["类型"] = [{"key": "类型", "name": "类型", "value": options}]
        # 地区
        area_ul = soup.select_one('ul#swiper-wrapper-bf2fa34483d1072d7')
        if area_ul:
            options = [{"n": "全部", "v": ""}]
            for li in area_ul.select('li.swiper-slide'):
                text = li.get_text(strip=True)
                if text and text != '全部':
                    options.append({"n": text, "v": text})
            if len(options) > 1:
                filters["地区"] = [{"key": "地区", "name": "地区", "value": options}]
        # 年份
        year_ul = soup.select_one('ul#swiper-wrapper-12a104d35656b8da2')
        if year_ul:
            options = [{"n": "全部", "v": ""}]
            for li in year_ul.select('li.swiper-slide'):
                text = li.get_text(strip=True)
                if text and text != '全部':
                    options.append({"n": text, "v": text})
            if len(options) > 1:
                filters["年份"] = [{"key": "年份", "name": "年份", "value": options}]
        # 语言
        lang_ul = soup.select_one('ul#swiper-wrapper-a5f98adb47edda68')
        if lang_ul:
            options = [{"n": "全部", "v": ""}]
            for li in lang_ul.select('li.swiper-slide'):
                text = li.get_text(strip=True)
                if text and text != '全部':
                    options.append({"n": text, "v": text})
            if len(options) > 1:
                filters["语言"] = [{"key": "语言", "name": "语言", "value": options}]
        
        return {"class": categories, "list": video_list, "filters": filters}

    def homeVideoContent(self):
        return self.homeContent(False)

    def categoryContent(self, tid, pg, filter, extend):
        page = int(pg) if pg else 1
        
        url = f"{self.site_url}/vodshow/{tid}.html"
        if page > 1:
            url = f"{self.site_url}/vodshow/{tid}/page/{page}.html"
        
        if extend:
            params = {}
            for key in ["类型", "地区", "年份", "语言"]:
                value = extend.get(key, "")
                if value and value != "全部":
                    params[key] = value
            if params:
                query_string = urllib.parse.urlencode(params)
                url += '?' + query_string
        
        resp = self.fetch(url, headers=self.headers)
        if not resp:
            return {"list": [], "page": page, "pagecount": 1, "limit": 24, "total": 0}
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        video_list = self._parse_list(soup)
        
        pagecount = 1
        total_elem = soup.select_one('div.pages span.total')
        if total_elem:
            total_match = re.search(r'(\d+)', total_elem.get_text(strip=True))
            if total_match:
                total = int(total_match.group(1))
                pagecount = max(1, (total + 23) // 24)
        else:
            page_links = soup.select('div.pages a')
            for a in page_links:
                text = a.get_text(strip=True)
                if text.isdigit():
                    pagecount = max(pagecount, int(text))
        
        return {
            "list": video_list,
            "page": page,
            "pagecount": pagecount,
            "limit": 24,
            "total": len(video_list) * pagecount
        }

    def detailContent(self, ids):
        if not ids:
            return {"list": []}
        vod_id = ids[0]
        url = f"{self.site_url}/voddetail/{vod_id}.html"
        resp = self.fetch(url, headers=self.headers)
        if not resp or resp.status_code != 200:
            return {"list": []}
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # ---- 名称 ----
        vod_name = ''
        for sel in ['h1', 'h2', '.title', '.vod-name', '.detail-title']:
            tag = soup.select_one(sel)
            if tag and tag.get_text(strip=True):
                vod_name = tag.get_text(strip=True)
                break
        if not vod_name:
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                vod_name = og_title['content'].strip()
        if not vod_name:
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text(strip=True)
                if ' - ' in title:
                    vod_name = title.split(' - ')[0].strip()
                else:
                    vod_name = title
        
        # ---- 图片 ----
        vod_pic = ''
        img = soup.select_one('div.vod-detail img, .detail-thumb img, .video-info img')
        if img:
            vod_pic = img.get('data-src') or img.get('src') or img.get('data-original') or ''
            if 'data:image' in vod_pic:
                vod_pic = ''
        
        # ---- 导演 / 主演 / 简介 ----
        director = ''
        actor = ''
        vod_content = ''
        desc_block = soup.select_one('.vod-detail') or soup.select_one('.detail-info') or soup.select_one('.video-detail')
        if desc_block:
            text_lines = desc_block.get_text(separator='\n', strip=True).splitlines()
            text_lines = [line.strip() for line in text_lines if line.strip()]
            for i, line in enumerate(text_lines):
                if '导演' in line:
                    m = re.search(r'导演\s*[:：]\s*(.*)', line)
                    if m:
                        director = m.group(1).strip()
                elif '主演' in line:
                    m = re.search(r'主演\s*[:：]\s*(.*)', line)
                    if m:
                        actor = m.group(1).strip()
                elif '简介' in line:
                    m = re.search(r'简介\s*[:：]\s*(.*)', line)
                    if m and m.group(1).strip():
                        vod_content = m.group(1).strip()
                    else:
                        if i + 1 < len(text_lines):
                            vod_content = text_lines[i+1].strip()
            if not vod_content and text_lines:
                for line in reversed(text_lines):
                    if len(line) > 20 and '导演' not in line and '主演' not in line and '类型' not in line:
                        vod_content = line
                        break
        
        # ---- 播放线路与选集 ----
        play_from_list = []
        play_url_list = []
        all_play_links = soup.select('a[href*="vodplay"]')
        if all_play_links:
            episodes = []
            for a in all_play_links:
                text = a.get_text(strip=True)
                href = a.get('href', '')
                if not text or not href:
                    continue
                if len(text) > 10:
                    continue
                episodes.append(f"{text}${href}")
            if episodes:
                play_from_list.append("默认线路")
                play_url_list.append('#'.join(episodes))
        
        if not play_from_list:
            containers = soup.select('div.pop-list-body.bj.pop-1')
            if not containers:
                containers = soup.select('.playlist-box, .episode-list, .vod-play-list')
            for idx, container in enumerate(containers, 1):
                line_name = f"线路{idx}"
                name_elem = container.select_one('.play-source-name, .source-title, h3')
                if name_elem:
                    line_name = name_elem.get_text(strip=True)
                links = container.select('a')
                if not links:
                    continue
                episodes = []
                for a in links:
                    text = a.get_text(strip=True)
                    href = a.get('href', '')
                    if not text or not href:
                        continue
                    if len(text) > 10:
                        continue
                    episodes.append(f"{text}${href}")
                if episodes:
                    play_from_list.append(line_name)
                    play_url_list.append('#'.join(episodes))
        
        vod_play_from = '$$$'.join(play_from_list) if play_from_list else '默认线路'
        vod_play_url = '$$$'.join(play_url_list) if play_url_list else ''
        
        result = [{
            "vod_id": vod_id,
            "vod_name": vod_name,
            "vod_pic": vod_pic,
            "vod_director": director,
            "vod_actor": actor,
            "vod_content": vod_content,
            "vod_play_from": vod_play_from,
            "vod_play_url": vod_play_url
        }]
        return {"list": result}

    def searchContent(self, key, quick, pg="1"):
        page = int(pg) if pg else 1
        url = f"{self.site_url}/vodsearch/{urllib.parse.quote(key)}-{page}.html"
        
        resp = self.fetch(url, headers=self.headers)
        if not resp:
            return {"list": [], "page": page, "pagecount": 1}
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        video_list = self._parse_list(soup)
        
        pagecount = 1
        pagination = soup.select('div.pages a')
        for a in pagination:
            text = a.get_text(strip=True)
            if text.isdigit():
                pagecount = max(pagecount, int(text))
        
        return {"list": video_list, "page": page, "pagecount": pagecount}

    def playerContent(self, flag, id, vipFlags):
        if id.startswith('/'):
            play_url = self.site_url + id
        else:
            play_url = id
        
        resp = self.fetch(play_url, headers=self.headers)
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')
            # iframe
            iframe = soup.select_one('iframe')
            if iframe and iframe.get('src'):
                real_url = iframe['src']
                if real_url.startswith('//'):
                    real_url = 'https:' + real_url
                return {"parse": 0, "url": real_url, "header": self.headers}
            # video标签
            video = soup.select_one('video source, video')
            if video:
                src = video.get('src') or video.get('data-src') or ''
                if src:
                    return {"parse": 0, "url": src, "header": self.headers}
            # script中的m3u8/mp4
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    m = re.search(r'["\'](https?://[^"\']+\.(?:m3u8|mp4)[^"\']*)["\']', script.string)
                    if m:
                        return {"parse": 0, "url": m.group(1), "header": self.headers}
        
        return {"parse": 1, "url": play_url, "header": self.headers}

    def _parse_list(self, soup):
        """解析视频列表，支持首页、分类、搜索"""
        videos = []
        items = soup.select('div.public-list-div.public-list-bj')
        if not items:
            items = soup.select('a.public-list-exp')
            if not items:
                return videos
        
        for item in items:
            if item.name == 'a':
                a_tag = item
            else:
                a_tag = item.select_one('a.public-list-exp')
            if not a_tag:
                continue
            
            href = a_tag.get('href', '')
            vod_id = ''
            if '/voddetail/' in href:
                vod_id = href.split('/voddetail/')[-1].replace('.html', '')
            
            name = ''
            name_tag = a_tag.select_one('div.public-list-button a.time-title.hide.ft4')
            if not name_tag:
                name_tag = a_tag.select_one('.time-title, .video-title, .title')
            if name_tag:
                name = name_tag.get_text(strip=True)
            if not name:
                img = a_tag.select_one('img')
                if img:
                    name = img.get('alt', '')
            
            vod_pic = ''
            img_tag = a_tag.select_one('img')
            if img_tag:
                vod_pic = img_tag.get('data-src') or img_tag.get('src') or img_tag.get('data-original') or ''
                if vod_pic.startswith('data:image'):
                    vod_pic = ''
            
            remarks = ''
            full_text = a_tag.get_text(' ', strip=True)
            if '豆瓣热榜' in full_text:
                m = re.search(r'豆瓣热榜\s*(\d+\.?\d*)', full_text)
                if m:
                    remarks = f"豆瓣 {m.group(1)}"
            if not remarks:
                parts = full_text.split()
                if parts:
                    potential = parts[-1]
                    if len(potential) < 10:
                        remarks = potential
            
            if name:
                videos.append({
                    "vod_id": vod_id,
                    "vod_name": name,
                    "vod_pic": vod_pic,
                    "vod_remarks": remarks
                })
        return videos