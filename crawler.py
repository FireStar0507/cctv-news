import requests
import json
import re
import sqlite3
import hashlib
from datetime import datetime
import time
import os
from bs4 import BeautifulSoup

class CCTVNewsCrawler:
    def __init__(self, db_path='cctv_news.db', content_base_dir='news'):
        self.db_path = db_path
        self.content_base_dir = content_base_dir
        self.init_database()
    
    def init_database(self):
        """初始化数据库结构"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS news (
                content_hash TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                publish_time TEXT,
                keywords TEXT,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                content_file_path TEXT
            )
        ''')
        
        c.execute('CREATE INDEX IF NOT EXISTS idx_publish_time ON news(publish_time)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_crawl_time ON news(crawl_time)')
        
        conn.commit()
        conn.close()
    
    def get_content_hash(self, news_item):
        """基于标题和摘要生成内容哈希"""
        content = f"{news_item['title']}{news_item['brief']}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def fetch_news_page(self, page_num):
        """获取新闻列表页数据"""
        api_url = f"https://news.cctv.com/2019/07/gaiban/cmsdatainterface/page/world_{page_num}.jsonp"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://news.cctv.com/world/',
            'Accept': '*/*'
        }
        
        try:
            print(f"正在获取第 {page_num} 页数据...")
            response = requests.get(api_url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                news_list = self.parse_jsonp(response.text)[:10]
                print(f"第 {page_num} 页获取到 {len(news_list)} 条新闻")
                return news_list
            else:
                print(f"第 {page_num} 页请求失败，状态码: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"获取第 {page_num} 页数据失败: {e}")
            return []
    
    def parse_jsonp(self, jsonp_data):
        """解析JSONP数据"""
        try:
            json_str = re.search(r'\{.*\}', jsonp_data).group()
            data = json.loads(json_str)
            return data['data']['list']
        except Exception as e:
            print(f"解析JSONP数据失败: {e}")
            return []
    
    def fetch_detailed_content(self, url):
        """获取新闻详细内容"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找内容区域
                content_area = soup.find('div', id='content_area')
                if content_area:
                    # 提取纯文本内容，保留段落结构
                    content_text = ""
                    for element in content_area.find_all(['p', 'div']):
                        text = element.get_text(strip=True)
                        if text:
                            content_text += text + "\n"
                    
                    return content_text.strip()
                else:
                    print(f"未找到内容区域: {url}")
                    return None
            else:
                print(f"获取详细内容失败，状态码: {response.status_code}, URL: {url}")
                return None
                
        except Exception as e:
            print(f"获取详细内容异常: {e}, URL: {url}")
            return None
    
    def get_content_file_path(self, content_hash, publish_time):
        """根据发布时间生成文件路径"""
        try:
            # 解析发布时间
            dt = datetime.strptime(publish_time, '%Y-%m-%d %H:%M:%S')
            year = dt.strftime('%Y')
            month = dt.strftime('%m')
            day = dt.strftime('%d')
            
            # 创建目录结构
            dir_path = os.path.join(self.content_base_dir, year, month, day)
            os.makedirs(dir_path, exist_ok=True)
            
            # 返回相对路径
            return os.path.join(year, month, day, f"{content_hash}.txt")
            
        except:
            # 如果时间解析失败，使用当前日期
            today = datetime.now()
            year = today.strftime('%Y')
            month = today.strftime('%m')
            day = today.strftime('%d')
            
            dir_path = os.path.join(self.content_base_dir, year, month, day)
            os.makedirs(dir_path, exist_ok=True)
            
            return os.path.join(year, month, day, f"{content_hash}.txt")
    
    def save_detailed_content(self, file_path, detailed_content):
        """保存详细内容到txt文件"""
        if not detailed_content:
            return False
        
        full_path = os.path.join(self.content_base_dir, file_path)
        
        try:
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(detailed_content)
            return True
        except Exception as e:
            print(f"保存详细内容失败: {e}")
            return False
    
    def save_news_to_db(self, news_list, fetch_detailed=True):
        """保存新闻到数据库"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        new_count = 0
        duplicate_count = 0
        
        for news_item in news_list:
            content_hash = self.get_content_hash(news_item)
            
            # 检查是否已存在
            c.execute('SELECT content_file_path FROM news WHERE content_hash = ?', (content_hash,))
            existing = c.fetchone()
            
            content_file_path = None
            
            # 如果需要获取详细内容且尚未获取
            if fetch_detailed and (not existing or not existing[0]):
                print(f"获取详细内容: {news_item['title'][:30]}...")
                detailed_content = self.fetch_detailed_content(news_item['url'])
                if detailed_content:
                    content_file_path = self.get_content_file_path(content_hash, news_item['focus_date'])
                    if not self.save_detailed_content(content_file_path, detailed_content):
                        content_file_path = None
                time.sleep(1)  # 请求间隔
            elif existing:
                content_file_path = existing[0]
            
            try:
                if existing:
                    # 更新记录（如果之前没有详细内容，现在有了）
                    if content_file_path and not existing[0]:
                        c.execute('''
                            UPDATE news 
                            SET content_file_path = ?, crawl_time = CURRENT_TIMESTAMP
                            WHERE content_hash = ?
                        ''', (content_file_path, content_hash))
                        new_count += 1  # 算作新增详细内容
                    else:
                        duplicate_count += 1
                else:
                    # 插入新记录
                    c.execute('''
                        INSERT INTO news 
                        (content_hash, title, summary, publish_time, keywords, content_file_path)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        content_hash,
                        news_item['title'],
                        news_item['brief'],
                        news_item['focus_date'],
                        news_item.get('keywords', ''),
                        content_file_path
                    ))
                    new_count += 1
                
            except Exception as e:
                print(f"保存新闻失败: {news_item['title']}, 错误: {e}")
        
        conn.commit()
        conn.close()
        
        return new_count, duplicate_count
    
    def get_statistics(self):
        """获取统计信息"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        stats = {}
        
        c.execute('SELECT COUNT(*) FROM news')
        stats['total_count'] = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM news WHERE content_file_path IS NOT NULL')
        stats['detailed_count'] = c.fetchone()[0]
        
        c.execute('SELECT MIN(publish_time), MAX(publish_time) FROM news')
        stats['time_range'] = c.fetchone()
        
        conn.close()
        return stats
    
    def run_crawler(self, fetch_detailed=True, max_pages=3):
        """运行爬虫"""
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 开始爬取新闻...")
        
        all_news = []
        for page_num in range(1, max_pages + 1):
            page_news = self.fetch_news_page(page_num)
            if page_news:
                all_news.extend(page_news)
            time.sleep(0.5)
        
        if not all_news:
            print("未获取到任何新闻数据")
            return
        
        print(f"获取到 {len(all_news)} 条新闻，开始处理详细内容..." if fetch_detailed else f"获取到 {len(all_news)} 条新闻")
        
        new_count, duplicate_count = self.save_news_to_db(all_news, fetch_detailed)
        
        # 显示统计
        stats = self.get_statistics()
        print(f"\n📊 爬取完成:")
        print(f"   新增/更新: {new_count} 条")
        print(f"   重复跳过: {duplicate_count} 条")
        print(f"   数据库总数: {stats['total_count']} 条")
        print(f"   含详细内容: {stats['detailed_count']} 条")
        print(f"   时间范围: {stats['time_range'][0]} 到 {stats['time_range'][1]}")

def main():
    """爬虫主函数"""
    crawler = CCTVNewsCrawler()
    crawler.run_crawler(fetch_detailed=True, max_pages=7)

if __name__ == "__main__":
    main()
