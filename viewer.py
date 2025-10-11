import sqlite3
import os
from datetime import datetime, timedelta

class NewsViewer:
    def __init__(self, db_path='cctv_news_simple.db', content_base_dir='news'):
        self.db_path = db_path
        self.content_base_dir = content_base_dir
    
    def view_recent_news(self, limit=10):
        """查看最近新闻"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            SELECT content_hash, title, summary, publish_time, keywords, content_file_path
            FROM news 
            ORDER BY publish_time DESC 
            LIMIT ?
        ''', (limit,))
        
        news_list = c.fetchall()
        conn.close()
        
        print(f"\n=== 最近 {len(news_list)} 条新闻 ===\n")
        
        for i, (content_hash, title, summary, pub_time, keywords, file_path) in enumerate(news_list, 1):
            print(f"{i}. {title}")
            print(f"   时间: {pub_time}")
            print(f"   关键词: {keywords}")
            print(f"   摘要: {summary}")
            print(f"   哈希: {content_hash}")
            print(f"   详细内容: {'✅' if file_path else '❌'}")
            print()
        
        return news_list
    
    def view_news_by_date(self, date_str, limit=20):
        """按日期查看新闻"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            SELECT content_hash, title, summary, publish_time, keywords, content_file_path
            FROM news 
            WHERE DATE(publish_time) = ?
            ORDER BY publish_time DESC 
            LIMIT ?
        ''', (date_str, limit))
        
        news_list = c.fetchall()
        conn.close()
        
        print(f"\n=== {date_str} 的新闻 ({len(news_list)} 条) ===\n")
        
        for i, (content_hash, title, summary, pub_time, keywords, file_path) in enumerate(news_list, 1):
            print(f"{i}. {title}")
            print(f"   时间: {pub_time}")
            print(f"   关键词: {keywords}")
            print(f"   哈希: {content_hash}")
            print(f"   详细内容: {'✅' if file_path else '❌'}")
            print()
        
        return news_list
    
    def search_news(self, keyword, limit=20):
        """搜索新闻"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            SELECT content_hash, title, summary, publish_time, keywords, content_file_path
            FROM news 
            WHERE title LIKE ? OR summary LIKE ? OR keywords LIKE ?
            ORDER BY publish_time DESC 
            LIMIT ?
        ''', (f'%{keyword}%', f'%{keyword}%', f'%{keyword}%', limit))
        
        results = c.fetchall()
        conn.close()
        
        print(f"\n=== 搜索 '{keyword}' 结果 ({len(results)} 条) ===\n")
        
        for i, (content_hash, title, summary, pub_time, keywords, file_path) in enumerate(results, 1):
            print(f"{i}. {title}")
            print(f"   时间: {pub_time}")
            print(f"   关键词: {keywords}")
            print(f"   哈希: {content_hash}")
            print(f"   详细内容: {'✅' if file_path else '❌'}")
            print()
        
        return results
    
    def read_detailed_content(self, content_hash):
        """读取详细内容"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('SELECT content_file_path FROM news WHERE content_hash = ?', (content_hash,))
        result = c.fetchone()
        conn.close()
        
        if not result or not result[0]:
            print("未找到详细内容")
            return None
        
        file_path = os.path.join(self.content_base_dir, result[0])
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"\n=== 详细内容 ===\n")
                print(content)
                print("\n" + "="*50)
                return content
        except Exception as e:
            print(f"读取详细内容失败: {e}")
            return None
    
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
        
        # 日期分布
        c.execute('''
            SELECT DATE(publish_time), COUNT(*)
            FROM news 
            GROUP BY DATE(publish_time)
            ORDER BY DATE(publish_time) DESC
            LIMIT 10
        ''')
        stats['recent_days'] = c.fetchall()
        
        conn.close()
        
        print(f"\n=== 数据库统计 ===")
        print(f"总新闻数: {stats['total_count']} 条")
        print(f"含详细内容: {stats['detailed_count']} 条")
        print(f"时间范围: {stats['time_range'][0]} 到 {stats['time_range'][1]}")
        print(f"\n最近10天新闻分布:")
        for date, count in stats['recent_days']:
            print(f"  {date}: {count} 条")
        
        return stats

def main():
    """查看器主函数 - 提供交互式菜单"""
    viewer = NewsViewer()
    
    while True:
        print("\n" + "="*60)
        print("           央视新闻查看器")
        print("="*60)
        print("1. 查看最近新闻")
        print("2. 按日期查看新闻")
        print("3. 搜索新闻")
        print("4. 读取详细内容")
        print("5. 查看统计信息")
        print("0. 退出")
        print("-"*60)
        
        choice = input("请选择操作 (0-5): ").strip()
        
        if choice == '1':
            try:
                limit = int(input("显示数量 (默认10): ") or "10")
                viewer.view_recent_news(limit)
            except ValueError:
                viewer.view_recent_news()
                
        elif choice == '2':
            date_str = input("请输入日期 (YYYY-MM-DD): ").strip()
            if date_str:
                try:
                    limit = int(input("显示数量 (默认20): ") or "20")
                    viewer.view_news_by_date(date_str, limit)
                except ValueError:
                    viewer.view_news_by_date(date_str)
            else:
                print("日期不能为空")
                
        elif choice == '3':
            keyword = input("请输入搜索关键词: ").strip()
            if keyword:
                try:
                    limit = int(input("显示数量 (默认20): ") or "20")
                    results = viewer.search_news(keyword, limit)
                    if results:
                        # 提供查看详细内容的选项
                        idx = input("输入编号查看详细内容 (0返回): ").strip()
                        if idx.isdigit() and 1 <= int(idx) <= len(results):
                            content_hash = results[int(idx)-1][0]
                            viewer.read_detailed_content(content_hash)
                except ValueError:
                    viewer.search_news(keyword)
            else:
                print("关键词不能为空")
                
        elif choice == '4':
            content_hash = input("请输入内容哈希: ").strip()
            if content_hash:
                viewer.read_detailed_content(content_hash)
            else:
                print("哈希值不能为空")
                
        elif choice == '5':
            viewer.get_statistics()
            
        elif choice == '0':
            print("再见！")
            break
            
        else:
            print("无效选择，请重新输入")

if __name__ == "__main__":
    main()