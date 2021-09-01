import json
import re
from concurrent.futures import ThreadPoolExecutor
import matchSpider
from mysql_utils import get_mysql_connect, close_connect
from baseSpider import BaseSpider

class MatchIdSpider(BaseSpider):
    def parse_match_id_detail(self, url,match_name):
        html = self.downloader(url)
        # print(html)
        list_fixture = json.loads(
            re.findall('var fixture = (\{.*?\});', str(html), re.S)[0])['datas']
        list_id_data = []
        for li in list_fixture:
            list_id_data.append(li['id'])
        print(f'总共爬取到比赛id数量：{len(list_id_data)},开始比赛爬虫...  \n {list_id_data}')
        self.run_matchSpider(match_name, list_id_data)
        pass

    def run_matchSpider(self,match_name, list_id_data):
        conn = get_mysql_connect()
        cursor = conn.cursor()
        matchSpider.main(list_id_data,conn,cursor)
        close_connect(conn, cursor)
        pass

def run_main():
    dict_match_data = {
        'ajia': '11', 'bajia': '10', 'dejia': '4', 'echao': '9', 'fajia': '6', 'hejia': '8', 'hanzhi': '71',
        'puchao': '7', 'rizhi': '49', 'tuchao': '13', 'xijia': '2', 'yijia': '5', 'yingchao': '1', 'yingguan': '12',
        'zhongchao': '3',
    }
    mis = MatchIdSpider(use_proxy=False)
    # 有效的比赛数据至 25767  全部 302099
    with ThreadPoolExecutor(max_workers=15) as t:
        for key, value in dict_match_data.items():
            url = f"http://www.tzuqiu.cc/competitions/{value}/show.do"
            print(f'\r开始爬取联赛  {key} 的比赛ID!   ',end='')
            t.submit(mis.parse_match_id_detail,url, key)

if __name__ == '__main__':
    run_main()