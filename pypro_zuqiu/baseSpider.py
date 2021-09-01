import requests
import time
import random
from kuaidaili import KuaiDaiLi

user_agent = ['wswp',
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
              "Opera/8.0 (Windows NT 5.1; U; en)",
              "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50",
              "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50",
              "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
              "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
              "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
              "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
              "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
              "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)\"",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
              "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
              "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
              "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36"]


class BaseSpider(object):
    def __init__(self, use_proxy=False):
        self.base_url = "http://www.tzuqiu.cc/"
        self.proxy_host_port = ""
        self.use_proxy = use_proxy
        if self.use_proxy:
            self.change_proxy()
        self.req = requests.Session()

    def change_proxy(self):
        print("开始获取/变更代理IP...")
        self.proxy_host_port = KuaiDaiLi().get_ip()
        print("新代理IP: ", self.proxy_host_port)

    def downloader(self, url):
        print("当前抓取页面：{}".format(url))
        while True:
            try:
                time.sleep(1)
                self.req.headers = {"User-Agent": random.choice(user_agent)}
                if self.use_proxy:
                    proxies = {
                        "http": "http://{}".format(self.proxy_host_port),
                        "https": "https://{}".format(self.proxy_host_port),
                    }

                    self.req.proxies = proxies
                    resp = self.req.get(url, timeout=10)
                else:
                    resp = self.req.get(url, timeout=10)

                html = resp.text
                if "Error: 403" in html:
                    if self.use_proxy:
                        self.change_proxy()
                        continue
                    else:
                        print("当前IP已被封，请使用代理IP， 或者等待24小时候后系统解封")
                        exit(0)
                if "White IP Failed" in html:
                    print('本机IP不在代理IP白名单内！！')
                    return None
                if "Error: 404" in html:
                    return None
                else:
                    return html
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if self.use_proxy:
                    print("代理IP超时，切换代理IP")
                    self.change_proxy()
                else:
                    print("请求超时...重试...")
                continue
            except requests.exceptions.ChunkedEncodingError:
                print("ChunkedEncodingError 1秒后重试...")
                time.sleep(1)
                continue
            except requests.exceptions.ProxyError:
                print("代理错误, 切换代理...")
                self.change_proxy()
                time.sleep(1)
            except Exception:
                if self.use_proxy:
                    print("请求异常，切换代理IP")
                    self.change_proxy()
                else:
                    time.sleep(1)
                    print("请求异常...重试...")
                continue
