import time
import requests


class KuaiDaiLi(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.name = "kuaidaili"
        # self.proxy_url = "http://dps.kdlapi.com/api/getdps/?orderid=992629910876290&num=1&pt=1&format=json&sep=1"
        self.proxy_url = "http://ent.kdlapi.com/api/getproxy/?orderid=942703004337984&num=1&protocol=2&method=1&an_ha=1&quality=3&format=json&sep=1"

    def get_ip(self):
        """
        每次获取一个IP
        :return:
        """
        while True:
            try:
                response = requests.get(self.proxy_url, timeout=10)
                # print(response.json())
                proxy_list = response.json()['data']['proxy_list']

            except:
                proxy_list = []
            if not proxy_list:
                print("获取IP异常，5秒后重试！")
                time.sleep(5)
            else:
                break
        return proxy_list[0]


if __name__ == '__main__':
    kdl = KuaiDaiLi()
    print(kdl.get_ip())
