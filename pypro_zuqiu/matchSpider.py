import re
import json
from pyquery import PyQuery as pq
from baseSpider import BaseSpider
from database_operation import insert_all_data, insert_match_data, insert_player_data
from mysql_utils import get_mysql_connect, close_connect

class MatchSpider(BaseSpider):
    def parse_match_detail(self, match_url,conn,cursor):
        html = self.downloader(match_url)
        # print(html)
        match_id = match_url.split("matches")[1]
        match_id = match_id.split("/")[1]
        if html:
            doc = pq(html)
            compt_tmp = doc("div.head_info > div > h1").text()
            compt = compt_tmp.split("比赛")[0]
            result_tmp = doc("td.result").text()
            result = result_tmp.replace(":", "-").replace(" ", "").replace("*", "")
            match_stage_tmp = doc("tr.match-stage > td.stat-box").text()
            match_stage = match_stage_tmp.replace("第", "").replace("轮", "")
            match_time_tmp = doc("td.match-info tr:nth-child(5) > td:nth-child(2)").text().strip()
            match_time = match_time_tmp.split(" ")[0]

            script = doc("script:nth-child(6)")

            try:
                home_team_name = re.findall("var homeTeamName = '(.*?)';", str(script), re.S)[0]
            except IndexError:
                self.change_proxy()
                return self.parse_match_detail(match_url)
            home_team_id = re.findall("var homeTeamId = '(.*?)';", str(script), re.S)[0]
            home_team_goal_time = []
            home_team_assist_time = []

            away_team_name = re.findall("var awayTeamName = '(.*?)';", str(script), re.S)[0]
            away_team_id = re.findall("var awayTeamId = '(.*?)';", str(script), re.S)[0]
            away_team_goal_time = []
            away_team_assist_time = []

            time_lines = json.loads(re.findall('var timelines = (\[.*?\]);', str(script), re.S)[0])
            time_lines_data = self.parse_time_lines(time_lines)

            # 比赛概况
            match_data = {"match_id": match_id, "compt": compt, "result": result, "home_team": home_team_name,
                          "home_team_id": home_team_id, "away_team": away_team_name, "away_team_id": away_team_id,
                          "match_stage": match_stage, "match_time": match_time}

            if time_lines_data:
                # 进球与助攻时间
                assit_event = []
                for data in time_lines_data:
                    if data[3] == "event-assist":
                        assit_event.append(data[2])
                for data in time_lines_data:
                    team_id = data[0]
                    player_id = data[1]
                    css = data[3]
                    minute = data[2]
                    if css in ["event-goal", "event-penalty-goal"]:
                        goal_way = ""
                        if css == "event-penalty-goal":
                            goal_way = "pg"
                        else:
                            goal_way = "ag" if minute in assit_event else "sg"
                        if str(team_id) == home_team_id:
                            home_team_goal_time.append((player_id, minute,goal_way))
                        elif str(team_id) == away_team_id:
                            away_team_goal_time.append((player_id, minute,goal_way))
                    if css in ["event-assist"]:
                        if str(team_id) == home_team_id:
                            home_team_assist_time.append((player_id, minute))
                        elif str(team_id) == away_team_id:
                            away_team_assist_time.append((player_id, minute))

                home_player_statistics = json.loads(
                    re.findall('var homePlayerStatistics = (\[.*?\]);', str(script), re.S)[0])
                home_player_statistics_data = self.player_status_analyse(home_player_statistics)

                away_player_statistics = json.loads(
                    re.findall('var awayPlayerStatistics = (\[.*?\]);', str(script), re.S)[0])
                away_player_statistics_data = self.player_status_analyse(away_player_statistics)

                home_player_statistics_data1 = self.player_status_analyse1(home_player_statistics)
                away_player_statistics_data1 = self.player_status_analyse1(away_player_statistics)
                player_data_list = home_player_statistics_data1 + away_player_statistics_data1
                self.save_player_data(player_data_list,conn,cursor)

                home_analyse_data = self.analyse_data(home_player_statistics_data, home_team_goal_time,
                                                      away_team_goal_time,
                                                      home_team_assist_time, match_data)
                away_analyse_data = self.analyse_data(away_player_statistics_data, away_team_goal_time,
                                                      home_team_goal_time,
                                                      away_team_assist_time, match_data)

                results = home_analyse_data + away_analyse_data

                player_stats_url = f"http://www.tzuqiu.cc/matches/{match_id}/playerStats.do"
                player_stats_data = self.parse_current_match_player_stats(player_stats_url)
                match_datas = []
                for player_data in player_stats_data:
                    stats_player_id = player_data[0]
                    for player_info in results:
                        player_id = player_info.get("player_id", "")
                        if str(stats_player_id) == str(player_id):
                            player_info["sm"] = player_data[1]
                            player_info["sz"] = player_data[2]
                            player_info["gr"] = player_data[3]
                            player_info["bqf"] = player_data[4]
                            player_info["yw"] = player_data[5]
                            player_info["bqd"] = player_data[6]
                            player_info["sw"] = player_data[7]
                            player_info["qd"] = player_data[8]
                            player_info["lj"] = player_data[9]
                            player_info["jw"] = player_data[10]
                            player_info["fd"] = player_data[11]
                            player_info["zyw"] = player_data[12]
                            player_info["fg"] = player_data[13]
                            player_info["zmsw"] = player_data[14]
                            player_info["gjcq"] = player_data[15]
                            player_info["cq"] = player_data[16]
                            player_info["ps"] = player_data[17]
                            player_info["ftp"] = player_data[18]
                            player_info["ftps"] = player_data[19]
                            player_info["cz"] = player_data[20]
                            player_info["cc"] = player_data[21]
                            player_info["zs"] = player_data[22]
                            match_datas.append(player_info)

                self.save_match_data(match_datas,conn,cursor)
                self.save_all_data(match_datas,conn,cursor)
            else:

                insert_match_data(conn, cursor, match_data)
                print(
                    f"比赛已存储 ... {match_data.get('compt', '')} 第{match_data.get('match_stage', '')}轮 ... {match_data.get('home_team', '')} vs {match_data.get('away_team', '')} ... 本场无队员数据")

    def save_match_data(self, datas,conn,cursor):
        for data in datas:
            item = {
                "match_id": data.get("match_id", ""),
                "match_stage": data.get("match_stage", ""),
                "player_id": data.get("player_id", ""),
                "compt": data.get("compt", ""),
                "match_time": data.get("match_time", ""),
                "home_team": data.get("home_team", ""),
                "home_team_id": data.get("home_team_id", ""),
                "result": data.get("result", ""),
                "away_team": data.get("away_team", ""),
                "away_team_id": data.get("away_team_id", ""),
                "home_away": data.get("home_away", ""),
                "shirt_no": data.get("shirt_no", ""),
                "player_name": data.get("player_name", ""),
                "country": data.get("country", ""),
                "in_time": str(data.get("in_time", 0)),
                "out_time": str(data.get("out_time", 0)),
                "goal_count": data.get("goal_count", 0),
                "goal_time": data.get("goal_time", ""),
                "assist_count": data.get("assist_count", 0),
                "assist_time": data.get("assist_time", ""),
                "lose_count": data.get("lose_count", 0),
                "lose_time": data.get("lose_time", ""),
                "qd": data.get("qd", 0)
            }

            insert_match_data(conn, cursor, item)
            print(
                f"比赛已存储 ... {data.get('compt', '')} 第{data.get('match_stage', '')}轮 ... {data.get('home_team', '')} vs {data.get('away_team', '')} ... 本场队员数据: {data.get('player_name', '')}")

    def save_player_data(self, datas,conn,cursor):
        for data in datas:
            insert_player_data(conn, cursor, data)
            print(
                f"队员数据已存储 ...{data}")

    def player_status_analyse1(self, dict_data):
        datas = []
        for data in dict_data:
            item = {}
            item['player_id'] = data.get('playerId', 0)
            item['team_id'] = data.get('teamId', 0)
            item['name'] = data['player'].get('nameZh', '')
            try:
                item['marketvalue'] = (data['player'].get('currentMarketValue', 0)) / 100000000  # 单位 亿 /100000000
            except:
                item['marketvalue'] = None

            item['country'] = data['player'].get('country1', '')
            item['position'] = data['player'].get('mainPosition', '')
            try:
                item['height'] = float(data['player'].get('height', '')[:-1])  # 去掉m
            except:
                item['height'] = None
            item['age'] = data['player'].get('age', 0)
            datas.append(item)
        return datas

    def save_all_data(self, datas,conn,cursor):
        for data in datas:
            if data.get("goal_time", "") == '':
                goaltime = ""
            else:
                goaltime = data.get("goal_time", "").split(",")
                goaltime = ",".join(list(map(lambda x: str(max(eval(x) - data.get("in_time", 0),1)),goaltime)))
            if data.get("lose_time", "") == '':
                losetime = ""
            else:
                losetime = data.get("lose_time", "").split(",")
                losetime = ",".join(list(map(lambda x: str(max(eval(x) - data.get("in_time", 0),1)),losetime)))
            if data.get("assist_time", "") == '':
                assisttime = ""
            else:
                assisttime = data.get("assist_time", "").split(",")
                assisttime = ",".join(list(map(lambda x: str(max(eval(x) - data.get("in_time", 0),1)),assisttime)))
            item = {
                "player_id": data.get("player_id", ""),
                "match_stage": data.get("match_stage", ""),
                "match_id": data.get("match_id", ""),
                "compt": data.get("compt", ""),
                "match_time": data.get("match_time", ""),
                "home_team": data.get("home_team", ""),
                "home_team_id": data.get("home_team_id", ""),
                "result": data.get("result", ""),
                "away_team": data.get("away_team", ""),
                "away_team_id": data.get("away_team_id", ""),
                "home_away": data.get("home_away", ""),
                "shirt_no": data.get("shirt_no", ""),
                "player_name": data.get("player_name", ""),
                "country": data.get("country", ""),
                "rate": data.get("rate", "0"),
                "in_time": str(data.get("in_time", 0), ),
                "out_time": str(data.get("out_time", 0), ),
                "full_time": str(data.get("out_time", 0) - data.get("in_time", 0)),
                "goal_count": data.get("goal_count", "0"),
                "goal_time": goaltime,
                "pg":data.get("pg",""),
                "ag":data.get("ag",""),
                "sg":data.get("sg",""),
                "sm": data.get("sm", "0"),
                "sz": data.get("sz", "0"),
                "gr": data.get("gr", "0"),
                "qd": data.get("qd", "0"),
                "bqf": data.get("bqf", "0"),
                "yw": data.get("yw", "0"),
                "bqd": data.get("bqd", "0"),
                "sw": data.get("sw", "0"),
                "assist_count": data.get("assist_count", "0"),
                "assist_time": assisttime,
                "gjcq": data.get("gjcq", "0"),
                "cq": data.get("cq", "0"),
                "ps": data.get("ps", "0"),
                "ftps": data.get("ftps", "0"),
                "cz": data.get("cz", "0"),
                "cc": data.get("cc", "0"),
                "zs": data.get("zs", "0"),
                "lose_count": data.get("lose_count", "0"),
                "lose_time": losetime,
                "lj": data.get("lj", "0"),
                "jw": data.get("jw", "0"),
                "fd": data.get("fd", "0"),
                "zyw": data.get("zyw", "0"),
                "fg": data.get("fg", "0"),
                "zmsw": data.get("zmsw", "0"),
            }


            insert_all_data(conn,cursor,item)
            print(
                f"综合数据已存储：{data.get('compt', '')} 第{data.get('match_stage', '')}轮 ... {data.get('home_team', '')} vs {data.get('away_team', '')} ...比分: {data.get('result', '')} ... 本场队员数据: {data.get('player_name', '')}")

    def parse_time_lines(self, dict_data):
        datas = []
        for data in dict_data:
            css = data.get('css', "")
            minute = data.get("minute", "")
            team_id = data.get("teamId", "")
            datas.append((team_id, data.get("playerId", ""), minute, css))
            # if css in ["event-goal", "event-penalty-goal", "event-assist"]:
            #     try:
            #         datas.append((team_id, data.get("playerId", ""), minute, css))
            #     except KeyError:
            #         continue
        return datas

    def player_status_analyse(self, dict_data):
        datas = []
        for data in dict_data:
            player_name = data.get('playerName', "")
            player_id = data.get("playerId", "")
            in_time = data.get("inTime", 0)
            out_time = data.get("outTime", 0)
            shirt_no = data.get("shirtNo", "")
            country_tmp = data.get("player", "")
            country = country_tmp.get("country1", "")
            rate = data.get("rate", "")
            home_away = data.get("homeAway", "")
            if in_time == 0 and out_time != 0:
                status = "首发"
            elif in_time != 0 and out_time != 0:
                status = "替补"
            else:
                status = "未上场"
            item = {
                "player_name": player_name,
                "player_id": str(player_id),
                "shirt_no": str(shirt_no),
                "country": country,
                "home_away": home_away,
                "in_time": in_time,
                "out_time": out_time,
                "status": status,
                "rate": str(rate)
            }
            datas.append(item)
        return datas

    def analyse_data(self, current_team_statistics_data, current_team_goal_time_data, other_team_goal_time_data,
                     current_team_assist_time_data, match_data):
        new_data = []
        for current_player_data in current_team_statistics_data:
            player_id = current_player_data.get("player_id", "")
            in_time = current_player_data.get('in_time', 0)
            out_time = current_player_data.get('out_time', 0)
            # 失球时间
            lose_time = []
            # 失球数
            lose_count = 0
            for other_goal in other_team_goal_time_data:
                other_goal_time = other_goal[1]
                if in_time < other_goal_time < out_time:
                    lose_count += 1
                    lose_time.append(str(other_goal_time))
            current_player_data["lose_count"] = str(lose_count)
            current_player_data["lose_time"] = ",".join(lose_time)
            # 进球
            goal_time = []
            # 进球时间
            goal_count = 0
            current_player_data["pg"] = current_player_data.get("pg",0)
            current_player_data["ag"] = current_player_data.get("ag",0)
            current_player_data["sg"] = current_player_data.get("sg",0)
            for current_goal in current_team_goal_time_data:
                current_goal_time = current_goal[1]
                if str(player_id) == str(current_goal[0]):
                    goal_count += 1
                    goal_time.append(str(current_goal_time))
                    current_player_data[current_goal[2]] = current_player_data[current_goal[2]] + 1
            current_player_data["goal_count"] = str(goal_count)
            current_player_data["goal_time"] = ",".join(goal_time)
            

            # 助攻时间
            assist_count = 0
            assist_time = []
            for current_assist in current_team_assist_time_data:
                current_assist_time = current_assist[1]
                if str(player_id) == str(current_assist[0]):
                    assist_count += 1
                    assist_time.append(str(current_assist_time))
            current_player_data["assist_count"] = str(assist_count)
            current_player_data["assist_time"] = ",".join(assist_time)
            current_player_data.update(match_data)
            new_data.append(current_player_data)
        return new_data

    def parse_current_match_player_stats(self, player_stats_url):
        html = self.downloader(player_stats_url)
        if html:
            doc = pq(html)
            home_offensive_data = doc("#homeOffensive > tbody")
            home_offensive_datas = self.get_offensive_data(home_offensive_data)

            away_offensive_data = doc("#awayOffensive > tbody")
            away_offensive_datas = self.get_offensive_data(away_offensive_data)

            home_defensive_data = doc("#homeDefensive > tbody")
            home_defensive_datas = self.get_defensive_data(home_defensive_data)

            away_defensive_data = doc("#awayDefensive > tbody")
            away_defensive_datas = self.get_defensive_data(away_defensive_data)

            home_pass_data = doc("#homePass > tbody")
            home_pass_datas = self.get_pass_data(home_pass_data)

            away_pass_data = doc("#awayPass > tbody")
            away_pass_datas = self.get_pass_data(away_pass_data)

            home_team_stats_datas = self.concat_stats_data(home_offensive_datas, home_defensive_datas, home_pass_datas)
            away_team_stats_datas = self.concat_stats_data(away_offensive_datas, away_defensive_datas, away_pass_datas)

            return home_team_stats_datas + away_team_stats_datas

    # 获取进攻数据
    def get_offensive_data(self, data):
        datas = []
        trs = data.children("tr")
        for tr in trs.items():
            player_id = tr.attr("data-playerid")
            # 射门
            sm = tr.children("td:nth-child(3)").text()
            # 射正
            sz = tr.children("td:nth-child(4)").text()
            # 过人
            gr = tr.children("td:nth-child(5)").text()
            # 被侵犯
            bqf = tr.children("td:nth-child(6)").text()
            # 越位
            yw = tr.children("td:nth-child(7)").text()
            # 被抢断
            bqd = tr.children("td:nth-child(8)").text()
            # 失误
            sw = tr.children("td:nth-child(9)").text()
            # 评分
            rate = tr.children("td:nth-child(10)").text()
            datas.append([player_id, sm, sz, gr, bqf, yw, bqd, sw, rate])
        return datas

    # 获取防守数据
    def get_defensive_data(self, data):
        datas = []
        trs = data.children("tr")
        for tr in trs.items():
            player_id = tr.attr("data-playerid")
            # 抢断
            qd = tr.children("td:nth-child(3)").text()
            # 拦截
            lj = tr.children("td:nth-child(4)").text()
            # 解围
            jw = tr.children("td:nth-child(5)").text()
            # 封堵
            fd = tr.children("td:nth-child(6)").text()
            # 造越位
            zyw = tr.children("td:nth-child(7)").text()
            # 犯规
            fg = tr.children("td:nth-child(8)").text()
            # 致命失误
            zmsw = tr.children("td:nth-child(9)").text()
            # 评分
            rate = tr.children("td:nth-child(10)").text()
            datas.append([player_id, qd, lj, jw, fd, zyw, fg, zmsw, rate])
        return datas

    # 获取组织数据
    def get_pass_data(self, data):
        datas = []
        trs = data.children("tr")
        for tr in trs.items():
            player_id = tr.attr("data-playerid")
            # 关键传球
            gjcq = tr.children("td:nth-child(3)").text()
            # 传球
            cq = tr.children("td:nth-child(4)").text()
            # PS%
            ps = tr.children("td:nth-child(5)").text()
            # FTP
            ftp = tr.children("td:nth-child(6)").text()
            # FTPS
            ftps = tr.children("td:nth-child(7)").text()
            # 传中
            cz = tr.children("td:nth-child(8)").text()
            # 长传
            cc = tr.children("td:nth-child(9)").text()
            # 直塞
            zs = tr.children("td:nth-child(10)").text()
            # 评分
            rate = tr.children("td:nth-child(11)").text()
            datas.append([player_id, gjcq, cq, ps, ftp, ftps, cz, cc, zs, rate])
        return datas

    def concat_stats_data(self, data1, data2, data3):
        datas = []
        for d_1 in data1:
            data = []
            p_id_1 = d_1[0]
            for d_2 in data2:
                p_id_2 = d_2[0]
                for d_3 in data3:
                    p_id_3 = d_3[0]
                    if p_id_1 == p_id_2 == p_id_3:
                        data.append(d_1[:-1] + d_2[1:-1] + d_3[1:])
            datas.append(data[0])
        return datas

def main(list_data,conn,cursor):
    ms = MatchSpider(use_proxy=False)
    # 有效的比赛数据至 25767  全部 302099
    for li in list_data:
        url = "http://www.tzuqiu.cc/matches/{}/stat.do".format(li)
        ms.parse_match_detail(url,conn,cursor)
    # 指定页面
    # ms.parse_match_detail("http://www.tzuqiu.cc/matches/25251/stat.do")

if __name__ == '__main__':
    list_data = [25767,25768,302099]
    conn = get_mysql_connect()
    cursor = conn.cursor()
    main(list_data,conn,cursor)
    close_connect(conn, cursor)