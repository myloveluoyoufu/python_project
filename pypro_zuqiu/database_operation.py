

def insert_all_data(conn,cursor,item):
    sql = f"insert ignore into zuqiu.all_data" \
          " (player_id,match_stage,match_id,compt," \
          "match_time,home_team,home_team_id,result,away_team," \
          "away_team_id,home_away,shirt_no,player_name,country," \
          "rate,in_time,out_time,full_time,goal_count,goal_time,sm,sz,gr," \
          "bqf,yw,bqd,sw,assist_count,assist_time,gjcq,cq,ps,ftps,cz,cc,zs," \
          "lose_count,lose_time,lj,jw,fd,zyw,fg,zmsw,qd,ag,pg,sg) VALUES " \
          "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    if cursor.execute(sql, [item['player_id'], item['match_stage'], item['match_id'],
                                 item['compt'], item['match_time'], item['home_team'], item['home_team_id'],
                                 item['result'], item['away_team'], item['away_team_id'], item['home_away'],
                                 item['shirt_no'], item['player_name'], item['country'], item['rate'], item['in_time'],
                                 item['out_time'], item['full_time'], item['goal_count'], item['goal_time'], item['sm'],
                                 item['sz'], item['gr'], item['bqf'], item['yw'], item['bqd'], item['sw'],
                                 item['assist_count'], item['assist_time'], item['gjcq'], item['cq'], item['ps'],
                                 item['ftps'], item['cz'], item['cc'], item['zs'], item['lose_count'],
                                 item['lose_time'], item['lj'], item['jw'], item['fd'], item['zyw'], item['fg'],
                                 item['zmsw'], item['qd'], item['ag'], item['pg'],
                                 item['sg']]) == 0:
        print('重复数据')
    conn.commit()
    pass


def insert_match_data(conn,cursor,item):
    try:
        sql = "insert ignore into zuqiu.match" \
              " (away_team,away_team_id,compt,home_away,home_team,home_team_id,match_id,match_stage,match_time,result) VALUES " \
              "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        if cursor.execute(sql, [item['away_team'], item['away_team_id'], item['compt'],
                                     item['home_away'], item['home_team'], item['home_team_id'], item['match_id'],
                                     item['match_stage'], item['match_time'], item['result']]) == 0:
            print('重复数据')
        conn.commit()
    except:
        print('数据无法存储，没有比赛信息....')
    pass

def insert_player_data(conn,cursor,item):
    sql = "insert ignore into zuqiu.player" \
          " (player_id,team_id,name,marketvalue,country,position,height,age) VALUES " \
          "(%s,%s,%s,%s,%s,%s,%s,%s)"

    if cursor.execute(sql, [item['player_id'], item['team_id'], item['name'],
                                 item['marketvalue'], item['country'], item['position'], item['height'],
                                 item['age']]) == 0:
        print('重复数据')
    conn.commit()
    pass

