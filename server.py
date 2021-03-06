import MySQLdb
import configparser
import concurrent.futures
import time
import queue
from flask import Flask, request
import boto3
from ec2_metadata import ec2_metadata

# Databaseの設定を読み込む
config = configparser.ConfigParser()
# config.read('config.ini')
config.read('/handson/config.ini')

app = Flask(__name__)
# FlaskのLogを非表示にする
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
# レスポンス時間測定用並行処理
executor = concurrent.futures.ThreadPoolExecutor(max_workers=80)

@app.route('/')
def index():
    html = '''
    <code style="white-space: pre-wrap;">
                worldskills Shanghai2021                                                                    
                                                                                
                      ::                                :-                      
                    .:=-.                               .:-:                    
                    .::=.                               :+==:                   
                    ....                                ....                    
                 .... .....                                                     
            .......:-===-:::.                        .    .:-----:::.           
          ...+++-::-=+++***+-                       ..:-+****+++======-.        
          ..-+%@*--=++*+#****-                     .-=+**********#####==        
       :==.::=-+===+++#+%%#**=                     :=*####****##*******-        
     :-==+. .:::..::--=+++**=                      ::=+++*******+++***+-::---.  
     =--===.  .:::..:::-----   .                  ..:-=++=-::.    ....-:.:::--  
     -=-=++=....-=-=--==.   :====-.            .=+==-:::.:::....... .:-:..::--  
     .--=+++==:--::==++*=::-+*+-++=.           :+*-===    .::=-==---==--:::..   
              :=+.:=**#+-++++*+***++-         :=+++++--::-----.:-+-             
               .:=+++*:   :==+++===+-         =----::==:: .--=::=-              
              .::-=--==..   .:-=+++-          .---:::.     ..:.:-:.             
            .=**+++=:-+**=.     ..                      .:-:--..:***-           
              ....  .:=+**+.                          .++**==:  .:::.           
                      .:-=+:                          -+*+=:                    
                                                                                
              Neng Neng                                  Qiao Qiao
              
    availability_zone: ''' + ec2_metadata.availability_zone + '''
    instance_id: ''' + ec2_metadata.instance_id + '''
    </code>
    '''
    return html
    
@app.route('/healthcheck')
@app.route('/favicon.ico')
def healthcheck():
    global time_q
    
    return "Current outstanding tasks: " + str(time_q.qsize()) + "\n"

# リクエスト受信時の時間をキューに保存する
def PutTime():
    global time_q
    time_q.put(time.time())

# サイズ１のキューを利用し、データベースへの照合を順次実行する
def Collation(name):
    global dbendpoint
    name = "'" + name + "'"
    
    # 接続する
    conn = MySQLdb.connect(
    user=config['Database']['user'],
    passwd=config['Database']['passwd'],
    host=dbendpoint,
    db=config['Database']['db'])
    # カーソルを取得する
    cur = conn.cursor()
    
    # キャッシュ判定
    sql = "SELECT name FROM Unicorn WHERE name LIKE " + name
    # コマンド実行
    cur.execute(sql)
    # 実行結果を取得する
    rows = cur.fetchall()
    
    if len(rows) == 0:
        print("[Info]", name, "Not Found, Cache missing")
        sql = "INSERT INTO Unicorn (name, hits) VALUES (" + name + ", 0)"
        # 実行結果を取得する
        cur.execute(sql)
        # 実行結果をコミット
        conn.commit()
        time.sleep(5)
    else:
        for row in rows:
            if name[1:-1] in row:
                print("[Info]", name, "Found, Cache hit")
                sql = "UPDATE Unicorn SET hits = hits + 1 WHERE name LIKE " + name;
                
                # 実行結果を取得する
                cur.execute(sql)
                # 実行結果をコミット
                conn.commit()
    
    sql = "SELECT * FROM Unicorn"
    cur.execute(sql)
    rows = cur.fetchall()
    
    # 接続を閉じる
    conn.close
    
    result_q.put(name)
    
    
    
@app.route('/order', methods=["GET",])
def order():
    global executor
    global result_q
    name = request.args.get('name')
    if name is not None:
        if not time_q.full():
            future_to_url = executor.submit(PutTime)
            Collation(name)
            res = result_q.get()
            
            elapsed_time = time.time() - time_q.get()
            res = res + " elapsed_time:{0}".format(round(elapsed_time, 5)) + "[sec]"
            print("[Info]", res)
            return res
        else:
            print("[Info] Server overload!")
            return "Server overload!\n"
            
    else:
        return "none"


if __name__ == '__main__':
    
    client = boto3.client('rds')
    response = client.describe_db_instances(
        DBInstanceIdentifier=config['Database']['dbidentifier'],
        )
    dbendpoint = response['DBInstances'][0]['Endpoint']['Address']
    
    # CreateVirtualClient()
    time_q = queue.Queue(maxsize=80)
    result_q = queue.Queue(maxsize=1)
    app.run(debug=False, host="0.0.0.0", port=80)
