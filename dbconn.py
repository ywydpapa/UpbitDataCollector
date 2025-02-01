import dotenv
import os
import pymysql


dotenv.load_dotenv()
hostenv = os.getenv("host")
userenv = os.getenv("user")
passwordenv = os.getenv("password")
dbenv = os.getenv("db")
charsetenv = os.getenv("charset")


def getkey(uno):
    global result
    db1 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur1 = db1.cursor()
    try:
        sql = "SELECT apikey1,apikey2 from traceUser WHERE userNo=%s and attrib not like %s"
        cur1.execute(sql,(uno, '%XXX'))
        result = cur1.fetchone()
    except Exception as e:
        print("Key 읽기 에러 ",e)
    finally:
        cur1.close()
        db1.close()
        return result


def getkeypond(uno):
    global result
    db1 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur1 = db1.cursor()
    try:
        sql = "SELECT apikey1,apikey2 from pondUser WHERE userNo=%s and attrib not like %s"
        cur1.execute(sql,(uno, '%XXX'))
        result = cur1.fetchone()
    except Exception as e:
        print("Key 읽기 에러 ",e)
    finally:
        cur1.close()
        db1.close()
        return result


def getcoinlist(uno):
    global coinlist
    db2 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur2 = db2.cursor()
    try:
        sql = "SELECT DISTINCT bidCoin from traceSetup WHERE userNo=%s and (regDate >= DATE_ADD(now(), INTERVAL -1 WEEK ) or attrib not like %s)" #1주 이내 거래 코인 목록
        cur2.execute(sql,(uno,"XXXUP%"))
        result = cur2.fetchall()
        coinlist = []
        for item in result:
            coinlist.append(item[0])
    except Exception as e:
        print("거래코인 목록 읽기 에러 ",e)
    finally:
        cur2.close()
        db2.close()
        return coinlist


def getcoinlistpond(uno):
    global coinlist
    db2 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur2 = db2.cursor()
    try:
        sql = "SELECT DISTINCT bidCoin from tradingSetup WHERE userNo=%s and (regDate >= DATE_ADD(now(), INTERVAL -1 WEEK ) or attrib not like %s)" #1주 이내 거래 코인 목록
        cur2.execute(sql,(uno,"XXXUP%"))
        result = cur2.fetchall()
        coinlist = []
        for item in result:
            coinlist.append(item[0])
    except Exception as e:
        print("거래코인 목록 읽기 에러 ",e)
    finally:
        cur2.close()
        db2.close()
        return coinlist


def insertLog(uno,ldata01,ldata02,ldata03,ldata04,ldata05,ldata06,ldata07,ldata08,ldata09,ldata10,ldata11,ldata12,ldata13):
    global rows
    db3 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur3 = db3.cursor()
    try:
        sql = ("insert into tradeLogDone (userNo,uuid,side,ord_type,price,market,created_at,volume,remaining_volume,reserved_fee,paid_fee,locked,executed_volume,trades_count)"
               " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        cur3.execute(sql,(uno,ldata01,ldata02,ldata03,ldata04,ldata05,ldata06,ldata07,ldata08,ldata09,ldata10,ldata11,ldata12,ldata13))
        db3.commit()
    except Exception as e:
        print("거래완료 기록 인서트 에러", e)
    finally:
        cur3.close()
        db3.close()


def checkuuid(uuid):
    global rows
    db4 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur4 = db4.cursor()
    try:
        sql = "select count(*) from tradeLogDone where uuid=%s"
        cur4.execute(sql,uuid)
        result = cur4.fetchone()
    except Exception as e:
        print("uuid 조회 에러",e)
    finally:
        cur4.close()
        db4.close()
        return result[0]


def insertAmt(uno,coinn,bal,loc,amt):
    global rows
    db5 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur5 = db5.cursor()
    try:
        sql = (
            "insert into tradeResult (userNo,coinName,balance,avgPrice,amt,regDate)"
            " values (%s,%s,%s,%s,%s,now())")
        cur5.execute(sql, (uno,coinn,bal,loc,amt))
        db5.commit()
    except Exception as e:
        print("지갑 기록 인서트 에러", e)
    finally:
        cur5.close()
        db5.close()


def userlist():
    global rows
    db6 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur6 = db6.cursor()
    try:
        sql = "select userNo from traceUser where apiKey1 is not null and attrib not like %s"
        cur6.execute(sql, 'XXXUP')
        rows = cur6.fetchall()
    except Exception as e:
        print("사용자 조회 에러",e)
    finally:
        cur6.close()
        db6.close()
        return rows


def userlistpond():
    global rows
    db6 = pymysql.connect(host=hostenv, user=userenv, password=passwordenv, db=dbenv, charset=charsetenv)
    cur6 = db6.cursor()
    try:
        sql = "select userNo from pondUser where apiKey1 is not null and attrib not like %s"
        cur6.execute(sql, 'XXXUP')
        rows = cur6.fetchall()
    except Exception as e:
        print("사용자 조회 에러",e)
    finally:
        cur6.close()
        db6.close()
        return rows

