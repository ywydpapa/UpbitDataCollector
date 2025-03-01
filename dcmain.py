import dotenv
import dbconn
import pyupbit
import os
import schedule
import time


dotenv.load_dotenv()
hostenv = os.getenv("host")
userenv = os.getenv("user")
passwordenv = os.getenv("password")
dbenv = os.getenv("db")
charsetenv = os.getenv("charset")


def gettradelog(uno):
    global tradelog
    keys = dbconn.getkey(uno)
    key1 = keys[0]
    key2 = keys[1]
    upbit = pyupbit.Upbit(key1, key2)
    coins = dbconn.getcoinlist(uno)
    tradelogsum = []
    for coin in coins:
        tradelog = upbit.get_order(coin,state='done')
        tradelogsum.append(tradelog)
    return tradelogsum


def gettradelogpond(uno):
    global tradelog
    keys = dbconn.getkeypond(uno)
    key1 = keys[0]
    key2 = keys[1]
    upbit = pyupbit.Upbit(key1, key2)
    coins = dbconn.getcoinlistpond(uno)
    tradelogsum = []
    for coin in coins:
        tradelog = upbit.get_order(coin,state='done')
        tradelogsum.append(tradelog)
    return tradelogsum


def setLog(uno):
    global rows
    try:
        traderesults= gettradelog(uno)
        for trade in traderesults:
            for item in trade:
                uuidchk = item["uuid"]
                if dbconn.checkuuid(uuidchk) != 0:
                    pass
                else:
                    if item["side"] == "ask":
                        print(item)
                        if item.get("price") is not None:
                            ldata01 = item["uuid"]
                            ldata02 = item["side"]
                            ldata03 = item["ord_type"]
                            ldata04 = item["price"]
                            ldata05 = item["market"]
                            ldata06 = item["created_at"]
                            ldata07 = item["volume"]
                            ldata08 = item["remaining_volume"]
                            ldata09 = item["reserved_fee"]
                            ldata10 = item["paid_fee"]
                            ldata11 = item["locked"]
                            ldata12 = item["executed_volume"]
                            ldata13 = item["trades_count"]
                            dbconn.insertLog(uno, ldata01, ldata02, ldata03, ldata04, ldata05, ldata06, ldata07, ldata08, ldata09, ldata10, ldata11,ldata12, ldata13)
                    else:
                        pass
    except Exception as e:
        print("거래 기록 에러 ",e, "사용자 :", uno)
    finally:
        pass


def setLogpond(uno):
    global rows
    try:
        traderesults= gettradelogpond(uno)
        for trade in traderesults:
            for item in trade:
                uuidchk = item["uuid"]
                if dbconn.checkuuid(uuidchk) != 0:
                    pass
                else:
                    if item["side"] == "ask":
                        print(item)
                        if item.get("price") is not None:
                            ldata01 = item["uuid"]
                            ldata02 = item["side"]
                            ldata03 = item["ord_type"]
                            ldata04 = item["price"]
                            ldata05 = item["market"]
                            ldata06 = item["created_at"]
                            ldata07 = item["volume"]
                            ldata08 = item["remaining_volume"]
                            ldata09 = item["reserved_fee"]
                            ldata10 = item["paid_fee"]
                            ldata11 = item["locked"]
                            ldata12 = item["executed_volume"]
                            ldata13 = item["trades_count"]
                            dbconn.insertLog(uno, ldata01, ldata02, ldata03, ldata04, ldata05, ldata06, ldata07, ldata08, ldata09, ldata10, ldata11,ldata12, ldata13)
                    else:
                        pass
    except Exception as e:
        print("거래 기록 에러 ",e, "사용자 :", uno)
    finally:
        pass


def runmain():
    try:
        users = dbconn.userlist()
        for user in users:
            try:
                setLog(user)
                time.sleep(1) #10초 대기 후 실행
            except Exception as e:
                continue
    except Exception as e:
        print("자동 반복 거래기록 실행 에러",e)
    finally:
        return True


def runmainpond():
    try:
        users = dbconn.userlistpond()
        for user in users:
            try:
                setLogpond(user)
                time.sleep(1) #10초 대기 후 실행
            except Exception as e:
                continue
    except Exception as e:
        print("자동 반복 거래기록 실행 에러",e)
    finally:
        return True


def runamt():
    try:
        users = dbconn.userlist()
        for user in users:
            try:
                items = getWallet(user)
                for item in items:
                    dbconn.insertAmt(user[0], item[0], float(item[1]), float(item[2]), float(item[3]))
                time.sleep(1) #1초 대기 후 실행
            except Exception as e:
                continue
    except Exception as e:
        print("자동 반복 자산 인서트 실행 에러",e)
    finally:
        return True


def runamtpond():
    try:
        users = dbconn.userlistpond()
        for user in users:
            try:
                items = getWalletpond(user)
                for item in items:
                    dbconn.insertAmt(user[0], item[0], float(item[1]), float(item[2]), float(item[3]))
                time.sleep(1) #1초 대기 후 실행
            except Exception as e:
                continue
    except Exception as e:
        print("Pond 반복 자산 인서트 실행 에러",e)
    finally:
        return True

def getWallet(uno):
    mycoins = []
    keys = dbconn.getkey(uno)
    key1 = keys[0]
    key2 = keys[1]
    upbit = pyupbit.Upbit(key1, key2)
    walletitems = upbit.get_balances()
    for wallet in walletitems:
        if wallet['currency'] != "KRW":
            coinn = "KRW-" + wallet['currency']
            curr = [coinn, float(wallet['balance'])+float(wallet['locked']), float(wallet['avg_buy_price']),round(float(wallet['balance'])+float(wallet['locked']) * float(wallet['avg_buy_price']), 0) ]
            mycoins.append(curr)
        elif wallet['currency'] == "KRW":
            curr = ["KRW", float(wallet['balance'])+float(wallet['locked']), 1.00, round((float(wallet['balance'])+float(wallet['locked']))*float(1.00),0) ]
            mycoins.append(curr)
        else:
            continue
    return mycoins


def getWalletpond(uno):
    mycoins = []
    keys = dbconn.getkeypond(uno)
    key1 = keys[0]
    key2 = keys[1]
    upbit = pyupbit.Upbit(key1, key2)
    walletitems = upbit.get_balances()
    for wallet in walletitems:
        if wallet['currency'] != "KRW":
            coinn = "KRW-" + wallet['currency']
            curr = [coinn, float(wallet['balance'])+float(wallet['locked']), float(wallet['avg_buy_price']),round(float(wallet['balance'])+float(wallet['locked']) * float(wallet['avg_buy_price']), 0) ]
            mycoins.append(curr)
        elif wallet['currency'] == "KRW":
            curr = ["KRW", float(wallet['balance'])+float(wallet['locked']), 1.00, round((float(wallet['balance'])+float(wallet['locked']))*float(1.00),0) ]
            mycoins.append(curr)
        else:
            continue
    return mycoins


schedule.every().day.at("01:00").do(runmain)
schedule.every().day.at("03:00").do(runmain)
schedule.every().day.at("05:00").do(runmain)
schedule.every().day.at("07:00").do(runmain)
schedule.every().day.at("09:00").do(runmain)
schedule.every().day.at("11:00").do(runmain)
schedule.every().day.at("13:00").do(runmain)
schedule.every().day.at("15:00").do(runmain)
schedule.every().day.at("17:00").do(runmain)
schedule.every().day.at("19:00").do(runmain)
schedule.every().day.at("21:00").do(runmain)
schedule.every().day.at("23:00").do(runmain)

schedule.every().day.at("02:00").do(runmainpond)
schedule.every().day.at("04:00").do(runmainpond)
schedule.every().day.at("06:00").do(runmainpond)
schedule.every().day.at("08:00").do(runmainpond)
schedule.every().day.at("10:00").do(runmainpond)
schedule.every().day.at("12:00").do(runmainpond)
schedule.every().day.at("14:00").do(runmainpond)
schedule.every().day.at("16:00").do(runmainpond)
schedule.every().day.at("18:00").do(runmainpond)
schedule.every().day.at("20:00").do(runmainpond)
schedule.every().day.at("22:00").do(runmainpond)
schedule.every().day.at("00:00").do(runmainpond)

schedule.every().day.at("00:30").do(runamt)
schedule.every().day.at("03:30").do(runamt)
schedule.every().day.at("06:30").do(runamt)
schedule.every().day.at("09:30").do(runamt)
schedule.every().day.at("12:30").do(runamt)
schedule.every().day.at("15:30").do(runamt)
schedule.every().day.at("18:30").do(runamt)
schedule.every().day.at("21:30").do(runamt)
schedule.every().day.at("23:30").do(runamt)

schedule.every().day.at("01:30").do(runamtpond)
schedule.every().day.at("04:30").do(runamtpond)
schedule.every().day.at("07:30").do(runamtpond)
schedule.every().day.at("10:30").do(runamtpond)
schedule.every().day.at("13:30").do(runamtpond)
schedule.every().day.at("16:30").do(runamtpond)
schedule.every().day.at("19:30").do(runamtpond)
schedule.every().day.at("22:30").do(runamtpond)

while True:
    schedule.run_pending()
    time.sleep(10)