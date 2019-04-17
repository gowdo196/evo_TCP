#-*- coding: utf8 -*-
import sys, os
import asyncio
import socket
import struct
import time
import datetime
import configparser
import requests
import xmltodict
import psycopg2
import psycopg2.extras
import threading
sys.path.append('C:\\Python34\\mysite\\bobo')
import evo_class

path = "C:\\Python34\\evo_TCP\\"
decode_ = "cp950"

def write_log_txt(object):
    f = open(path+"daily_log\\evo_TCP_"+str(datetime.datetime.now())[0:10].replace('-','')+"_log.txt","a")

    f.write("\n["+str(datetime.datetime.now())+"]"+str(object))
    f.close()
# 取出 ini 中的設定
def get_ini_str(section, key):
    config_ = configparser.ConfigParser()
    config_.read(path+"evo_TCP.ini")
    return config_.get(section, key)

# 建一個 DB_cursor_
def build_ApexDB_cursor_(set_client_encoding):
    #建立ApexDB_cursor_
    ApexDB_IP_ = get_ini_str("Apex", "DB_IP_")
    ApexDB_Port_ = get_ini_str("Apex", "DB_Port_")
    ApexDB_DB_ = get_ini_str("Apex", "DB_DB_")
    ApexDB_User_ = get_ini_str("Apex", "DB_Name_")
    ApexDB_Pwd_ = get_ini_str("Apex", "DB_Pwd_")

    ApexDB_str_ = "host="+ApexDB_IP_+" port="+ApexDB_Port_+ " user="+ApexDB_User_ + " dbname="+ApexDB_DB_ + " password="+ApexDB_Pwd_
    
    try:
        ApexDB_conn_ = psycopg2.connect(ApexDB_str_)
    except:
        write_log_txt("==== build_ApexDB_cursor_ ERROR ===="+str(sys.exc_info()[0]))
        sys.exit(0)    
    
    ApexDB_conn_.autocommit = True
    ApexDB_conn_.set_client_encoding(set_client_encoding)
    ApexDB_cursor_ = ApexDB_conn_.cursor()
    return ApexDB_cursor_
# 建一個 socket連線
def build_sock(ip_port_from_ini):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(ip_port_from_ini)
    except:
        write_log_txt("build_sock.ip_port_from_ini="+str(ip_port_from_ini[0])+":"+str(ip_port_from_ini[1])+"====ERROR===="+str(sys.exc_info()))
    return sock
def evo_socket():
    global push_socket
    global ApexDB_cursor_
    #seq = seq + 1
    #斷線序號記下來重連的時候改R message重送
    Lmsg = 'L0000000022APEX-TCB       0010060'
    push_socket.send(Lmsg.encode('ascii'))
    ApexDB_cursor_ = build_ApexDB_cursor_('big5')
    daily_sn = max(repeat_pxbs_orse(ApexDB_cursor_)[0][0],repeat_pxmh_orse(ApexDB_cursor_)[0][0])

    for item in get_ini_str('submit_bhno','list').split(','):
        Rmsg = 'R'+daily_sn+'01322'+item+' *******'
        push_socket.send(Rmsg.encode('ascii'))
        daily_sn = str(int(daily_sn)+1).rjust(7,'0')
    """
    Rmsg = 'R'+0000001+'01322822 *******'
    push_socket.send(Rmsg.encode('ascii'))
    Rmsg = 'R000000201322823 *******'
    push_socket.send(Rmsg.encode('ascii'))
    Rmsg = 'R000000301322824 *******'
    push_socket.send(Rmsg.encode('ascii'))
    """
    print("==== evo TCP PUSH ==== sock.connect start"+daily_sn)
    write_log_txt("==== evo TCP PUSH ==== sock.connect start"+daily_sn)
    recv_data = b''

    while True:
        try:
            #sock.setblocking(0)
            #select.select([sock], [], [], 3)
            #檢查時間,早上8點就直接清盤
            nowtime = str(datetime.datetime.now())[11:19].replace(':','')
            if nowtime > '075000' and nowtime < '075900':
                YD_daily_clear()
            recv_data = recv_recursive(recv_data + push_socket.recv(1024),ApexDB_cursor_)
            if len(recv_data) < 1:
                time.sleep(1)
            #20170430 連續收到長度0的資料時中止執行
        except:
            write_log_txt(str(sys.exc_info()))
            break
def evo_server(server):
    client, addr = server.accept()
    write_log_txt("Acepted connection from: %s:%d" % (addr[0],addr[1]))
    #print ("Acepted connection from: %s:%d" % (addr[0],addr[1]))
    hkc_recv_data = b''
    while True:
        try:
            hkc_recv_data = hkc_recv_recursive(hkc_recv_data + client.recv(2048))
        except:
            client.close()
            client, addr = server.accept()
            write_log_txt("except Acepted connection from: %s:%d" % (addr[0],addr[1]))

def hkc_recv_recursive(recv_data):
    global order_server_socket
    #丟進來就要解到沒有結尾符號 才丟出去
    #write_log_txt('before hkc_recv_recursive ='+str(len(recv_data)))
    if len(recv_data) > 0:
        write_log_txt("HKC Received: %s" % recv_data)
        while True:
            try:
                if recv_data.find(b'/>') == -1:#代表這段已經不完整了 下次傳進來再一起解
                    break

                single_data = recv_data[:recv_data.index(b'/>')+2]#bstr[:13+<SBFORDER ... +2], 用 "/>" 裁切
                print("send single_data: %s" % str(single_data[13:]))
                order_server_socket.send(single_data)
                recv_data = recv_data[recv_data.index(b'/>')+2:]
            except:
                order_server_socket.close()
                order_server_socket = build_sock((get_ini_str('order_server','ip'),int(get_ini_str('order_server','port'))))
        #write_log_txt('after hkc_recv_recursive ='+str(len(recv_data)))
    return recv_data
def recv_recursive(recv_data,ApexDB_cursor_):
    global order_server_socket
    #丟進來就要解到沒有200 165 才丟出去
    write_log_txt('before recursive ='+str(len(recv_data)))
    if len(recv_data) > 0:
        while True:
            if recv_data[:1] == b'H':#b'H0000000004    '
                #YD_Heartbeat(sock)
                Heartbeat()
                recv_data = recv_data[15:]

            if recv_data[:1] == b'C':#b'C0000000004    '
                write_log_txt("recv_recursive daily future clear")
                recv_data = recv_data[15:]
                YD_daily_clear()

            if recv_data.find(b'\n') == -1:#代表這段已經不完整了 下次傳進來再一起解
                break

            if recv_data[:1] == b'1':#委回長度200
                single_data = recv_data[:200]
                #write_log_txt(single_data)
                #if single_data[1:8].decode('big5').strip() > pxbs_orse:#檢查orse今天有沒有重覆
                order_server_socket.send(tran_future_102(single_data,ApexDB_cursor_))
                print('====102 send finish====')
                #single_data = b'100257121892823 2600065          114A023TXFD7                   R0009401.00S0L        0001        0000        00011  020501060410134953320NYW                                0000010931                \n'
            elif recv_data[:1] == b'2':#成回長度165
                single_data = recv_data[:165]
                #if single_data[1:8].decode('big5').strip() > pxmh_orse:#檢查orse今天有沒有重覆
                order_server_socket.send(tran_future_103(single_data,ApexDB_cursor_))
                print('====103 send finish====')
                #single_data = b'200257131542823 2600065          144A023TXFD7     0009800.00S0L        0001               00000037 1349533200205                                NYW                 \n'
            else:
                #b'???' 看有沒有漏網之魚
                write_log_txt('====recv_recursive else====')
                write_log_txt(recv_data)

            recv_data = recv_data[recv_data.index(b'\n')+1:]

        write_log_txt('after recursive ='+str(len(recv_data)))
    return recv_data

def tran_future_102(data,DB_cursor):
    #write_log_txt("tran_future_102 start")
    symb = data[40:60].decode('big5').strip()#symb agsy 商品代碼 char[20]
    bs_ = data[75:76].decode('big5').strip()#買賣別
    kind = '1'
    lino = '1'
    #市場別
    if symb[2] == 'O':
        lino = '2'
        prodtype = '2'#選擇權單式
        if '/' in symb or ':' in symb or '-' in symb:
            prodtype = '3'#選擇權複式
            kind = '10'
    else:
        prodtype = '1'#期貨單式
        if '/' in symb or ':' in symb or '-' in symb:
            prodtype = '4'#期貨複式
            kind = '10'

    orst_dict = {'1':'10','2':'20','3':'30','6':'15'}
    type_dict = {'1':'0','2':'20','3':'30','6':'15'}
    orcn_dict = {'R':'0','F':'10','I':'20','Q':'0'}
    ortr_dict = {'0':'0','1':'1','2':'4','':''}
    #商品代碼轉換
    a = evo_class.YuanDa(symb, bs_, prodtype)
    tdate = str(int(str(datetime.datetime.now())[0:4])-1911)+"/"+str(datetime.datetime.now())[5:7]+"/"+str(datetime.datetime.now())[8:10]
    extm = data[129:131].decode('big5').strip()+":"+data[131:133].decode('big5').strip()+":"+data[133:135].decode('big5').strip()#委託回報時間
    fbhno = data[12:16].decode('big5').strip()#分公司代碼
    sett = data[16:23].decode('big5').strip()#投資人帳號
    comm = data[35:40].decode('big5').strip()#委託書號
    orcn = orcn_dict[data[64:65].decode('big5')]#委託條件         char[1]
    orpr = data[65:75].decode('big5').strip()#orpr nepr dura duor委託價格
    ortr = ortr_dict[data[76:77].decode('big5')]#沖銷別(開平倉碼) char[1]
    orpt = evo_class.get_orpt(data[77:78].decode('big5'),'F')#價格別(ZF 委託方式)
    if data[114:115].decode('big5').strip() == '1':
        orsh = data[78:90].decode('big5').strip()#委託量
    else:
        orsh = data[90:102].decode('big5').strip()
    duov = data[90:102].decode('big5').strip()#改前量(主動BeforeQty)
    duva = data[103:114].decode('big5').strip()#改後量(主動AfterQty)
    nesh = data[103:114].decode('big5').strip()#改後量
    orst = orst_dict[data[114:115].decode('big5').strip()]#委託性質
    type = type_dict[data[114:115].decode('big5').strip()]
    duit = data[129:131].decode('big5')+":"+data[131:133].decode('big5')+":"+data[133:135].decode('big5')+"."+data[135:138].decode('big5')#委託回報時間
    orse = data[1:8].decode('big5').strip()#exse orse 流水序號data[90:98].decode('big5').strip()
    agls = evo_class.get_bs(bs_) #買賣別
    """
    a.exdt#履約日(第一隻腳)
    a.losh#買賣別(第一隻腳)
    a.capu#權利別(第一隻腳)
    lots   成交口數(第一隻腳)
    pric   成交價(第一隻腳)
    a.stpr#履約價(第一隻腳)
    a.seed#履約日(第二隻腳)
    selt   成交口數(第二隻腳)
    sepr   成交價(第二隻腳)
    a.sesp#履約價(第二隻腳)
    a.secp#權利別(第二隻腳)
    a.sels#買賣別(第二隻腳)
    """
    sqlstr = "select dfhf.sf_proxy_pxbs_insert('"+str(datetime.datetime.now())[0:10].replace('-','')+"','"+extm+"','D','"+fbhno+"','"+sett+"','"+comm+"','"+symb+"','"+bs_+"','"+lino+"','"+kind+"','"+orsh+"','"+duov+"','"+nesh+"','"+orpr+"','"+data[77:78].decode('big5')+"','"+ortr+"','"+data[64:65].decode('big5')+"','"+orse+"','"+prodtype+"','"+data[114:115].decode('big5').strip()+"','"+data.decode('big5').strip()+"');"
    write_log_txt(sqlstr)    
    DB_cursor.execute(sqlstr)

    result = "<OBFORDER type='"+type+"' date='"+tdate+"' lino='"+lino+"' orse='APX000"+comm+"' kind='"+kind+"' comp='"+fbhno+"' cosy='F021000' \
sett='"+sett+"' comm='"+comm+"' mark='TAIMEX' symb='"+symb+"' agsy='"+symb+"' agls='"+agls+"' exdt='"+a.exdt+"' stpr='"+a.stpr+"' capu='"+a.capu+"' losh='"+a.losh+"' \
seed='"+a.seed+"' sesp='"+a.sesp+"' secp='"+a.secp+"' sels='"+a.sels+"' orpt='"+orpt+"' orpr='"+orpr+"' nepr='"+orpr+"' orsh='"+orsh+"' nesh='"+nesh+"' orcn='"+orcn+"' ortr='"+ortr+"' \
orsc='' orpp='10773' orst='"+orst+"' orex='0' orsa='ProxyTcp' orcl='999' orme='1' orad='192.168.11.247' coer='0' prio='0' orvl='2' \
orif='0' toke='' ibco='' ibse='' ibsa='' ibcl='' ibme='' memo='' duii='"+tdate+"' duit='"+duit+"' duov='"+duov+"' duva='"+duva+"' \
duos='0' duor='"+orsh+"' dura='"+orsh+"' logs='"+tdate+"' duse='0' culi='' dumm='' error-code='' cause='' source='txn'/>"
    big_endian_result_length = struct.pack('>I',len(result.encode('ascii'))+13)
    big_endian_sett = struct.pack('>I',int(sett))
    Type = chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00) #Type Filter 沒有使用

    return big_endian_result_length+big_endian_sett+Type.encode('ascii')+result.encode('ascii')
def tran_future_103(data,DB_cursor):
    global temp_XF_data
    prodtype_dict = {"2":"2","3":"3","4":"1","5":"4"}
    prodtype = prodtype_dict[data[34:35].decode('big5').strip()]
    symb = data[40:50].decode('big5').strip()#symb agsy 商品代碼
    tdate = str(datetime.datetime.now())[0:10].replace('-','/')
    extm = data[99:101].decode('big5').strip()+":"+data[101:103].decode('big5').strip()+":"+data[103:105].decode('big5').strip()
    fbhno = data[12:16].decode('big5').strip()#分公司代碼    
    sett = data[16:23].decode('big5').strip()#投資人帳號
    comm = data[35:40].decode('big5').strip()#委託書號
    agpr = data[50:60].decode('big5').strip()# agpr pxsu 成交價格
    pric = '0'#成交價(第一隻腳)
    sepr = '0'#成交價(第二隻腳)
    selt = ''
    lots = data[63:75].decode('big5').strip()#成交量
    exse = data[1:8].decode('big5').strip()#exse orse 流水序號data[90:98].decode('big5').strip()
    bs_ = data[60:61].decode('big5').strip()
    extr = data[61:62].decode('big5').strip()#沖銷別
    agls = evo_class.get_bs(bs_) #買賣別
    if data[98:99] in [b'1',b'2']:
        if temp_XF_data == b'':#遇到複式單其中一隻腳,暫存後return
            temp_XF_data = data
            msg = chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)
            return struct.pack('>I',13)+struct.pack('>I',2600065)+msg.encode('ascii')
        else:#只處理複式單第二隻腳,取回pxbs商品代碼,清空暫存
            find_symb_from_pxbs_by_comm = "select comm,symb from dfhf.tb_future_pxbs where tdate = '"+tdate.replace('/','')+"' and comm = '"+comm+"'"
            DB_cursor.execute(find_symb_from_pxbs_by_comm)
            result = DB_cursor.fetchall()
            if len(result) > 0:
                symb = result[0][1]#商品代碼(複式單)
                sepr = temp_XF_data[50:60].decode('big5').strip()#成交價(第二隻腳)
                selt = temp_XF_data[63:75].decode('big5').strip()#成交口數(第二隻腳)
                agpr = str(float(agpr)-float(sepr))#成交價差
                write_log_txt(result[0][1]+", sepr="+sepr+", selt="+selt+", agpr="+str(agpr))
                data = data + b'&' + temp_XF_data
            temp_XF_data = b''

    #商品代碼轉換
    a = evo_class.YuanDa(symb#商品代碼         char[20]
                            ,data[60:61].decode('big5')#買賣別
                            ,prodtype)

    if prodtype == '1':#期貨單式
        lino = '1'
        kind = '1'
    elif prodtype == '4':#期貨複式
        lino = '1'
        kind = '10'
    elif prodtype == '2':#選擇權單式
        lino = '2'
        kind = '1'
    elif prodtype == '3':#選擇權複式
        lino = '2'
        kind = '10'
    """
    a.exdt#履約日(第一隻腳)
    a.losh#買賣別(第一隻腳)
    a.capu#權利別(第一隻腳)
    lots   成交口數(第一隻腳)
    pric   成交價(第一隻腳)
    a.stpr#履約價(第一隻腳)
    a.seed#履約日(第二隻腳)
    selt   成交口數(第二隻腳)
    sepr   成交價(第二隻腳)
    a.sesp#履約價(第二隻腳)
    a.secp#權利別(第二隻腳)
    a.sels#買賣別(第二隻腳)
    """
    sqlstr = "select dfhf.sf_proxy_pxmh_insert ('"+tdate.replace('/','')+"','"+extm+"','D','"+sett+"','"+comm+"','"+symb+"','"+bs_+"','"+lino+"','"+lots+"','"+agpr+"','"+exse+"','"+prodtype+"','"+data.decode('big5').strip()+"')"
    write_log_txt(sqlstr)
    DB_cursor.execute(sqlstr)
    result = "<OBFEXECU date='"+tdate+"' \
extm='"+extm+"' lino='"+lino+"' cosy='F021000' comp='"+fbhno+"' cocs='F021' comm='"+comm+"' \
mark='TAIMEX' kind='"+kind+"' agsy='"+symb+"' symb='"+symb+"' exdt='"+a.exdt+"' stpr='"+a.stpr+"' capu='"+a.capu+"' losh='"+a.losh+"' \
agls='"+agls+"' aglt='"+lots+"' lots='"+lots+"' pric='"+pric+"' agpr='"+str(int(float(agpr)*10000))+"' pxsu='"+agpr+"' sett='"+sett+"' dign='1' orpp='0' \
orad='192.168.11.247' targ='4' exse='"+exse+"' orse='APX000"+comm+"' \
exsr='TMP_109' seed='"+a.seed+"' sesp='"+a.sesp+"' secp='"+a.secp+"' sels='"+a.sels+"' selt='"+selt+"' sepr='"+sepr+"'/>"

    #帳號也當數字算出Big endian(我也很無言 不要問)
    big_endian_result_length = struct.pack('>I',len(result.encode('ascii'))+13)
    big_endian_sett = struct.pack('>I',int(sett))
    Type = chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00) #Type Filter 沒有使用

    return big_endian_result_length+big_endian_sett+Type.encode('ascii')+result.encode('ascii')

def Heartbeat():
    global order_server_socket
    msg = chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)+chr(0x00)
    try:
        order_server_socket.send(struct.pack('>I',13)+struct.pack('>I',2600065)+msg.encode('ascii'))
    except:
        order_server_socket.close()
        order_server_socket = build_sock((get_ini_str('order_server','ip'),int(get_ini_str('order_server','port'))))
        write_log_txt("Heartbeat error :"+str(sys.exc_info()))
def YD_auto_Heartbeat():
    global push_socket
    msg = 'H0000000004    '
    while True:
        time.sleep(15)
        try:
            push_socket.send(msg.encode('ascii'))
            #write_log_txt("YD_auto_Heartbeat send")
        except:
            connection_re_try()
            write_log_txt("YD_auto_Heartbeat error :"+str(sys.exc_info()))
            break
def YD_daily_clear():
    try:
        django_ip_port = get_ini_str('evo_server','ip')+":"+get_ini_str('evo_server','django_port')
        r=requests.get('http://'+django_ip_port+'/proxy_server_start_active/')
        data = xmltodict.parse(r._content.decode('utf8'))
        write_log_txt("Daily_Clean_value complete from"\
        +data['TARoot']['server_init']['Daily_Clean_value']+" to "\
        +data['TARoot']['server_init']['today'])
        if data['TARoot']['server_init']['Daily_Clean_value'] < data['TARoot']['server_init']['today']:
            os._exit(0)#通常這是7;30左右收到,清盤結束直接關閉,直到8:30讓排程重啟
    except:
        write_log_txt("YD_daily_clear error="+str(sys.exc_info()))
def connection_re_try():
    global push_socket
    global ApexDB_cursor_
    try:
        write_log_txt("connection_re_try start")
        ApexDB_cursor_.close()
        push_socket.close()
        push_socket = build_sock((get_ini_str('YuanDa_Push','ip'),int(get_ini_str('YuanDa_Push','port'))))
        
        threading.Thread(target=evo_socket ,args = ()).start()
        threading.Thread(target=YD_auto_Heartbeat ,args = ()).start()
    except:
        write_log_txt("connection_re_try error :"+str(sys.exc_info()))

def repeat_pxbs_orse(DB_cursor):
    results = [('0000001',)]
    try:
        sqlstr = "select orse from dfhf.tb_future_pxbs ORDER BY orse desc limit 1"
        DB_cursor.execute(sqlstr)
        results = DB_cursor.fetchall()
        if len(results) == 0:
            results = [('0000001',)]
    except:
        write_log_txt("====ERROR==== repeat_pxbs_exse_check "+str(sys.exc_info()))
    return results

def repeat_pxmh_orse(DB_cursor):
    results = [('0000001',)]
    try:
        sqlstr = "select orse from dfhf.tb_future_pxmh ORDER BY orse desc limit 1;"
        DB_cursor.execute(sqlstr)
        results = DB_cursor.fetchall()
        if len(results) == 0:
            results = [('0000001',)]
    except:
        write_log_txt("====ERROR==== repeat_pxmh_exse_check "+str(sys.exc_info()))
    return results

if __name__ == '__main__':
    global temp_XF_data
    global push_socket
    global order_server_socket
    global ApexDB_cursor_
    push_socket = build_sock((get_ini_str('YuanDa_Push','ip'),int(get_ini_str('YuanDa_Push','port'))))
    order_server_socket = build_sock((get_ini_str('order_server','ip'),int(get_ini_str('order_server','port'))))
    evo_server_socket_from_ini = (get_ini_str('evo_server','ip'),int(get_ini_str('evo_server','port')))
    temp_XF_data = b''
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(evo_server_socket_from_ini)
        server.listen(5)
    except:
        write_log_txt("====ERROR==== sock.connect.init"+str(sys.exc_info()))

    threading.Thread(target=evo_socket ,args = ()).start()
    threading.Thread(target=evo_server ,args = (server,)).start()
    threading.Thread(target=YD_auto_Heartbeat ,args = ()).start()