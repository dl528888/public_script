#!/usr/bin/env python
#Author: Denglei
#Email: dl528888@gmail.com
#QQ: 244979152
#coding=utf-8
import MySQLdb
import datetime
import xlwt
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText

from email.utils import COMMASPACE,formatdate
from email import encoders

import os

def send_mail(server, fro, to, subject, text, files=[]):
    assert type(server) == dict
    assert type(to) == list
    assert type(files) == list

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['Subject'] = subject
    msg['To'] = COMMASPACE.join(to) #COMMASPACE==', '
    msg['Date'] = formatdate(localtime=True)
    msg.attach(MIMEText(text))

    for f in files:
        part = MIMEBase('application', 'octet-stream') #'octet-stream': binary data
        part.set_payload(open(f, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    import smtplib
    smtp = smtplib.SMTP(server['name'], server['port'])
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login(server['user'], server['passwd'])
    smtp.sendmail(fro, to, msg.as_string())
    smtp.close()


def get_mysql_data(sql):
    cur.execute(sql)
    results=cur.fetchall()
    return results
def cover_excel(msg,start_time):
    #wb = xlwt.Workbook()
    ws = wb.add_sheet(start_time,cell_overwrite_ok=True)
    count=len(msg)
    x=msg
    title=['时间'.encode('utf8'),'所属组'.encode('utf8'),'主机IP'.encode('utf8'),'CPU逻辑核数(单位:个)'.encode('utf8'),'CPU空闲值(单位:%)'.encode('utf8'),'可用内存值(单位:GB)'.encode('utf8'),'总内存值(单位:GB)'.encode('utf8'),'公网进入流量(单位:kbps)'.encode('utf8'),'公网流出流量(单位:kbps)'.encode('utf8')]
    x.insert(0,title)
    for j in range(0,9):
        for i in range(0,count):
            if i == 0:
    #ws.write(i,j,title[j].decode('utf8'))
                value=x[0]
            else:
                value=x[i]
      if isinstance(value[j],long) or isinstance(value[j],int) or isinstance(value[j],float):
    ws.write(i,j,value[j])
      else:
                ws.write(i,j,value[j].decode('utf8'))
    #wb.save('/tmp/zabbix_log/chance_zabbix_monitor_test.xls')
def run_select(start_time,end_time):
    get_cpu_idle_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,round(avg(hi.value_avg),2) as Cpu_Idle  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends hi on  i.itemid = hi.itemid  where  i.key_='system.cpu.util[,idle]' and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    cpu_idle_result=get_mysql_data(get_cpu_idle_sql)
    get_cpu_num_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,avg(hi.value_avg) as Cpu_Number  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='system.cpu.num' and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    cpu_num_result=get_mysql_data(get_cpu_num_sql)
    get_mem_avai_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,round(avg(hi.value_avg/1024/1024/1024),2) as Memory_Avaiable  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='vm.memory.size[available]'  and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    mem_avai_result=get_mysql_data(get_mem_avai_sql)
    #get_mem_free_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,hi.value_avg/1024/1024/1024 as Memory_Avaiable  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='vm.memory.size[free]'  and  hi.clock >= UNIX_TIMESTAMP('%s') and  hi.clock < UNIX_TIMESTAMP('%s') and g.name like '%%广告%%';"%(start_time,end_time)
    #mem_free_result=get_mysql_data(get_mem_free_sql)
    get_mem_total_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,round(avg(hi.value_avg/1024/1024/1024),2) as Memory_Total  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='vm.memory.size[total]' and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    mem_total_result=get_mysql_data(get_mem_total_sql)
    get_em2_in_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,round(avg(hi.value_avg/1000),2) as Network_Eth0_In  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='net.if.in[em2]' and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    em2_in_result=get_mysql_data(get_em2_in_sql)
    get_em2_out_sql="select from_unixtime(hi.clock,'%%Y-%%m-%%d %%T') as Date,g.name as Group_Name,h.host as Host,round(avg(hi.value_avg/1000),2) as Network_Eth0_In  from hosts_groups hg join groups g on g.groupid = hg.groupid join items i on hg.hostid = i.hostid join hosts h on h.hostid=i.hostid join trends_uint hi on  i.itemid = hi.itemid  where  i.key_='net.if.out[em2]' and  hi.clock >= UNIX_TIMESTAMP('%s 20:00:00') and  hi.clock < UNIX_TIMESTAMP('%s 00:00:00') and g.name like '%%广告%%' group by h.host;"%(start_time,end_time)
    em2_out_result=get_mysql_data(get_em2_out_sql)
    msg=[list(i) for i in cpu_num_result]
    for i in msg:
        for ii in cpu_idle_result:
      if i[0] ==ii[0] and i[1] == ii[1] and i[2] == ii[2]:
    i[3]=int(i[3])
    #msg.append([i[0],i[1],i[2],int(i[3]),ii[3]])
    i.append(int(ii[3]))
  for iii in mem_avai_result:
      if i[0] ==iii[0] and i[1] == iii[1] and i[2] == iii[2]:
    i.append(round(float(iii[3]),2))
  for iiii in mem_total_result:
      if i[0] ==iiii[0] and i[1] == iiii[1] and i[2] == iiii[2]:
          i.append(int(iiii[3]))
  for a in em2_in_result:
      if i[0] == a[0] and i[1] == a[1] and i[2] == a[2]:
    i.append(int(a[3]))
        if len(i) == 7:
      i.append(0)
  for b in em2_out_result:
      if i[0] == b[0] and i[1] == b[1] and i[2] == b[2]:
    i.append(int(b[3]))
  if len(i) == 8:
      i.append(0)
    cover_excel(msg,start_time)
def main():
    for i in range(7,0,-1):
        start_time=((datetime.datetime.now() - datetime.timedelta(days = i))).strftime("%Y-%m-%d")
        end_time=((datetime.datetime.now() - datetime.timedelta(days = i-1))).strftime("%Y-%m-%d")
  run_select(start_time,end_time)
if __name__ == "__main__":
    default_encoding = 'utf-8'
    if sys.getdefaultencoding() != default_encoding:
        reload(sys)
        sys.setdefaultencoding(default_encoding)
    if os.path.exists("/tmp/zabbix_log/"):
        os.mkdir("/tmp/zabbix_log/")
    conn=MySQLdb.connect(host='10.10.14.19',user='zabbix',passwd='zabbix',port=3306,charset="utf8")
    cur=conn.cursor()
    conn.select_db('zabbix')
    wb = xlwt.Workbook()
    main()
    wb.save('/tmp/zabbix_log/chance_zabbix_monitor_hour_avg.xls')
    cur.close()
    conn.close()
    #follow is send mail
    server = {'name':'smtp.163.com', 'user':'ops_monitor', 'passwd':'xxxx', 'port':25}
    fro = 'xxx@163.com'
    to = ['xx@xx.com','244979152@qq.com']
    now_time=((datetime.datetime.now() - datetime.timedelta(days = 1))).strftime("%Y/%m/%d")
    last_time=((datetime.datetime.now() - datetime.timedelta(days = 7))).strftime("%Y/%m/%d")
    subject = 'xx平台监控数据【%s-%s】'%(last_time,now_time)
    text = 'xx你好,附件是畅思平台最近一周每天20至24点平均值监控数据，请查收!\n有问题请联系邓磊.'
    files = ['/tmp/zabbix_log/chance_zabbix_monitor_hour_avg.xls']
    send_mail(server, fro, to, subject, text, files=files)
