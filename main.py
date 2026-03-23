from flow import (is_session_expired ,parse_table ,safe_filename ,load_env_file,
                  trigger_report ,query_report ,download_report)
from datetime import datetime, timedelta
from collections import defaultdict
from login    import login_session
from logger   import get_logger
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import pandas as pd
import time ,json



### Config
MAX_RETRY = 10
QUERY_RANGE_MINUTES = 5
service  = 'CpsMonitor'


# Selected Report_Type
report_types  = [
                'HistoryOrder', # Deposit
                'AnpayOrder'    # Withdraw
                ]

# Selected Regulator_Type
regulator_map = {
                 '630045110000010' : 'ASIC' ,
                 '630045110000074' : 'FCA'  ,
                 '630045110000054' : 'VFSC1',
                 '630045110000029' : 'VFSC2'
                 }

# Selected start_date ~ end_date
start_date ,end_date = '2026-03-20','2026-03-23'

start = datetime.strptime(start_date, '%Y-%m-%d')
end = datetime.strptime(end_date, '%Y-%m-%d')
date_list = [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end - start).days + 1)]

now = datetime.now(ZoneInfo('Asia/Taipei'))
start_time = now - timedelta(minutes=QUERY_RANGE_MINUTES)
end_time   = now + timedelta(minutes=QUERY_RANGE_MINUTES)
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') 

credentials = load_env_file(r"./credentials.env")
username ,plain_password ,secret_key  = (credentials.get('cps_account'   ,None),
                                         credentials.get('cps_password'  ,None),
                                         credentials.get('cps_secret_key',None))

### Trigger Case
for report_type in report_types:
    logger = get_logger(service=service ,logger_name=f'{service}.{report_type}' ,stage='init')
    now = datetime.now(ZoneInfo('Asia/Taipei'))
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    next_hour = current_hour + timedelta(hours=1)
    session = login_session(username ,plain_password ,secret_key ,logger)

    page_responses = {}
    logger.extra["stage"] = "trigger report"
    for regulator ,regulator_v in regulator_map.items():
        retry = 0
        for date in date_list:
            logger.info("trigger start", extra={"event": "start", "status": "ok", "regulator": regulator_v,"date": date})
            while True:
                try:
                    response, trigger_time = trigger_report(session ,regulator ,date ,date ,report_type)
                    if is_session_expired(BeautifulSoup(response.text, "html.parser")):  
                        retry += 1
                        logger.warning("session expired", extra={"event": "end","status": "ok","regulator": regulator_v, "retry_count": retry})
                        if retry > MAX_RETRY:
                            logger.exception("login retry exceeded", extra={"event": "end","status": "error","regulator": regulator_v})
                            raise Exception("重新登入失敗次數過多")
                        time.sleep(2)
                        session = login_session(username, plain_password, secret_key)
                        time.sleep(1)
                        continue
                    page_responses[regulator] = response.text
                    logger.info("trigger success", extra={"event": "end" ,"status": "ok" ,"regulator": regulator_v})
                    break
                except Exception as e:
                    logger.exception("trigger failed", extra={"event": "end" ,"status": "error" ,"regulator": regulator_v})
                    raise        
    
    ### Query Case
    code_00_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "00"]
    code_01_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "01"]
    if code_00_keys:   
        logger.extra["stage"] = "query report"
        logger.info("query report start", extra={"event": "end", "status": "start", "report_type": report_type, "start_time": start_time_str, "end_time" : end_time_str})
        page_resp = query_report(session ,start_time_str ,end_time_str ,report_type = report_type)
        
        if is_session_expired(BeautifulSoup(page_resp.text, "html.parser")): # --- session 過期檢查 --- 
            logger.error("session expired during query", extra={"event": "end","status": "error","report_type": report_type})
            raise Exception("❌ 查詢報表時 session 已過期")
        
        ids = parse_table(page_resp.text)                                    # --- 解析 table ---
        if not ids:
            logger.warning("no report found", extra={"event": "end" ,"status": "empty" ,"report_type": report_type ,"count": 0})
        else:
            logger.info("query report success", extra={"event": "end" ,"status": "ok" ,"report_type": report_type ,"count": len(ids)})
            
            ### Export / Download Case
            df_map = defaultdict(list)
            logger.extra["stage"] = "download report"
            for _id in ids:    
                time.sleep(0.5)
                report_id, report_time, report_counts = _id[0], _id[6], _id[7]
                logger.info("download start", extra={"event": "end", "report_id": report_id,"report_time": report_time})        
                for retry in range(MAX_RETRY):
                    try:
                        time.sleep(1)
                        df = download_report(session, report_id)
                        col = df.get('商户名称')
                        regulator = col.dropna().iloc[0] if col is not None and not col.dropna().empty else ''
                        
                        col1 = df.get('回调时间')
                        recall_time = col1.dropna().iloc[0] if col1 is not None and not col1.dropna().empty else ''
                        dt = datetime.strptime(recall_time, '%Y/%m/%d %H:%M:%S')
                        file_date = dt.strftime('%Y%m%d')
                        
                        group_key = {'HistoryOrder':'Deposit', 'AnpayOrder':'Withdraw'}
                        report_type_safe = safe_filename(group_key[report_type])
                        regulator_safe   = safe_filename(regulator)
                        tag = f"CPS_{report_type_safe}_{regulator_safe}({file_date}).csv"
                        df.to_csv(tag, index=False, encoding='utf-8-sig')
                        
                        # tag = f"CPS_{report_type_safe}_{regulator_safe}"
                        # df['tag'] = tag
                        # df_map[tag].append(df)
                        logger.info("download success", extra={"event": "end","status": "ok","report_id": report_id,"file_name": tag,"counts":len(df)})
                        break
                    except Exception:
                        if retry == MAX_RETRY - 1:
                            logger.exception("download failed", extra={"event": "end", "status": "fail","report_id": report_id})
                        else:
                            logger.warning("download retry", extra={"event": "end","status": "retry","report_id": report_id,"retry_count": retry})
            
            # for tag, df_list in df_map.items():
            #     merged_df = pd.concat(df_list, ignore_index=True)
            #     filename = f"{tag}.csv"
            #     merged_df.to_csv(filename, index=False, encoding='utf-8-sig')
            #     logger.info("merge success", extra={"event": "end" ,"tag": tag ,"file_name": filename ,"total_rows": len(merged_df)})
            
    if code_01_keys:                                                
        logger.warning("no report triggered", extra={"event": "end" ,"status": "empty" ,"regulators": code_01_keys})             

