from flow import (is_session_expired ,parse_table ,safe_filename ,load_env_file,
                  trigger_report ,query_report ,download_report)
from datetime import datetime, timedelta
from login    import login_session
from logger   import get_logger
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import time ,json 


### Config
MAX_RETRY = 5
service ,report_type = 'CpsMonitor' ,'HistoryOrder' 
logger = get_logger(service=service ,logger_name=f'{service}.{report_type}' ,stage='init')

now = datetime.now(ZoneInfo('Asia/Taipei'))
current_hour = now.replace(minute=0, second=0, microsecond=0)
next_hour = current_hour + timedelta(hours=1)
credentials = load_env_file(r"C:\Users\peter.chang/credentials.env")
username ,plain_password ,secret_key  = (credentials.get('cps_account'   ,None),
                                         credentials.get('cps_password'  ,None), 
                                         credentials.get('cps_secret_key',None)) 
session = login_session(username ,plain_password ,secret_key ,logger)


### Trigger Case
regulator_map = {'630045110000010' : 'ASIC',
                 '630045110000074' : 'FCA' ,
                 '630045110000054' : 'VFSC1',
                 '630045110000029' : 'VFSC2'}
page_responses = {}
logger.extra["stage"] = "trigger report"
for regulator ,regulator_v in regulator_map.items():
    retry = 0
    while True:
        date = now.strftime('%Y-%m-%d')
        try:
            response, trigger_time = trigger_report(session ,regulator ,date ,date ,report_type)
            if is_session_expired(BeautifulSoup(response.text, "html.parser")):  
                retry += 1
                if retry > MAX_RETRY:
                    raise Exception("重新登入失敗次數過多")
                time.sleep(2)
                session = login_session(username, plain_password, secret_key)
                time.sleep(1)
                continue
            page_responses[regulator] = response.text
            break
        except Exception as e:
            raise        
   
### Query Case
code_00_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "00"]
code_01_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "01"]
if code_00_keys:   
    start_time_str = current_hour.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str   = next_hour.strftime('%Y-%m-%d %H:%M:%S')
    page_resp = query_report(session ,start_time_str ,end_time_str ,report_type = report_type)
    
    if is_session_expired(BeautifulSoup(page_resp.text, "html.parser")): # --- session 過期檢查 --- 
        raise Exception("❌ 查詢報表時 session 已過期")
    
    ids = parse_table(page_resp.text)                                    # --- 解析 table ---
    if not ids:
        pass
    else:
        for _id in ids:    
            time.sleep(0.5)
            report_id, report_time, report_counts = _id[0], _id[6], _id[7]    
            for retry in range(MAX_RETRY):
                try:
                    df = download_report(session, report_id)
                    col = df.get('商户名称')
                    regulator = col.dropna().iloc[0] if col is not None and not col.dropna().empty else ''
                    group_key = {'HistoryOrder':'Deposit', 'AnpayOrder':'Withdraw'}
                    report_type_safe = safe_filename(group_key[report_type])
                    regulator_safe   = safe_filename(regulator)
                    now = datetime.now(ZoneInfo('Asia/Taipei'))
                    timestamp = now.strftime('%Y%m%d%H%M%S')
                    filename = f"CPS_{report_type_safe}_{regulator_safe}_{timestamp}.csv"
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    break
                except Exception:
                    if retry == MAX_RETRY - 1:
                        pass
                    else:
                        pass
if code_01_keys: 
    pass                                               
