from flow import (is_session_expired ,parse_table ,safe_filename ,load_env_file,
                  trigger_report ,query_report ,download_report)
from datetime import datetime, timedelta
from login    import login_session
from logger   import get_logger
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import time ,json ,uuid

metrics = {"trigger_total": 0,
           "trigger_success": 0,
           "trigger_fail": 0,
           "query_reports": 0,
           "empty_reports": 0,
           "download_total": 0,
           "download_success": 0,
           "download_fail": 0,
           "download_retry": 0,
           "session_expired_count": 0}

### Config
MAX_RETRY = 5
service ,report_type = 'CpsMonitor' ,'AnpayOrder'  #'HistoryOrder' ,'AnpayOrder'
logger = get_logger(service=service ,logger_name=f'{service}.{report_type}' ,stage='init')
trace_id = str(uuid.uuid4())
logger.extra["trace_id"] = trace_id

now = datetime.now(ZoneInfo('Asia/Taipei'))
current_hour = now.replace(minute=0, second=0, microsecond=0)
next_hour = current_hour + timedelta(hours=1)
credentials = load_env_file(r"C:\Users\peter.chang/credentials.env")
username ,plain_password ,secret_key  = (credentials.get('cps_account'   ,None),
                                         credentials.get('cps_password'  ,None), 
                                         credentials.get('cps_secret_key',None)) 
session = login_session(username ,plain_password ,secret_key ,logger)


### Trigger Case
regulator_map = {
        '630045110000010' : 'ASIC',        # 掛牌
        '630045110000074' : 'FCA' ,        # 掛牌
        '630045110000054' : 'VFSC1',       # 營利
        '630045110000029' : 'VFSC2',       # 營利
        # '630045110000069' : 'VIG Group-MU' # 機構 -> 國外大型機構(投信、投顧)
    }
page_responses = {}
logger.extra["stage"] = "trigger report"
for regulator ,regulator_v in regulator_map.items():
    retry = 0
    logger.info("trigger start", extra={"event": "start", "status": "ok", "regulator": regulator_v})
    while True:
        date = now.strftime('%Y-%m-%d')
        try:
            metrics["trigger_total"] += 1
            response, trigger_time = trigger_report(session ,regulator ,date ,date ,report_type)
            if is_session_expired(BeautifulSoup(response.text, "html.parser")):  
                retry += 1
                logger.warning("session expired", extra={"event": "end","status": "ok","regulator": regulator_v, "retry_count": retry})
                metrics["session_expired_count"] += 1
                if retry > MAX_RETRY:
                    logger.exception("login retry exceeded", extra={"event": "end","status": "error","regulator": regulator_v})
                    raise Exception("重新登入失敗次數過多")
                time.sleep(2)
                session = login_session(username, plain_password, secret_key)
                time.sleep(1)
                continue
            page_responses[regulator] = response.text
            logger.info("trigger success", extra={"event": "end" ,"status": "ok" ,"regulator": regulator_v})
            metrics["trigger_success"] += 1
            break
        except Exception as e:
            logger.exception("trigger failed", extra={"event": "end" ,"status": "error" ,"regulator": regulator_v})
            metrics["trigger_fail"] += 1
            raise        
   
### Query Case
code_00_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "00"]
code_01_keys = [k for k, v in page_responses.items() if json.loads(v).get("retCode") == "01"]
if code_00_keys:   
    logger.extra["stage"] = "query report"
    start_time_str = current_hour.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str   = next_hour.strftime('%Y-%m-%d %H:%M:%S')
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
        metrics["query_count"] = len(ids)
        
        ### Export / Download Case
        logger.extra["stage"] = "download report"
        for _id in ids:    
            time.sleep(0.5)
            metrics["download_total"] += 1
            report_id, report_time, report_counts = _id[0], _id[6], _id[7]
            logger.info("download start", extra={"event": "end", "report_id": report_id,"report_time": report_time,
                        "count": int(report_counts) if report_counts else 0
                    })        
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
                    logger.info("download success", extra={"event": "end","status": "ok","report_id": report_id,"file_name": filename})
                    metrics["download_success"] += 1
                    break
                except Exception:
                    if retry == MAX_RETRY - 1:
                        logger.exception("download failed", extra={"event": "end", "status": "fail","report_id": report_id})
                        metrics["download_fail"] += 1
                    else:
                        logger.warning("download retry", extra={"event": "end","status": "retry","report_id": report_id,"retry_count": retry})
                        metrics["download_retry"] += 1 
if code_01_keys:                                                
    logger.warning("no report triggered", extra={"event": "end" ,"status": "empty" ,"regulators": code_01_keys})
    metrics["empty_reports"] += 1
    
logger.extra["stage"] = "job_summary"
logger.info("job summary", extra={
    "event": "end",
    "status": "ok" if metrics["download_fail"] == 0 and metrics["trigger_fail"] == 0 else "warning",

    "metrics": {
        # ===== PM / 流程健康 =====
        "trigger": {
            "total": metrics["trigger_total"],
            "success": metrics["trigger_success"],
            "fail": metrics["trigger_fail"],
            "success_rate": round(metrics["trigger_success"] / metrics["trigger_total"], 2) if metrics["trigger_total"] else 0
        },

        # ===== 營運 / 數據量 =====
        "query": {
            "report_count": metrics["query_count"],
            "empty_reports": metrics["empty_reports"]
        },

        # ===== 核心產出（最重要）=====
        "download": {
            "total": metrics["download_total"],
            "success": metrics["download_success"],
            "fail": metrics["download_fail"],
            "retry": metrics["download_retry"],
            "success_rate": round(metrics["download_success"] / metrics["download_total"], 2) if metrics["download_total"] else 0
        },

        # ===== 風控 / 穩定性 =====
        "stability": {
            "session_expired": metrics["session_expired_count"]
        }
    }
})