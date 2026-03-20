from bs4 import BeautifulSoup
from datetime import datetime
import requests


# Common Function
def is_session_expired(soup):
    scripts = soup.find_all("script")
    return any("window.open" in s.text and "login" in s.text for s in scripts)

def parse_table(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find('table')
    if not table:
        return []
    
    columns = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
    ids     = [[td.get_text(strip=True) for i, td in enumerate(tr.find_all('td'))]# if i == 0 or i == 2 or i == 6]
                for tr in table.find('tbody').find_all('tr')]
    return ids

def safe_filename(s):
    import re
    return re.sub(r'[\\/*?:"<>|]', '_', str(s))

def load_env_file(path):
    env = {}
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            env[key.strip()] = value.strip()
    return env



### Trigger Flow
def trigger_report(session, regulator ,begin_time: str, end_time: str, report_type: str):  # 1️⃣ 觸發報表產生
    """
    trigger report API
    :param session: requests.Session()
    :param dateType    : callbackTime ,createtime
    :param begin_time  : 'YYYY-MM-DD HH:MM:SS'
    :param end_time    : 'YYYY-MM-DD HH:MM:SS'
    """
    if report_type == 'HistoryOrder' :#'HistoryOrder' ,'AnpayOrder'
        url = 'https://admin-cf.v-payment.io/bm_console/order/exportOldOrder'
        data = {'queryTxnCd': '','queryChanlCd': '','queryTxnCurrency': 'ALL',
                'merCd': regulator,'merCn': '' ,'merOrderNo': '','txnCd': '','txnOrderNo': '',
                'uid': '','chanlCd': '','txnSta': '','dateType': 'callbackTime','beginTime': f'{begin_time} 00:00:00','endTime'  : f'{end_time} 23:59:59',
                'txnCurrency': 'ALL','collectionStatus': '','address': '','mac': '','bankTransferOrderType': '','cardName': '','clOrderNo': '',}
    
    elif report_type == 'AnpayOrder' :
        url = 'https://admin-cf.v-payment.io/bm_console/anpay/exportOrder'
        data = {'queryChanlCd': '','merCd': regulator,'merCn': '',
                'batchNo': '','merOrderNo': '','txnOrderNo': '','subOrderNo': '','chanlCd': '',
                'txnSta': '','cardName': '','cardNo': '','accAttr': '','txnTp': '',
                'dateType': 'callbackTime','beginTime': f'{begin_time} 00:00:00','endTime': f'{end_time} 23:59:59',
                'txnCurrency': 'ALL','uid': '','splitOrder': '','bankTransferOrderType': '',}
            
    # 可視情況加入 session headers
    session.headers.update({
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'x-requested-with': 'XMLHttpRequest'
    })
    
    response = session.post(url, data=data)
    response.raise_for_status()

    # 本地觸發時間
    trigger_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return response ,trigger_time

def query_report(session, start_time: str, end_time: str,report_type: str):                # 2️⃣ 設定時間區間查詢報表
    """
    query report by time range
    """
    url = 'https://admin-cf.v-payment.io/bm_console/reportRecord/pageList'
    
    data = {
        'numPerPage': '200',
        'inputItem': '1',
        'pageNum': '1',
        'reportType': report_type,  # 'AnpayOrder','HistoryOrder'
        'isFirst': '111',
        'queryStartCreatedAt': f'{start_time} 00:00:00',
        'queryEndCreatedAt'  : f'{end_time} 23:59:59',
        'systemType': 'S0001',
        'createName': 'square.chang',
    }

    session.headers.update({
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
    })
    
    response = session.post(url, data=data)
    response.raise_for_status()
    return response



### Download Flow
def download_report(session: requests.Session, report_id: str ):                           # 3️⃣ 根據每個 id 匯出報表 => 這邊
    from io import BytesIO
    import pandas as pd

    api_url = "https://admin-cf.v-payment.io/bm_console/reportRecord/downloadByClient"

    payload = {"id": report_id}

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest"
    }


    # 1️⃣ 拿 presigned URL
    response = session.post(api_url, data=payload, headers=headers)
    response.raise_for_status()
    data = response.json()

    if not data.get("success"):
        raise Exception(f"API 回傳失敗: {data}")

    download_url = data["url"]


    # 2️⃣ 下載檔案
    file_res = session.get(download_url)
    file_res.raise_for_status()

    # 🔥 Debug 重點
    content_type = file_res.headers.get("Content-Type", "")
    content_length = file_res.headers.get("Content-Length", "unknown")

    # 前 200 bytes 預覽
    preview = file_res.content[:200]

    # ⚠️ HTML 檢查
    if file_res.content.startswith(b"<"):
        # print("⚠️ 偵測到 HTML 回應（可能是錯誤頁）")
        try:
            print(preview.decode(errors="ignore"))
        except:
            pass
        raise ValueError("回傳 HTML，不是報表")

    file_bytes = BytesIO(file_res.content)

    # 3️⃣ 判斷格式
    try:
        if "excel" in content_type or "spreadsheet" in content_type:
            # 🔥 判斷 xls vs xlsx（用 magic number）
            if file_res.content.startswith(b"\xd0\xcf\x11\xe0"):
                df = pd.read_excel(file_bytes, engine="xlrd")
            elif file_res.content.startswith(b"PK"):
                df = pd.read_excel(file_bytes, engine="openpyxl")
            else:
                raise ValueError("未知 Excel 格式")
        elif "csv" in content_type or "text" in content_type:
            df = pd.read_csv(file_bytes)

        else:
            raise ValueError(f"未知格式: {content_type}")

    except Exception as e:
        file_bytes.seek(0)
        try:
            text_preview = file_bytes.read(300).decode(errors="ignore")
        except:
            pass
        raise

    return df