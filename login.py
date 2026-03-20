import requests ,hashlib ,pyotp ,time 


def login_session(username: str, plain_password: str, secret_key: str ,logger) -> requests.Session:
    session = requests.Session()
    login_url = "https://admin-cf.v-payment.io/bm_console/login"

    # 設定 stage
    logger.extra["stage"] = "authentication"
    logger.info("start login", extra={"event": "start" ,"status": "ok" ,"username": username})

    try:
        # 1️⃣ Transformer password
        fxSysPwd = hashlib.md5(plain_password.encode()).hexdigest()
        logger.info("password hashed", extra={"event": "end" ,"status": "ok" ,"step": "password_hashed"})

        
        # 2️⃣ Getting Login Page 
        start = time.time()
        r1 = session.get(login_url)
        r1.raise_for_status()
        logger.info("login page fetched", extra={"event": "end" ,"status": "ok" ,"step": "get_login_page" ,"http_status": r1.status_code})
        
        # 3️⃣ Input Account / Transformer password
        login_data = {
            "loginName": username,
            "fxSysPwd": fxSysPwd,
            "changeCaptchaCode": "false"
        }
        start = time.time()
        r2 = session.post(login_url, data=login_data, allow_redirects=False)

        if r2.status_code == 302:
            logger.info("login success", extra={"event": "end" ,"status": "ok" ,"step": "submit_login" ,"redirect": r2.headers.get("Location")})
        else:
            logger.error("login failed", extra={"event": "end" ,"status": "error" ,"step": "submit_login" ,"http_status": r2.status_code})
            raise Exception("登入失敗，未成功跳轉")

        # 4️⃣ 2FA
        totp = pyotp.TOTP(secret_key)
        code = totp.now()
        logger.info("2fa generated", extra={"event": "end", "status": "ok", "step": "generate_2fa"})
        
        # 5️⃣ 2FA submit
        twofa_url = "https://admin-cf.v-payment.io/bm_console/twoFAValid"
        twofa_data = {"twoFAKey": "","twoFAKeyPath": "","twoFAPwd": code,"loginHref": "/bm_console/logout" }
        start = time.time()
        r3 = session.post(twofa_url, data = twofa_data, allow_redirects = False)

        if r3.status_code == 302:
            logger.info("2fa success", extra={"event": "end" ,"status": "ok" ,"step": "submit_2fa" ,"redirect": r3.headers.get("Location")})
        else:
            logger.error("2fa failed", extra={"event": "end" ,"status": "error" ,"step": "submit_2fa" ,"http_status": r3.status_code})

        logger.info("login pipeline success", extra={"event": "end" ,"status": "ok"})
        return session
    
    except Exception as e:
        logger.exception("login pipeline failed", extra={"event": "end", "status": "error", "error_type": type(e).__name__})
        raise