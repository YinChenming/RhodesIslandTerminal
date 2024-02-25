import logging
import re

import requests

_osvLoger = logging.getLogger("OnlineService_Logger")


class CaptchaError(Exception):
    pass


class PasswordError(Exception):
    pass


class CookiesError(Exception):
    pass


class ParamsError(Exception):
    pass


class OnlineService:
    HEADERS = {'accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,\
*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'zh-CN,zh;q=0.9',
               'cache-control': 'max-age=0', 'dnt': '1',
               "Content-Type": "application/x-www-form-urlencoded",
               'ses-ch-ua': '"Microsoft Edge";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
               'ses-ch-ua-mobile': '?0',
               'ses-ch-ua-platform': '"Windows"', 'ses-fetch-dest': 'document', 'ses-fetch-mode': 'navigate',
               'ses-fetch-site': 'none', 'ses-fetch-user': '?1', 'upgrade-insecure-requests': '1',
               'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.50'}
    URLS = {
        "login": "https://ak.hypergryph.com/user/login",
        "phone_password": "https://as.hypergryph.com/user/auth/v1/token_by_phone_password",
        "send_phone_code": "https://as.hypergryph.com/general/v1/send_phone_code",
        "phone_code": "https://as.hypergryph.com/user/auth/v1/token_by_phone_code",
        "get_basic": "https://as.hypergryph.com/user/info/v1/basic",
        "post_basic": "https://as.hypergryph.com/u8/user/info/v1/basic",
        "gacha": "https://ak.hypergryph.com/user/api/inquiry/gacha",
        "hg": "https://web-api.hypergryph.com/account/info/hg",
        "ak-b": "https://web-api.hypergryph.com/account/info/ak-b",
    }

    def __init__(self, headers: dict = None):
        _osvLoger.info("initialize online service.")
        self.session = requests.session()
        # self.session.headers = self.HEADERS if headers is None else headers

    def clear_cookies(self):
        _osvLoger.debug("clear cookies.")
        self.session.cookies.clear()

    def set_cookies(self, cookies: dict):
        _osvLoger.debug(f"set cookies '{cookies}'")
        self.session.cookies.update(cookies)

    def get_cookies(self, key: str):
        cookies = self.session.cookies.get(key)
        _osvLoger.debug(f"get cookies '{cookies}'")
        return cookies

    def get_json(self, method: str, web: str, data: dict or str = None, json: dict = None, params: dict = None,
                 timeout: int or float = 5, exc_info: bool = True, stack_info: bool = True,
                 return0: bool = False, to_json: bool = True):
        method = method.strip().upper()
        url = self.URLS.get(web, web)
        if not re.findall(r"https?://.+\..+", url):
            _osvLoger.error(f"'{url}' is not a website.")
            raise ValueError(f"'{url}' is not a website.")

        try:
            req = self.session.request(method, url, params=params, data=data, json=json, timeout=timeout)
        except Exception as e:
            _osvLoger.error(f"meet error when visit website '{url}': {e.__class__.__name__}: {str(e)}",
                            exc_info=exc_info, stack_info=stack_info)
            if return0:
                return 0
            else:
                raise e
        http_code = req.status_code
        _osvLoger.debug(f"{method} website '{url}', respond {http_code} and contents is '{req.content}'.")
        if to_json:
            try:
                req = req.json()
            except Exception as e:
                _osvLoger.error(f"meet error when decode json '{req}': {e.__class__.__name__}: {str(e)}",
                                exc_info=exc_info, stack_info=stack_info)
                if return0:
                    return 0
                else:
                    raise e
        if to_json and req.get("statusCode") is not None or http_code >= 300 or http_code < 200:
            _osvLoger.error("request's http code={}; statusCode:{}; request's message: '{}'".
                            format(http_code, req.get('statusCode'), req.get('message', '')), stack_info=stack_info)
            if return0:
                return 0
            else:
                raise ParamsError(f"bad params with <Respond [{http_code}]>: '{req}'")
        return req

    def login_phone_password(self, phone: str, password: str) -> str:
        """

        :param phone:
        :param password:
        :return: token:登录成功；0:密码错误; -1:人机验证
        """
        if re.fullmatch(r"^1(3\d|4[5-9]|5[0-35-9]|6[567]|7[0-8]|8\d|9[0-35-9])\d{8}$", phone) is None:
            _osvLoger.error(f"login_phone_password: '{phone}' is not a legal phone number.")
            raise ValueError(f"'{phone}' is not a legal phone number.")
        req = self.get_json("POST", "phone_password", json={"phone": phone, "password": password})
        if req.get('status') == 0 or req.get('status') == '0':
            _osvLoger.info(f"login_phone_password: phone '{phone}' successfully login.")
            self.get_json("POST", "hg", json={"content": req.get("data", {}).get("token")})
            return req.get("data", {}).get("token")
        elif req.get('status') == 100:
            _osvLoger.info(f"login_phone_password: phone'{phone}' try to login with wrong password.\
req.message: " + req.get("msg", "") + req.get("message", ""))
            raise PasswordError(str(req))
        elif req.get('status') == 1:
            _osvLoger.info(f"login_phone_password: req.message: " + req.get("msg", "") + req.get("message", ""))
            raise CaptchaError(str(req))
        else:
            _osvLoger.error(f"login_phone_password: cannot analise json '{req}'")
            return req

    def login_cookies(self, cookies: dict = None, channel_id: int = 1) -> str:
        """

        :param cookies: cookies 的 ACCOUNT or ACCOUNT_AK_B 字段
        :param channel_id:
        :return:
        """
        if cookies is not None:
            self.set_cookies(cookies)
        req = self.get_json("GET", "hg" if channel_id == 1 else "ak-b")
        if req.get("code") == 0:
            _osvLoger.info(f"login_cookies: cookies '{cookies}' successfully login.")
            return req.get("data", {}).get("content")
        else:
            _osvLoger.error(f"login_cookies: req.message: " + req.get("msg", "") + req.get("message", ""))
            raise CookiesError(f"login_cookies: req.message: " + req.get("msg", "") + req.get("message", ""))

    def get_cookies_from_token(self, token: str, channel_id: int = 1) -> str:
        key = "ACCOUNT" if channel_id == 1 else "ACCOUNT_AK_B"
        if self.session.cookies.get(key) is not None:
            return self.session.cookies.get(key)
        req = self.get_json("POST", "hg" if channel_id == 1 else "ak-b", data={"content": token})
        if req.get("code") == 0:
            _osvLoger.info(f"get_cookies_from_token: successfully get cookies.")
            return self.session.cookies.get(key)
        else:
            _osvLoger.error(f"get_cookies_from_token: req.message: " + req.get("msg", "") + req.get("message", ""))
            raise CookiesError(f"get_cookies_from_token: req.message: " + req.get("msg", "") + req.get("message", ""))

    def get_basic(self, token: str, channel_id: 2 or 1 = 1):
        """
        {"uid":"15*****44","guest":0,"channelMasterId":1,"nickName":"*****#4459"}
        :param token:
        :param channel_id:
        :return:
        """
        if not isinstance(token, str):  # or len(token) != 24:
            _osvLoger.error(f"get_basic: bad token {token}.")
            raise ValueError(f"bad token {token}.")
        req = self.get_json("post", "post_basic", data={"appId": 1, "channelMasterId": channel_id, "channelToken":
            "{\"token\":\"%s\"}" % token} if channel_id == 1 else {"token": token}, return0=True)
        if req == 0 or req.get("code") != 0:
            return dict()
        result = req.get('data', {})

        if channel_id == 2:
            _osvLoger.info("get_basic: successfully get bilibili basic.")
            return result

        req = self.get_json("GET", "get_basic", params={"token": token}, return0=True)
        if req == 0:
            _osvLoger.info("get_basic: unknown error.")
        else:
            result.update(req.get("data", {}))
            _osvLoger.info("get_basic: successfully get basic.")
        return result

    def get_gacha(self, token: str, channel_id: 1 or 2 = 1):
        """
        generator,失败直接退出
        :param token:
        :param channel_id:
        :return:
        """
        if not isinstance(token, str):  # or len(token) != 24:
            _osvLoger.error(f"get_gacha: bad token '{token}'.")
            raise ValueError(f"bad token '{token}'.")
        req = self.get_json("GET", "gacha", params={"page": 1, "token": token, "channelId": channel_id})
        if req["code"] != 0:
            _osvLoger.error("get_gacha: req.message: " + req.get("msg", "") + req.get("message", ""))
            return 0
        total = req.get("data", {}).get("pagination", {}).get("total", 0)
        _osvLoger.info(f"get gacha page {1}, total {total}.")
        yield req.get("data", {}).get("list", [])
        for page in range(2, (total - 1) // 10 + 1 + 1):
            req = self.get_json("GET", "gacha", params={"page": page, "token": token, "channelId": channel_id},
                                return0=True)
            if req == 0 or req["code"] != 0:
                _osvLoger.error("get_gacha: req = \"" + str(req) + "\"")
                return 0
            _osvLoger.info(f"get gacha page {page}, total {total}.")
            yield req.get("data", {}).get("list", [])


if __name__ == "__main__":
    pass
