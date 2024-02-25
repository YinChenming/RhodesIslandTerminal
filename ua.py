import json

from database import UserModel, GachaModel
from online_service import *
import logging

_uaLogger = logging.getLogger("UALogger")

class LoginError(Exception):
    pass


class UserAgent:
    userDb = UserModel()
    gachaDb = GachaModel()
    osv = OnlineService()
    __pool = {}

    def __new__(cls, *args, **kwargs):
        uid = kwargs.get("uid")
        if uid is None:
            uid = args[0]
        inst = cls.__pool.get(uid)
        if inst is None:
            raise ValueError("user has not logged in.")
        elif inst is Ellipsis:
            inst = super().__new__(cls)
            cls.__pool[uid] = inst
        return inst

    def __init__(self, uid: int, phone: int or str = None, username: str = None, token: str = None,
                 channel_id: int = 1, mode: str = "unknown"):
        _uaLogger.info(f"initialize user(uid={uid}, name='{username}').")
        self.uid = uid
        self.username = username
        self.phone = phone
        self.token = token
        self.channel_id = channel_id
        self.mode = mode

    def __str__(self):
        res = "user(uid="+str(self.uid)
        if self.username is not None:
            res += ", username="+str(self.username)
        if self.phone is not None:
            res += ", phone="+str(self.phone)
        res += ", 服务器:"+("官服" if self.channel_id == 1 else "B服")
        res += ", 有网络连接" if self.has_connection() else ", 无网络连接"
        res += ", 登录方式:"+self.mode+")"
        return res

    def logout(self):
        del self.__pool[self.uid]

    def user_execute(self, sql: str, sql_val: tuple = ()):
        return self.userDb.execute(sql, sql_val)

    # --------- methods from gachaDb ---------
    def gacha_execute(self, sql: str, sql_val: tuple = ()):
        return self.gachaDb.execute(sql, sql_val)

    def get_total(self, earliest_time: str or int or float = None, max_cnt: int = None):
        return self.gachaDb.get_total(self.uid, earliest_time, max_cnt)

    def get_duration(self):
        return self.gachaDb.get_duration(self.uid)

    def get_rarity(self,  earliest_time: str or int or float = None):
        return self.gachaDb.get_rarity(self.uid, earliest_time)

    def get_remains(self):
        return self.gachaDb.get_remains(self.uid)

    def get_counts(self,  earliest_time: str or int or float = None):
        return self.gachaDb.get_pools(self.uid, earliest_time)

    def get_operators(self, rarity: int = 5, earliest_time: str or int or float = None):
        return self.gachaDb.get_operators(self.uid, rarity, earliest_time)

    def has_connection(self):
        return self.token is not None

    def update(self):
        results = [0][:] * 4
        for line in self.osv.get_gacha(token=self.token, channel_id=self.channel_id):
            r = self.gachaDb.loads(uid=self.uid, js=line)
            results = [a + b for a, b in zip(results, r)]
        return results

    def dump(self, file, file_type=None):
        return self.gachaDb.dump(self.uid, file, file_type=file_type)

    @staticmethod
    def is_password(password: str):
        len_c = 8 <= len(password) <= 16
        char_c = False
        num_c = False
        other_c = False
        for i in password:
            if i.isalpha():
                char_c = True
            elif i.isnumeric():
                num_c = True
            else:
                other_c = True
        return len_c and char_c and num_c and other_c

    @classmethod
    def login_phone_password(cls, phone: int or str, password: str):
        if not re.findall(r"^\d{11}$", str(phone)):
            _uaLogger.error(f"'{phone}' is not a legal phone number.")
            raise ValueError(f"'{phone}' is not a legal phone number.")
        elif not cls.is_password(password):
            _uaLogger.error(f"'{password}' is not a legal password.")
            raise ValueError(f"'{password}' is not a legal password.")
        try:
            token = cls.osv.login_phone_password(str(phone), password)
        except PasswordError as e:
            _uaLogger.error(f"meet {e.__class__.__name__} when login by phone and password: {str(e)}")
            raise ValueError(f"wrong password '{password}'.")
        except CaptchaError as e:
            _uaLogger.error(f"need captcha check.")
            raise e
        except Exception as e:
            _uaLogger.error(f"meet {e.__class__.__name__} when login by phone and password: {str(e)}")
            raise e
        _uaLogger.info(f"user(phone='{phone}') successfully log in")
        req = cls.osv.get_basic(token)

        if not req:
            _uaLogger.error(f"login error because of bad token")
            raise LoginError

        uid = int(req["uid"])
        channel_id = req["channelMasterId"]
        username = req["nickName"]
        cls.__pool[uid] = Ellipsis

        not_exists = cls.userDb.insert_user(uid, channel_id, phone=phone, username=username)
        if not not_exists:
            cls.userDb.update_user(uid, channel_id, phone=phone, username=username, update_time=True)

        cookies = cls.osv.get_cookies_from_token(token, channel_id=1)
        if cookies is not None:
            _uaLogger.info("update cookies.")
            cls.userDb.update_user(uid, channel_id, cookies=cookies, update_time=False)
        return cls(uid=uid, phone=phone, username=username, token=token, channel_id=channel_id, mode="phone_password")

    @classmethod
    def login_local(cls, uid: int = None, phone: int or str = None, username: str = None, channel_id: int = 1):
        """
        B服存在如下现象:可以使用过期 cookies 获得 token,也可以通过获得的 token 正常拿到 cookies,但在获取用户信息和抽卡信息时返回'token错误
        或已失效',因此只能通过是否能正常更新用户信息判断是否登录成功.
        :param uid:
        :param phone:
        :param username:
        :param channel_id:
        :return:
        """
        results = cls.userDb.get_identities(uid=uid, phone=phone, username=username, channel_id=channel_id)
        if not results:
            _uaLogger.error("no user with " + (f" phone: {phone} " * bool(phone)) +
                            (f"username: '{username}'"*bool(username)) + ".")
            return "no user."
        i = yield results
        if i is None:
            return "bad index."
        results = results[i]
        uid = results[0]
        phone = results[1]
        username = results[2]
        cookies = results[4]
        cls.__pool[uid] = Ellipsis

        try:
            token = cls.osv.login_cookies({"ACCOUNT" if channel_id == 1 else "ACCOUNT_AK_B": cookies},
                                          channel_id=channel_id)
        except Exception:
            _uaLogger.debug("no Internet connection.")
            token = None
        if token is not None:
            req = cls.osv.get_basic(token, channel_id)
        else:
            req = {}
        if req:
            uid = int(req["uid"])
            channel_id = req["channelMasterId"]
            username = req["nickName"]
            cls.userDb.update_user(uid, channel_id, username=username, update_time=True)
        else:
            token = None  # 保证网络连接正常显示
            cls.userDb.update_user(uid, channel_id, cookies=None, update_time=False)
        yield cls(uid=uid, phone=phone, username=username, channel_id=channel_id, token=token, mode="local")
        return

    @classmethod
    def login_token(cls, token: str or dict):
        if isinstance(token, str):
            try:
                token = json.loads(token)
                token = token.get("data", {}).get("content", "")
            except Exception:
                pass
        elif isinstance(token, dict):
            token = token.get("data", {}).get("token", "")
        else:
            _uaLogger.error(f"token must be str or dict, not {type(token)}")
            raise TypeError(f"token must be str or dict, not {type(token)}")
        if token:
            channel_id = 1 if len(token) == 24 else 2
        else:
            _uaLogger.error("bad token")
            raise ValueError("bad token")
        try:
            cookies = cls.osv.get_cookies_from_token(token=token, channel_id=channel_id)
        except Exception as e:
            _uaLogger.error(f"bad token '{token}'(channelId={channel_id}), meet error ({e.__class__.__name__}): {e}")
            raise ValueError(f"bad token '{token}'")
        _uaLogger.info(f"get cookies by token '{token}'.")
        result = cls.osv.get_basic(token=token, channel_id=channel_id)
        if not result:
            _uaLogger.error(f"bad token '{token}'(channelId={channel_id})")
            raise ValueError(f"bad token '{token}'")
        uid = int(result.get("uid"))
        username = result.get("nickName")
        cls.__pool[uid] = Ellipsis
        not_exists = cls.userDb.insert_user(uid, channel_id, username=username, cookies=cookies)
        if not not_exists:
            cls.userDb.update_user(uid, channel_id, username=username, cookies=cookies, update_time=True)
        return cls(uid=uid, username=username, token=token, channel_id=channel_id, mode="token")


if __name__ == "__main__":
    pass
