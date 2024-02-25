import ua
from ua import UserAgent
import logging
import time
import os

_terminalLogger = logging.getLogger("TerminalLogger")
LOG_FILE = "./log/" + time.strftime("%Y%m%d") + ".log"
SUPERUSER_PSW = os.environ.get("RIT_SUPERUSER_PSW")
if not os.path.exists(".\\log"):
    os.makedirs(".\\log")


class Terminal:
    __instance = None
    __local_namespace = {}
    __global_namespace = {}
    __version__ = (0, 1, 5)
    _VERSION_NAME = "beta"
    TRANSFER = "?"

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            for name, func in cls.__dict__.items():
                if name[:3] == "do_":
                    name = name[3:].replace("__", cls.TRANSFER).split("_")
                    father = cls.__local_namespace
                elif name[:4] == "gdo_":
                    name = name[4:].replace("__", cls.TRANSFER).split("_")
                    father = cls.__global_namespace
                else:
                    continue
                for route in name:
                    father[route] = father.get(route, {})
                    father = father[route]
                father[...] = func

        return cls.__instance

    def __init__(self):
        _terminalLogger.debug("initialize terminal.")
        self.loc = []
        self.user: UserAgent = None
        self.trials = 10
        self.is_superuser: bool = False

    @staticmethod
    def print_table(data, headers: tuple or list = None, width: tuple or list = None, index: bool = True,
                    end: str = None, border: bool = True):
        data = data.__iter__()
        try:
            first_line = next(data)
        except StopIteration:
            _terminalLogger.info("print blank table.")
            return
        i = -1
        if headers is None:
            headers = ("Untitled",) * len(first_line)
        else:
            headers = tuple(headers)
        if width is None:
            width = ["{:^%ds}" % max(len(i), len(str(j))) for i, j in zip(headers, first_line)]
        else:
            width = ["{:^%ds}" % i for i in width]

        if not (len(first_line) == len(headers) == len(width)):
            raise ValueError("length of headers and width does not match data.")

        if index:
            headers = ("index",) + headers
            width = ["{:^5s}"] + width
            first_line = [0] + [item for item in first_line]

        if border:
            print("-" * int((sum(int(w[3:-2]) for w in width) + 2) * 1.3))

        print("|" if border else "", end="")
        print(*(w.format(str(d)) for w, d in zip(width, headers)), sep="|", end="")
        print("|" if border else "", end="\n")

        if border:
            print("-" * int((sum(int(w[3:-2]) for w in width) + 2) * 1.3))

        print("|" if border else "", end="")
        print(*(w.format(str(d)) for w, d in zip(width, first_line)), sep="|", end="")
        print("|" if border else "", end="\n")
        for i, line in enumerate(data):
            if index:
                line = [i + 1] + [item for item in line]
            print("|" if border else "", end="")
            print(*(w.format(str(d)) for w, d in zip(width, line)), sep="|", end="")
            print("|" if border else "", end="\n")

        if border:
            print("-" * int((sum(int(w[3:-2]) for w in width) + 2) * 1.3))

        if end is None:
            print(f"total: {i + 2}")
        else:
            print(end)

        if border:
            print("-" * int((sum(int(w[3:-2]) for w in width) + 2) * 1.3))

    def gdo_debug(self, debug: str, *args):
        if debug.lower() == "on":
            level = logging.DEBUG
            print("debug on")
            _terminalLogger.info("debug on")
        elif debug.lower() == "off":
            level = logging.INFO
            print("debug off")
            _terminalLogger.info("debug off")
        else:
            print(f"no level '{debug}'")
            return
        _terminalLogger.setLevel(level)
        return

    def help(self, *args):
        print("""
        ----------全局指令----------

        exit    退出终端(博士……我也没有整天叫你工作啦……适当的休息还是必要的)
        version 版本号(由可露希尔小姐倾情提供)
        about   关于Rhodes Island Terminal(看来博士的失忆确实很严重了呢……)
        eval    奇怪的指令,要不要试着输点东西呢(博士……我在看着你……)
        """)
        print("""
        -----------局部指令----------""")
        return

    def DO(self, command: str):
        loc = self.loc if self.loc else ("index",)
        _terminalLogger.debug(f"/{'/'.join(loc)}:do command '{command}'.")
        command = command.strip().replace("_", self.TRANSFER)
        route = command.split()
        if not command:
            return  # True #忽略空输入,去除注释后空输入返回上一界面
        elif route[0].lower() == "help":
            if isinstance(self.trials, float) and time.time() - self.trials >= 10 * 60:
                self.trials = 10
            elif isinstance(self.trials, float):
                print("""
                少女在你的怀里睡着了,还是不要打扰她了……
                """)
                return
            elif self.trials == 0:
                print("""
                阿米娅太累了,睡着了zzz
                """)
                self.trials = time.time()
                return
            print("""
            博士……我们现在在……噢,在"{}"目录.有什么阿米娅能做的吗?欸,指令表!好的博士,指令表就在下面了,希望阿米娅有帮上忙……
            """.format('/'.join(self.loc if self.loc else ("index",)) + '/'))
            self.help()
            self.trials = self.trials - 1 if self.trials > 0 else time.time()

        func = self.__local_namespace
        try:
            for rt in loc:
                func = func[rt]
        except KeyError:
            func = {}
        run = None
        for i in range(len(route)):
            func = func.get(route[i])
            if func is None:
                break
            elif callable(func.get(Ellipsis)):
                run = (func[Ellipsis], [self] + [s.replace(self.TRANSFER, "_") for s in route[i+1:]])
        if run is not None:
            return run[0](*run[1])

        func = self.__global_namespace
        run = None
        for i in range(len(route)):
            func = func.get(route[i])
            if func is None:
                break
            elif callable(func.get(Ellipsis)):
                run = (func[Ellipsis], [self] + [s.replace(self.TRANSFER, "_") for s in route[i+1:]])
        if run is not None:
            return run[0](*run[1])
        else:
            print(f"no command '{command.replace(self.TRANSFER, '_')}'")
            return

    def menu_index(self):
        _terminalLogger.info("menu index")
        print("""
        欢迎使用Rhodes Island Terminal
        输入 'help' 获得更多帮助
        """)
        if self.user is not None:
            ...
        e = None
        while e is None:
            e = self.DO(input(">>>"))
        print("正在退出...")
        time.sleep(0.5)
        raise SystemExit(0)

    def do_index_login(self, *args):
        print(*args)    # test

    def do_index_help(self, *args):
        print("""
        login   phone_password [phone password] 手机号+密码登录
                local                           本地登录,会根据保存的cookies自动尝试联网
                token [token]                   token登录,B服唯一的登录方法
                token获取方法:登录后根据所在服务器选择对应链接访问,将页面内所有内容粘贴至输入即可
                官服:https://web-api.hypergryph.com/account/info/hg
                B服:https://web-api.hypergryph.com/account/info/ak-b
        """)
        return

    def gdo_version(self, *args):
        print("welcome to use RIT(Rhodes Island Terminal) v{}({}版)".format(
            ".".join(map(str, self.__version__)), self._VERSION_NAME))
        return

    def gdo_about(self, *args):
        # noinspection SpellCheckingInspection
        print("""
        欢迎使用RIT,这是由同人开发的软件,与官方无关
        开发人员:
        程序: 在班外的学生
        特别致谢: hitake
        建议或bug反馈: zbwdxs@qq.com
        """)
        return

    def gdo_exit(self, *args):
        print("退出程序...")
        _terminalLogger.info("exit terminal")
        time.sleep(0.5)
        raise SystemExit

    def gdo_eval(self, *args):
        if not self.is_superuser:
            if input("请输入管理员密码:") != SUPERUSER_PSW:
                print("密码错误.")
                return
            else:
                self.is_superuser = True
        try:
            print(eval(input("输入神秘指令:")))
        except Exception as e:
            print(f"{e.__class__.__name__}: '{e}'")

    def do_index_login_phone__password(self, *args):
        self.loc.append("login_PhonePassword")
        flag = len(args) == 2
        while True:
            phone = input("手机号: ") if not flag else args[0]
            password = input("密码: ") if not flag else args[1]
            if not (phone and password):
                print("退出登录...")
                self.loc.pop()
                return
            else:
                print("Connect to RHODES ISLAND...")
                try:
                    user = UserAgent.login_phone_password(phone, password)
                except ua.PasswordError:
                    print("用户名或密码错误.")
                except ua.CaptchaError:
                    print("需要人机验证,请使用token登录.")
                except ua.LoginError:
                    print("登录失败.")
                except Exception as e:
                    print(e.__class__.__name__, str(e))
                else:
                    self.loc.pop()
                    self.user = user
                    break
            if flag:
                return
        return self.menu_user()

    def do_index_login_local(self, *args):
        self.loc.append("login_local")
        print("正在使用本地登录,请注意,您只能查看本地数据,无法更新.")
        while True:
            uid = input("uid: ")
            uid = int(uid) if uid.isnumeric() else None
            phone = input("手机号: ")
            phone = phone if phone else None
            username = input("用户名: ")
            username = username if username else None
            channel_id = input("服务器(官服填1, B服填2): ")
            channel_id = int(channel_id) if channel_id.isnumeric() else None
            generator = UserAgent.login_local(uid=uid, phone=phone, username=username, channel_id=channel_id)
            try:
                users = next(generator)
            except Exception as e:
                print("无用户登录记录,请先登录.")
                _terminalLogger.debug(f"login local: no user ({e.__class__.__name__}: '{e}')")
                self.loc.pop()
                return
            self.print_table([(i + 1,) + user[0:3] for i, user in enumerate(users)],
                             ["index", "uid", "手机号", "用户名"],
                             index=False)
            i = input("请输入用户前的序号,留空退出登录:")
            while True:
                if not i:
                    generator.close()
                    self.loc.pop()
                    return
                elif i.isdigit() and 0 < int(i) <= len(users):
                    user = generator.send(int(i) - 1)
                    self.user = user
                    print("登录成功,正在跳转...")
                    self.loc.pop()
                    return self.menu_user()
                else:
                    i = input("非法输入,请重输:")

    def do_index_login_token(self, *args):
        token = input("请输入token字符串:") if not args else args[0]
        if not token:
            return
        try:
            user = UserAgent.login_token(token)
        except Exception as e:
            print(f"{e.__class__.__name__}: {e}")
            return
        self.user = user
        return self.menu_user()

    def menu_user(self):
        _terminalLogger.info(f"menu {self.user}")
        self.loc.append("user")
        print(f"""
        Dr.{self.user.username.rsplit("#", 1)[0]} 欢迎回来!
        """)
        if self.user.has_connection():
            r = self.user.update()
            print("更新抽卡数据成功!共更新{}条数据,其中{}条已录入.".format(*r))
        e = None
        while e is None:
            stdin = input(">>>")
            e = self.DO(stdin)
        self.loc.pop()
        print("正在登出...")
        self.user.logout()
        self.user = None
        _terminalLogger.info("menu user: logout")
        time.sleep(0.5)

    def do_user_help(self, *args):
        print("""
        basic   博士的个人信息(欸,博士还需要看自己的资料吗?)
        update  更新数据(可露希尔提醒,罗德岛维护期间请勿操作终端,否则后果自负)
        logout  登出(博士的……新人格?)
        summary 寻访简报(真的只是简报啦……)
        view    total [max] 详细数据
                rarity      各稀有度干员统计
        """)

    def do_user_basic(self, *args):
        print(self.user)
        return

    def do_user_view_total(self, max_cnt: int = None, *args):
        self.print_table(self.user.get_total(max_cnt=max_cnt), headers=["时间", "卡池", "序号", "干员", "稀有度"],
                         width=[19, 10, 5, 8, 2], index=True)

    def do_user_view_rarity(self, *args):
        val = {(str(i + 1) + "星"): j for i, j in self.user.get_rarity().items()}.items()
        self.print_table(val, headers=["稀有度", "总数"], width=None, index=False)

    def do_user_logout(self, *args):
        return True

    def do_user_update(self, *args):
        if self.user.has_connection():
            r = self.user.update()
            print("更新抽卡数据成功!共更新{}条数据,其中{}条已录入.".format(*r))
        else:
            print("无网络,无法更新数据.")

    def do_user_summary(self, *args):
        duration = self.user.get_duration()
        counts = self.user.get_counts()
        rarity = self.user.get_rarity()
        cnt_sum = sum(c[1] for c in counts)

        if cnt_sum == 0:
            print("没有抽卡记录看个毛线,快去玩明日方舟!!!")
            return

        """
        最近来了很多新人.然而新面孔越多,您越要多加注意.--星熊
        这么久没见,德克萨斯身边多了不少人嘛!哈哈,这很好!也让我和他们认识一下!--拉普兰德
        你的账号是现代的,抽卡却相当古老.你究竟是什么人?--塞雷娅
        """
        print(f"DR.{self.user.username}, 你在{duration[0]}至{duration[1]}的时间内,共抽卡{cnt_sum}次,")
        print("其中6星干员{}位({:.2f}%),5星干员{}位({:.2f}%),4星干员{}位({:.2f}%).".format(
            rarity[5], rarity[5] / cnt_sum * 100, rarity[4], rarity[4] / cnt_sum * 100, rarity[3],
            rarity[3] / cnt_sum * 100))

    def do_user_dump(self, *args):
        if len(args) == 1:
            file = args[0]
        else:
            file = input("请输入导出文件路径:")
        try:
            md5, sha256 = self.user.dump(file)
        except Exception:
            print("路径非法!")
            return
        print(f"""
        导出成功!文件校验码如下:
        md5:{md5}
        sha256:{sha256}
        """)
        return


if __name__ == "__main__":
    pass
