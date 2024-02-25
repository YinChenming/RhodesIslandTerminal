import glob
import hashlib
import os
import sqlite3
import atexit
import logging
import datetime
import json

_dbLogger = logging.getLogger("DataBaseLogger")


def _e():
    for item in globals().values():
        if isinstance(item, (sqlite3.Connection, sqlite3.Cursor, SqlConnection)):
            item.close()


atexit.register(_e)


class SqlConnection:
    _instance = None
    ENCODING = "utf-8"

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, database: str, initializations: tuple or list = tuple(), *args, echo: bool = False, **kwargs):
        db_path = os.path.split(database)[0]
        if db_path and not os.path.exists(db_path):
            os.makedirs(db_path, exist_ok=True)
        database = os.path.abspath(database)
        self.database = database
        self.echo = echo
        self.connection = sqlite3.connect(database, *args, **kwargs)
        self.cursor = self.connection.cursor()
        _dbLogger.info(f"connect to database '{database}'.")

        exist = bool(self.cursor.execute("select * from sqlite_master limit 1").fetchall())

        if not exist:
            _dbLogger.info(f"initialize database '{database}'.")

            if isinstance(initializations, str):
                initializations = (initializations,)

            for line in initializations:
                if isinstance(line, (list, tuple)):
                    sql = line[0]
                    sql_val = line[1]
                elif isinstance(line, str):
                    if os.path.isfile(line):
                        sql_val = ()
                        with open(line, "r", encoding=self.ENCODING) as f:
                            sql = f.read()
                    elif os.path.isdir(line):
                        for file in glob.glob(os.path.join(line, "*.sql")):
                            with open(file, "r", encoding=self.ENCODING) as f:
                                try:
                                    self.cursor.execute(f.read())
                                except Exception as e:
                                    _dbLogger.error(
                                        f"meet {e.__class__.__name__} when initialize {self.__class__.__name__}:{e}")
                                    raise e
                                finally:
                                    _dbLogger.debug(f"{self.__class__.__name__} do sql '{sql}'")
                        continue
                    else:
                        sql = line
                        sql_val = ()
                else:
                    continue

                try:
                    self.cursor.execute(sql, sql_val)
                except Exception as e:
                    _dbLogger.error(f"meet {e.__class__.__name__} when initialize {self.__class__.__name__}: \
{e}")
                    raise e
                finally:
                    _dbLogger.debug(f"{self.__class__.__name__} do sql '{sql}'")

    # 别写__del__,会出事(logging无法记录)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def commit(self):
        _dbLogger.debug(f"database '{self.database}' commit.")
        return self.connection.commit()

    def rollback(self):
        _dbLogger.debug(f"database '{self.database}' rollback.")
        return self.connection.rollback()

    def close(self):
        try:
            self.cursor.close()
            self.connection.close()
        except Exception:
            pass
        finally:
            _dbLogger.debug(f"database '{self.database}' is closed.")

    def execute(self, sql, args=tuple(), debug: bool = False):
        sql_tr = sql.replace("?", "{}").format(*map(repr, args))
        try:
            self.cursor.execute(sql, args)
        except Exception as e:
            msg = f"meet {e.__class__.__name__} when do sql '{sql_tr}'."
            if debug:
                _dbLogger.debug(msg)
            else:
                _dbLogger.error(msg)
            raise e
        else:
            _dbLogger.debug(f"{self.__class__.__name__} do sql '{sql_tr}'.")
            if self.echo:
                print(sql_tr)
        return self.cursor


class UserModel(SqlConnection):
    DATABASE = "./data/users.db"
    DB_KEY = "Secret key for users.db"
    DB_INIT = (
        "CREATE TABLE users(\
uid integer NOT NULL UNIQUE,\
phone INTEGER,\
username TEXT,\
channel_id INTEGER DEFAULT 1 CHECK (channel_id in (1,2)),\
cookies TEXT,\
first_time DATETIME DEFAULT (DATETIME('now', 'localtime')),\
latest_time DATETIME DEFAULT (DATETIME('now', 'localtime'))\
)",
    )

    def __init__(self):
        super().__init__(self.DATABASE, self.DB_INIT)

    @staticmethod
    def prepare_sql(uid: int = None, phone: str = None, username: str = None, channel_id: int = None,
                    cookies: str = None, join: str = "AND", username_mode: str = "LIKE"):
        username_mode = username_mode.strip().upper()
        sql = ()
        sql_val = ()
        if channel_id is not None:
            sql += ("channel_id=?",)
            sql_val += (channel_id,)
        if uid is not None:
            sql += ("uid=?",)
            sql_val += (uid,)
        if phone is not None:
            sql += ("phone=?",)
            sql_val += (phone,)
        if username is not None:
            sql += ("username " + username_mode + " ?",)
            if username_mode == "=":
                sql_val += (username,)
            elif username_mode == "LIKE":
                sql_val += (f"%{username}%",)
            elif username_mode == "GLOB":
                sql_val += (f"*{username}*",)
            else:
                _dbLogger.error(f"username_mode must be 'LIKE', 'GLOB' or '=', not '{username_mode}'.")
                raise ValueError(f"username_mode must be 'LIKE', 'GLOB' or '=', not '{username_mode}'.")
        if cookies is not None:
            sql += ("cookies=?",)
            sql_val += (cookies,)
        return (" " + join + " ").join(sql), sql_val

    def get_identities(self, *, uid: int = None, phone: str = None, username: str = None, channel_id: int = None,
                       limit: int = None, offset: int = None) -> iter:
        sql, sql_val = self.prepare_sql(uid=uid, phone=phone, username=username, channel_id=channel_id)
        sql = "SELECT uid, phone, username, channel_id, cookies, first_time, latest_time FROM users" + \
              (" WHERE " if sql else "") + sql + ("" if limit is None else " LIMIT ?") + \
              ("" if offset is None else " OFFSET ?")
        sql_val += (() if limit is None else (limit,)) + (() if offset is None else (offset,))
        _dbLogger.info("get identities.")
        return self.execute(sql, sql_val).fetchall()

    def insert_user(self, uid: int, channel_id: int, /, *, phone: str = None, username: str = None,
                    cookies: str = None) -> bool:
        sql = "INSERT INTO users(uid, channel_id, first_time"
        sql_val = (uid, channel_id)
        if phone is not None:
            sql += ", phone"
            sql_val += (phone,)
        if username is not None:
            sql += ", username"
            sql_val += (username,)
        if cookies is not None:
            sql += ", cookies"
            sql_val += (cookies,)
        sql += ") VALUES (?, ?, DATETIME('now', 'localtime')" + ",?" * (len(sql_val) - 2) + ")"
        try:
            self.execute(sql, sql_val)
        except Exception as e:
            _dbLogger.error(f"meet {e.__class__.__name__} when insert user(uid='{uid}'): {str(e)}")
            return False
        else:
            self.commit()
            _dbLogger.info("insert user.")
            return True

    def delete_user(self, uid: int = None, phone: str = None, username: str = None, channel_id: int = 1) -> bool:
        sql, sql_val = self.prepare_sql(uid=uid, phone=phone, username=username, channel_id=channel_id)
        sql = "DELETE FROM users" + " WHERE " * bool(sql) + sql
        try:
            self.execute(sql, sql_val)
        except Exception as e:
            _dbLogger.error(f"meet {e.__class__.__name__} when delete user(uid='{uid}'): {str(e)}")
            return False
        else:
            self.commit()
            _dbLogger.info("delete user.")
            return True

    def update_user(self, uid: int, channel_id: int = None, /, *, phone: str = None, username: str = None,
                    cookies: str = None, update_time: bool = False) -> bool:
        sql, sql_val = self.prepare_sql(phone=phone, username=username, cookies=cookies, join=", ", username_mode="=")
        if not sql:
            return True
        if update_time:
            sql += ", " * bool(sql) + " latest_time=DATETIME('now', 'localtime') "
        sql = "UPDATE users SET " + sql + " WHERE uid=? AND channel_id=?"
        sql_val += (uid, channel_id)
        try:
            self.execute(sql, sql_val)
        except Exception as e:
            _dbLogger.error(f"meet {e.__class__.__name__} when update user (uid='{uid}'): {str(e)}")
            return False
        else:
            self.commit()
            _dbLogger.info("update user.")
            return True


class GachaModel(SqlConnection):
    DATABASE = "./data/AkGacha.db"
    DB_KEY = "Secret key for AkGacha.db"
    DB_INIT = (
        "CREATE TABLE gacha(uid INTEGER NOT NULL, ts DATETIME NOT NULL, sequence INTEGER DEFAULT 0 CHECK\
(sequence BETWEEN 0 AND 10), pool TEXT DEFAULT '常驻标准寻访', operator TEXT NOT NULL, isNew BOOL DEFAULT\
false, UNIQUE(uid, ts, sequence))",

        "CREATE TABLE operators(name TEXT NOT NULL UNIQUE, rarity INTEGER NOT NULL CHECK\
(rarity BETWEEN 0 AND 5))",

        "CREATE VIEW gacha_view AS select uid, ts, sequence, pool, ROW_NUMBER() OVER(PARTITION BY uid, \
pool ORDER BY ts ASC, sequence ASC) AS row, operators.name, isNew, operators.rarity FROM gacha LEFT JOIN operators \
ON gacha.operator=operators.name",

        "CREATE TRIGGER insert_gacha BEFORE INSERT ON gacha FOR EACH ROW \n\
WHEN EXISTS (SELECT * FROM gacha WHERE uid=new.uid AND ts=new.ts AND sequence>new.sequence) OR NOT EXISTS \
(SELECT * FROM operators WHERE name=new.operator)\n\
BEGIN\nSELECT RAISE(ROLLBACK,'operator not found') WHERE NOT EXISTS (SELECT * FROM operators WHERE \
name=new.operator);\n\
SELECT RAISE(ROLLBACK,'INSERT FORBIDDEN');\n\
END;"
    )

    def __init__(self):
        super().__init__(self.DATABASE, self.DB_INIT)

    def get_rarity(self, uid: int, earliest_time: str or int or float = None):
        """
        返回 dict[星级-1:数量]
        :param uid:
        :param earliest_time:
        :return:
        """
        if isinstance(earliest_time, (int, float)):
            earliest_time = datetime.datetime.fromtimestamp(earliest_time).strftime("%Y-%m-%d %H:%M:%S")
        sql = "SELECT rarity, count(name) FROM gacha_view WHERE uid=? "
        sql_val = (uid,)
        if earliest_time is not None:
            sql += "AND time >= ? "
            sql_val += (earliest_time,)
        sql += "GROUP BY rarity ORDER BY rarity DESC"
        result = {2: 0, 3: 0, 4: 0, 5: 0}
        result.update(dict(self.execute(sql, sql_val).fetchall()))
        _dbLogger.info("get rarity.")
        return result

    def get_total(self, uid: int, earliest_time: str or int or float = None, max_cnt: int = None,
                  ascending: bool = True):
        """
        返回 tuple[tuple[时间, 卡池, 序号, 名字, 星级]]
        :param uid:
        :param earliest_time:
        :param max_cnt:
        :param ascending:
        :return:
        """
        if isinstance(earliest_time, (int, float)):
            earliest_time = datetime.datetime.fromtimestamp(earliest_time).strftime("%Y-%m-%d %H:%M:%S")
        sql = "SELECT ts, pool, row, name, PRINTF('%d星', rarity+1) AS rarity FROM gacha_view WHERE uid=?"
        sql_val = (uid,)
        if earliest_time is not None:
            sql += "AND time >= ? "
            sql_val += (earliest_time,)
        asc_char = "ASC" if ascending else "DESC"
        sql += "ORDER BY ts {0}, sequence {0}".format(asc_char)
        if max_cnt is not None:
            sql += " LIMIT ?"
            sql_val += (max_cnt,)
        _dbLogger.info("get total.")
        return tuple(self.execute(sql, sql_val).fetchall())

    def get_duration(self, uid: int):
        """
        返回[最大值,最小值]
        :param uid:
        :return:
        """
        sql = "SELECT MIN(ts), MAX(ts) FROM gacha WHERE uid=?"
        sql_val = (uid,)
        _dbLogger.info("get duration.")
        return self.execute(sql, sql_val).fetchone()

    def get_pools(self, uid: int, earliest_time: str or int or float = None):
        """
        返回tuple[tuple[卡池,抽数]]
        :param uid:
        :param earliest_time:
        :return:
        """
        if isinstance(earliest_time, (int, float)):
            earliest_time = datetime.datetime.fromtimestamp(earliest_time).strftime("%Y-%m-%d %H:%M:%S")
        sql = "SELECT pool, COUNT(name) cnt_op, AVG(rarity) mean_rar  FROM gacha_view WHERE uid=? "
        sql_val = (uid,)
        if earliest_time is not None:
            sql += "AND time >= ? "
            sql_val += (earliest_time,)
        sql += "GROUP BY pool ORDER BY MIN(ts) ASC"
        _dbLogger.info("get counts.")
        return tuple(self.execute(sql, sql_val).fetchall())

    def get_remains(self, uid: int):
        """
        返回tuple[tuple[卡池, 距离上个6星抽数]]
        :param uid:
        :return:
        """
        sql = ("SELECT pool, cnt FROM (SELECT pool, cnt-1 as cnt, ROW_NUMBER() OVER(PARTITION BY pool ORDER BY cnt ASC)"
               " AS row FROM (SELECT pool, rarity, ROW_NUMBER() OVER(PARTITION BY pool ORDER BY ts DESC, sequence DESC)"
               " AS cnt FROM gacha_view WHERE uid=?) WHERE rarity=5) WHERE row=1 UNION ALL SELECT pool, COUNT(operator)"
               " AS cnt FROM gacha WHERE uid=? GROUP BY pool HAVING pool NOT IN (SELECT pool FROM gacha_view WHERE"
               " uid=? GROUP BY pool HAVING MAX(rarity)=5)")
        sql_val = (uid,) * 3
        results = self.execute(sql, sql_val).fetchall()
        _dbLogger.info("get remains.")
        return tuple(results)

    def get_operators(self, uid: int, rarity: int = 5, earliest_time: str or int or float = None):
        """
        返回 dict[卡池:list[tuple[干员, 抽数]]]
        :param uid:
        :param rarity:
        :param earliest_time:
        :return:
        """
        if isinstance(earliest_time, (int, float)):
            earliest_time = datetime.datetime.fromtimestamp(earliest_time).strftime("%Y-%m-%d %H:%M:%S")

        sql = "SELECT pool FROM gacha_view WHERE uid=? GROUP BY pool HAVING MAX(rarity)>=?"
        sql_val = (uid, rarity)
        if earliest_time is not None:
            sql += "AND time >= ? "
            sql_val += (earliest_time,)
        pools = self.execute(sql, sql_val).fetchall()
        results = {}

        for pool in pools:
            pool = pool[0]
            sql = ("SELECT name, row-LEAD(row, -1, 0) OVER(ORDER BY row) cnt FROM gacha_view WHERE uid=? AND pool=? "
                   "AND rarity>=? ORDER BY row ASC")
            sql_val = (uid, pool, rarity)
            results[pool] = self.execute(sql, sql_val).fetchall()
        _dbLogger.info("get operators.")
        return results

    def loads(self, uid: int, js: str or dict or list):
        """
        返回tuple[总条数, 错误条数, 总干员, 错误干员]
        :param uid:
        :param js:
        :return:
        """
        tp = str(type(js))
        if isinstance(js, str):
            try:
                js = json.loads(js)
            except Exception:
                _dbLogger.error(f"'{js}' is not a legal json string.")
                raise ValueError(f"'{js}' is not a legal json string.")
        if isinstance(js, dict):
            js = js.get("data", {}).get("list", [])

        _dbLogger.info(f"load json from '{tp}': {js}")
        cnt_ga = err_ga = 0

        for line in js:
            ts = datetime.datetime.fromtimestamp(line["ts"]).strftime("%Y-%m-%d %H:%M:%S")
            pool = line["pool"]
            start = (len(line['chars']) - 1) // 9  # 0/1
            for j, char in enumerate(line['chars']):
                name = char['name']
                rarity = char['rarity']
                isNew = char['isNew']
                try:
                    self.execute("insert into operators(name, rarity) values (?,?)", (name, rarity), debug=True)
                except Exception as e:
                    _dbLogger.debug(f"meet {e.__class__} when load json[name={name}, rarity={rarity}]: {str(e)} ")

                try:
                    self.execute("insert into gacha(uid,ts,pool,sequence, operator, isNew) values (?,?,?,?,?,?)",
                                 (uid, ts, pool, start + j, name, isNew), debug=True)
                except Exception as e:
                    _dbLogger.debug(f"meet {e.__class__} when load json[name={name}, rarity={rarity}]: {str(e)} ")
                    err_ga += 1
                finally:
                    cnt_ga += 1
        _dbLogger.info(f"insert {cnt_ga} gacha line({err_ga} fail).")
        self.commit()
        return cnt_ga, err_ga

    def load(self, uid: int, fp):
        return self.loads(uid=uid, js=json.load(fp))

    def dump(self, uid: int, file: str, *, file_type: str = None, separators: tuple = None, indent: int = 4):
        """
        CSV使用utf-8-sig编码,JSON使用utf-8编码
        :param uid:
        :param file:
        :param file_type:
        :param separators:
        :param indent:
        :return:
        """
        file = os.path.abspath(file)
        file_path = os.path.split(file)[0]
        if file_path and not os.path.exists(file_path):
            os.makedirs(file_path)
        if file_type is None:
            file_type = os.path.splitext(file)[-1][1:]
        file_type = file_type.lower()

        if file_type == "csv":
            with open(file, "w", encoding="utf-8-sig") as f:
                f.write("时间,卡池,序号,干员,稀有度\n")
                for line in self.get_total(uid):
                    f.write(",".join(list(map(str, line))) + "\n")
        elif file_type == "json":
            data = self.execute("SELECT ts, sequence, pool, name, rarity, isNew FROM gacha_view WHERE uid=? \
ORDER BY ts ASC, sequence ASC", (uid,)).fetchall()
            total = len(data)
            new_data = []
            for line in data:
                ts = int(datetime.datetime.strptime(line[0], "%Y-%m-%d %H:%M:%S").timestamp())
                seq = line[1]
                pool = line[2]
                if seq == 0 or seq == 1:
                    # noinspection PyTypeChecker
                    new_data.append({"ts": ts, "pool": pool, "chars":
                        [{"name": line[3], "rarity": line[4], "isNew": line[5]}] +
                        [None] * (9 if seq == 1 else 0)})
                else:
                    new_data[-1]["chars"][seq - 1] = {"name": line[3], "rarity": line[4], "isNew": line[5]}
            new_data.reverse()

            js = {"code": 0, "data": {"list": new_data, "pagination": {"current": 1, "total": total}},
                  "msg": "This file is made by Rhodes Island Terminal, for reference only."}
            with open(file, "w", encoding="utf-8") as f:
                json.dump(js, f, separators=separators, indent=indent)
        else:
            _dbLogger.error(f"file type must be 'csv' or 'json', not '{file_type}'")
            raise ValueError(f"file type must be 'csv' or 'json', not '{file_type}'")
        with open(file, "rb") as f:
            md5 = hashlib.md5(f.read()).hexdigest()
            f.seek(0, 0)
            sha256 = hashlib.sha256(f.read()).hexdigest()
        _dbLogger.info(f"dump file '{file}' as {file_type} (md5:{md5}, sha256:{sha256}).")
        return md5, sha256


if __name__ == "__main__":
    pass
