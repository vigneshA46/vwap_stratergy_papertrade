import time as t
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
from io import StringIO
import os
from dotenv import load_dotenv
from dhanhq import marketfeed


# =========================
# CONFIG
# =========================

load_dotenv()

CLIENT_ID=os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

IST = pytz.timezone("Asia/Kolkata")
TRADE_DATE=datetime.now(IST).strftime("%Y-%m-%d")


INSTRUMENT_URL = "https://api.dhan.co/v2/instrument/NSE_FNO"
HIST_URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

IST = pytz.timezone("Asia/Kolkata")

TARGET_SYMBOL = "NIFTY"
INTERVAL = "1"

START_TIME = time(9, 16)
FORCE_EXIT_TIME = time(15, 20)

LOT_SIZE = 65
LOTS = 1

MTM_SL = -3000
MTM_TARGET = 3000


# =========================
# UTIL
# =========================

def log(msg):
    print(msg)




def calculate_pnl(pos, entry, exit):
    if pos == "CE":
        return (exit - entry) * LOT_SIZE * LOTS
    else:
        return (exit - entry) * LOT_SIZE * LOTS


# =========================
# DHAN HELPERS
# =========================

def load_fno_master():
    r = requests.get(INSTRUMENT_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text), header=None, low_memory=False)

    df.columns = [
        "EXCH_ID","SEGMENT","SECURITY_ID","ISIN","INSTRUMENT",
        "UNDERLYING_SECURITY_ID","UNDERLYING_SYMBOL","SYMBOL_NAME",
        "DISPLAY_NAME","INSTRUMENT_TYPE","SERIES","LOT_SIZE",
        "SM_EXPIRY_DATE","STRIKE_PRICE","OPTION_TYPE","TICK_SIZE",
        "EXPIRY_FLAG","BRACKET_FLAG","COVER_FLAG","ASM_GSM_FLAG",
        "ASM_GSM_CATEGORY","BUY_SELL_INDICATOR",
        "BUY_CO_MIN_MARGIN_PER","BUY_CO_SL_RANGE_MAX_PERC",
        "BUY_CO_SL_RANGE_MIN_PERC","BUY_BO_MIN_MARGIN_PER",
        "BUY_BO_PROFIT_RANGE_MAX_PERC","BUY_BO_PROFIT_RANGE_MIN_PERC",
        "MTF_LEVERAGE","RESERVED"
    ]
    return df


def get_nearest_nifty_fut(df, trade_date):
    futs = df[
        (df["INSTRUMENT"] == "FUTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL)
    ].copy()

    futs["SM_EXPIRY_DATE"] = pd.to_datetime(futs["SM_EXPIRY_DATE"])
    futs = futs[futs["SM_EXPIRY_DATE"] >= trade_date]

    return futs.sort_values("SM_EXPIRY_DATE").iloc[0]


def calculate_strikes(fut_price, step=50):
    atm = round(fut_price / step) * step
    ce_strike = atm - 2 * step
    pe_strike = atm + 2 * step
    return atm, ce_strike, pe_strike


def find_option(df, strike, opt_type, trade_date):
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    opt = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL) &
        (df["STRIKE_PRICE"] == float(strike)) &
        (df["OPTION_TYPE"] == opt_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ].copy()

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]


def fetch_one_candle(security_id, exchange, instrument, from_dt, to_dt,vwap_state):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange,
        "instrument": instrument,
        "interval": INTERVAL,
        "oi": True,
        "fromDate": f"{from_dt}",
        "toDate": f"{to_dt}"
    }

    r = requests.post(HIST_URL, headers=HEADERS, json=payload)
    data = r.json()

    if not data.get("timestamp"):
        return None

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"]
    })

    dt_index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt_index.dt.tz_convert("Asia/Kolkata")

    row = df.iloc[-1].copy()

    if vwap_state is not None:
        pv = row["close"] * row["volume"]
        vwap_state["cum_pv"] += pv
        vwap_state["cum_vol"] += row["volume"]

        if vwap_state["cum_vol"] > 0:
            row["vwap"] = vwap_state["cum_pv"] / vwap_state["cum_vol"]
        else:
            row["vwap"] = None
    else:
        row["vwap"] = None

    return row

def wait_for_start():
    print("⏳ Waiting for 09:16:00 ...")
    while True:
        now = datetime.now(IST).time()
        if now >= time(9, 16):
            print("✅ Market Start Triggered")
            break
        time.sleep(1)


def fetch_fut_candle(security_id):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTIDX",
        "interval": "1",
        "oi": True,
        "fromDate": f"2026-02-02 09:14:00",
        "toDate": f"2026-02-02 09:16:00"
    }

    r = requests.post(HIST_URL, headers=HEADERS, json=payload)
    data = r.json()
    
    if not data.get("timestamp"):
        return None

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"]
    })

    dt_index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt_index.dt.tz_convert("Asia/Kolkata")

    row = df.iloc[-1].copy()
    return row


    return {
        "security_id": tick["security_id"],
        "price": float(tick["LTP"]),
        "avg": float(tick["avg_price"]),
        "time": tick["LTT"],
        "volume": tick["volume"]
    }

def normalize_tick(tick):
    if tick.get("type") != "Quote Data":
        return None

    return {
        "security_id": tick["security_id"],
        "price": float(tick["LTP"]),
        "avg": float(tick["avg_price"]),
        "time": tick["LTT"],
        "volume": tick["volume"]
    }


# =========================
# CANDLE BUILDER
# =========================

class CandleBuilder:
    def __init__(self):
        self.current_minute = None
        self.ohlc = None

    def update(self, price, tick_time):
        now = datetime.now(IST)
        t = datetime.strptime(tick_time, "%H:%M:%S").replace(
        year=now.year, month=now.month, day=now.day
        )
        minute = t.replace(second=0)

        closed = None

        if self.current_minute != minute:
            closed = self.ohlc
            self.current_minute = minute
            self.ohlc = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": price
            }
        else:
            self.ohlc["high"] = max(self.ohlc["high"], price)
            self.ohlc["low"] = min(self.ohlc["low"], price)
            self.ohlc["close"] = price

        return closed


# =========================
# ENGINE
# =========================

class VWAPVirtualEngine:

    def __init__(self, trade_date):
        self.ce_vwap_state = {"cum_pv": 0, "cum_vol": 0}
        self.pe_vwap_state = {"cum_pv": 0, "cum_vol": 0}
        self.trade_date = pd.to_datetime(trade_date)

        self.ce_pos = None
        self.pe_pos = None

        self.prev_ce = None
        self.prev_pe = None

        self.day_mtm = 0
        self.locked = False


    def execute_orders(self, ce, pe):
        pass


    def risk_check(self):
        if self.day_mtm <= MTM_SL or self.day_mtm >= MTM_TARGET:
            self.locked = True
            log("🔒 MTM LOCKED")

            


    def process_signals(self, ce, pe):

        if self.prev_ce is not None and not self.ce_pos and not self.locked:
            if self.prev_ce["close"] <= self.prev_ce["vwap"] and ce["close"] > ce["vwap"]:
                self.ce_pos = {"price": ce["open"]}
                log(f"{ce['datetime']} ENTRY CE @ {ce['open']}")

        if self.ce_pos and self.prev_ce["close"] >= self.prev_ce["vwap"] and ce["close"] < ce["vwap"]:
            pnl = calculate_pnl("CE", self.ce_pos["price"], ce["open"])
            self.day_mtm += pnl
            log(f"{ce['datetime']} EXIT CE @ {ce['open']} PNL {pnl}")
            self.ce_pos = None

        if self.prev_pe is not None and not self.pe_pos and not self.locked:
            if self.prev_pe["close"] <= self.prev_pe["vwap"] and pe["close"] > pe["vwap"]:
                self.pe_pos = {"price": pe["open"]}
                log(f"{pe['datetime']} ENTRY PE @ {pe['open']}")

        if self.pe_pos and self.prev_pe["close"] >= self.prev_pe["vwap"] and pe["close"] < pe["vwap"]:
            pnl = calculate_pnl("PE", self.pe_pos["price"], pe["open"])
            self.day_mtm += pnl
            log(f"{pe['datetime']} EXIT PE @ {pe['open']} PNL {pnl}")
            self.pe_pos = None


    def run(self):
        trade_date = datetime.now(IST).strftime("%Y-%m-%d")

        wait_for_start()


        log("🚀 ENGINE STARTED (VIRTUAL MODE)")

        fno_df = load_fno_master()
        fut = get_nearest_nifty_fut(fno_df, self.trade_date)

        print(fut["SECURITY_ID"])
        # --- FUT REF
        from_dt =  "2026-02-02 09:14:00"
        to_dt   =  "2026-02-02 09:16:00"
        print(from_dt)
        print(to_dt)

        fut_candle = fetch_fut_candle(fut["SECURITY_ID"])
        print("future candle")
        print(fut_candle)
        ref_price = fut_candle["close"]
        print(ref_price)

        atm, ce_strike, pe_strike = calculate_strikes(ref_price)
        print("CE price , PE pRice",ce_strike,pe_strike)

        ce = find_option(fno_df, ce_strike, "CE", self.trade_date)
        pe = find_option(fno_df, pe_strike, "PE", self.trade_date)

        self.ce_id = ce["SECURITY_ID"]
        self.pe_id = pe["SECURITY_ID"]
        print(self.ce_id , self.pe_id)
        

        feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        [
            (marketfeed.NSE_FNO, str(self.ce_id), marketfeed.Quote),
            (marketfeed.NSE_FNO, str(self.pe_id), marketfeed.Quote),
            
        ],
        version="v2"
    )

        print("✅ Live Feed Started")

        # ---------------- INIT ONCE ----------------
        builders = {
            str(self.ce_id): CandleBuilder(),
            str(self.pe_id): CandleBuilder()
            }
        states = {
            str(self.ce_id): {"vwap": {"cum_pv":0,"cum_vol":0}},
            str(self.pe_id): {"vwap": {"cum_pv":0,"cum_vol":0}}
            }

        buffer = {}

        for sec in [self.ce_id,self.pe_id]:
            sec = str(sec)
            builders[sec] = CandleBuilder()

        # ---------------- START FEED ----------------
        feed.run_forever()

        # ---------------- MAIN LOOP ----------------
        while True:
            tick = feed.get_data()

            nt = normalize_tick(tick)
            if not nt:
                continue

            sec = str(nt["security_id"])
            price = nt["price"]
            avg = nt["avg"]
            ttime = nt["time"]
            volume = nt["volume"]

            candle = builders[sec].update(price, ttime)
            if candle:
                print(candle)
                candle["datetime"] = builders[sec].current_minute

                # VWAP
                state = states[sec]["vwap"]
                pv = candle["close"] * candle.get("volume", 1)
                state["cum_pv"] += pv
                state["cum_vol"] += candle.get("volume", 1)
                candle["vwap"] = state["cum_pv"] / state["cum_vol"]

                buffer[sec] = candle

                if len(buffer) == 2:
                    ce = buffer[str(self.ce_id)]
                    pe = buffer[str(self.pe_id)]

                    engine.execute_orders(ce, pe)
                    engine.risk_check()
                    engine.process_signals(ce, pe)

                    engine.prev_ce = ce
                    engine.prev_pe = pe

                    buffer.clear()

            if datetime.now(IST).time() >= FORCE_EXIT_TIME:
                engine.square_off_all()
                break

        log(f"✅ DAY MTM : {self.day_mtm}")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    engine = VWAPVirtualEngine(TRADE_DATE)
    engine.run()
