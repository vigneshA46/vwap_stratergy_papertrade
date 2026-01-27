import time as t
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
from io import StringIO
import os
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

TRADE_DATE="2026-01-22"

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
        return (entry - exit) * LOT_SIZE * LOTS


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

def fetch_fut_candle(security_id,from_dt, to_dt):
    payload = {
        "securityId": f"{security_id}",
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTIDX",
        "interval": "1",
        "oi": True,
        "fromDate": str(from_dt),
        "toDate": str(to_dt)
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

        log("🚀 ENGINE STARTED (VIRTUAL MODE)")

        fno_df = load_fno_master()
        fut = get_nearest_nifty_fut(fno_df, self.trade_date)
        print(fut["SECURITY_ID"])
        # --- FUT REF
        from_dt =  pd.to_datetime(f"{TRADE_DATE} 09:14:00")
        to_dt   =  pd.to_datetime(f"{TRADE_DATE} 09:16:00")
        print(from_dt)
        print(to_dt)

        fut_candle = fetch_fut_candle(fut["SECURITY_ID"],from_dt,to_dt)
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
        

        virtual_dt = pd.to_datetime(f"{TRADE_DATE} {START_TIME}")
        print(virtual_dt)
        while virtual_dt.time() <= FORCE_EXIT_TIME:

            from_dt = virtual_dt.strftime("%Y-%m-%d %H:%M:%S")
            to_dt = (virtual_dt + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
            from_dt = pd.to_datetime(from_dt).strftime("%Y-%m-%d %H:%M:%S")
            to_dt   = pd.to_datetime(to_dt).strftime("%Y-%m-%d %H:%M:%S")

            ce_candle = fetch_one_candle(self.ce_id, "NSE_FNO", "OPTIDX", from_dt, to_dt,self.ce_vwap_state)
            pe_candle = fetch_one_candle(self.pe_id, "NSE_FNO", "OPTIDX", from_dt, to_dt,self.pe_vwap_state)

            if ce_candle is None or pe_candle is None:
                virtual_dt += timedelta(minutes=1)
                continue

            self.execute_orders(ce_candle, pe_candle)
            self.risk_check()
            self.process_signals(ce_candle, pe_candle)

            self.prev_ce = ce_candle
            self.prev_pe = pe_candle

            virtual_dt += timedelta(minutes=1)

        log(f"✅ DAY MTM : {self.day_mtm}")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    engine = VWAPVirtualEngine("2026-01-27")
    engine.run()
