"""
Terminal Dashboard with OKX Paper Trading Integration
Real OKX API (Demo mode) + Terminal Spreadsheet Dashboard
"""
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import deque
import threading
import random
import httpx
import hmac
import hashlib
import base64
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pandas as pd


# ===== OKX Paper Trading Classes =====
class OKXCredentials:
    def __init__(self, api_key: str, secret_key: str, passphrase: str, demo: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.demo = demo


class OrderSide:
    BUY = "buy"
    SELL = "sell"


class OrderType:
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus:
    PENDING = "pending"
    LIVE = "live"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class PaperOrder:
    id: str
    client_oid: str
    symbol: str
    side: str
    order_type: str
    size: float
    price: Optional[float]
    status: str
    filled_size: float = 0.0
    avg_price: float = 0.0
    fee: float = 0.0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    signal_info: str = ""


@dataclass
class PaperPosition:
    symbol: str
    side: str
    size: float
    entry_price: float
    mark_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    leverage: int = 1


@dataclass
class PaperAccount:
    balance: float = 10000.0
    available: float = 10000.0
    frozen: float = 0.0
    total_fees: float = 0.0


class OKXPaperTrader:
    def __init__(self, api_key: str, secret_key: str, passphrase: str, demo: bool = True):
        self.creds = {
            "api_key": api_key,
            "secret_key": secret_key,
            "passphrase": passphrase,
            "demo": True
        }
        self.client = httpx.AsyncClient(timeout=30.0)
        self.account = PaperAccount()
        self.orders: Dict[str, Any] = {}
        self.positions: Dict[str, Any] = {}
        self.order_history: List[Dict] = []
        self.market_data: Dict[str, Dict] = {}
        self.maker_fee = 0.0008
        self.taker_fee = 0.0010
        self.slippage_bps = 2
        self.latency_ms = 50
        
    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = timestamp + method + request_path + body
        mac = hmac.new(
            self.creds["secret_key"].encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode()
    
    def _get_headers(self, method: str, request_path: str, body: str = "", timestamp: str = None) -> Dict[str, str]:
        # Timestamp ISO 8601 unico (mesmo usado na assinatura) - exigencia da OKX
        ts = timestamp or (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
        sign = self._sign(ts, method, request_path, body)
        headers = {
            "OK-ACCESS-KEY": self.creds["api_key"],
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.creds["passphrase"],
            "Content-Type": "application/json",
            "x-simulated-trading": "1"
        }
        return headers
    
    async def _request(self, method: str, endpoint: str, params: Dict = None, body: Dict = None) -> Dict:
        url = f"https://www.okx.com{endpoint}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        body_str = json.dumps(body) if body else ""
        headers = self._get_headers(method, endpoint, body_str)
        try:
            if method == "GET":
                resp = await self.client.get(url, headers=headers)
            elif method == "POST":
                resp = await self.client.post(url, headers=headers, content=json.dumps(body) if body else "")
            else:
                raise ValueError(f"Unsupported method")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"code": "500", "msg": str(e)}
    
    async def get_ticker(self, symbol: str) -> Dict:
        return await self._request("GET", f"/api/v5/market/ticker?instId={symbol}")
    
    async def get_candles(self, symbol: str, bar: str = "1m", limit: int = 100) -> Dict:
        return await self._request("GET", f"/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}")
    
    async def get_instruments(self, inst_type: str = "SWAP") -> Dict:
        return await self._request("GET", f"/api/v5/public/instruments?instType={inst_type}")
    
    def _simulate_fill(self, side: str, size: float, price: Optional[float], symbol: str) -> Dict:
        ticker = self.market_data.get(symbol, {})
        bid = float(ticker.get("bidPx", 0))
        ask = float(ticker.get("askPx", 0))
        last = float(ticker.get("last", 0))
        
        if bid == 0 and ask == 0 and last == 0:
            mid = 50000 if "BTC" in symbol else 3000 if "ETH" in symbol else 100
        else:
            mid = (bid + ask) / 2 if bid and ask else last
        
        slippage = mid * (2 / 10000)
        
        if side == "buy":
            fill_price = (ask if ask else mid) + slippage
        else:
            fill_price = (bid if bid else mid) - slippage
        
        fee_rate = 0.0010  # taker fee
        fee = fill_price * size * 0.0010
        
        return {"fill_price": fill_price, "fee": fee, "mid_price": mid}
    
    async def update_market_data(self, symbols: List[str]):
        for symbol in symbols:
            ticker = await self._request("GET", f"/api/v5/market/ticker?instId={symbol}")
            if "data" in ticker and ticker["data"]:
                self.market_data[symbol] = ticker["data"][0]
    
    async def paper_place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: Optional[float] = None,
        leverage: int = 1,
        signal_info: str = "",
        tgt_ccy: Optional[str] = None,
        size_is_quote: bool = False
    ) -> Dict:
        """Ordem REAL de mercado na OKX demo (spot, tdMode=cash).
        - size_is_quote=True (market buy): sz em USDT (moeda de cotacao) = valor em $.
        - senao: sz em quantidade do ativo base (ex.: BTC)."""
        body = {
            "instId": symbol,
            "tdMode": "cash",
            "side": side,
            "ordType": "market",
            "sz": str(size),
        }
        if tgt_ccy:
            body["tgtCcy"] = tgt_ccy
        resp = await self._request("POST", "/api/v5/trade/order", body=body)
        if resp.get("code") != "0":
            return {"ok": False, "error": resp.get("msg") or resp.get("code") or "erro desconhecido"}
        ord_id = resp["data"][0]["ordId"]
        # Aguarda o preenchimento e busca o fill real (preco + fee + quantidade da OKX)
        for _ in range(12):
            await asyncio.sleep(0.5)
            fill = await self._get_fill(symbol, ord_id)
            if fill:
                return {
                    "ok": True, "ord_id": ord_id, "side": side, "size": size,
                    "avg_px": fill["avg_px"], "fee": fill["fee"],
                    "fill_sz": fill.get("fill_sz") or 0.0,
                    "fill_ccy": fill.get("fill_ccy") or ""
                }
        return {"ok": False, "error": "ordem enviada mas fill nao confirmado", "ord_id": ord_id}

    async def _get_fill(self, symbol: str, ord_id: str) -> Optional[Dict]:
        resp = await self._request("GET", f"/api/v5/trade/fills?instId={symbol}&ordId={ord_id}")
        data = resp.get("data") or []
        if not data:
            return None
        f = data[0]
        try:
            avg_px = float(f.get("fillPx") or f.get("avgPx") or 0)
            fee = float(f.get("fee") or 0)
            fill_sz = float(f.get("fillSz") or 0)
        except (TypeError, ValueError):
            return None
        if avg_px <= 0:
            return None
        return {"avg_px": avg_px, "fee": fee, "fill_sz": fill_sz,
                "fill_ccy": f.get("fillCcy") or ""}

    async def paper_place_limit(self, symbol: str, side: str, size: float,
                                limit_px: float, signal_info: str = "") -> Dict:
        """Ordem LIMIT real na OKX demo (spot, cash). size em ativo base."""
        body = {
            "instId": symbol, "tdMode": "cash", "side": side,
            "ordType": "limit", "sz": str(size), "px": str(limit_px),
        }
        resp = await self._request("POST", "/api/v5/trade/order", body=body)
        if resp.get("code") != "0":
            return {"ok": False, "error": resp.get("msg") or resp.get("code") or "erro"}
        return {"ok": True, "ord_id": resp["data"][0]["ordId"]}

    async def get_order_state(self, symbol: str, ord_id: str) -> Optional[Dict]:
        resp = await self._request("GET", f"/api/v5/trade/order?instId={symbol}&ordId={ord_id}")
        data = resp.get("data") or []
        if not data:
            return None
        o = data[0]
        try:
            return {
                "state": o.get("state"),
                "avg_px": float(o.get("avgPx") or 0),
                "fee": float(o.get("fee") or 0),
                "filled_sz": float(o.get("accFillSz") or 0),
            }
        except (TypeError, ValueError):
            return {"state": o.get("state"), "avg_px": 0.0, "fee": 0.0, "filled_sz": 0.0}

    async def cancel_order(self, symbol: str, ord_id: str) -> bool:
        resp = await self._request("POST", "/api/v5/trade/cancel",
                                   body={"instId": symbol, "ordId": ord_id})
        return resp.get("code") == "0"
    
    def update_mark_prices(self):
        for key, pos in self.positions.items():
            symbol = pos["symbol"]
            ticker = self.market_data.get(symbol, {})
            last = float(ticker.get("last", 0))
            if last > 0:
                pos["mark_price"] = last
                if pos["side"] == "long":
                    pos["unrealized_pnl"] = (last - pos["entry_price"]) * pos["size"]
                else:
                    pos["unrealized_pnl"] = (pos["entry_price"] - last) * pos["size"]
    
    def get_summary(self) -> Dict:
        self.update_mark_prices()
        total_unrealized = sum(p["unrealized_pnl"] for p in self.positions.values())
        total_realized = sum(p.get("realized_pnl", 0) for p in self.positions.values())
        return {
            "balance": self.account.balance,
            "available": self.account.available,
            "frozen": self.account.frozen,
            "total_pnl": total_realized + total_unrealized,
            "realized_pnl": total_realized,
            "unrealized_pnl": total_unrealized,
            "positions": len(self.positions),
            "open_orders": len([o for o in self.orders.values() if o.get("status") == "LIVE"])
        }
    
    async def close(self):
        await self.client.aclose()


# ===== Terminal Dashboard (Paper Trading Version) =====
class C:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    BG_DARK = '\033[48;5;234m'
    BG_BLACK = '\033[40m'


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def hide_cursor():
    print('\033[?25l', end='')


def show_cursor():
    print('\033[?25h', end='')


@dataclass
class Trade:
    id: int
    timestamp: str
    pair: str
    direction: str
    bet: float
    expiration: int
    account: str
    probability: float
    result: Optional[float] = None
    status: str = "Aguardando"
    cumulative_pnl: float = 0.0


class Trader:
    def __init__(self, name: str, pair: str, bet_size: float = 10.0, expiration: int = 60):
        self.name = name
        self.pair = pair
        self.bet_size = bet_size
        self.expiration = expiration
        self.trades: List[Trade] = []
        self.next_id = 1
        self.wins = 0
        self.losses = 0
        self.pending = 0
        self.total_pnl = 0.0
        self.best_trade = 0.0
        self.worst_trade = 0.0
        self.current_streak = 0
        self.streak_type = ""
        self.enabled = True
        self.last_update = datetime.now(timezone.utc)
        self.cumulative_pnl = 0.0
        
        # OKX Paper Trading
        self.okx_symbol = pair.upper().replace("-OTC", "").replace("USDT", "") + "-USDT"
        self.paper_position = None
        
    def add_trade(self, direction: str, probability: float, signal_info: str = "") -> Trade:
        trade = Trade(
            id=self.next_id,
            timestamp=datetime.now(timezone.utc).strftime("%m/%d %H:%M:%S"),
            pair=self.pair,
            direction=direction,
            bet=self.bet_size,
            expiration=self.expiration,
            account="OKX-PAPER",
            probability=round(probability, 2),
            status="Aguardando",
            cumulative_pnl=self.cumulative_pnl,
        )
        self.trades.append(trade)
        self.next_id += 1
        self.pending += 1
        return trade
    
    def resolve_trade(self, trade_id: int, result: float):
        for trade in self.trades:
            if trade.id == trade_id and trade.status == "Aguardando":
                trade.result = round(result, 2)
                trade.status = "WIN" if result > 0 else "LOSS"
                self.pending -= 1
                if result > 0:
                    self.wins += 1
                else:
                    self.losses += 1
                self.total_pnl += result
                self.cumulative_pnl += result
                trade.cumulative_pnl = self.cumulative_pnl
                self.best_trade = max(self.best_trade, result)
                self.worst_trade = min(self.worst_trade, result)
                
                if self.streak_type == "" or \
                   (self.streak_type == "WIN" and result > 0) or \
                   (self.streak_type == "LOSS" and result < 0):
                    self.current_streak += 1 if result > 0 else -1
                else:
                    self.current_streak = 1 if result > 0 else -1
                self.streak_type = "WIN" if result > 0 else "LOSS"
                return trade
        return None
    
    def get_stats(self) -> dict:
        total_closed = self.wins + self.losses
        win_rate = (self.wins / total_closed * 100) if total_closed > 0 else 0.0
        return {
            "name": self.name,
            "pair": self.pair,
            "enabled": self.enabled,
            "wins": self.wins,
            "losses": self.losses,
            "pending": self.pending,
            "win_rate": round(win_rate, 1),
            "total_pnl": round(self.total_pnl, 2),
            "current_streak": self.current_streak,
            "streak_type": self.streak_type,
            "bet_size": self.bet_size,
            "expiration": self.expiration
        }


class TerminalDashboard:
    def __init__(self, real_mode=True):
        self.traders: Dict[str, Trader] = {}
        self.running = True
        self.update_interval = 2.0
        self.width = 170
        self.height = 55
        try:
            size = os.get_terminal_size()
            self.width = min(size.columns, 190)
            self.height = min(size.lines, 65)
        except:
            pass
        self.log_messages: deque = deque(maxlen=15)
        self.real_mode = True
        
        # OKX Paper Trader
        self.okx_trader = None
        
    def add_trader(self, trader: Trader):
        self.traders[trader.name] = trader
    
    def log(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_messages.append(f"{ts} {message}")
    
    def get_total_stats(self) -> dict:
        total_pnl = sum(t.total_pnl for t in self.traders.values())
        total_wins = sum(t.wins for t in self.traders.values())
        total_losses = sum(t.losses for t in self.traders.values())
        total_pending = sum(t.pending for t in self.traders.values())
        total_closed = total_wins + total_losses
        win_rate = (total_wins / total_closed * 100) if total_closed > 0 else 0.0
        best = max((t.best_trade for t in self.traders.values()), default=0.0)
        worst = min((t.worst_trade for t in self.traders.values()), default=0.0)
        max_streak = 0
        streak_type = ""
        for t in self.traders.values():
            if abs(t.current_streak) > abs(max_streak):
                max_streak = t.current_streak
                streak_type = t.streak_type
        return {
            "total_pnl": round(total_pnl, 2), "win_rate": round(win_rate, 1),
            "wins": total_wins, "losses": total_losses, "pending": total_pending,
            "operations": total_closed + total_pending, "traders_count": len(self.traders),
            "streak": max_streak, "streak_type": streak_type
        }
    
    def c(self, text: str, color: str) -> str:
        return color + text + C.RESET
    
    def pnl_str(self, value: float) -> str:
        if value > 0: return self.c("+" + "{:.2f}".format(value), C.BRIGHT_GREEN)
        elif value < 0: return self.c("{:.2f}".format(value), C.BRIGHT_RED)
        return self.c("{:.2f}".format(value), C.BRIGHT_YELLOW)
    
    def render_header(self) -> List[str]:
        stats = self.get_total_stats()
        now = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        lines = []
        lines.append(self.c("=" * self.width, C.BG_DARK + C.BRIGHT_CYAN))
        title = " OKX PAPER TRADING - REAL API DEMO "
        left = self.c("|", C.BRIGHT_CYAN) + " " + self.c(title, C.BOLD + C.BRIGHT_CYAN)
        right = self.c(datetime.now().strftime("%m/%d/%Y %H:%M:%S"), C.DIM) + " " + self.c("|", C.BRIGHT_CYAN)
        lines.append(left + " " * (self.width - len(left) - len(right) - 2) + right)
        lines.append(self.c("=" * self.width, C.BG_DARK + C.BRIGHT_CYAN))
        
        pnl_display = self.pnl_str(stats['total_pnl'])
        win_rate = stats['win_rate']
        win_color = C.BRIGHT_GREEN if win_rate >= 60 else (C.YELLOW if win_rate >= 50 else C.BRIGHT_RED)
        
        stat_boxes = [
            self.c("TOTAL P&L", C.CYAN) + ": " + self.pnl_str(stats['total_pnl']),
            self.c("WIN RATE", C.CYAN) + ": " + (C.BRIGHT_GREEN if win_rate >= 60 else (C.YELLOW if win_rate >= 50 else C.BRIGHT_RED)) + "{:.1f}%".format(win_rate) + C.RESET,
            self.c("OPS", C.CYAN) + ": " + self.c(str(stats['operations']), C.BRIGHT_WHITE),
            self.c("TRADERS", C.CYAN) + ": " + self.c(str(stats['traders_count']), C.BRIGHT_WHITE),
        ]
        
        streak_str = ""
        if stats['streak'] != 0:
            streak_color = C.BRIGHT_GREEN if stats['streak'] > 0 else C.BRIGHT_RED
            streak_str = " " + self.c("STREAK", C.CYAN) + ": " + self.c("{:+d}".format(stats['streak']), streak_color) + " (" + stats['streak_type'] + ")"
        
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + ("  |  ".join(stat_boxes) + streak_str).ljust(self.width - 4) + " " + self.c("|", C.BRIGHT_CYAN))
        
        sub = "    " + self.c("WINS:", C.DIM) + " " + self.c(str(stats['wins']), C.BRIGHT_GREEN) + "  |  "
        sub += self.c("LOSSES:", C.DIM) + " " + self.c(str(stats['losses']), C.BRIGHT_RED) + "  |  "
        sub += self.c("PENDING:", C.DIM) + " " + self.c(str(stats['pending']), C.BRIGHT_YELLOW)
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + sub.ljust(self.width - 4) + " " + self.c("|", C.BRIGHT_CYAN))
        lines.append(self.c("=" * self.width, C.BG_DARK + C.BRIGHT_CYAN))
        return lines
    
    def render_traders_table(self, okx_summary: Dict) -> List[str]:
        lines = []
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("TRADERS SPREADSHEET [OKX PAPER]", C.BOLD + C.BRIGHT_CYAN))
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
        
        header = ("  " + 
            self.c("{:<4}".format("#"), C.BOLD + C.DIM) + " " +
            self.c("{:<10}".format("TRADER"), C.BOLD + C.WHITE) + " " +
            self.c("{:<12}".format("PAR"), C.BOLD + C.CYAN) + " " +
            self.c("{:>8}".format("BET"), C.BOLD + C.YELLOW) + " " +
            self.c("{:>5}".format("EXP"), C.BOLD + C.DIM) + " " +
            self.c("{:>4}".format("ST"), C.BOLD) + " " +
            self.c("{:>6}".format("WINS"), C.BOLD + C.BRIGHT_GREEN) + " " +
            self.c("{:>7}".format("LOSSES"), C.BOLD + C.BRIGHT_RED) + " " +
            self.c("{:>7}".format("WR%"), C.BOLD + C.BRIGHT_CYAN) + " " +
            self.c("{:>12}".format("P&L"), C.BOLD + C.BRIGHT_WHITE) + " " +
            self.c("{:>14}".format("ACUMULADO"), C.BOLD + C.BRIGHT_YELLOW) + " " +
            self.c("{:<10}".format("STATUS"), C.BOLD + C.DIM))
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + header)
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
        
        for idx, (name, trader) in enumerate(self.traders.items()):
            stats = trader.get_stats()
            status = self.c("ON", C.BRIGHT_GREEN) if stats['enabled'] else self.c("OFF", C.BRIGHT_RED)
            win_color = C.BRIGHT_GREEN if stats['win_rate'] >= 60 else (C.YELLOW if stats['win_rate'] >= 50 else C.BRIGHT_RED)
            streak_str = " " + self.c("{:+d}".format(stats['current_streak']), C.BRIGHT_GREEN if stats['current_streak'] > 0 else C.BRIGHT_RED) if stats['current_streak'] != 0 else ""
            bet_str = "${:.2f}".format(stats.get('bet_size', 0))
            exp_str = "{}s".format(stats.get('expiration', 0))
            
            row = ("  " + 
                self.c("{:<4}".format(idx + 1), C.DIM) + " " +
                self.c("{:<10}".format(name), C.BRIGHT_WHITE) + " " +
                self.c("{:<12}".format(stats['pair']), C.CYAN) + " " +
                self.c("{:>8}".format(bet_str), C.YELLOW) + " " +
                self.c("{:>5}".format(exp_str), C.DIM) + " " +
                self.c("{:>4}".format(status), C.BOLD) + " " +
                self.c("{:>6}".format(stats['wins']), C.BRIGHT_GREEN) + " " +
                self.c("{:>7}".format(stats['losses']), C.BRIGHT_RED) + " " +
                self.c("{:>7.1f}%".format(stats['win_rate']), win_color) + " " +
                self.c("{:>12}".format(self.pnl_str(stats['total_pnl'])), C.BOLD) + " " +
                self.c("{:>14}".format(self.pnl_str(stats['total_pnl'])), C.BRIGHT_YELLOW) + " " +
                self.c("{:<10}".format("ON" if stats['enabled'] else "OFF"), C.BRIGHT_GREEN if stats['enabled'] else C.BRIGHT_RED) + streak_str)
            lines.append(self.c("|", C.BRIGHT_CYAN) + " " + row)
            if idx < len(self.traders) - 1:
                lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
        
        # OKX Account Summary
        if okx_summary:
            lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
            okx_line = ("  " + 
                self.c("OKX ACCOUNT:", C.BOLD + C.BRIGHT_MAGENTA) + " " +
                self.c("Bal:", C.DIM) + " " + self.c("${:.2f}".format(okx_summary.get('balance', 0)), C.BRIGHT_WHITE) + "  |  " +
                self.c("Avail:", C.DIM) + " " + self.c("${:.2f}".format(okx_summary.get('available', 0)), C.BRIGHT_GREEN) + "  |  " +
                self.c("PnL:", C.DIM) + " " + self.pnl_str(okx_summary.get('total_pnl', 0)) + "  |  " +
                self.c("Pos:", C.DIM) + " " + self.c(str(okx_summary.get('positions', 0)), C.BRIGHT_WHITE) + "  |  " +
                self.c("Fees:", C.DIM) + " " + self.c("${:.4f}".format(okx_summary.get('total_fees', 0)), C.YELLOW))
            lines.append(self.c("|", C.BRIGHT_CYAN) + " " + okx_line)
        
        return lines
    
    def render_operations_sheet(self) -> List[str]:
        lines = []
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("OPERACOES SPREADSHEET (ULTIMAS 50)", C.BOLD + C.BRIGHT_CYAN))
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
        
        all_trades = []
        for trader in self.traders.values():
            for t in trader.trades:
                if t.status != "Aguardando":
                    all_trades.append({
                        'id': t.id, 'timestamp': t.timestamp, 'pair': t.pair,
                        'direction': t.direction, 'bet': t.bet, 'expiration': t.expiration,
                        'account': t.account, 'probability': t.probability,
                        'result': t.result, 'status': t.status, 'trader': trader.name,
                        'cumulative_pnl': t.cumulative_pnl
                    })
        
        all_trades.sort(key=lambda x: x['id'])
        all_trades = all_trades[-50:]
        
        if not all_trades:
            lines.append(self.c("|", C.BRIGHT_CYAN) + "   " + self.c("Aguardando operacoes...", C.DIM))
            return lines
        
        header = ("  " + 
            self.c("{:>4}".format("#"), C.BOLD + C.DIM) + " " +
            self.c("{:<10}".format("HORA"), C.BOLD + C.DIM) + " " +
            self.c("{:<10}".format("TRADER"), C.BOLD + C.WHITE) + " " +
            self.c("{:<12}".format("PAR"), C.BOLD + C.CYAN) + " " +
            self.c("{:<4}".format("DIR"), C.BOLD) + " " +
            self.c("{:>8}".format("BET"), C.BOLD + C.YELLOW) + " " +
            self.c("{:>5}".format("EXP"), C.BOLD + C.DIM) + " " +
            self.c("{:>6}".format("PROB"), C.BOLD + C.BRIGHT_CYAN) + " " +
            self.c("{:>10}".format("RESULT"), C.BOLD) + " " +
            self.c("{:>6}".format("ST"), C.BOLD + C.DIM) + " " +
            self.c("{:>12}".format("P&L ACC"), C.BOLD + C.BRIGHT_YELLOW) + " " +
            self.c("{:>5}".format("W"), C.BOLD + C.BRIGHT_GREEN) + " " +
            self.c("{:>5}".format("L"), C.BOLD + C.BRIGHT_RED) + " " +
            self.c("{:>6}".format("WR%"), C.BOLD + C.BRIGHT_CYAN))
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + header)
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM))
        
        rw = rl = 0
        for t in all_trades:
            rw += 1 if t['status'] == "WIN" else 0
            rl += 1 if t['status'] == "LOSS" else 0
            rt = rw + rl
            rwr = (rw / rt * 100) if rt > 0 else 0.0
            
            dir_t = "UP" if t['direction'] == "CALL" else "DN"
            dir_c = C.BRIGHT_GREEN if t['direction'] == "CALL" else C.BRIGHT_MAGENTA
            prob_c = C.BRIGHT_GREEN if t['probability'] >= 0.7 else (C.YELLOW if t['probability'] >= 0.6 else C.BRIGHT_RED)
            res_c = C.BRIGHT_GREEN if t['result'] and t['result'] > 0 else C.BRIGHT_RED
            res_str = self.c("${:+.2f}".format(t['result']), res_c) if t['result'] is not None else self.c("--", C.DIM)
            st_c, st_s = (C.BRIGHT_GREEN, "WIN") if t['status'] == "WIN" else (C.BRIGHT_RED, "LOSS")
            bet_s = "${:.2f}".format(t['bet'])
            
            row = ("  " + self.c("{:>4}".format(t['id']), C.DIM) + " " +
                self.c("{:<10}".format(t['timestamp']), C.DIM) + " " +
                self.c("{:<10}".format(t['trader']), C.WHITE) + " " +
                self.c("{:<12}".format(t['pair']), C.CYAN) + " " +
                dir_c + "{:<4}".format(dir_t) + C.RESET + " " +
                self.c("{:>8}".format("${:.2f}".format(t['bet'])), C.YELLOW) + " " +
                self.c("{:>5}".format(str(t['expiration']) + "s"), C.DIM) + " " +
                prob_c + "{:>6.2f}".format(t['probability']) + C.RESET + " " +
                res_str + " " + self.c("{:>6}".format(st_s), st_c) + " " +
                self.c("{:>12}".format(self.pnl_str(t['cumulative_pnl'])), C.BRIGHT_YELLOW) + " " +
                self.c("{:>5}".format(rw), C.BRIGHT_GREEN) + " " +
                self.c("{:>5}".format(rl), C.BRIGHT_RED) + " " +
                self.c("{:>6.1f}%".format(rwr), C.BRIGHT_CYAN))
            lines.append(self.c("|", C.BRIGHT_CYAN) + " " + row)
        
        return lines
    
    def render_logs(self) -> List[str]:
        lines = [self.c("|", C.BRIGHT_CYAN) + " " + self.c("LOGS & SIGNAIS", C.BOLD + C.BRIGHT_CYAN),
                 self.c("|", C.BRIGHT_CYAN) + " " + self.c("-" * (self.width - 4), C.DIM)]
        logs = list(self.log_messages)
        for msg in logs[-12:]:
            lines.append(self.c("|", C.BRIGHT_CYAN) + "   " + msg)
        return lines
    
    def render_footer(self) -> List[str]:
        lines = [self.c("=" * self.width, C.BG_DARK + C.BRIGHT_CYAN)]
        controls = "Controles: [Q]uit  [R]efresh  [S]ignal  [+/-]Trader  [C]losePos  [Q]uit"
        lines.append(self.c("|", C.BRIGHT_CYAN) + " " + controls.ljust(self.width - 4) + " " + self.c("|", C.BRIGHT_CYAN))
        return lines
    
    def render(self, okx_summary: Dict = None):
        clear_screen()
        hide_cursor()
        all_lines = []
        all_lines.extend(self.render_header())
        all_lines.extend(self.render_traders_table(okx_summary or {}))
        all_lines.append(self.c("|", C.BRIGHT_CYAN))
        all_lines.extend(self.render_operations_sheet())
        all_lines.append(self.c("|", C.BRIGHT_CYAN))
        all_lines.extend(self.render_logs())
        all_lines.extend(self.render_footer())
        sys.stdout.write("\n".join(all_lines))
        sys.stdout.flush()
    
    async def run_async(self, okx_trader, check_signals_fn):
        self.log("OKX Paper Trading iniciado")
        self.okx_trader = okx_trader
        try:
            while self.running:
                okx_summary = okx_trader.get_summary() if okx_trader else {}
                await check_signals_fn()
                self.render(okx_trader.get_summary())
                await asyncio.sleep(self.update_interval)
        except KeyboardInterrupt:
            pass
        finally:
            show_cursor()
            clear_screen()
            print("\nOKX Paper Trading finalizado.")
    
    def run(self, okx_trader, check_signals_fn):
        """Synchronous wrapper - runs async loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run_async(okx_trader, check_signals_fn))
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()
            show_cursor()
            clear_screen()
            print("\nOKX Paper Trading finalizado.")


# ===== Main Integration =====
class OKXPaperTerminal:
    def __init__(self, api_key: str, secret_key: str, passphrase: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.dashboard = TerminalDashboard(real_mode=True)
        self.okx_trader = None
        self.running = False
        # specs spot por simbolo (lotSz/minSz) e operacoes abertas
        self.lot_sizes: Dict[str, Dict] = {}
        self._open: Dict[str, Dict] = {}
        # ===== V2 orderflow engine =====
        self.engine = None
        self.engine_task = None
        self.sym_trader: Dict[str, str] = {}       # okx_symbol -> nome do trader
        self.zone_state: Dict[str, Dict] = {}      # symbol -> maquina de estados
        self.v2_config = {
            "threshold": float(os.environ.get("V2_THRESHOLD", "2.0")),
            "min_levels": int(os.environ.get("V2_MIN_LEVELS", "3")),
            "min_denom_frac": float(os.environ.get("V2_MIN_DENOM", "0.002")),
            "ema_period": int(os.environ.get("V2_EMA", "12")),
            "candle_seconds": int(os.environ.get("V2_CANDLE_SECS", "300")),
            "retest_timeout": int(os.environ.get("V2_RETEST_TIMEOUT", "120")),
            "pos_timeout": int(os.environ.get("V2_POS_TIMEOUT", "300")),
            "rr": float(os.environ.get("V2_RR", "2.0")),
            "stop_buffer_pct": float(os.environ.get("V2_STOP_BUFFER", "0.0004")),
            "cooldown": int(os.environ.get("V2_COOLDOWN", "90")),
        }
    
    async def initialize(self):
        self.okx_trader = OKXPaperTrader(
            api_key=self.api_key,
            secret_key=self.secret_key,
            passphrase=self.passphrase,
            demo=True
        )
        
        # Test connection
        instruments = await self.okx_trader._request("GET", "/api/v5/public/instruments?instType=SWAP")
        if "code" in instruments and instruments["code"] != "0":
            raise Exception(f"OKX API Error: {instruments}")
        print(f"OKX Paper Trading conectado (Demo: True)")

        # Specs spot (lotSz/minSz) dos 4 pares - para converter $ em quantidade
        for sym in ("BTC-USDT", "ETH-USDT", "BNB-USDT", "SOL-USDT"):
            r = await self.okx_trader._request("GET", f"/api/v5/public/instruments?instType=SPOT&instId={sym}")
            d = (r.get("data") or [None])
            if d and d[0]:
                inst = d[0]
                self.lot_sizes[sym] = {
                    "lotSz": inst.get("lotSz"), "minSz": inst.get("minSz"),
                    "tickSz": inst.get("tickSz")
                }

        # Saldo real da conta demo (USDT spot) para o dashboard
        try:
            bal = await self.okx_trader._request("GET", "/api/v5/account/balance?ccy=USDT")
            det = ((bal.get("data") or [{}])[0].get("details") or [{}])[0]
            usdt = float(det.get("availBal") or det.get("cashBal") or 0)
            if usdt > 0:
                self.okx_trader.account.balance = usdt
                self.okx_trader.account.available = usdt
        except Exception:
            pass
    
    def add_trader(self, name: str, pair: str, bet_size: float = 10.0, expiration: int = 60):
        trader = Trader(name, pair, bet_size, expiration)
        self.dashboard.add_trader(trader)
        return trader
    
    async def update_market_data(self, symbols: List[str]):
        for symbol in symbols:
            ticker = await self.okx_trader._request("GET", f"/api/v5/market/ticker?instId={symbol}")
            if "data" in ticker and ticker["data"]:
                self.okx_trader.market_data[symbol] = ticker["data"][0]
    
    async def check_signals(self):
        """Loop V2 (a cada ~2s): monitora retests e posicoes abertas (stop/alvo/tempo)."""
        if not self.engine:
            return
        now = time.time()
        for sym, st in list(self.zone_state.items()):
            try:
                if st["state"] == "RETEST":
                    await self._check_retest(sym, st, now)
                elif st["state"] == "IN_POS":
                    await self._check_position(sym, st, now)
            except Exception as e:
                self.dashboard.log(f"Erro monitor {sym}: {str(e)[:60]}")

    async def _check_retest(self, sym: str, st: dict, now: float):
        if not st.get("ord_id"):
            st["state"] = "IDLE"
            return
        cfg = self.v2_config
        if now - st["t0"] > cfg["retest_timeout"]:
            await self.okx_trader.cancel_order(sym, st["ord_id"])
            self.dashboard.log(f"{self.sym_trader.get(sym, sym)}: retest expirou - ordem cancelada")
            st["state"] = "IDLE"
            st["ord_id"] = None
            return
        info = await self.okx_trader.get_order_state(sym, st["ord_id"])
        if not info:
            return
        if info["state"] == "filled":
            st["ord_id"] = None
            await self._enter_position(sym, st, info["avg_px"], info["fee"])
        elif info["state"] in ("canceled", "cancelled"):
            st["state"] = "IDLE"
            st["ord_id"] = None

    async def _enter_position(self, sym: str, st: dict, avg_px: float, fee: float):
        cfg = self.v2_config
        zone = st["zone"]
        side = st["side"]
        st["state"] = "IN_POS"
        st["entry_px"] = avg_px
        st["fee_open"] = fee
        st["opened_at"] = time.time()
        if side == "buy":
            st["stop_px"] = round(zone.price_min * (1 - cfg["stop_buffer_pct"]), 8)
        else:
            st["stop_px"] = round(zone.price_max * (1 + cfg["stop_buffer_pct"]), 8)
        risk = abs(avg_px - st["stop_px"])
        if side == "buy":
            st["target_px"] = round(avg_px + risk * cfg["rr"], 8)
        else:
            st["target_px"] = round(avg_px - risk * cfg["rr"], 8)
        name = self.sym_trader.get(sym, sym)
        trader = self.dashboard.traders.get(name)
        direction = "CALL" if side == "buy" else "PUT"
        sig = f"Zona {zone.direction} {zone.levels}x ~{zone.avg_ratio}x {zone.price_min}-{zone.price_max}"
        trade = trader.add_trade(direction, 0.0, signal_info=sig) if trader else None
        st["trade_id"] = trade.id if trade else None
        self.dashboard.log(
            f"ENTRADA {name} {direction} {st['qty']} @ {avg_px:.2f} | stop {st['stop_px']:.2f} | alvo {st['target_px']:.2f}")

    async def _check_position(self, sym: str, st: dict, now: float):
        px = self.engine.last_price.get(sym)
        if not px:
            return
        cfg = self.v2_config
        if now - st["opened_at"] > cfg["pos_timeout"]:
            await self._close_position(sym, st, "timeout")
            return
        side = st["side"]
        if side == "buy":
            if px <= st["stop_px"]:
                await self._close_position(sym, st, "stop")
            elif px >= st["target_px"]:
                await self._close_position(sym, st, "alvo")
        else:
            if px >= st["stop_px"]:
                await self._close_position(sym, st, "stop")
            elif px <= st["target_px"]:
                await self._close_position(sym, st, "alvo")

    async def _close_position(self, sym: str, st: dict, motivo: str):
        side = st["side"]
        qty = st["qty"]
        close_side = "sell" if side == "buy" else "buy"
        if close_side == "buy":
            resp = await self.okx_trader.paper_place_order(sym, close_side, qty, tgt_ccy="base_ccy")
        else:
            resp = await self.okx_trader.paper_place_order(sym, close_side, qty)
        if not resp.get("ok"):
            self.dashboard.log(f"{sym}: fechamento ({motivo}) falhou: {resp.get('error')} - nova tentativa em 3s")
            await asyncio.sleep(3)
            if close_side == "buy":
                resp = await self.okx_trader.paper_place_order(sym, close_side, qty, tgt_ccy="base_ccy")
            else:
                resp = await self.okx_trader.paper_place_order(sym, close_side, qty)
        if not resp.get("ok"):
            self.dashboard.log(f"{sym}: posicao NAO fechada ({motivo}) - requer atencao")
            return
        exit_px = resp["avg_px"]
        fees = abs(st.get("fee_open") or 0) + abs(resp.get("fee") or 0)
        if side == "buy":
            result = round((exit_px - st["entry_px"]) * qty - fees, 2)
        else:
            result = round((st["entry_px"] - exit_px) * qty - fees, 2)
        name = self.sym_trader.get(sym, sym)
        trader = self.dashboard.traders.get(name)
        if trader and st.get("trade_id"):
            resolved = trader.resolve_trade(st["trade_id"], result)
        try:
            self.okx_trader.account.balance = round((self.okx_trader.account.balance or 0) + result, 2)
            self.okx_trader.account.available = max(0.0, round((self.okx_trader.account.available or 0) + result, 2))
        except Exception:
            pass
        self.dashboard.log(
            f"FECHADO {name} [{motivo.upper()}] {'WIN' if result > 0 else 'LOSS'} ${result:+.2f} "
            f"({st['entry_px']:.2f}->{exit_px:.2f})")
        st["state"] = "IDLE"
        st["ord_id"] = None
        st["cooldown_until"] = time.time() + self.v2_config["cooldown"]

    async def _on_candle_close(self, candle):
        """Candle de 1min fechado: detecta zonas e tenta entrada no retest."""
        try:
            from orderflow_engine import detect_stacked_zones
        except ImportError:
            return
        cfg = self.v2_config
        sym = candle.symbol
        st = self.zone_state.get(sym)
        if not st:
            return
        self.dashboard.log(
            f"Candle {sym}: {candle.trades} trades | delta {candle.delta():+.4f} | buy {candle.buy_vol:.4f} sell {candle.sell_vol:.4f}")
        zones = detect_stacked_zones(candle, threshold=cfg["threshold"],
                                     min_levels=cfg["min_levels"],
                                     min_denom_frac=cfg["min_denom_frac"])
        for z in zones:
            self.dashboard.log(
                f"ZONA {sym} {z.direction.upper()} {z.levels}lv ~{z.avg_ratio}x "
                f"{z.price_min:.2f}-{z.price_max:.2f} | delta {candle.delta():+.4f}")
        if not zones or st["state"] != "IDLE":
            return
        if time.time() < st.get("cooldown_until", 0):
            return
        zones.sort(key=lambda z: (z.levels, z.avg_ratio), reverse=True)
        zone = zones[0]
        px = self.engine.last_price.get(sym)
        if not px:
            return
        # filtro de tendencia: continuacao exige alinhamento com EMA(curto prazo)
        ema = self.engine.ema(sym, cfg["ema_period"])
        if ema is None:
            self.dashboard.log(f"{sym}: aguardando EMA ({len(self.engine.close_history.get(sym) or [])}/{cfg['ema_period']} candles)")
            return
        if zone.direction == "buy" and px < ema:
            self.dashboard.log(f"{sym}: zona BUY sem alinhamento (px {px:.2f} < EMA {ema:.2f}) - descartada")
            return
        if zone.direction == "sell" and px > ema:
            self.dashboard.log(f"{sym}: zona SELL sem alinhamento (px {px:.2f} > EMA {ema:.2f}) - descartada")
            return
        await self._try_open_retest(sym, st, zone, px)

    async def _try_open_retest(self, sym: str, st: dict, zone, px: float):
        name = self.sym_trader.get(sym, sym)
        trader = self.dashboard.traders.get(name)
        if not trader:
            return
        cfg = self.v2_config
        side = "buy" if zone.direction == "buy" else "sell"
        if side == "buy":
            limit_px = zone.price_min
            if px <= limit_px:
                self.dashboard.log(f"{sym}: zona BUY ja quebrada (px {px:.2f} <= {limit_px:.2f})")
                return
        else:
            limit_px = zone.price_max
            if px >= limit_px:
                self.dashboard.log(f"{sym}: zona SELL ja quebrada (px {px:.2f} >= {limit_px:.2f})")
                return
        info = self.lot_sizes.get(sym) or {}
        try:
            tick_sz = float(info.get("tickSz") or 0.1)
        except (TypeError, ValueError):
            tick_sz = 0.1
        limit_px = round(limit_px / tick_sz) * tick_sz
        qty = self._round_spot_qty(sym, trader.bet_size / limit_px)
        if qty <= 0:
            return
        # PUT (sell) em spot exige ter o ativo
        if side == "sell":
            base_ccy = sym.split("-")[0]
            try:
                b = await self.okx_trader._request("GET", f"/api/v5/account/balance?ccy={base_ccy}")
                det = ((b.get("data") or [{}])[0].get("details") or [{}])[0]
                have = float(det.get("availBal") or 0)
            except Exception:
                have = 0.0
            if have < qty:
                self.dashboard.log(f"{sym}: sem {base_ccy} p/ PUT ({have:.6f} < {qty:.6f})")
                return
        resp = await self.okx_trader.paper_place_limit(sym, side, qty, limit_px)
        if not resp.get("ok"):
            self.dashboard.log(f"{sym}: ordem limit rejeitada: {resp.get('error')}")
            return
        st.update({"state": "RETEST", "ord_id": resp["ord_id"], "zone": zone,
                   "side": side, "qty": qty, "t0": time.time()})
        self.dashboard.log(
            f"{name}: retest {side} {qty} @ limit {limit_px:.2f} (zona {zone.price_min:.2f}-{zone.price_max:.2f})")

    def _round_spot_qty(self, symbol: str, qty: float) -> float:
        """Arredonda para baixo ao multiplo do lote minimo do par spot."""
        import math
        info = self.lot_sizes.get(symbol) or {}
        try:
            lot = float(info.get("lotSz") or 1e-8)
        except (TypeError, ValueError):
            lot = 1e-8
        if lot <= 0:
            lot = 1e-8
        return round(math.floor(qty / lot) * lot, 12)
    def run(self):
        """Synchronous entry point - creates and runs event loop"""
        # Create and run event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_loop())
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()
            show_cursor()
            clear_screen()
            print("\nOKX Paper Trading finalizado.")
    
    async def _run_loop(self):
        print("Iniciando OKX Paper Trading Terminal...")
        
        # Add traders
        self.add_trader("Alpha", "BTCUSDT", 12.50, 60)
        self.add_trader("Beta", "ETHUSDT", 10.00, 60)
        self.add_trader("Gamma", "BNBUSDT", 8.00, 60)
        self.add_trader("Delta", "SOLUSDT", 12.00, 60)
        
        print(f"Conectando OKX API (Demo)...")
        
        # Initial market data
        symbols = [t.okx_symbol for t in self.dashboard.traders.values()]
        await self.update_market_data(symbols)
        
        print(f"OKX Paper Trading conectado - {len(self.dashboard.traders)} traders")
        print("Dashboard rodando... Ctrl+C para sair\n")

        # ===== V2: inicia o feed de footprint (trades com lado agressor) =====
        try:
            from orderflow_engine import FootprintFeed
            symbols = [t.okx_symbol for t in self.dashboard.traders.values()]
            self.sym_trader = {t.okx_symbol: name for name, t in self.dashboard.traders.items()}
            for s in symbols:
                self.zone_state[s] = {
                    "state": "IDLE", "ord_id": None, "zone": None, "t0": 0,
                    "cooldown_until": 0, "entry_px": 0.0, "qty": 0.0,
                    "side": None, "stop_px": 0.0, "target_px": 0.0,
                    "trade_id": None, "opened_at": 0,
                }
            self.engine = FootprintFeed(symbols, candle_seconds=self.v2_config["candle_seconds"],
                                        on_candle_close=self._on_candle_close)
            self.engine_task = asyncio.create_task(self.engine.run())
            # pre-carrega closes historicos p/ a EMA iniciar pronta (sem warm-up a cada restart)
            try:
                for sym in symbols:
                    r = await self.okx_trader._request(
                        "GET", f"/api/v5/market/history-candles?instId={sym}&bar=5m&limit=60")
                    hist = r.get("data") or []
                    for c in reversed(hist):  # mais antigo -> mais recente
                        self.engine.close_history.setdefault(sym, []).append(float(c[4]))
                    self.dashboard.log(f"Historico {sym}: {len(hist)} closes 5m carregados p/ EMA")
            except Exception as e:
                self.dashboard.log(f"Historico p/ EMA falhou: {e}")
            self.dashboard.log(f"Engine V2 (footprint real) iniciado - candle {self.v2_config['candle_seconds']}s")
        except Exception as e:
            self.dashboard.log(f"Engine V2 falhou ao iniciar: {e}")

        # Web dashboard acessivel pelo navegador (porta $PORT na Railway)
        try:
            self.web = WebDashboard(self)
            self.web.start()
        except Exception as e:
            print(f"Web dashboard nao iniciado: {e}")

        try:
            await self.dashboard.run_async(self.okx_trader, self.check_signals)
        finally:
            if self.engine:
                self.engine.stop()
            if self.engine_task:
                self.engine_task.cancel()
                try:
                    await self.engine_task
                except (asyncio.CancelledError, Exception):
                    pass
        
        await self.okx_trader.close()


# ===== Web Dashboard (HTTP - acessivel pelo navegador) =====
class WebDashboard:
    """Serve uma pagina HTML + /api/state com o estado ao vivo do bot paper."""

    HTML = """<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OKX Paper Trading - Dashboard</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0b0f17;color:#d7e0ea;font:14px/1.45 Consolas,Menlo,monospace;padding:16px}
h1{font-size:18px;color:#5eead4;letter-spacing:1px}
.sub{color:#64748b;font-size:12px;margin:4px 0 14px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:14px}
.card{background:#111827;border:1px solid #1f2a3d;border-radius:8px;padding:10px 12px}
.card .k{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
.card .v{font-size:20px;font-weight:bold;margin-top:2px}
.pos{color:#34d399}.neg{color:#f87171}.neu{color:#fbbf24}
table{width:100%;border-collapse:collapse;margin-bottom:14px;background:#0f1626;border-radius:8px;overflow:hidden}
th{background:#16223a;color:#5eead4;text-align:left;padding:7px 10px;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
td{padding:6px 10px;border-top:1px solid #182338;font-size:13px;white-space:nowrap}
tr:hover td{background:#141e31}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:bold}
.pill.on{background:#065f46;color:#6ee7b7}.pill.off{background:#7f1d1d;color:#fca5a5}
.pill.win{background:#065f46;color:#6ee7b7}.pill.loss{background:#7f1d1d;color:#fca5a5}.pill.w8{background:#374151;color:#cbd5e1}
.logs{background:#0a0f1a;border:1px solid #1f2a3d;border-radius:8px;padding:10px;font-size:12px;color:#94a3b8;max-height:220px;overflow-y:auto}
.logs b{color:#5eead4}
.mkt{display:inline-block;margin:0 14px 8px 0;background:#111827;border:1px solid #1f2a3d;padding:6px 10px;border-radius:6px;font-size:12px}
.mkt .sym{color:#5eead4;font-weight:bold}
#live{color:#34d399;font-size:11px}
#live.off{color:#f87171}
</style></head><body>
<h1>OKX PAPER TRADING <span id="live">● AO VIVO</span></h1>
<div class="sub" id="clock">conectando...</div>
<div class="cards" id="cards"></div>
<div class="mkt" id="market"></div>
<h1 style="font-size:14px;margin-bottom:8px">TRADERS</h1>
<table id="traders"><thead></thead><tbody></tbody></table>
<h1 style="font-size:14px;margin-bottom:8px">OPERACOES (ULTIMAS 50)</h1>
<table id="ops"><thead></thead><tbody></tbody></table>
<h1 style="font-size:14px;margin-bottom:8px">OPERACOES ABERTAS (EXECUCAO REAL)</h1>
<table id="pos"><thead></thead><tbody></tbody></table>
<h1 style="font-size:14px;margin-bottom:8px">LOGS &amp; SINAIS</h1>
<div class="logs" id="logs"></div>
<script>
const fmt=v=>v==null?'--':Number(v).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
const cls=v=>v>0?'pos':(v<0?'neg':'neu');
const pnl=v=>`<span class="${cls(v)}">${v>0?'+':''}${fmt(v)}</span>`;
async function refresh(){
  try{
    const r=await fetch('/api/state',{cache:'no-store'});
    if(!r.ok) throw 0;
    const s=await r.json();
    document.getElementById('live').className='';
    document.getElementById('clock').textContent='Atualizado: '+s.local_ts+'  |  '+s.ts;
    const st=s.stats,ac=s.account||{};
    const cards=[
      ['P&L TOTAL',pnl(st.total_pnl),st.total_pnl],
      ['WIN RATE',`<span class="${st.win_rate>=60?'pos':(st.win_rate>=50?'neu':'neg')}">${st.win_rate}%</span>`,0],
      ['OPS',st.operations,0],['WINS',`<span class="pos">${st.wins}</span>`,0],
      ['LOSSES',`<span class="neg">${st.losses}</span>`,0],
      ['SALDO OKX','$'+fmt(ac.balance),0],
      ['DISPONIVEL','$'+fmt(ac.available),0],
      ['P&L OKX',pnl(ac.total_pnl),ac.total_pnl||0],
      ['POSICOES',ac.positions??'--',0]
    ];
    document.getElementById('cards').innerHTML=cards.map(c=>`<div class="card"><div class="k">${c[0]}</div><div class="v">${c[1]}</div></div>`).join('');
    const mk=symbol=>(s.market||{})[symbol]||{};
    document.getElementById('market').innerHTML=Object.keys(s.market||{}).map(sym=>{
      const m=mk(sym);const l=parseFloat(m.last),o=parseFloat(m.open24h);
      const ch=o?((l-o)/o*100):null;
      const dl=m.delta;
      return `<span class="mkt"><span class="sym">${sym}</span> ${fmt(m.last)} <span class="${cls(ch)}">${ch==null?'':(ch>0?'+':'')+fmt(ch)+'%'}</span> ${dl==null?'':`<span class="${cls(dl)}">Δ ${dl>0?'+':''}${fmt(dl)}</span>`}</span>`;
    }).join('');
    document.getElementById('traders').innerHTML=`<thead><tr><th>#</th><th>TRADER</th><th>PAR</th><th>BET</th><th>EXP</th><th>ST</th><th>WINS</th><th>LOSSES</th><th>WR%</th><th>P&L</th><th>STREAK</th></tr></thead><tbody>`+s.traders.map((t,i)=>`<tr><td>${i+1}</td><td>${t.name}</td><td>${t.pair}</td><td>$${fmt(t.bet_size)}</td><td>${t.expiration}s</td><td><span class="pill ${t.enabled?'on':'off'}">${t.enabled?'ON':'OFF'}</span></td><td class="pos">${t.wins}</td><td class="neg">${t.losses}</td><td class="${t.win_rate>=60?'pos':(t.win_rate>=50?'neu':'neg')}">${t.win_rate}%</td><td>${pnl(t.total_pnl)}</td><td class="${cls(t.current_streak)}">${t.current_streak?((t.current_streak>0?'+':'')+t.current_streak+' '+t.streak_type):'--'}</td></tr>`).join('')+`</tbody>`;
    document.getElementById('ops').innerHTML=`<thead><tr><th>HORA</th><th>TRADER</th><th>PAR</th><th>DIR</th><th>BET</th><th>PROB</th><th>RESULTADO</th><th>ST</th><th>P&L ACC</th></tr></thead><tbody>`+(s.operations.length?s.operations.slice().reverse().map(o=>`<tr><td>${o.timestamp}</td><td>${o.trader}</td><td>${o.pair}</td><td class="${o.direction==='CALL'?'pos':'neg'}">${o.direction==='CALL'?'▲ CALL':'▼ PUT'}</td><td>$${fmt(o.bet)}</td><td>${fmt(o.probability)}</td><td>${o.result==null?'--':pnl(o.result)}</td><td><span class="pill ${o.status==='WIN'?'win':(o.status==='LOSS'?'loss':'w8')}">${o.status==='Aguardando'?'AGUARDANDO':o.status}</span></td><td>${pnl(o.cumulative_pnl)}</td></tr>`).join(''):`<tr><td colspan="9" style="color:#64748b">Aguardando operacoes...</td></tr>`)+`</tbody>`;
    const pos=(s.positions||[]);
    document.getElementById('pos').innerHTML=`<thead><tr><th>TRADER</th><th>SIMBOLO</th><th>LADO</th><th>TAMANHO</th><th>ENTRADA</th><th>STOP</th><th>ALVO</th><th>STATUS</th></tr></thead><tbody>`+(pos.length?pos.map(p=>`<tr><td>${p.trader}</td><td>${p.symbol}</td><td class="${p.side==='buy'?'pos':'neg'}">${p.side==='buy'?'▲ COMPRA':'▼ VENDA'}</td><td>${p.size}</td><td>${p.entry_price==null?'--':'$'+fmt(p.entry_price)}</td><td>${p.stop_px==null?'--':'$'+fmt(p.stop_px)}</td><td>${p.target_px==null?'--':'$'+fmt(p.target_px)}</td><td><span class="pill ${p.status==='ABERTA'?'win':'w8'}">${p.status}</span></td></tr>`).join(''):`<tr><td colspan="8" style="color:#64748b">Nenhuma operacao (aguardando zona de footprint)</td></tr>`)+`</tbody>`;
    document.getElementById('logs').innerHTML=(s.logs||[]).map(l=>`<div>${l.replace(/Analisando|SINAL|RESOLVIDO|Erro/g,'<b>$&</b>')}</div>`).join('')||'<div>sem logs</div>';
  }catch(e){
    document.getElementById('live').className='off';
    document.getElementById('live').textContent='● SEM SINAL';
  }
}
setInterval(refresh,3000);refresh();
</script></body></html>"""

    def __init__(self, terminal):
        self.terminal = terminal
        self.port = int(os.environ.get("PORT", "8080"))
        self._httpd = None

    def _state(self) -> dict:
        term = self.terminal
        dash = term.dashboard
        state = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "local_ts": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "stats": dash.get_total_stats(),
            "traders": [tr.get_stats() for tr in dash.traders.values()],
            "logs": list(dash.log_messages),
        }
        if term.okx_trader:
            try:
                state["account"] = term.okx_trader.get_summary()
            except Exception:
                state["account"] = {}
            state["positions"] = [
                {"symbol": p.get("symbol"), "side": p.get("side"),
                 "size": p.get("size"), "entry_price": round(p.get("entry_price", 0), 4),
                 "mark_price": round(p.get("mark_price", 0), 4),
                 "unrealized_pnl": round(p.get("unrealized_pnl", 0), 2)}
                for p in term.okx_trader.positions.values()
            ]
            state["market"] = {
                sym: {"last": t.get("last"), "bidPx": t.get("bidPx"),
                      "askPx": t.get("askPx"), "open24h": t.get("open24h")}
                for sym, t in term.okx_trader.market_data.items()
            }
        # delta do footprint (candle corrente) em USDT, por simbolo
        if term.engine:
            try:
                for sym, es in term.engine.state().items():
                    if sym in state["market"]:
                        d = es.get("delta") or 0
                        last = float(state["market"][sym].get("last") or 0)
                        state["market"][sym]["delta"] = round(d * last, 2) if last else None
            except Exception:
                pass
        # Operacoes V2: retests aguardando e posicoes abertas (stop/alvo)
        state["positions"] = []
        for sym, st in (term.zone_state or {}).items():
            stt = st.get("state")
            if stt in ("RETEST", "IN_POS"):
                trader_nm = term.sym_trader.get(sym, sym)
                if stt == "RETEST":
                    state["positions"].append({
                        "trader": trader_nm, "symbol": sym,
                        "side": st.get("side"), "size": st.get("qty"),
                        "entry_price": None, "opened_at": "retest",
                        "stop_px": None, "target_px": None,
                        "status": "RETEST",
                    })
                else:
                    state["positions"].append({
                        "trader": trader_nm, "symbol": sym,
                        "side": st.get("side"), "size": st.get("qty"),
                        "entry_price": st.get("entry_px"),
                        "opened_at": datetime.fromtimestamp(st.get("opened_at") or 0).strftime("%H:%M:%S"),
                        "stop_px": st.get("stop_px"), "target_px": st.get("target_px"),
                        "status": "ABERTA",
                    })
        ops = []
        for name, tr in dash.traders.items():
            for td in tr.trades:
                ops.append({
                    "trader": name, "id": td.id, "timestamp": td.timestamp,
                    "pair": td.pair, "direction": td.direction, "bet": td.bet,
                    "probability": td.probability, "result": td.result,
                    "status": td.status, "cumulative_pnl": td.cumulative_pnl,
                })
        ops.sort(key=lambda o: (o["trader"], o["id"]))
        state["operations"] = ops[-50:]
        return state

    def start(self):
        if self._httpd:
            return
        try:
            self._httpd = ThreadingHTTPServer(("0.0.0.0", self.port), _WebHandler)
            self._httpd.dashboard = self
            thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
            thread.start()
            print(f"Web dashboard no ar: http://0.0.0.0:{self.port}")
        except Exception as e:
            print(f"Web dashboard indisponivel na porta {self.port}: {e}")
            self._httpd = None


class _WebHandler(BaseHTTPRequestHandler):
    server_version = "OKXPaperWeb/1.0"

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/state":
            body = json.dumps(self.server.dashboard._state()).encode()
            ctype = "application/json"
        elif path in ("/", "/index.html"):
            body = self.server.dashboard.HTML.encode()
            ctype = "text/html; charset=utf-8"
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    def log_message(self, *args):
        pass


def main():
    print("=" * 70)
    print("  OKX PAPER TRADING - TERMINAL DASHBOARD  ")
    print("=" * 70)
    print()
    print("Modo: PAPER TRADING (Demo OKX) - Sem dinheiro real")
    print("Estratégia: Momentum demo (substitua por Stacked Imbalance real)")
    print()

    # Credenciais via variáveis de ambiente (fallback = conta demo local)
    api_key = os.environ.get("OKX_API_KEY", "68a958fc-bf85-4e91-be43-848e10b337f6")
    secret_key = os.environ.get("OKX_API_SECRET", "35E362F2A508AEAF999750B62000F6B3")
    passphrase = os.environ.get("OKX_PASSPHRASE", "@Extreme123")

    bot = OKXPaperTerminal(
        api_key=api_key,
        secret_key=secret_key,
        passphrase=passphrase
    )
    
    try:
        # Initialize async components
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot.initialize())
        # Run the bot (creates its own event loop internally)
        bot.run()
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        print("\nFinalizado.")


if __name__ == "__main__":
    print("=" * 70)
    print("  OKX PAPER TRADING - TERMINAL DASHBOARD  ")
    print("=" * 70)
    print()
    print("CONFIGURE SUA PASSPHRASE OKX NO CÓDIGO ANTES DE RODAR!")
    print()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\nFinalizado.")