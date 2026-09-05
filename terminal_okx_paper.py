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
        """Check for stacked imbalance signals using OrderFlowChart and execute paper trades"""
        for name, trader in self.dashboard.traders.items():
            if not trader.enabled:
                continue
            
            symbol = trader.okx_symbol
            
            # Skip if already has pending trade
            if trader.pending > 0:
                continue
            
            try:
                # Get orderbook depth (bid/ask levels) from OKX
                orderbook = await self.okx_trader._request("GET", 
                    f"/api/v5/market/books?instId={symbol}&sz=50")
                
                if "data" not in orderbook or not orderbook["data"]:
                    self.dashboard.log(f"Sem orderbook para {symbol}")
                    continue
                
                ob = orderbook["data"][0]
                ts = int(ob["ts"])
                ts_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                
                # Get recent trades for OHLC
                trades_resp = await self.okx_trader._request("GET",
                    f"/api/v5/market/trades?instId={symbol}&limit=100")
                
                if "data" not in trades_resp or not trades_resp["data"]:
                    self.dashboard.log(f"Sem trades para {symbol}")
                    continue
                
                self.dashboard.log(f"Analisando {symbol}: {len(ob['bids'])} bids, {len(ob['asks'])} asks, {len(trades_resp['data'])} trades")
                
                # Build orderflow dataframe from orderbook + trades
                # Create a unique identifier for current candle (1-minute)
                candle_ts = int(ts_dt.timestamp() // 60) * 60
                candle_id = f"candle_{candle_ts}"
                
                # Build orderflow rows from orderbook
                rows = []
                for bid_price, bid_sz, *_ in ob["bids"][:50]:
                    rows.append({
                        "bid_size": float(bid_sz),
                        "price": float(bid_price),
                        "ask_size": 0.0,
                        "identifier": candle_id
                    })
                
                for ask_price, ask_sz, *_ in ob["asks"][:50]:
                    rows.append({
                        "bid_size": 0.0,
                        "price": float(ask_price),
                        "ask_size": float(ask_sz),
                        "identifier": candle_id
                    })
                
                if not rows:
                    continue
                
                df_of = pd.DataFrame(rows)
                df_of["timestamp"] = ts_dt
                df_of = df_of.set_index("timestamp")
                
                # Build OHLC from recent trades
                trade_list = trades_resp["data"]
                if not trade_list:
                    continue
                
                prices = [float(t["px"]) for t in trade_list]
                volumes = [float(t["sz"]) for t in trade_list]
                
                ohlc_row = {
                    "identifier": candle_id,
                    "open": prices[-1],   # oldest
                    "high": max(prices),
                    "low": min(prices),
                    "close": prices[0],   # newest
                    "volume": sum(volumes),
                    "timestamp": datetime.fromtimestamp(candle_ts, tz=timezone.utc)
                }
                df_ohlc = pd.DataFrame([ohlc_row]).set_index("timestamp")
                
                # Use OrderFlowChart to detect stacked imbalances
                from OrderFlow import OrderFlowChart
                chart = OrderFlowChart(
                    df_of, df_ohlc, identifier_col="identifier",
                    stacked_threshold=3.0,
                    stacked_min_levels=3,
                    show_stacked=True
                )
                
                fig = chart.plot(return_figure=True)
                summary = chart.stacked_summary
                
                if summary is None or summary.empty:
                    self.dashboard.log(f"{symbol}: Sem stacked imbalances")
                    continue
                
                # Check for stacked imbalances in current candle
                current_stacks = summary[summary["identifier"] == candle_id]
                if current_stacks.empty:
                    self.dashboard.log(f"{symbol}: Sem stacked no candle atual")
                    continue
                
                # Find strongest stacked imbalance
                strongest = current_stacks.loc[current_stacks["avg_ratio"].idxmax()]
                direction = "CALL" if strongest["direction"] == "buy" else "PUT"
                prob = min(0.95, 0.55 + (strongest["avg_ratio"] - 3.0) * 0.1)

                signal_info = f"Stacked {strongest['direction']} {int(strongest['levels'])}x @ {strongest['avg_ratio']:.1f}x | Price {strongest['price_min']:.2f}-{strongest['price_max']:.2f}"

                # ===== Execucao REAL na OKX demo (spot) =====
                # 1) preco atual para converter bet ($) em quantidade do ativo
                ticker = self.okx_trader.market_data.get(symbol)
                if not ticker or not float(ticker.get("last") or 0):
                    t = await self.okx_trader._request("GET", f"/api/v5/market/ticker?instId={symbol}")
                    tdata = t.get("data") or [{}]
                    if tdata:
                        self.okx_trader.market_data[symbol] = tdata[0]
                        ticker = tdata[0]
                last_px = float((ticker or {}).get("last") or 0)
                if last_px <= 0:
                    self.dashboard.log(f"{symbol}: sem preco de mercado para dimensionar ordem")
                    continue

                side = "buy" if direction == "CALL" else "sell"
                raw_qty = trader.bet_size / last_px
                qty = self._round_spot_qty(symbol, raw_qty)
                if qty <= 0:
                    self.dashboard.log(f"{symbol}: valor {trader.bet_size} abaixo do minimo spot")
                    continue

                # PUT em spot exige ter o ativo na conta demo
                if side == "sell":
                    base_ccy = symbol.split("-")[0]
                    try:
                        b = await self.okx_trader._request("GET", f"/api/v5/account/balance?ccy={base_ccy}")
                        det = ((b.get("data") or [{}])[0].get("details") or [{}])[0]
                        have = float(det.get("availBal") or 0)
                    except Exception:
                        have = 0.0
                    if have < qty:
                        self.dashboard.log(f"{symbol}: sem {base_ccy} suficiente p/ PUT (tem {have:.6f}, precisa {qty:.6f})")
                        continue

                # CALL (buy): ordem market em USDT (quote) = valor exato da bet.
                # PUT (sell): ordem market em BTC (base) = quantidade calculada.
                if side == "buy":
                    resp = await self.okx_trader.paper_place_order(symbol, side, trader.bet_size, signal_info=signal_info)
                else:
                    resp = await self.okx_trader.paper_place_order(symbol, side, qty, signal_info=signal_info)
                if not resp.get("ok"):
                    self.dashboard.log(f"{symbol} {direction}: ordem rejeitada: {str(resp.get('error'))[:70]}")
                    continue

                # 2) ordem preenchida de verdade -> registra trade (qty real do fill)
                trade = trader.add_trade(direction, prob, signal_info=signal_info)
                filled_qty = float(resp.get("fill_sz") or qty or 0)
                if filled_qty <= 0:
                    filled_qty = qty
                self._open[trader.name] = {
                    "trade_id": trade.id, "side": side, "qty": filled_qty,
                    "entry_px": resp["avg_px"], "fee_open": resp["fee"],
                    "symbol": symbol, "opened_at": datetime.now(timezone.utc).strftime("%H:%M:%S")
                }
                self.dashboard.log(f"SINAL {trade.id}: {name} {trader.pair} {direction} @ {prob:.2f} | {signal_info} | exec {side} {filled_qty} @ {resp['avg_px']:.2f}")

                # 3) fecha apos a expiracao e resolve com P&L REAL
                asyncio.create_task(self._auto_resolve(trader, trade, side, filled_qty))
                
            except Exception as e:
                self.dashboard.log(f"Erro analise {name}: {str(e)[:60]}")
    
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
    
    async def _auto_resolve(self, trader: Trader, trade, side: str, qty: float):
        await asyncio.sleep(trader.expiration)
        name = trader.name
        op = self._open.pop(name, None)
        if not op:
            return
        symbol = op.get("symbol") or trader.okx_symbol
        close_side = "sell" if side == "buy" else "buy"
        # Fechamento: CALL -> vende o BTC comprado (sz base). PUT -> recompra o BTC (tgtCcy=base_ccy).
        if close_side == "sell":
            resp = await self.okx_trader.paper_place_order(symbol, close_side, qty)
        else:
            resp = await self.okx_trader.paper_place_order(symbol, close_side, qty, tgt_ccy="base_ccy")
        if not resp.get("ok"):
            self.dashboard.log(f"{name}: falha ao fechar ({resp.get('error')}) - tentando de novo")
            await asyncio.sleep(5)
            if close_side == "sell":
                resp = await self.okx_trader.paper_place_order(symbol, close_side, qty)
            else:
                resp = await self.okx_trader.paper_place_order(symbol, close_side, qty, tgt_ccy="base_ccy")
        if not resp.get("ok"):
            self.dashboard.log(f"{name}: posicao NAO fechada ({resp.get('error')}) - atencao")
            self._open[name] = op
            return
        # P&L real: diferenca de preco x quantidade - fees (ambos os lados)
        exit_px = resp["avg_px"]
        fee_close = abs(resp.get("fee") or 0)
        fee_open = abs(op.get("fee_open") or 0)
        if side == "buy":      # comprou e vendeu (long)
            gross = (exit_px - op["entry_px"]) * qty
        else:                   # vendeu e recomprou (short)
            gross = (op["entry_px"] - exit_px) * qty
        result = round(gross - fee_open - fee_close, 2)
        try:
            self.okx_trader.account.balance = round(
                (self.okx_trader.account.balance or 0) + result, 2)
            self.okx_trader.account.available = max(
                0.0, round((self.okx_trader.account.available or 0) + result, 2))
        except Exception:
            pass
        resolved = trader.resolve_trade(trade.id, result)
        if resolved:
            self.dashboard.log(f"RESOLVIDO {resolved.id}: {name} {resolved.direction} | {'WIN' if result > 0 else 'LOSS'} ${result:+.2f} (px {op['entry_px']:.2f}->{exit_px:.2f})")
    
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

        # Web dashboard acessivel pelo navegador (porta $PORT na Railway)
        try:
            self.web = WebDashboard(self)
            self.web.start()
        except Exception as e:
            print(f"Web dashboard nao iniciado: {e}")

        await self.dashboard.run_async(self.okx_trader, self.check_signals)
        
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
      return `<span class="mkt"><span class="sym">${sym}</span> ${fmt(m.last)} <span class="${cls(ch)}">${ch==null?'':(ch>0?'+':'')+fmt(ch)+'%'}</span></span>`;
    }).join('');
    document.getElementById('traders').innerHTML=`<thead><tr><th>#</th><th>TRADER</th><th>PAR</th><th>BET</th><th>EXP</th><th>ST</th><th>WINS</th><th>LOSSES</th><th>WR%</th><th>P&L</th><th>STREAK</th></tr></thead><tbody>`+s.traders.map((t,i)=>`<tr><td>${i+1}</td><td>${t.name}</td><td>${t.pair}</td><td>$${fmt(t.bet_size)}</td><td>${t.expiration}s</td><td><span class="pill ${t.enabled?'on':'off'}">${t.enabled?'ON':'OFF'}</span></td><td class="pos">${t.wins}</td><td class="neg">${t.losses}</td><td class="${t.win_rate>=60?'pos':(t.win_rate>=50?'neu':'neg')}">${t.win_rate}%</td><td>${pnl(t.total_pnl)}</td><td class="${cls(t.current_streak)}">${t.current_streak?((t.current_streak>0?'+':'')+t.current_streak+' '+t.streak_type):'--'}</td></tr>`).join('')+`</tbody>`;
    document.getElementById('ops').innerHTML=`<thead><tr><th>HORA</th><th>TRADER</th><th>PAR</th><th>DIR</th><th>BET</th><th>PROB</th><th>RESULTADO</th><th>ST</th><th>P&L ACC</th></tr></thead><tbody>`+(s.operations.length?s.operations.slice().reverse().map(o=>`<tr><td>${o.timestamp}</td><td>${o.trader}</td><td>${o.pair}</td><td class="${o.direction==='CALL'?'pos':'neg'}">${o.direction==='CALL'?'▲ CALL':'▼ PUT'}</td><td>$${fmt(o.bet)}</td><td>${fmt(o.probability)}</td><td>${o.result==null?'--':pnl(o.result)}</td><td><span class="pill ${o.status==='WIN'?'win':(o.status==='LOSS'?'loss':'w8')}">${o.status==='Aguardando'?'AGUARDANDO':o.status}</span></td><td>${pnl(o.cumulative_pnl)}</td></tr>`).join(''):`<tr><td colspan="9" style="color:#64748b">Aguardando operacoes...</td></tr>`)+`</tbody>`;
    const pos=(s.positions||[]);
    document.getElementById('pos').innerHTML=`<thead><tr><th>TRADER</th><th>SIMBOLO</th><th>LADO</th><th>TAMANHO</th><th>ENTRADA</th><th>ABERTA EM</th></tr></thead><tbody>`+(pos.length?pos.map(p=>`<tr><td>${p.trader}</td><td>${p.symbol}</td><td class="${p.side==='buy'?'pos':'neg'}">${p.side==='buy'?'▲ COMPRA':'▼ VENDA'}</td><td>${p.size}</td><td>$${fmt(p.entry_price)}</td><td>${p.opened_at||'--'}</td></tr>`).join(''):`<tr><td colspan="6" style="color:#64748b">Nenhuma operacao aberta (aguardando sinal)</td></tr>`)+`</tbody>`;
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
        # Operacoes abertas (ordens reais aguardando fechamento)
        state["positions"] = []
        for nm, op in term._open.items():
            state["positions"].append({
                "trader": nm, "symbol": op.get("symbol"),
                "side": op.get("side"), "size": op.get("qty"),
                "entry_price": op.get("entry_px"),
                "opened_at": op.get("opened_at")
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