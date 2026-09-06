"""
Orderflow Engine V2 — Footprint real via trades com lado agressor (OKX).

Diferente da V1 (snapshot do order book), esta engine acumula TRADES EXECUTADOS
com o lado do taker (campo `side` do WebSocket de trades da OKX) dentro de cada
candle de 1 minuto, formando o footprint verdadeiro:

    por preco P no candle: buy_vol[P] = volume agressor COMPRADOR (lifted offer)
                           sell_vol[P] = volume agressor VENDEDOR (hit the bid)

Detecao de stacked imbalance (definicao profissional, diagonal — TapeDelta/Exocharts):
    nivel com dominancia COMPRADORA  se buy_vol[P]  / sell_vol[P-tick] >= threshold
    nivel com dominancia VENDEDORA   se sell_vol[P] / buy_vol[P+tick]  >= threshold
    zona (stack) = min_levels+ niveis consecutivos com a MESMA dominancia.
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

try:
    import websockets
except ImportError:
    websockets = None


@dataclass
class FootprintCandle:
    symbol: str
    ts_start: int                 # epoch s do inicio do candle
    footprint: Dict[float, Dict[str, float]] = field(default_factory=dict)
    trades: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0

    def add_trade(self, px: float, sz: float, side: str):
        px = round(px, 8)
        cell = self.footprint.setdefault(px, {"buy": 0.0, "sell": 0.0})
        if side == "buy":
            cell["buy"] += sz
            self.buy_vol += sz
        else:
            cell["sell"] += sz
            self.sell_vol += sz
        self.trades += 1
        if self.open == 0:
            self.open = self.high = self.low = px
        self.high = max(self.high, px)
        self.low = min(self.low, px)
        self.close = px

    def delta(self) -> float:
        return self.buy_vol - self.sell_vol


@dataclass
class StackedZone:
    symbol: str
    direction: str                # "buy" | "sell"
    price_min: float
    price_max: float
    levels: int
    avg_ratio: float
    ts_start: int                 # candle onde foi detectada
    mid_price: float = 0.0
    # area da zona (para stop/alvo)
    @property
    def zone_center(self) -> float:
        return (self.price_min + self.price_max) / 2


def detect_stacked_zones(candle: FootprintCandle,
                         threshold: float = 2.5,
                         min_levels: int = 3,
                         min_denom_frac: float = 0.01) -> List[StackedZone]:
    """Detecta zonas de stacked imbalance no footprint de UM candle fechado.

    min_denom_frac: fracao do volume total do candle exigida no denominador da
    razao (evita ratios absurdos tipo 758x gerados por 1 trade minusculo).
    """
    if not candle or candle.trades < 10 or len(candle.footprint) < min_levels:
        return []
    vol_total = candle.buy_vol + candle.sell_vol
    min_denom = max(vol_total * min_denom_frac, 1e-9)
    prices = sorted(candle.footprint.keys())
    tick = _estimate_tick(prices)
    zones: List[StackedZone] = []

    buy_dom = {}   # preco -> ratio buy_vol[P] / sell_vol[P-tick]
    sell_dom = {}  # preco -> ratio sell_vol[P] / buy_vol[P+tick]

    for i, p in enumerate(prices):
        cell = candle.footprint[p]
        # diagonal: compra em P vs venda no tick abaixo (P - tick)
        below = _find_price(prices, p - tick)
        if below is not None:
            sell_below = candle.footprint[below]["sell"]
            if sell_below >= min_denom:
                r = cell["buy"] / sell_below
                if r >= threshold:
                    buy_dom[p] = r
        # diagonal: venda em P vs compra no tick acima (P + tick)
        above = _find_price(prices, p + tick)
        if above is not None:
            buy_above = candle.footprint[above]["buy"]
            if buy_above >= min_denom:
                r = cell["sell"] / buy_above
                if r >= threshold:
                    sell_dom[p] = r

    # runs consecutivas por preco adjacente (usando a grade real de precos)
    zones += _runs_to_zones(candle, "buy", buy_dom, prices, min_levels)
    zones += _runs_to_zones(candle, "sell", sell_dom, prices, min_levels)
    return zones


def _estimate_tick(prices: List[float]) -> float:
    if len(prices) < 2:
        return 0.1
    diffs = [round(b - a, 8) for a, b in zip(prices, prices[1:]) if b > a]
    if not diffs:
        return 0.1
    from collections import Counter
    return Counter(diffs).most_common(1)[0][0]


def _find_price(prices: List[float], target: float) -> Optional[float]:
    """Devolve o preco mais proximo dentro de 25% do tick (grade continua)."""
    if not prices:
        return None
    import bisect
    i = bisect.bisect_left(prices, target)
    best = None
    for idx in (i - 1, i):
        if 0 <= idx < len(prices):
            if best is None or abs(prices[idx] - target) < abs(best - target):
                best = prices[idx]
    if best is not None and abs(best - target) <= max(target * 1e-6, 1e-8):
        return best
    return best if best is not None and abs(best - target) < abs(target) * 5e-5 else None


def _runs_to_zones(candle, direction, dom_map, prices, min_levels) -> List[StackedZone]:
    """Converte niveis dominantes consecutivos (na grade de precos) em zonas."""
    zones = []
    if not dom_map:
        return zones
    # percorre os precos em ordem; um nivel pertence a run se dom_map[p] existe
    run: List[float] = []
    prev = None
    for p in prices:
        if p in dom_map:
            if prev is not None and abs(p - prev) > _estimate_tick(prices) * 1.5:
                run = []  # quebrou continuidade da grade
            run.append(p)
        else:
            if len(run) >= min_levels:
                zones.append(_make_zone(candle, direction, dom_map, run))
            run = []
        prev = p
    if len(run) >= min_levels:
        zones.append(_make_zone(candle, direction, dom_map, run))
    return zones


def _make_zone(candle, direction, dom_map, run) -> StackedZone:
    ratios = [dom_map[p] for p in run]
    return StackedZone(
        symbol=candle.symbol,
        direction=direction,
        price_min=min(run),
        price_max=max(run),
        levels=len(run),
        avg_ratio=round(sum(ratios) / len(ratios), 2),
        ts_start=candle.ts_start,
        mid_price=candle.close,
    )


class FootprintFeed:
    """Mantem WebSocket de trades (4 simbolos) e acumula candles de 1min."""

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(self, symbols: List[str], candle_seconds: int = 60,
                 on_candle_close: Optional[Callable] = None):
        self.symbols = symbols
        self.candle_seconds = candle_seconds
        self.on_candle_close = on_candle_close
        self.current: Dict[str, FootprintCandle] = {}
        self.closed: Dict[str, FootprintCandle] = {}   # ultimo candle fechado
        self.last_price: Dict[str, float] = {}
        self.last_ts: Dict[str, int] = {}
        self.close_history: Dict[str, List[float]] = {}  # closes p/ EMA de tendencia
        self._ws = None
        self._running = False

    def _candle_key(self, ts_s: int) -> int:
        return (ts_s // self.candle_seconds) * self.candle_seconds

    async def _rollover(self):
        """Verifica mudanca de candle e promove current -> closed."""
        for sym in self.symbols:
            now_key = self._candle_key(int(time.time()))
            cur = self.current.get(sym)
            if cur is not None and cur.ts_start != now_key:
                self.closed[sym] = cur
                self.close_history.setdefault(sym, []).append(cur.close)
                if len(self.close_history[sym]) > 500:
                    self.close_history[sym] = self.close_history[sym][-500:]
                if self.on_candle_close:
                    try:
                        res = self.on_candle_close(cur)
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception as e:
                        print(f"[engine] erro no on_candle_close de {sym}: {e}")
                self.current[sym] = FootprintCandle(symbol=sym, ts_start=now_key)

    async def run(self):
        if websockets is None:
            raise RuntimeError("websockets nao instalado")
        self._running = True
        args = [{"channel": "trades", "instId": s} for s in self.symbols]
        while self._running:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    # inicia candles se necessario
                    for sym in self.symbols:
                        k = self._candle_key(int(time.time()))
                        if sym not in self.current:
                            self.current[sym] = FootprintCandle(symbol=sym, ts_start=k)
                    while self._running:
                        # rollover periodico + leitura com timeout curto
                        await self._rollover()
                        try:
                            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
                        except asyncio.TimeoutError:
                            continue
                        if msg.get("event"):
                            continue
                        for d in msg.get("data") or []:
                            sym = d.get("instId")
                            if sym not in self.current:
                                continue
                            px = float(d.get("px") or 0)
                            sz = float(d.get("sz") or 0)
                            side = d.get("side")
                            if px <= 0 or sz <= 0 or side not in ("buy", "sell"):
                                continue
                            self.last_price[sym] = px
                            self.last_ts[sym] = int(d.get("ts") or 0)
                            self.current[sym].add_trade(px, sz, side)
            except Exception as e:
                if self._running:
                    await asyncio.sleep(3)  # reconecta

    def stop(self):
        self._running = False

    def ema(self, symbol: str, period: int = 20) -> Optional[float]:
        """EMA simples dos closes de candles 1m (tendencia de curto prazo)."""
        closes = self.close_history.get(symbol) or []
        if len(closes) < period:
            return None
        k = 2.0 / (period + 1)
        ema = closes[0]
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)
        return ema

    def state(self) -> Dict:
        """Estado compacto p/ dashboard."""
        out = {}
        for sym in self.symbols:
            cur = self.current.get(sym)
            out[sym] = {
                "price": self.last_price.get(sym),
                "candle_trades": cur.trades if cur else 0,
                "candle_buy": round(cur.buy_vol, 6) if cur else 0,
                "candle_sell": round(cur.sell_vol, 6) if cur else 0,
                "delta": round(cur.delta(), 6) if cur else 0,
            }
        return out
