import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


def calculate_stacked_imbalance(
    df: pd.DataFrame,
    threshold_ratio: float = 3.0,
    min_stack: int = 3,
    price_col: str = 'price',
    bid_col: str = 'bid_size',
    ask_col: str = 'ask_size',
    identifier_col: str = 'identifier'
) -> pd.DataFrame:
    """
    Calculate Stacked Imbalance for footprint data.
    
    Logic:
    - For each price level in a candle, compare aggressive volume at that price
      vs passive volume at adjacent price (diagonal comparison)
    - Buy Imbalance: ask_size at price P > threshold * bid_size at price P-1 (tick below)
    - Sell Imbalance: bid_size at price P > threshold * ask_size at price P+1 (tick above)
    - Stacked: min_stack+ consecutive price levels with same direction imbalance
    
    Returns DataFrame with new columns:
    - 'imbalance_type': 'buy', 'sell', 'none'
    - 'imbalance_ratio': calculated ratio
    - 'stacked': True/False
    - 'stack_id': group id for stacked sequences
    """
    df = df.copy()
    
    # Ensure identifier is a column, not index
    if identifier_col in df.index.names:
        df = df.reset_index()
    
    # Remove duplicate identifier column if exists
    if identifier_col in df.columns and df.index.name == identifier_col:
        df = df.reset_index(drop=True)
    
    # Add result columns
    for col in ['imbalance_type', 'imbalance_ratio', 'stacked', 'stack_id']:
        if col not in df.columns:
            if col == 'imbalance_type':
                df[col] = 'none'
            elif col == 'imbalance_ratio':
                df[col] = 0.0
            elif col == 'stacked':
                df[col] = False
            elif col == 'stack_id':
                df[col] = 0
    
    if df.empty:
        return df
    
    # Process per candle (identifier)
    for identifier in df[identifier_col].unique():
        mask = df[identifier_col] == identifier
        candle_df = df.loc[mask].sort_values(price_col).reset_index(drop=True)
        
        if len(candle_df) < min_stack:
            continue
        
        prices = candle_df[price_col].values
        bids = candle_df[bid_col].values
        asks = candle_df[ask_col].values
        
        # Calculate tick size (assume uniform)
        tick_size = np.median(np.diff(prices)) if len(prices) > 1 else 1.0
        
        # Diagonal comparison: ask at P vs bid at P-tick (buy imbalance)
        #                          bid at P vs ask at P+tick (sell imbalance)
        buy_ratios = np.zeros(len(candle_df))
        sell_ratios = np.zeros(len(candle_df))
        
        for i in range(len(candle_df)):
            p = prices[i]
            
            # Buy: aggressive buyers (ask) at P vs passive sellers (bid) at P-tick
            idx_below = np.where(np.abs(prices - (p - tick_size)) < tick_size * 0.5)[0]
            if len(idx_below) > 0 and bids[idx_below[0]] > 0:
                buy_ratios[i] = asks[i] / bids[idx_below[0]]
            
            # Sell: aggressive sellers (bid) at P vs passive buyers (ask) at P+tick
            idx_above = np.where(np.abs(prices - (p + tick_size)) < tick_size * 0.5)[0]
            if len(idx_above) > 0 and asks[idx_above[0]] > 0:
                sell_ratios[i] = bids[i] / asks[idx_above[0]]
        
        # Determine imbalance type per level
        imbalance_type = np.full(len(candle_df), 'none', dtype=object)
        imbalance_ratio = np.zeros(len(candle_df))
        
        buy_mask = buy_ratios >= threshold_ratio
        sell_mask = sell_ratios >= threshold_ratio
        
        imbalance_type[buy_mask] = 'buy'
        imbalance_type[sell_mask] = 'sell'
        imbalance_ratio[buy_mask] = buy_ratios[buy_mask]
        imbalance_ratio[sell_mask] = sell_ratios[sell_mask]
        
        # Find stacked sequences (consecutive same direction)
        stacked = np.zeros(len(candle_df), dtype=bool)
        stack_id = np.zeros(len(candle_df), dtype=int)
        current_stack = 0
        
        for direction in ['buy', 'sell']:
            dir_mask = imbalance_type == direction
            if not dir_mask.any():
                continue
            
            # Find consecutive runs
            run_start = None
            run_len = 0
            
            for i in range(len(candle_df) + 1):  # +1 to flush at end
                if i < len(candle_df) and dir_mask[i]:
                    if run_start is None:
                        run_start = i
                    run_len += 1
                else:
                    if run_len >= min_stack:
                        current_stack += 1
                        stacked[run_start:run_start + run_len] = True
                        stack_id[run_start:run_start + run_len] = current_stack
                    run_start = None
                    run_len = 0
        
        # Write back to main dataframe
        df.loc[mask, 'imbalance_type'] = imbalance_type
        df.loc[mask, 'imbalance_ratio'] = imbalance_ratio
        df.loc[mask, 'stacked'] = stacked
        df.loc[mask, 'stack_id'] = stack_id
    
    return df


def get_stacked_summary(df: pd.DataFrame, identifier_col: str = 'identifier') -> pd.DataFrame:
    """Get summary of stacked imbalances per candle."""
    stacked_df = df[df['stacked'] == True].copy()
    if stacked_df.empty:
        return pd.DataFrame(columns=[identifier_col, 'stack_id', 'direction', 'price_min', 'price_max', 'levels', 'avg_ratio'])
    
    summary = stacked_df.groupby([identifier_col, 'stack_id']).agg(
        direction=('imbalance_type', 'first'),
        price_min=('price', 'min'),
        price_max=('price', 'max'),
        levels=('price', 'count'),
        avg_ratio=('imbalance_ratio', 'mean')
    ).reset_index()
    
    return summary


def enrich_orderflow_for_plotting(
    df: pd.DataFrame,
    threshold_ratio: float = 3.0,
    min_stack: int = 3
) -> pd.DataFrame:
    """
    Enrich orderflow data with stacked imbalance markers for plotting.
    Adds columns that OrderFlowChart can use for coloring.
    """
    df = calculate_stacked_imbalance(df, threshold_ratio, min_stack)
    
    # Create visual marker columns
    df['stacked_buy'] = (df['stacked'] & (df['imbalance_type'] == 'buy')).astype(int)
    df['stacked_sell'] = (df['stacked'] & (df['imbalance_type'] == 'sell')).astype(int)
    df['stacked_any'] = df['stacked'].astype(int)
    
    # For heatmap text annotation
    def format_stack_text(row):
        if row['stacked_buy']:
            return f"UP {row['imbalance_ratio']:.1f}x"
        elif row['stacked_sell']:
            return f"DN {row['imbalance_ratio']:.1f}x"
        elif row['imbalance_type'] != 'none':
            return f"{row['imbalance_type'][:1].upper()} {row['imbalance_ratio']:.1f}x"
        return ""
    
    df['stack_text'] = df.apply(format_stack_text, axis=1)
    
    return df


if __name__ == "__main__":
    # Quick test with synthetic data
    np.random.seed(42)
    n_levels = 20
    base_price = 50000
    tick = 0.5
    
    # Create synthetic footprint with stacked buy imbalance at top
    prices = np.array([base_price + i * tick for i in range(n_levels)])
    bids = np.random.randint(10, 100, n_levels).astype(float)
    asks = np.random.randint(10, 100, n_levels).astype(float)
    
    # Inject stacked buy imbalance at prices 5-9 (5 levels)
    for i in range(5, 10):
        asks[i] = bids[i-1] * 4.0  # 4x threshold
    
    # Inject stacked sell imbalance at prices 12-15 (4 levels)
    for i in range(12, 16):
        bids[i] = asks[i+1] * 3.5  # 3.5x threshold
    
    test_df = pd.DataFrame({
        'price': prices,
        'bid_size': bids,
        'ask_size': asks,
        'identifier': 'test_candle'
    })
    
    result = enrich_orderflow_for_plotting(test_df)
    print("=== Stacked Imbalance Test ===")
    print(result[['price', 'bid_size', 'ask_size', 'imbalance_type', 'imbalance_ratio', 'stacked', 'stack_text']].to_string())
    print("\n=== Summary ===")
    print(get_stacked_summary(result))