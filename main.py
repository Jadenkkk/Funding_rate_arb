import ccxt
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import random
import logging
import argparse
from tabulate import tabulate
import matplotlib.pyplot as plt
import seaborn as sns
import concurrent.futures  # 병렬 처리를 위해 추가
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of exchanges to scan
EXCHANGES = ["binanceusdm", "kucoinfutures", "bybit", "hyperliquid"]

# Fetch tickers for a given exchange
def fetch_tickers(exchange_id):
    """
    Fetch tickers for a given exchange.
    
    Args:
        exchange_id (str): The ID of the exchange to fetch tickers from.
        
    Returns:
        list: A list of ticker symbols for the given exchange.
    """
    exchange_client = getattr(ccxt, exchange_id)()
    
    if exchange_id == "krakenfutures":
        x = exchange_client.fetchTickers()
        return [y for y in x.keys() if y.split(":")[0][-4:] == "/USD"]
    elif exchange_id == "kucoinfutures":
        x = exchange_client.fetchMarkets()
        return [x['symbol'] for x in x if x['symbol'][-5:] == ":USDT"]
    elif exchange_id in ["okx", "gate"]:
        x = exchange_client.fetchTickers()
        return [y for y in x.keys() if y[-5:] == "/USDT"]
    elif exchange_id in ["bybit", "binanceusdm"]:
        x = exchange_client.fetchTickers()
        return [y for y in x.keys() if y[-5:] == ":USDT"]
    elif exchange_id == "hyperliquid":
        x = exchange_client.fetchMarkets()
        return [market['symbol'] for market in x if market['quote'] == 'USDC']

# OKX 거래소 전용 심볼 변환 함수 추가
def convert_symbol_for_okx(symbol):
    """OKX 거래소에 맞게 심볼 형식 변환"""
    # 슬래시를 하이픈으로 변환하고 '-SWAP' 추가
    if '/' in symbol:
        base, quote = symbol.split('/')
        return f"{base}-{quote}-SWAP"
    return symbol

# Fetch funding rate with retry mechanism for rate limiting.
def fetch_funding_rate_with_retry(client, raw_symbol, exchange_id, max_retries=5, initial_delay=1):
    """
    Fetch funding rate with retry mechanism for rate limiting.
    
    Args:
        client (ccxt.Exchange): The exchange client.
        raw_symbol (str): The raw symbol to fetch funding rate for.
        exchange_id (str): The ID of the exchange.
        max_retries (int): Maximum number of retries.
        initial_delay (float): Initial delay between retries in seconds.
        
    Returns:
        list: A list of funding rate data.
    """
    for attempt in range(max_retries):
        try:
            if exchange_id == "hyperliquid":
                return client.fetchFundingRateHistory(raw_symbol, limit=2)
            else:
                return client.fetchFundingRateHistory(raw_symbol, limit=2)
        except ccxt.RateLimitExceeded as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Rate limit exceeded for {exchange_id}, retrying in {delay:.2f} seconds...")
            time.sleep(delay)
        except Exception as e:
            # 상세 에러 메시지 숨기기 - DEBUG 레벨로 변경
            logger.debug(f"Error fetching {raw_symbol} on {exchange_id}: {str(e)}")
            return []

# Fetch funding rates for all symbols on a given exchange using parallel processing
def fetch_funding_rates(exchange_id, symbol_map, position=0):
    """
    Fetch funding rates for all symbols on a given exchange using parallel processing.
    
    Args:
        exchange_id (str): Exchange ID
        symbol_map (dict): Mapping of symbols
        position (int): Position for progress bar display
    """
    # Hyperliquid에 대한 특별 설정 (필요시)
    options = {}
    client = getattr(ccxt, exchange_id)({"options": options})
    
    data = []
    
    # 거래소별 처리할 심볼 필터링
    symbols_to_process = []
    for symbol, norm_to_raw in symbol_map.items():
        if exchange_id in norm_to_raw:
            symbols_to_process.append((symbol, norm_to_raw[exchange_id]))
    
    # 병렬 처리 제한 (hyperliquid에 대해 설정 조정)
    max_workers = 5 if exchange_id == "hyperliquid" else min(10, len(symbols_to_process))
    
    # 진행 상황 표시 및 카운터 초기화
    success_count = 0
    error_count = 0
    
    # 모든 거래소에 대한 진행 표시줄 미리 초기화 (위치 지정)
    with tqdm(
        total=len(symbols_to_process), 
        desc=f"{exchange_id}", 
        leave=True,
        position=position
    ) as progress_bar:
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for symbol, raw_symbol in symbols_to_process:
                futures.append(
                    executor.submit(
                        fetch_funding_rate_with_retry, client, raw_symbol, exchange_id
                    )
                )
                # Hyperliquid에 대해서만 요청 간 지연 추가 (지연 시간 감소)
                if exchange_id == "hyperliquid":
                    time.sleep(0.2)  # 0.2초로 지연 감소
            
            # 병렬 처리 결과 수집
            for future in concurrent.futures.as_completed(futures):
                try:
                    rates = future.result()
                    if rates:
                        # 펀딩률 데이터 추가
                        for r in rates:
                            r['exchange'] = exchange_id
                            # 정규화된 심볼 정보 찾기
                            idx = futures.index(future)
                            symbol, _ = symbols_to_process[idx]
                            r['norm_symbol'] = symbol
                            data.append(r)
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.debug(f"Error in future processing on {exchange_id}: {str(e)}")
                    error_count += 1
                
                # 진행 표시줄 업데이트
                progress_bar.update(1)
    
    # 이 메시지는 유지 (INFO 레벨)
    logger.info(f"{exchange_id}: {success_count} symbols processed successfully, {error_count} errors")
    return data

# Calculate arbitrage opportunities from funding rate data
def calculate_arbitrage_opportunities(data):
    """
    Calculate arbitrage opportunities from funding rate data.
    
    Args:
        data (pd.DataFrame): Funding rate data for all exchanges and symbols.
        
    Returns:
        pd.DataFrame: A DataFrame of arbitrage opportunities.
    """
    spreads = []
    for symbol, group in data.groupby('norm_symbol'):
        if len(group) < 2:
            continue
        for i, row1 in group.iterrows():
            for j, row2 in group.iterrows():
                if i < j:
                    spread = abs(row1['pctAnnualFundingRate'] - row2['pctAnnualFundingRate'])
                    spreads.append({
                        'symbol': symbol,
                        'exchange1': row1['exchange'],
                        'exchange2': row2['exchange'],
                        'rate1': row1['pctAnnualFundingRate'],
                        'rate2': row2['pctAnnualFundingRate'],
                        'spread': spread
                    })

    spreads_df = pd.DataFrame(spreads)
    opportunities = spreads_df[spreads_df['spread'] > 1.0]

    opportunities['long_exchange'] = opportunities.apply(lambda row: row['exchange1'] if row['rate1'] < row['rate2'] else row['exchange2'], axis=1)
    opportunities['short_exchange'] = opportunities.apply(lambda row: row['exchange2'] if row['rate1'] < row['rate2'] else row['exchange1'], axis=1)

    return opportunities.sort_values('spread', ascending=False)

# Visualize top arbitrage opportunities
def visualize_opportunities(opportunities, top_n=10):
    """
    Visualize top arbitrage opportunities.
    
    Args:
        opportunities (pd.DataFrame): DataFrame of arbitrage opportunities.
        top_n (int): Number of top opportunities to visualize.
    """
    top_opportunities = opportunities.head(top_n)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='symbol', y='spread', data=top_opportunities)
    plt.title(f'Top {top_n} Arbitrage Opportunities')
    plt.xlabel('Symbol')
    plt.ylabel('Spread (%)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('arbitrage_opportunities.png')
    logger.info("Visualization saved as 'arbitrage_opportunities.png'")

# Main function to run the arbitrage scanner
def main(min_spread=1.0, top_n=10, output_json=False):
    logger.info("Starting Crypto Arbitrage Scanner")
    
    # Fetch tickers using parallel processing
    all_tickers = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(EXCHANGES)) as executor:
        future_to_exchange = {executor.submit(fetch_tickers, exchange_id): exchange_id 
                             for exchange_id in EXCHANGES}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_exchange), 
                          total=len(EXCHANGES), desc="Fetching tickers"):
            exchange_id = future_to_exchange[future]
            try:
                all_tickers[exchange_id] = future.result()
            except Exception as exc:
                logger.error(f"{exchange_id} generated an exception: {exc}")
                all_tickers[exchange_id] = []

    # Process tickers
    unique_tickers = {}
    for tickers in all_tickers.values():
        for ticker in tickers:
            ticker = ticker.split("/")[0]
            unique_tickers[ticker] = unique_tickers.get(ticker, 0) + 1

    symbols = [ticker for ticker, count in unique_tickers.items() if count == len(EXCHANGES)]

    # Create symbol map
    symbol_map = {symbol: {exchange_id: next((tick for tick in all_tickers[exchange_id] if tick.split("/")[0] == symbol), None) 
                           for exchange_id in EXCHANGES} 
                  for symbol in symbols}

    logger.info(f'Total Common Assets found Across All Exchanges: {len(symbols)}')

    # 모든 거래소에 대해 동시에 작업 시작 (병렬 처리)
    all_data = []
    futures_dict = {}
    
    # 화면 지우기 및 커서 위치 재설정 (진행 표시줄이 제대로 표시되도록)
    print("\033[2J\033[H", end="")  # 화면 지우기 및 커서 위치 초기화
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(EXCHANGES)) as executor:
        # 각 거래소에 대한 작업 동시 제출
        for i, exchange_id in enumerate(EXCHANGES):
            # 이 메시지를 INFO 레벨로 복원
            logger.info(f"Starting to process {exchange_id}")
            futures_dict[exchange_id] = executor.submit(
                fetch_funding_rates, exchange_id, symbol_map, position=i
            )
        
        # 모든 거래소의 작업 완료 대기
        for exchange_id, future in futures_dict.items():
            try:
                exchange_data = future.result()  # 여기서 작업 완료까지 대기
                all_data.extend(exchange_data)
                # 이 메시지는 계속 숨김
                logger.debug(f"Successfully fetched {len(exchange_data)} records from {exchange_id}")
            except Exception as exc:
                logger.warning(f"{exchange_id} generated an exception: {exc}")
    
    # 데이터 검증
    if len(all_data) == 0:
        logger.warning("No data collected. Exiting.")
        return
    
    # Process data
    data = pd.DataFrame(all_data)
    data = data.drop('info', axis=1, errors='ignore')
    data = data.sort_values(['exchange', 'norm_symbol', 'timestamp'])

    # Calculate intervals
    data['interval_hours'] = data.groupby(['exchange', 'norm_symbol'])['timestamp'].diff() / (1000 * 60 * 60)
    data['interval_hours'] = data['interval_hours'].fillna(0).round()

    # Keep only the latest data for each exchange and symbol
    data = data.groupby(['exchange', 'norm_symbol']).last().reset_index()

    # Calculate rates
    data['annual_adj_mult'] = 365 * 24 / data['interval_hours'].replace(0, 24)
    data['annualFundingRate'] = data['fundingRate'] * data['annual_adj_mult']
    data['pctAnnualFundingRate'] = data['annualFundingRate'] * 100

    # Calculate arbitrage opportunities - Pandas 경고 수정
    opportunities = calculate_arbitrage_opportunities(data)
    opportunities_copy = opportunities.copy()
    filtered_opportunities = opportunities_copy[opportunities_copy['spread'] >= min_spread]
    filtered_copy = filtered_opportunities.copy()
    
    # DeprecationWarning 해결
    highest_spreads = filtered_copy.groupby('symbol', as_index=False).apply(
        lambda x: x.loc[x['spread'].idxmax()]
    ).reset_index(drop=True)
    
    # 복사본 생성 후 .loc[] 사용으로 경고 제거
    highest_spreads.loc[:, 'long_exchange'] = highest_spreads.apply(
        lambda row: row['exchange1'] if row['rate1'] < row['rate2'] else row['exchange2'], 
        axis=1
    )
    
    highest_spreads.loc[:, 'short_exchange'] = highest_spreads.apply(
        lambda row: row['exchange2'] if row['rate1'] < row['rate2'] else row['exchange1'], 
        axis=1
    )
    
    # Display top opportunities
    top_opportunities = highest_spreads.sort_values('spread', ascending=False).head(top_n)
    
    # Add rank column and format APY
    top_opportunities = top_opportunities.reset_index(drop=True)
    top_opportunities.index = top_opportunities.index + 1  # Start index at 1
    
    # Create display dataframe with formatted columns
    display_df = top_opportunities[['symbol', 'long_exchange', 'short_exchange', 'spread']].copy()
    display_df.columns = ['Symbol', 'Long Exchange', 'Short Exchange', 'APY']
    display_df['APY'] = display_df['APY'].apply(lambda x: f"{x:.2f}%")
    
    # 인덱스에 'Rank'라는 이름 추가
    print("\nTop Arbitrage Opportunities:")
    table = tabulate(display_df, headers='keys', tablefmt='pretty', showindex=True)
    # 'Rank' 레이블 추가
    table_lines = table.split('\n')
    header_line = table_lines[0].replace('  ', 'Rank', 1)
    table_lines[0] = header_line
    print('\n'.join(table_lines))

    # Visualize opportunities
    visualize_opportunities(highest_spreads, top_n)
    
    # JSON 출력 옵션 추가
    if output_json:
        # 결과를 JSON으로 변환하기 위한 데이터 준비
        json_data = []
        for i, row in top_opportunities.reset_index().iterrows():
            json_data.append({
                "rank": i + 1,
                "symbol": row['symbol'],
                "long_exchange": row['long_exchange'],
                "short_exchange": row['short_exchange'],
                "apy": f"{row['spread']:.2f}%"
            })
        
        # JSON 파일로 저장
        with open("arbitrage_data.json", "w") as f:
            json.dump(json_data, f)

    logger.info("Crypto Arbitrage Scanner completed successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Arbitrage Scanner")
    parser.add_argument("--min_spread", type=float, default=1.0, help="Minimum spread to consider (default: 1.0)")
    parser.add_argument("--top_n", type=int, default=10, help="Number of top opportunities to display (default: 10)")
    parser.add_argument("--output_json", type=str, default="false", help="Output results as JSON (default: false)")
    args = parser.parse_args()
    
    output_json = args.output_json.lower() == "true"
    main(args.min_spread, args.top_n, output_json)