"""
Funding Rates Fetcher - Fixed Version
Fetches funding rates from 9 exchanges for top 20 coins by market cap.
"""

import json
import requests
from datetime import datetime

DATA_FILE = 'data.json'

# Top 20 coins by market cap
COINS = [
    'BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'TRX',
    'TON', 'SUI', 'XLM', 'HBAR', 'DOT', 'BCH', 'LTC', 'UNI', 'APT', 'NEAR'
]


def fetch_binance():
    """Fetch funding rates from Binance using premiumIndex endpoint"""
    rates = {}
    try:
        # Use premiumIndex which returns current funding rate for all symbols
        url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        for item in data:
            symbol = item.get('symbol', '')
            for coin in COINS:
                if symbol == f'{coin}USDT':
                    rate = float(item.get('lastFundingRate', 0))
                    rates[coin] = rate
                    break
        
        print(f"  Binance: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Binance error: {e}")
    
    return rates


def fetch_bybit():
    """Fetch funding rates from Bybit v5 API"""
    rates = {}
    try:
        url = 'https://api.bybit.com/v5/market/tickers?category=linear'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get('retCode') == 0:
            for item in data.get('result', {}).get('list', []):
                symbol = item.get('symbol', '')
                for coin in COINS:
                    if symbol == f'{coin}USDT':
                        rate_str = item.get('fundingRate', '0')
                        if rate_str:
                            rate = float(rate_str)
                            rates[coin] = rate
                        break
        
        print(f"  Bybit: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Bybit error: {e}")
    
    return rates


def fetch_okx():
    """Fetch funding rates from OKX"""
    rates = {}
    try:
        # OKX can fetch all funding rates at once
        url = 'https://www.okx.com/api/v5/public/funding-rate?instId='
        
        for coin in COINS:
            try:
                inst_url = f'https://www.okx.com/api/v5/public/funding-rate?instId={coin}-USDT-SWAP'
                response = requests.get(inst_url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == '0' and data.get('data'):
                    rate_str = data['data'][0].get('fundingRate', '0')
                    if rate_str:
                        rate = float(rate_str)
                        rates[coin] = rate
            except:
                continue
        
        print(f"  OKX: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  OKX error: {e}")
    
    return rates


def fetch_bitget():
    """Fetch funding rates from Bitget"""
    rates = {}
    try:
        url = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == '00000':
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                for coin in COINS:
                    if symbol == f'{coin}USDT':
                        rate_str = item.get('fundingRate', '0')
                        if rate_str:
                            rate = float(rate_str)
                            rates[coin] = rate
                        break
        
        print(f"  Bitget: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Bitget error: {e}")
    
    return rates


def fetch_gate():
    """Fetch funding rates from Gate.io"""
    rates = {}
    try:
        url = 'https://api.gateio.ws/api/v4/futures/usdt/contracts'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        for item in data:
            name = item.get('name', '')
            for coin in COINS:
                if name == f'{coin}_USDT':
                    rate_str = item.get('funding_rate', '0')
                    if rate_str:
                        rate = float(rate_str)
                        rates[coin] = rate
                    break
        
        print(f"  Gate.io: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Gate.io error: {e}")
    
    return rates


def fetch_kucoin():
    """Fetch funding rates from KuCoin"""
    rates = {}
    try:
        url = 'https://api-futures.kucoin.com/api/v1/contracts/active'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == '200000':
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                for coin in COINS:
                    if symbol == f'{coin}USDTM':
                        rate_str = item.get('fundingFeeRate', '0')
                        if rate_str:
                            rate = float(rate_str)
                            rates[coin] = rate
                        break
        
        print(f"  KuCoin: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  KuCoin error: {e}")
    
    return rates


def fetch_mexc():
    """Fetch funding rates from MEXC"""
    rates = {}
    try:
        url = 'https://contract.mexc.com/api/v1/contract/funding_rate'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                for coin in COINS:
                    if symbol == f'{coin}_USDT':
                        rate_str = item.get('fundingRate', '0')
                        if rate_str:
                            rate = float(rate_str)
                            rates[coin] = rate
                        break
        
        print(f"  MEXC: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  MEXC error: {e}")
    
    return rates


def fetch_hyperliquid():
    """Fetch funding rates from Hyperliquid (1h funding, convert to 8h)"""
    rates = {}
    try:
        url = 'https://api.hyperliquid.xyz/info'
        payload = {"type": "metaAndAssetCtxs"}
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        
        if len(data) >= 2:
            meta = data[0]
            contexts = data[1]
            
            for i, asset in enumerate(meta.get('universe', [])):
                coin = asset.get('name', '')
                if coin in COINS and i < len(contexts):
                    # Hyperliquid has 1h funding, multiply by 8 for 8h equivalent
                    rate_str = contexts[i].get('funding', '0')
                    if rate_str:
                        rate = float(rate_str) * 8
                        rates[coin] = rate
        
        print(f"  Hyperliquid: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Hyperliquid error: {e}")
    
    return rates


def fetch_lighter():
    """Fetch funding rates from Lighter (1h funding, convert to 8h)"""
    rates = {}
    try:
        url = 'https://mainnet.zklighter.elliot.ai/api/v1/funding-rates'
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Handle different response formats
        funding_data = data.get('funding_rates', data) if isinstance(data, dict) else data
        
        if isinstance(funding_data, list):
            for item in funding_data:
                symbol = str(item.get('symbol', item.get('market', ''))).upper()
                for coin in COINS:
                    if coin in symbol:
                        rate_str = item.get('funding_rate', item.get('fundingRate', '0'))
                        if rate_str:
                            # Lighter has 1h funding, multiply by 8 for 8h equivalent
                            rate = float(rate_str) * 8
                            rates[coin] = rate
                        break
        
        print(f"  Lighter: {len(rates)} coins fetched")
    except Exception as e:
        print(f"  Lighter error: {e}")
    
    return rates


def calculate_sentiment(avg_rate):
    """Determine market sentiment based on funding rate"""
    if avg_rate > 0.0005:  # > 0.05%
        return 'bullish'
    elif avg_rate < -0.0005:  # < -0.05%
        return 'bearish'
    else:
        return 'neutral'


def main():
    print("=" * 50)
    print("Funding Rates Fetcher - Fixed Version")
    print("=" * 50)
    
    # Fetch from all exchanges
    print("\nFetching funding rates...")
    
    all_rates = {
        'binance': fetch_binance(),
        'bybit': fetch_bybit(),
        'okx': fetch_okx(),
        'bitget': fetch_bitget(),
        'gate': fetch_gate(),
        'kucoin': fetch_kucoin(),
        'mexc': fetch_mexc(),
        'hyperliquid': fetch_hyperliquid(),
        'lighter': fetch_lighter()
    }
    
    # Build coin data with averages
    coins_data = []
    
    for coin in COINS:
        coin_rates = {}
        valid_rates = []
        
        for exchange, rates in all_rates.items():
            if coin in rates:
                rate = rates[coin]
                coin_rates[exchange] = rate
                valid_rates.append(rate)
            else:
                coin_rates[exchange] = None
        
        avg_rate = sum(valid_rates) / len(valid_rates) if valid_rates else 0
        sentiment = calculate_sentiment(avg_rate)
        
        coins_data.append({
            'coin': coin,
            'rates': coin_rates,
            'average': avg_rate,
            'sentiment': sentiment
        })
    
    # Calculate sentiment summary
    bullish_count = sum(1 for c in coins_data if c['sentiment'] == 'bullish')
    bearish_count = sum(1 for c in coins_data if c['sentiment'] == 'bearish')
    neutral_count = sum(1 for c in coins_data if c['sentiment'] == 'neutral')
    
    # Build output
    output = {
        'coins': coins_data,
        'summary': {
            'bullish': bullish_count,
            'bearish': bearish_count,
            'neutral': neutral_count
        },
        'exchanges': [
            {'id': 'binance', 'name': 'Binance', 'type': 'cex'},
            {'id': 'bybit', 'name': 'Bybit', 'type': 'cex'},
            {'id': 'okx', 'name': 'OKX', 'type': 'cex'},
            {'id': 'bitget', 'name': 'Bitget', 'type': 'cex'},
            {'id': 'gate', 'name': 'Gate.io', 'type': 'cex'},
            {'id': 'kucoin', 'name': 'KuCoin', 'type': 'cex'},
            {'id': 'mexc', 'name': 'MEXC', 'type': 'cex'},
            {'id': 'hyperliquid', 'name': 'Hyperliquid', 'type': 'dex'},
            {'id': 'lighter', 'name': 'Lighter', 'type': 'dex'}
        ],
        'lastUpdated': datetime.utcnow().isoformat() + 'Z'
    }
    
    # Print summary
    print(f"\n시장 심리 요약:")
    print(f"  🟢 강세: {bullish_count}")
    print(f"  ⚪ 중립: {neutral_count}")
    print(f"  🔴 약세: {bearish_count}")
    
    # Print sample data
    print(f"\n샘플 데이터:")
    for coin_data in coins_data[:3]:
        print(f"  {coin_data['coin']}: 평균 {coin_data['average']*100:.4f}% ({coin_data['sentiment']})")
    
    # Save to JSON
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved to {DATA_FILE}")


if __name__ == '__main__':
    main()
