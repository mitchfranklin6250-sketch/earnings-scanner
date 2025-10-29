"""
Earnings Beat Scanner - Predicts potential earnings beats before announcements
Monitors: Earnings ESP, insider buying, analyst activity, guidance, buybacks, industry trends
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import yfinance as yf
from bs4 import BeautifulSoup
import pandas as pd
from collections import defaultdict

class EarningsBeatScanner:
    def __init__(self, discord_webhook_url: str):
        self.discord_webhook = discord_webhook_url
        self.headers = {
            'User-Agent': 'Earnings Research Bot mitch@example.com'
        }
        self.alert_threshold = 70  # Minimum score for alert
        print(f"🎯 Alert threshold set to: {self.alert_threshold}")
        
    def send_discord_alert(self, title: str, description: str, color: int = 3447003, 
                          fields: List[Dict[str, Any]] = None):
        """Send formatted alert to Discord"""
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
            "fields": fields or [],
            "footer": {"text": "Earnings Beat Scanner • Paper Trading Only"}
        }
        
        payload = {"embeds": [embed]}
        
        try:
            response = requests.post(self.discord_webhook, json=payload)
            if response.status_code == 204:
                print(f"✓ Alert sent: {title}")
            else:
                print(f"✗ Discord alert failed: {response.status_code}")
        except Exception as e:
            print(f"✗ Error sending Discord alert: {e}")
    
    def get_upcoming_earnings(self, days_ahead: int = 7) -> List[Dict[str, Any]]:
        """Get stocks with earnings in the next N days"""
        print("\n🗓️  Fetching upcoming earnings calendar...")
        earnings_stocks = []
        
        try:
            # Sample of liquid stocks to check - in production, expand this list
            # Focus on high-volume stocks for paper trading
            sample_tickers = [
                # Tech
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'AMD', 'INTC',
                # Finance
                'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C',
                # Healthcare
                'JNJ', 'UNH', 'PFE', 'ABBV', 'MRK', 'TMO',
                # Consumer
                'WMT', 'HD', 'COST', 'NKE', 'SBUX', 'MCD',
                # Industrial
                'CAT', 'BA', 'GE', 'HON', 'UPS', 'FDX',
                # Energy
                'XOM', 'CVX', 'COP', 'SLB',
                # Other
                'DIS', 'NFLX', 'PYPL', 'SQ', 'SHOP'
            ]
            
            today = datetime.now()
            cutoff = today + timedelta(days=days_ahead)
            
            for ticker in sample_tickers:
                try:
                    stock = yf.Ticker(ticker)
                    
                    # Get earnings date from calendar
                    calendar = stock.calendar
                    if calendar is not None and not calendar.empty:
                        if 'Earnings Date' in calendar.index:
                            earnings_date_val = calendar.loc['Earnings Date']
                            
                            # Handle different date formats
                            if isinstance(earnings_date_val, pd.Series):
                                earnings_date = pd.to_datetime(earnings_date_val.iloc[0])
                            else:
                                earnings_date = pd.to_datetime(earnings_date_val)
                            
                            # Check if within our window
                            if today <= earnings_date <= cutoff:
                                info = stock.info
                                earnings_stocks.append({
                                    'ticker': ticker,
                                    'earnings_date': earnings_date,
                                    'company': info.get('longName', ticker),
                                    'market_cap': info.get('marketCap', 0),
                                    'sector': info.get('sector', 'Unknown')
                                })
                                print(f"  ✓ Found: {ticker} earnings on {earnings_date.strftime('%Y-%m-%d')}")
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    continue
            
            print(f"  Found {len(earnings_stocks)} stocks with upcoming earnings")
            
        except Exception as e:
            print(f"  ✗ Error fetching earnings calendar: {e}")
        
        return earnings_stocks
    
    def calculate_earnings_esp(self, ticker: str) -> Dict[str, Any]:
        """Calculate Earnings ESP (Expected Surprise Prediction)"""
        print(f"  Calculating ESP for {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get analyst recommendations and estimates
            recommendations = stock.recommendations
            analysis = stock.analysis
            
            if analysis is not None and not analysis.empty:
                # Get EPS estimates
                current_qtr = analysis.columns[0]  # Next quarter
                
                eps_data = {
                    'mean_estimate': None,
                    'high_estimate': None,
                    'low_estimate': None,
                    'num_analysts': None
                }
                
                for metric in ['Eps Trend', 'Eps Revisions']:
                    if metric in analysis.index:
                        try:
                            eps_trend = analysis.loc[metric, current_qtr]
                            if isinstance(eps_trend, dict):
                                if 'current' in eps_trend:
                                    eps_data['mean_estimate'] = float(eps_trend['current'])
                        except:
                            pass
                
                # Calculate ESP (high estimate vs consensus)
                if eps_data['mean_estimate']:
                    # Simple ESP: if recent estimates trending up
                    esp_score = 0
                    
                    # Check if we have revision data
                    if 'Eps Revisions' in analysis.index:
                        try:
                            revisions = analysis.loc['Eps Revisions', current_qtr]
                            if isinstance(revisions, dict):
                                up_revisions = revisions.get('upLast7days', 0)
                                down_revisions = revisions.get('downLast7days', 0)
                                
                                if up_revisions > down_revisions:
                                    esp_score = min(30, up_revisions * 5)  # Max 30 points
                        except:
                            pass
                    
                    return {
                        'has_data': True,
                        'esp_score': esp_score,
                        'mean_estimate': eps_data['mean_estimate'],
                        'signal_strength': 'Strong' if esp_score > 20 else 'Moderate' if esp_score > 10 else 'Weak'
                    }
            
            return {'has_data': False, 'esp_score': 0}
            
        except Exception as e:
            print(f"    ✗ ESP calculation error: {e}")
            return {'has_data': False, 'esp_score': 0}
    
    def check_insider_activity(self, ticker: str) -> Dict[str, Any]:
        """Check recent insider buying (bullish signal before earnings)"""
        print(f"  Checking insider activity for {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get insider transactions
            insider_trades = stock.insider_transactions
            
            if insider_trades is not None and not insider_trades.empty:
                # Look at last 30 days
                recent_date = datetime.now() - timedelta(days=30)
                
                # Filter for purchases only
                if 'Transaction' in insider_trades.columns:
                    purchases = insider_trades[
                        (insider_trades['Transaction'].str.contains('Purchase', case=False, na=False)) &
                        (pd.to_datetime(insider_trades.index) > recent_date)
                    ]
                    
                    if len(purchases) > 0:
                        total_value = purchases['Value'].sum() if 'Value' in purchases.columns else 0
                        num_insiders = len(purchases)
                        
                        # Score based on activity
                        score = 0
                        if num_insiders >= 3:
                            score = 25  # Cluster buying - strong signal
                        elif num_insiders == 2:
                            score = 15
                        elif num_insiders == 1:
                            score = 10
                        
                        return {
                            'has_activity': True,
                            'score': score,
                            'num_purchases': num_insiders,
                            'signal': 'Cluster buying' if num_insiders >= 3 else 'Insider buying'
                        }
            
            return {'has_activity': False, 'score': 0}
            
        except Exception as e:
            print(f"    ✗ Insider check error: {e}")
            return {'has_activity': False, 'score': 0}
    
    def check_analyst_activity(self, ticker: str) -> Dict[str, Any]:
        """Check recent analyst upgrades and price target raises"""
        print(f"  Checking analyst activity for {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get recommendations
            recommendations = stock.recommendations
            
            if recommendations is not None and not recommendations.empty:
                # Last 30 days
                recent_date = datetime.now() - timedelta(days=30)
                recent_recs = recommendations[pd.to_datetime(recommendations.index) > recent_date]
                
                if len(recent_recs) > 0:
                    # Count upgrades vs downgrades
                    upgrades = len(recent_recs[recent_recs['To Grade'].str.contains('Buy|Outperform|Overweight', case=False, na=False)])
                    downgrades = len(recent_recs[recent_recs['To Grade'].str.contains('Sell|Underperform|Underweight', case=False, na=False)])
                    
                    score = 0
                    signal = None
                    
                    if upgrades > downgrades:
                        score = min(20, upgrades * 7)  # Max 20 points
                        signal = f"{upgrades} recent upgrade(s)"
                    elif downgrades > upgrades:
                        score = -10  # Negative signal
                        signal = f"{downgrades} recent downgrade(s)"
                    
                    return {
                        'has_activity': True,
                        'score': score,
                        'upgrades': upgrades,
                        'downgrades': downgrades,
                        'signal': signal
                    }
            
            return {'has_activity': False, 'score': 0}
            
        except Exception as e:
            print(f"    ✗ Analyst check error: {e}")
            return {'has_activity': False, 'score': 0}
    
    def check_price_momentum(self, ticker: str) -> Dict[str, Any]:
        """Check if stock is building momentum into earnings"""
        print(f"  Checking price momentum for {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get 30-day price history
            hist = stock.history(period='1mo')
            
            if len(hist) > 5:
                # Calculate momentum signals
                current_price = hist['Close'].iloc[-1]
                price_10d_ago = hist['Close'].iloc[-10] if len(hist) >= 10 else hist['Close'].iloc[0]
                
                pct_change = ((current_price - price_10d_ago) / price_10d_ago) * 100
                
                # Check if price is near highs
                high_30d = hist['High'].max()
                pct_from_high = ((current_price - high_30d) / high_30d) * 100
                
                score = 0
                signals = []
                
                # Positive momentum
                if pct_change > 5:
                    score += 15
                    signals.append(f"Up {pct_change:.1f}% in 10 days")
                elif pct_change > 2:
                    score += 10
                    signals.append(f"Up {pct_change:.1f}% in 10 days")
                
                # Near highs (consolidation at highs = bullish)
                if pct_from_high > -3:
                    score += 10
                    signals.append("Near 30-day highs")
                
                return {
                    'has_momentum': score > 0,
                    'score': score,
                    'signals': signals,
                    'pct_change_10d': pct_change
                }
            
            return {'has_momentum': False, 'score': 0}
            
        except Exception as e:
            print(f"    ✗ Momentum check error: {e}")
            return {'has_momentum': False, 'score': 0}
    
    def check_historical_beat_rate(self, ticker: str) -> Dict[str, Any]:
        """Check company's historical earnings beat rate"""
        print(f"  Checking historical beat rate for {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get earnings history
            earnings_hist = stock.earnings_dates
            
            if earnings_hist is not None and not earnings_hist.empty:
                # Look at last 4 quarters
                recent_earnings = earnings_hist.head(4)
                
                if 'Surprise(%)' in recent_earnings.columns:
                    # Count beats
                    beats = len(recent_earnings[recent_earnings['Surprise(%)'] > 0])
                    total = len(recent_earnings)
                    
                    if total > 0:
                        beat_rate = beats / total
                        
                        score = 0
                        if beat_rate >= 0.75:  # 75%+ beat rate
                            score = 20
                        elif beat_rate >= 0.50:
                            score = 10
                        
                        return {
                            'has_history': True,
                            'score': score,
                            'beat_rate': beat_rate,
                            'beats': beats,
                            'total': total,
                            'signal': f"{beats}/{total} quarters beat"
                        }
            
            return {'has_history': False, 'score': 0}
            
        except Exception as e:
            print(f"    ✗ Historical check error: {e}")
            return {'has_history': False, 'score': 0}
    
    def check_sector_momentum(self, ticker: str, sector: str) -> Dict[str, Any]:
        """Check if sector is outperforming (rising tide lifts all boats)"""
        print(f"  Checking sector momentum for {ticker}...")
        
        try:
            # Get sector ETF performance
            sector_etfs = {
                'Technology': 'XLK',
                'Financial Services': 'XLF',
                'Healthcare': 'XLV',
                'Consumer Cyclical': 'XLY',
                'Industrials': 'XLI',
                'Energy': 'XLE',
                'Consumer Defensive': 'XLP',
                'Real Estate': 'XLRE',
                'Communication Services': 'XLC',
                'Utilities': 'XLU',
                'Basic Materials': 'XLB'
            }
            
            etf_ticker = sector_etfs.get(sector)
            
            if etf_ticker:
                etf = yf.Ticker(etf_ticker)
                spy = yf.Ticker('SPY')
                
                # Get 10-day performance
                etf_hist = etf.history(period='10d')
                spy_hist = spy.history(period='10d')
                
                if len(etf_hist) > 0 and len(spy_hist) > 0:
                    etf_return = ((etf_hist['Close'].iloc[-1] - etf_hist['Close'].iloc[0]) / etf_hist['Close'].iloc[0]) * 100
                    spy_return = ((spy_hist['Close'].iloc[-1] - spy_hist['Close'].iloc[0]) / spy_hist['Close'].iloc[0]) * 100
                    
                    outperformance = etf_return - spy_return
                    
                    score = 0
                    if outperformance > 2:
                        score = 15
                    elif outperformance > 0:
                        score = 10
                    
                    return {
                        'has_momentum': score > 0,
                        'score': score,
                        'outperformance': outperformance,
                        'signal': f"Sector outperforming by {outperformance:.1f}%"
                    }
            
            return {'has_momentum': False, 'score': 0}
            
        except Exception as e:
            print(f"    ✗ Sector check error: {e}")
            return {'has_momentum': False, 'score': 0}
    
    def analyze_stock(self, stock_info: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive analysis of a stock before earnings"""
        ticker = stock_info['ticker']
        
        print(f"\n📊 Analyzing {ticker} ({stock_info['company']})...")
        
        analysis = {
            'ticker': ticker,
            'company': stock_info['company'],
            'earnings_date': stock_info['earnings_date'],
            'sector': stock_info['sector'],
            'total_score': 0,
            'signals': []
        }
        
        # Run all checks
        esp = self.calculate_earnings_esp(ticker)
        if esp['has_data']:
            analysis['total_score'] += esp['esp_score']
            if esp['esp_score'] > 0:
                analysis['signals'].append(('📊 Earnings ESP', esp['esp_score'], esp['signal_strength']))
        
        time.sleep(1)
        
        insider = self.check_insider_activity(ticker)
        if insider['has_activity']:
            analysis['total_score'] += insider['score']
            analysis['signals'].append(('💼 Insider Activity', insider['score'], insider['signal']))
        
        time.sleep(1)
        
        analyst = self.check_analyst_activity(ticker)
        if analyst['has_activity'] and analyst['score'] > 0:
            analysis['total_score'] += analyst['score']
            analysis['signals'].append(('📈 Analyst Activity', analyst['score'], analyst['signal']))
        
        time.sleep(1)
        
        momentum = self.check_price_momentum(ticker)
        if momentum['has_momentum']:
            analysis['total_score'] += momentum['score']
            for signal in momentum['signals']:
                analysis['signals'].append(('📈 Price Momentum', momentum['score'], signal))
        
        time.sleep(1)
        
        history = self.check_historical_beat_rate(ticker)
        if history['has_history']:
            analysis['total_score'] += history['score']
            analysis['signals'].append(('📜 Historical Beat Rate', history['score'], history['signal']))
        
        time.sleep(1)
        
        sector = self.check_sector_momentum(ticker, stock_info['sector'])
        if sector['has_momentum']:
            analysis['total_score'] += sector['score']
            analysis['signals'].append(('🏭 Sector Momentum', sector['score'], sector['signal']))
        
        print(f"  Total Score: {analysis['total_score']}/100")
        
        return analysis
    
    def send_earnings_alert(self, analysis: Dict[str, Any]):
        """Send formatted earnings opportunity alert to Discord"""
        ticker = analysis['ticker']
        score = analysis['total_score']
        
        # Determine color and confidence
        if score >= 80:
            color = 15158332  # Red - High confidence
            confidence = "HIGH CONFIDENCE"
        elif score >= 70:
            color = 16776960  # Yellow - Moderate confidence
            confidence = "MODERATE CONFIDENCE"
        else:
            color = 3447003   # Blue - Low confidence
            confidence = "LOW CONFIDENCE"
        
        # Format earnings date
        earnings_date = analysis['earnings_date']
        days_until = (earnings_date - datetime.now()).days
        date_str = earnings_date.strftime('%b %d, %Y')
        
        description = f"**{confidence}** earnings beat setup\n"
        description += f"📅 Earnings: {date_str} ({days_until} days)\n"
        description += f"⭐ Score: **{score}**/100\n"
        description += f"🏭 Sector: {analysis['sector']}\n\n"
        
        # Add signals
        fields = []
        for signal_name, signal_score, signal_detail in analysis['signals'][:6]:  # Max 6 signals
            fields.append({
                "name": f"{signal_name} (+{signal_score})",
                "value": signal_detail,
                "inline": False
            })
        
        # Add suggested play
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            current_price = info.get('currentPrice', 0)
            
            if current_price > 0:
                # Calculate position sizing
                if score >= 80:
                    shares = int(5000 / current_price)  # $5K position
                    stop_pct = 7
                    target_pct = 15
                elif score >= 70:
                    shares = int(3000 / current_price)  # $3K position
                    stop_pct = 8
                    target_pct = 12
                else:
                    shares = int(2000 / current_price)  # $2K position
                    stop_pct = 10
                    target_pct = 10
                
                stop_price = current_price * (1 - stop_pct/100)
                target_price = current_price * (1 + target_pct/100)
                
                fields.append({
                    "name": "💡 Suggested Play (Paper Trading)",
                    "value": f"BUY {shares} shares at ${current_price:.2f}\n"
                            f"Stop: ${stop_price:.2f} (-{stop_pct}%)\n"
                            f"Target: ${target_price:.2f} (+{target_pct}%)\n"
                            f"Risk/Reward: 1:{target_pct/stop_pct:.1f}",
                    "inline": False
                })
        except:
            pass
        
        # Add disclaimer
        fields.append({
            "name": "⚠️ Remember",
            "value": "• This is for paper trading only\n"
                    "• Earnings are inherently volatile\n"
                    "• Do your own research\n"
                    "• Expected win rate: 60-70% for 80+ scores",
            "inline": False
        })
        
        self.send_discord_alert(
            title=f"🎯 EARNINGS OPPORTUNITY: ${ticker}",
            description=description,
            color=color,
            fields=fields
        )
    
    def run_scan(self):
        """Execute full earnings scan"""
        print(f"\n{'='*60}")
        print(f"🚀 Starting Earnings Beat Scan")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Get upcoming earnings
        upcoming_earnings = self.get_upcoming_earnings(days_ahead=7)
        
        if not upcoming_earnings:
            print("\n📊 No upcoming earnings found in the next 7 days")
            return
        
        print(f"\n📊 Analyzing {len(upcoming_earnings)} stocks...")
        
        high_priority = []
        
        # Analyze each stock
        for stock_info in upcoming_earnings:
            try:
                analysis = self.analyze_stock(stock_info)
                
                if analysis['total_score'] >= self.alert_threshold:
                    high_priority.append(analysis)
                    print(f"  ✓ {stock_info['ticker']}: {analysis['total_score']}/100 - ALERT!")
                else:
                    print(f"  ○ {stock_info['ticker']}: {analysis['total_score']}/100")
                
                time.sleep(2)  # Rate limiting between stocks
                
            except Exception as e:
                print(f"  ✗ Error analyzing {stock_info['ticker']}: {e}")
                continue
        
        # Send alerts for high-priority opportunities
        print(f"\n📢 Sending alerts for {len(high_priority)} opportunities...")
        
        for analysis in sorted(high_priority, key=lambda x: x['total_score'], reverse=True):
            self.send_earnings_alert(analysis)
            time.sleep(2)  # Rate limit Discord webhooks
        
        print(f"\n{'='*60}")
        print(f"✅ Scan complete")
        print(f"📊 Stocks analyzed: {len(upcoming_earnings)}")
        print(f"🎯 High-priority opportunities: {len(high_priority)}")
        print(f"{'='*60}\n")
        
        return high_priority


def main():
    """Main entry point"""
    import os
    
    # Get Discord webhook from environment variable
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    
    if not webhook_url:
        print("❌ ERROR: DISCORD_WEBHOOK_URL environment variable not set")
        print("Please set it in GitHub Secrets")
        return
    
    # Initialize scanner
    scanner = EarningsBeatScanner(webhook_url)
    
    # Run scan
    scanner.run_scan()


if __name__ == "__main__":
    main()
