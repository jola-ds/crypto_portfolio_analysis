# Cryptocurrency Comparative Analysis Pipeline

> **End-to-end data pipeline and analytical framework for Bitcoin (BTC) and Ethereum (ETH) market analysis**

A Python-based system for fetching, processing, and analysing cryptocurrency market data with focus on comparative trend analysis, risk assessment, and correlation dynamics.

- ✅ Automatic data fetching and processing
- ✅ Comparative trend analysis (price, volume, moving averages)
- ✅ Comprehensive risk profiling (volatility, VaR, drawdowns)
- ✅ Correlation analysis (static, rolling, lead-lag, monthly patterns)
- ✅ Risk-adjusted performance metrics (Sharpe ratio, Sortino ratio)
- ✅ Visualizations and reporting

---

## Project Structure

```txt
crypto-analysis/
│
├── crypto_pipeline.py          # Data fetching and processing pipeline
├── crypto_analysis.ipynb       # Step-by-step analysis notebook
├── README.md                   # This file
├── requirements.txt            # Python dependencies
```

---

### Prerequisites

- Python 3.8 or higher

### Installation

```bash
# Clone repository
git clone https://github.com/jola-ds/crypto_portfolio_analysis.git
cd crypto_portfolio_analysis
```

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

```bash
# Install dependencies
pip install -r requirements.txt
```

```bash
# Run pipeline
python data_pipeline.py
```

```bash
# Explore notebook
jupyter notebook comparative_analysis.ipynb
```

## License

MIT License

## Acknowledgments

- **Data Source**: CoinGecko API

## Disclaimer

This project is for **educational purposes only**.
