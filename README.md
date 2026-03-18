# 🚀 AI Data Engineering Pipeline

## Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the application
python main.py

# 3. Upload CSV
Select option 1
Enter: examples/sample_sales.csv

# 4. Query naturally
Select option 2
Query: "show me total sales by region"
```

## Features

✅ Upload CSV files
✅ Automatic data cleaning
✅ Anomaly detection & auto-fixing
✅ Natural language queries
✅ Data quality scoring
✅ Statistical analysis
✅ Visualizations

## Documentation

- **Quick Start**: Run `python main.py` and follow prompts
- **Sample Data**: Check `examples/sample_sales.csv`
- **Configuration**: Edit `.env` file for API keys

## Natural Language Queries

Examples:
- "show me total sales by region"
- "average price per product"
- "count orders by category"

The system auto-corrects spelling mistakes!

## Project Structure
```
AI_DATA_PIPELINE2/
├── main.py                 # Main application
├── requirements.txt        # Dependencies
├── config/                 # Configuration
├── src/                    # Source code
│   ├── database/          # Database operations
│   ├── ingestion/         # Data loading
│   ├── processing/        # Data cleaning
│   ├── intelligence/      # Anomaly detection
│   ├── nlp/               # Natural language
│   ├── analytics/         # Statistics & viz
│   └── monitoring/        # Logging & alerts
├── data/                   # Data storage
└── examples/              # Sample data
```

## Requirements

- Python 3.8+
- See `requirements.txt`

## License

MIT License
