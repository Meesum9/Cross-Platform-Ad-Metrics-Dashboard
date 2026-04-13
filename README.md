# Multi-Channel Ad Performance Analytics Dashboard

A data engineering pipeline that compares Facebook Ads and Google Ads performance metrics using Python ETL, BigQuery, and dbt for actionable marketing insights.

## Overview

This project implements a complete data engineering pipeline to analyze and compare advertising performance across Facebook and Google Ads platforms. The pipeline ingests raw advertising data, transforms it into a unified schema, calculates key performance metrics, and enables visualization through Looker Studio.

## Architecture

### Data Pipeline
- **Ingestion**: Python ETL script normalizes data from multiple advertising platforms
- **Storage**: Google BigQuery for scalable data warehousing
- **Transformation**: dbt models for metric calculation and data modeling
- **Visualization**: Looker Studio for interactive dashboards

### Tech Stack
- **Python**: pandas, numpy for data processing
- **BigQuery**: Cloud data warehouse for analytics
- **dbt**: Data transformation tool for SQL models
- **Looker Studio**: Business intelligence visualization

## Key Metrics Calculated

The pipeline calculates the following performance metrics:
- **CPC** (Cost Per Click): Average cost per click
- **CTR** (Click-Through Rate): Clicks divided by impressions
- **CPA** (Cost Per Acquisition): Cost per conversion
- **Conversion Rate**: Conversions divided by clicks
- **Conversions per $1K Spend**: Efficiency metric

## Project Structure

```
marketing-channel-analytics/
├── data/
│   ├── raw/              # Original advertising platform data
│   └── staging/          # Combined, cleaned data
├── dbt/
│   ├── models/           # dbt SQL models
│   │   ├── stg_ads_combined.sql          # Staging layer
│   │   ├── fct_daily_performance.sql      # Daily metrics
│   │   ├── fct_platform_comparison.sql    # Platform comparison
│   │   ├── fct_top_campaigns.sql          # Top campaigns
│   │   ├── fct_weekly_trends.sql         # Weekly trends
│   │   └── sources.yml                   # Source definitions
│   ├── dbt_project.yml   # dbt configuration
│   └── profiles.yml      # BigQuery credentials
├── generate_sample_data.py      # Sample data generator
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- Google Cloud account with BigQuery enabled
- dbt CLI installed
- Looker Studio account

### Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Generate sample data (or place your own data in `data/raw/`):
```bash
python generate_sample_data.py
```

3. Create BigQuery dataset:
```bash
bq mk analytics
```

4. Load data to BigQuery:
```bash
bq load --autodetect analytics.ads_combined data/staging/ads_combined.csv
```

5. Configure dbt:
   - Edit `dbt/profiles.yml`
   - Replace `your-project-id` with your GCP project ID

6. Run dbt models:
```bash
cd dbt
dbt run
```

## Sample Results

The pipeline generates comparative metrics:

| Platform | Total Spend | Avg CPC | Avg CTR | Avg CPA | Conversions/$1K |
|----------|-------------|---------|---------|---------|-----------------|
| Facebook | $77,679.67  | $1.05   | 3.73%   | $30.89  | 32.38           |
| Google   | $57,977.72  | $0.60   | 3.89%   | $18.76  | 53.30           |

## dbt Models

### Staging Layer
- `stg_ads_combined`: Raw data normalization and metric calculation

### Transformation Layer
- `fct_daily_performance`: Daily aggregated metrics by platform
- `fct_platform_comparison`: Platform-level summary statistics
- `fct_top_campaigns`: Best performing campaigns ranked by conversions
- `fct_weekly_trends`: Weekly aggregated data for trend analysis

## Troubleshooting

**BigQuery load error**: Ensure CSV files have headers and use UTF-8 encoding

**dbt run fails**: Verify `profiles.yml` contains correct GCP project ID and credentials

**Looker Studio connection**: Confirm BigQuery dataset has appropriate sharing permissions

## Future Enhancements

- Additional advertising platforms (Instagram, LinkedIn, TikTok)
- Multi-touch attribution modeling
- Automated pipeline scheduling
- Performance anomaly detection and alerting
- Machine learning for spend optimization
