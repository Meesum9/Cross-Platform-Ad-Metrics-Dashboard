# Multi-Channel Ad Performance Analytics Dashboard

A data engineering pipeline that compares Facebook Ads and Google Ads performance metrics using Python ETL, BigQuery, and dbt for actionable marketing insights.

## 📋 Project Overview

**What it does:** Compares advertising performance across two major channels  
**Data sources:** Facebook Ads + Google Ads (Kaggle datasets)  
**Tech stack:** Python, BigQuery, dbt, Looker Studio  
**Time to complete:** ~1 hour  

## 🎯 What You'll Build

A dashboard that answers:
- Which channel has lower Cost per Click (CPC)?
- Which channel has higher Click-Through Rate (CTR)?
- Which channel converts better?
- How does performance trend over time?

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Google Cloud account with BigQuery
- dbt installed
- Looker Studio account (free)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Transform Data
Download datasets from Kaggle:
- Facebook Ads: https://www.kaggle.com/datasets/loveall/facebook-ad-campaign-data
- Google Ads: https://www.kaggle.com/datasets/loveall/google-ad-campaign-data

Place them in `data/raw/` directory, then run:
```bash
python setup_fb_google_simple.py
```

**Note:** The script will create sample data if no Kaggle files are found, so you can test immediately.

### Step 3: Load to BigQuery
```bash
# Create dataset
bq mk analytics

# Load data
bq load --autodetect analytics.ads_combined data/staging/ads_combined.csv
```

### Step 4: Configure dbt
Edit `dbt/profiles.yml` and replace `your-project-id` with your actual GCP project ID.

### Step 5: Run dbt Models
```bash
cd dbt
dbt run
```

### Step 6: Build Dashboard
Connect Looker Studio to your BigQuery `analytics` dataset and create visualizations for:
- CPC comparison
- CTR comparison
- Cost per conversion
- Daily trends

## 📊 Expected Output

Your dashboard will show metrics like:

```
Facebook Ads          Google Ads
CPC: $1.24           CPC: $0.89
CTR: 5.2%            CTR: 3.8%
Spend: $12,500       Spend: $11,200
Conversions: 234     Conversions: 189
```

## 📁 Project Structure

```
marketing-channel-analytics/
├── data/
│   ├── raw/              # Original Kaggle datasets
│   └── staging/          # Combined, cleaned data
├── dbt/
│   ├── models/           # dbt SQL models
│   ├── dbt_project.yml   # dbt configuration
│   └── profiles.yml      # BigQuery credentials
├── setup_fb_google_simple.py  # ETL script
├── dbt_models_simple.sql       # SQL queries (reference)
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── CHOOSE_YOUR_PATH.md        # Project options
└── README_FB_GOOGLE_SIMPLE.md # Detailed guide
```

## 🎓 Interview Script (30 seconds)

*"I built a channel comparison dashboard using Facebook and Google Ads data from Kaggle. I wrote a Python ETL script to normalize the data, loaded it to BigQuery, used dbt to calculate metrics like CPC and CTR, and created a Looker Studio dashboard. It shows which channel is more cost-efficient and has better engagement rates."*

## 💡 Tips

- Start with sample data (the script generates it automatically)
- Use BigQuery's free tier (1TB/month)
- Looker Studio is free and integrates seamlessly
- dbt Cloud has a free tier for small projects

## 🔧 Troubleshooting

**BigQuery load error:** Ensure your CSV has headers and uses UTF-8 encoding

**dbt run fails:** Check your `profiles.yml` configuration for BigQuery credentials

**Looker Studio connection:** Make sure your BigQuery dataset is shared with the correct permissions

## 📚 Additional Resources

- [dbt documentation](https://docs.getdbt.com/)
- [BigQuery quickstart](https://cloud.google.com/bigquery/docs/quickstarts)
- [Looker Studio tutorials](https://support.google.com/looker-studio)

## 🎉 Next Steps

Once this works, you can:
- Add more data sources (Instagram, LinkedIn, etc.)
- Implement attribution modeling
- Add automated scheduling
- Create alerts for performance drops

---

**Ready to build? Follow the Quick Start steps above!** 🚀
