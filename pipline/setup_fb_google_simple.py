"""
ETL Script for Facebook + Google Ads Marketing ROI Dashboard
Combines and normalizes data from both platforms for analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os

def load_facebook_ads():
    """
    Load Facebook Ads data from Kaggle dataset
    Expected columns: ad_id, campaign_id, spend, impressions, clicks, conversions, date
    """
    try:
        # Try common file names
        possible_names = [
            'facebook_ads_data.csv',
            'facebook_ad_campaign_data.csv',
            'FB_ad_campaign_data.csv',
            'facebook_ads.csv'
        ]
        
        df = None
        for name in possible_names:
            file_path = Path('data/raw') / name
            if file_path.exists():
                df = pd.read_csv(file_path)
                print(f"✓ Loaded Facebook Ads from {name}")
                break
        
        if df is None:
            # Create sample data if no file found
            print("⚠ No Facebook Ads file found, creating sample data")
            df = create_sample_facebook_data()
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Standardize column names
        column_mapping = {
            'ad_id': 'ad_id',
            'campaign_id': 'campaign_id',
            'spend': 'spend',
            'impressions': 'impressions',
            'clicks': 'clicks',
            'conversions': 'conversions',
            'date': 'date',
            'amount_spent': 'spend',
            'fb_campaign_id': 'campaign_id'
        }
        
        df = df.rename(columns={col: column_mapping.get(col, col) for col in df.columns})
        
        # Add platform identifier
        df['platform'] = 'facebook'
        
        # Ensure required columns exist
        required_cols = ['platform', 'spend', 'impressions', 'clicks', 'date']
        for col in required_cols:
            if col not in df.columns:
                if col == 'conversions':
                    df[col] = 0
                else:
                    raise ValueError(f"Missing required column: {col}")
        
        return df
    
    except Exception as e:
        print(f"Error loading Facebook Ads: {e}")
        return create_sample_facebook_data()


def load_google_ads():
    """
    Load Google Ads data from Kaggle dataset
    Expected columns: ad_id, campaign_id, cost, impressions, clicks, conversions, date
    """
    try:
        # Try common file names
        possible_names = [
            'google_ads_data.csv',
            'google_ad_campaign_data.csv',
            'google_ads.csv'
        ]
        
        df = None
        for name in possible_names:
            file_path = Path('data/raw') / name
            if file_path.exists():
                df = pd.read_csv(file_path)
                print(f"✓ Loaded Google Ads from {name}")
                break
        
        if df is None:
            # Create sample data if no file found
            print("⚠ No Google Ads file found, creating sample data")
            df = create_sample_google_data()
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Standardize column names
        column_mapping = {
            'ad_id': 'ad_id',
            'campaign_id': 'campaign_id',
            'cost': 'spend',
            'impressions': 'impressions',
            'clicks': 'clicks',
            'conversions': 'conversions',
            'date': 'date',
            'campaign_id_': 'campaign_id'
        }
        
        df = df.rename(columns={col: column_mapping.get(col, col) for col in df.columns})
        
        # Add platform identifier
        df['platform'] = 'google'
        
        # Ensure required columns exist
        required_cols = ['platform', 'spend', 'impressions', 'clicks', 'date']
        for col in required_cols:
            if col not in df.columns:
                if col == 'conversions':
                    df[col] = 0
                else:
                    raise ValueError(f"Missing required column: {col}")
        
        return df
    
    except Exception as e:
        print(f"Error loading Google Ads: {e}")
        return create_sample_google_data()


def create_sample_facebook_data():
    """Create sample Facebook Ads data for testing"""
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', '2024-01-31', freq='D')
    
    data = []
    for date in dates:
        for i in range(5):  # 5 campaigns per day
            data.append({
                'platform': 'facebook',
                'campaign_id': f'fb_{i+1}',
                'ad_id': f'fb_ad_{i+1}_{date.strftime("%Y%m%d")}',
                'spend': round(np.random.uniform(200, 800), 2),
                'impressions': np.random.randint(5000, 20000),
                'clicks': np.random.randint(200, 800),
                'conversions': np.random.randint(5, 30),
                'date': date.strftime('%Y-%m-%d')
            })
    
    df = pd.DataFrame(data)
    print("✓ Created sample Facebook Ads data")
    return df


def create_sample_google_data():
    """Create sample Google Ads data for testing"""
    np.random.seed(43)
    dates = pd.date_range('2024-01-01', '2024-01-31', freq='D')
    
    data = []
    for date in dates:
        for i in range(5):  # 5 campaigns per day
            data.append({
                'platform': 'google',
                'campaign_id': f'google_{i+1}',
                'ad_id': f'google_ad_{i+1}_{date.strftime("%Y%m%d")}',
                'spend': round(np.random.uniform(150, 600), 2),
                'impressions': np.random.randint(8000, 25000),
                'clicks': np.random.randint(300, 1000),
                'conversions': np.random.randint(8, 35),
                'date': date.strftime('%Y-%m-%d')
            })
    
    df = pd.DataFrame(data)
    print("✓ Created sample Google Ads data")
    return df


def calculate_metrics(df):
    """Calculate derived metrics"""
    # CPC = Cost per Click
    df['cpc'] = df['spend'] / df['clicks'].replace(0, 1)
    
    # CTR = Click-Through Rate (clicks / impressions)
    df['ctr'] = (df['clicks'] / df['impressions'].replace(0, 1)) * 100
    
    # CPA = Cost per Acquisition (spend / conversions)
    df['cpa'] = df['spend'] / df['conversions'].replace(0, 1)
    
    # Conversion Rate
    df['conversion_rate'] = (df['conversions'] / df['clicks'].replace(0, 1)) * 100
    
    return df


def combine_data(facebook_df, google_df):
    """Combine both datasets"""
    # Ensure same columns
    common_cols = list(set(facebook_df.columns) & set(google_df.columns))
    facebook_df = facebook_df[common_cols]
    google_df = google_df[common_cols]
    
    # Concatenate
    combined = pd.concat([facebook_df, google_df], ignore_index=True)
    
    # Sort by date
    combined['date'] = pd.to_datetime(combined['date'])
    combined = combined.sort_values('date')
    
    return combined


def save_data(df, output_path):
    """Save combined data to CSV"""
    # Create directory if it doesn't exist
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"✓ Saved combined data to {output_path}")
    print(f"  Total rows: {len(df)}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Platforms: {df['platform'].unique().tolist()}")


def main():
    """Main ETL pipeline"""
    print("=" * 60)
    print("Facebook + Google Ads ETL Pipeline")
    print("=" * 60)
    
    # Load data
    print("\n[1/4] Loading data...")
    facebook_df = load_facebook_ads()
    google_df = load_google_ads()
    
    # Calculate metrics
    print("\n[2/4] Calculating metrics...")
    facebook_df = calculate_metrics(facebook_df)
    google_df = calculate_metrics(google_df)
    
    # Combine data
    print("\n[3/4] Combining data...")
    combined_df = combine_data(facebook_df, google_df)
    combined_df = calculate_metrics(combined_df)
    
    # Save data
    print("\n[4/4] Saving data...")
    save_data(combined_df, 'data/staging/ads_combined.csv')
    
    # Print summary
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    
    for platform in ['facebook', 'google']:
        platform_data = combined_df[combined_df['platform'] == platform]
        print(f"\n{platform.upper()} ADS:")
        print(f"  Total Spend: ${platform_data['spend'].sum():,.2f}")
        print(f"  Total Clicks: {platform_data['clicks'].sum():,.0f}")
        print(f"  Total Conversions: {platform_data['conversions'].sum():,.0f}")
        print(f"  Avg CPC: ${platform_data['cpc'].mean():.2f}")
        print(f"  Avg CTR: {platform_data['ctr'].mean():.2f}%")
        print(f"  Avg CPA: ${platform_data['cpa'].mean():.2f}")
    
    print("\n" + "=" * 60)
    print("✓ ETL Pipeline Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Load data to BigQuery:")
    print("   bq load --autodetect analytics.ads_combined data/staging/ads_combined.csv")
    print("2. Run dbt models:")
    print("   cd dbt && dbt run")
    print("3. Build dashboard in Looker Studio")


if __name__ == "__main__":
    main()
