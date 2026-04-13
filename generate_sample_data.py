"""
Generate realistic sample data for Facebook and Google Ads
Data matches the schema expected by dbt models
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def generate_facebook_ads_data(days=90, campaigns_per_day=3):
    """Generate realistic Facebook Ads data"""
    np.random.seed(42)
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    # Facebook tends to have higher CTR but higher CPC
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        
        for campaign in range(1, campaigns_per_day + 1):
            campaign_id = f"fb_campaign_{campaign:03d}"
            ad_id = f"fb_ad_{campaign}_{day}"
            
            # Facebook typical metrics
            impressions = np.random.randint(5000, 25000)
            clicks = np.random.randint(int(impressions * 0.03), int(impressions * 0.08))  # 3-8% CTR
            spend = round(clicks * np.random.uniform(0.80, 1.50), 2)  # $0.80-$1.50 CPC
            conversions = np.random.randint(int(clicks * 0.02), int(clicks * 0.05))  # 2-5% conversion rate
            
            data.append({
                'platform': 'facebook',
                'campaign_id': campaign_id,
                'ad_id': ad_id,
                'spend': spend,
                'impressions': impressions,
                'clicks': clicks,
                'conversions': conversions,
                'date': current_date.strftime('%Y-%m-%d')
            })
    
    df = pd.DataFrame(data)
    print(f"Generated {len(df)} Facebook Ads records")
    return df

def generate_google_ads_data(days=90, campaigns_per_day=3):
    """Generate realistic Google Ads data"""
    np.random.seed(43)
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    # Google tends to have lower CTR but lower CPC
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        
        for campaign in range(1, campaigns_per_day + 1):
            campaign_id = f"google_campaign_{campaign:03d}"
            ad_id = f"google_ad_{campaign}_{day}"
            
            # Google typical metrics
            impressions = np.random.randint(8000, 35000)
            clicks = np.random.randint(int(impressions * 0.02), int(impressions * 0.05))  # 2-5% CTR
            spend = round(clicks * np.random.uniform(0.50, 1.20), 2)  # $0.50-$1.20 CPC
            conversions = np.random.randint(int(clicks * 0.03), int(clicks * 0.06))  # 3-6% conversion rate
            
            data.append({
                'platform': 'google',
                'campaign_id': campaign_id,
                'ad_id': ad_id,
                'spend': spend,
                'impressions': impressions,
                'clicks': clicks,
                'conversions': conversions,
                'date': current_date.strftime('%Y-%m-%d')
            })
    
    df = pd.DataFrame(data)
    print(f"Generated {len(df)} Google Ads records")
    return df

def save_datasets(facebook_df, google_df):
    """Save datasets to raw folder"""
    raw_dir = Path('data/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Save Facebook data
    facebook_path = raw_dir / 'facebook_ads_data.csv'
    facebook_df.to_csv(facebook_path, index=False)
    print(f"✓ Saved Facebook Ads to {facebook_path}")
    
    # Save Google data
    google_path = raw_dir / 'google_ads_data.csv'
    google_df.to_csv(google_path, index=False)
    print(f"✓ Saved Google Ads to {google_path}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("Data Summary")
    print("=" * 60)
    
    print("\nFacebook Ads:")
    print(f"  Total records: {len(facebook_df)}")
    print(f"  Total spend: ${facebook_df['spend'].sum():,.2f}")
    print(f"  Total clicks: {facebook_df['clicks'].sum():,.0f}")
    print(f"  Total conversions: {facebook_df['conversions'].sum():,.0f}")
    print(f"  Avg CPC: ${facebook_df['spend'].sum() / facebook_df['clicks'].sum():.2f}")
    print(f"  Avg CTR: {(facebook_df['clicks'].sum() / facebook_df['impressions'].sum() * 100):.2f}%")
    
    print("\nGoogle Ads:")
    print(f"  Total records: {len(google_df)}")
    print(f"  Total spend: ${google_df['spend'].sum():,.2f}")
    print(f"  Total clicks: {google_df['clicks'].sum():,.0f}")
    print(f"  Total conversions: {google_df['conversions'].sum():,.0f}")
    print(f"  Avg CPC: ${google_df['spend'].sum() / google_df['clicks'].sum():.2f}")
    print(f"  Avg CTR: {(google_df['clicks'].sum() / google_df['impressions'].sum() * 100):.2f}%")
    
    print("\n" + "=" * 60)

def main():
    """Main function to generate sample data"""
    print("=" * 60)
    print("Generating Sample Marketing Data")
    print("=" * 60)
    
    # Generate data
    print("\nGenerating Facebook Ads data...")
    facebook_df = generate_facebook_ads_data(days=90, campaigns_per_day=3)
    
    print("\nGenerating Google Ads data...")
    google_df = generate_google_ads_data(days=90, campaigns_per_day=3)
    
    # Save data
    print("\nSaving datasets...")
    save_datasets(facebook_df, google_df)
    
    print("\n✓ Sample data generation complete!")
    print("\nNext steps:")
    print("1. Run the ETL script: python setup_fb_google_simple.py")
    print("2. Load to BigQuery: bq load --autodetect analytics.ads_combined data/staging/ads_combined.csv")

if __name__ == "__main__":
    main()
