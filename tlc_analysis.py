import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# ============================================================================
# STEP 1: LOAD AND PREPARE DATA
# ============================================================================

def load_and_sample_data(file_path, sample_size=500000):
    """
    Load TLC data and create a manageable sample
    """
    # Option A: Random sample
    df = pd.read_csv(file_path, nrows=sample_size)
    
    # Option B: If file is too large, use chunking
    # chunks = []
    # for chunk in pd.read_csv(file_path, chunksize=100000):
    #     sample = chunk.sample(frac=0.1)
    #     chunks.append(sample)
    #     if len(pd.concat(chunks)) >= sample_size:
    #         break
    # df = pd.concat(chunks).iloc[:sample_size]
    
    return df

def clean_data(df):
    """
    Clean and prepare the dataset
    """
    # Parse datetime columns
    df['pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    # Extract time features
    df['hour'] = df['pickup_datetime'].dt.hour
    df['day_of_week'] = df['pickup_datetime'].dt.dayofweek
    df['month'] = df['pickup_datetime'].dt.month
    df['year'] = df['pickup_datetime'].dt.year
    
    # Calculate fare per mile
    df['fare_per_mile'] = df['fare_amount'] / df['trip_distance']
    
    # Remove outliers and invalid trips
    df = df[
        (df['trip_distance'] > 0) & 
        (df['trip_distance'] < 100) &
        (df['fare_amount'] > 0) &
        (df['fare_amount'] < 500) &
        (df['fare_per_mile'] > 0) &
        (df['fare_per_mile'] < 50)
    ]
    
    return df

# ============================================================================
# STEP 2: ANALYZE MANHATTAN DOMINANCE
# ============================================================================

def analyze_geographic_distribution(df):
    """
    Quantify Manhattan's share of rides
    """
    # Classify zones (adjust based on your zone definitions)
    # Assume you have PULocationID and DOLocationID columns
    manhattan_zones = range(4, 234)  # Example: adjust to actual Manhattan zone IDs
    
    df['pickup_manhattan'] = df['PULocationID'].isin(manhattan_zones)
    df['dropoff_manhattan'] = df['DOLocationID'].isin(manhattan_zones)
    df['manhattan_trip'] = df['pickup_manhattan'] | df['dropoff_manhattan']
    
    # Calculate statistics
    results = {
        'total_trips': len(df),
        'manhattan_trips': df['manhattan_trip'].sum(),
        'manhattan_percentage': (df['manhattan_trip'].sum() / len(df)) * 100,
        'manhattan_revenue': df[df['manhattan_trip']]['fare_amount'].sum(),
        'total_revenue': df['fare_amount'].sum(),
        'manhattan_revenue_pct': (df[df['manhattan_trip']]['fare_amount'].sum() / 
                                   df['fare_amount'].sum()) * 100
    }
    
    print(f"Manhattan dominance:")
    print(f"  Trips: {results['manhattan_percentage']:.1f}%")
    print(f"  Revenue: {results['manhattan_revenue_pct']:.1f}%")
    
    return results

# ============================================================================
# STEP 3: IDENTIFY HIGH/LOW DEMAND ZONES
# ============================================================================

def segment_zones_by_demand(df):
    """
    Categorize zones into high/medium/low demand
    """
    # Count trips by pickup zone
    zone_stats = df.groupby('PULocationID').agg({
        'fare_amount': ['count', 'sum', 'mean'],
        'trip_distance': 'mean',
        'fare_per_mile': 'mean'
    }).reset_index()
    
    zone_stats.columns = ['zone_id', 'trip_count', 'total_revenue', 
                          'avg_fare', 'avg_distance', 'avg_fare_per_mile']
    
    # Calculate trips per hour (assuming data span)
    hours_in_data = (df['pickup_datetime'].max() - 
                     df['pickup_datetime'].min()).total_seconds() / 3600
    zone_stats['trips_per_hour'] = zone_stats['trip_count'] / hours_in_data
    zone_stats['revenue_per_hour'] = zone_stats['total_revenue'] / hours_in_data
    
    # Segment into tiers
    zone_stats['demand_tier'] = pd.qcut(zone_stats['trips_per_hour'], 
                                         q=3, 
                                         labels=['Low', 'Medium', 'High'])
    
    # Display top zones
    print("\nTop 10 highest demand zones:")
    print(zone_stats.nlargest(10, 'trips_per_hour')[
        ['zone_id', 'trips_per_hour', 'revenue_per_hour', 'avg_fare']
    ])
    
    return zone_stats

# ============================================================================
# STEP 4: ESTIMATE PRICE ELASTICITY
# ============================================================================

def estimate_demand_elasticity(df, zone_stats):
    """
    Estimate how demand responds to price changes
    """
    # Merge zone stats with trip data
    df_analysis = df.merge(zone_stats[['zone_id', 'demand_tier']], 
                           left_on='PULocationID', 
                           right_on='zone_id', 
                           how='left')
    
    # Aggregate by zone-hour combinations
    demand_data = df_analysis.groupby(['PULocationID', 'hour']).agg({
        'fare_amount': 'count',
        'fare_per_mile': 'mean'
    }).reset_index()
    demand_data.columns = ['zone_id', 'hour', 'trip_count', 'avg_price']
    
    # Remove zeros and take logs
    demand_data = demand_data[
        (demand_data['trip_count'] > 0) & 
        (demand_data['avg_price'] > 0)
    ]
    demand_data['log_quantity'] = np.log(demand_data['trip_count'])
    demand_data['log_price'] = np.log(demand_data['avg_price'])
    
    # Run regression: log(quantity) = a + b*log(price) + controls
    X = demand_data[['log_price', 'hour']]
    y = demand_data['log_quantity']
    
    # Add hour dummies
    X = pd.get_dummies(X, columns=['hour'], drop_first=True)
    
    model = LinearRegression()
    model.fit(X, y)
    
    elasticity = model.coef_[0]  # Coefficient on log_price
    
    print(f"\nEstimated price elasticity: {elasticity:.3f}")
    print(f"Interpretation: A 10% price increase leads to a {elasticity*10:.1f}% change in demand")
    
    return elasticity, model

# ============================================================================
# STEP 5: SIMULATE DIFFERENTIATED PRICING SCENARIOS
# ============================================================================

def simulate_pricing_scenarios(df, zone_stats, elasticity):
    """
    Predict revenue impact of differentiated pricing
    """
    # Merge demand tiers
    df_sim = df.merge(zone_stats[['zone_id', 'demand_tier', 'trips_per_hour']], 
                      left_on='PULocationID', 
                      right_on='zone_id', 
                      how='left')
    
    # Define pricing scenarios
    scenarios = {
        'baseline': {'High': 1.0, 'Medium': 1.0, 'Low': 1.0},
        'conservative': {'High': 1.15, 'Medium': 1.0, 'Low': 0.90},
        'moderate': {'High': 1.25, 'Medium': 1.0, 'Low': 0.85},
        'aggressive': {'High': 1.40, 'Medium': 1.0, 'Low': 0.75}
    }
    
    results = []
    
    for scenario_name, price_multipliers in scenarios.items():
        df_scenario = df_sim.copy()
        
        # Apply price changes
        df_scenario['price_multiplier'] = df_scenario['demand_tier'].map(price_multipliers)
        df_scenario['new_fare'] = df_scenario['fare_amount'] * df_scenario['price_multiplier']
        
        # Apply demand response (elasticity)
        df_scenario['demand_change'] = df_scenario['price_multiplier'] ** elasticity
        df_scenario['weight'] = df_scenario['demand_change']  # For weighted calculations
        
        # Calculate metrics
        scenario_result = {
            'scenario': scenario_name,
            'baseline_revenue': df_sim['fare_amount'].sum(),
            'new_revenue': (df_scenario['new_fare'] * df_scenario['weight']).sum(),
            'baseline_trips': len(df_sim),
            'new_trips': df_scenario['weight'].sum(),
        }
        
        scenario_result['revenue_change_pct'] = (
            (scenario_result['new_revenue'] - scenario_result['baseline_revenue']) / 
            scenario_result['baseline_revenue'] * 100
        )
        scenario_result['trips_change_pct'] = (
            (scenario_result['new_trips'] - scenario_result['baseline_trips']) / 
            scenario_result['baseline_trips'] * 100
        )
        
        results.append(scenario_result)
    
    results_df = pd.DataFrame(results)
    print("\nScenario Analysis Results:")
    print(results_df[['scenario', 'revenue_change_pct', 'trips_change_pct']])
    
    return results_df

# ============================================================================
# STEP 6: VISUALIZE RESULTS
# ============================================================================

def create_visualizations(df, zone_stats, results_df):
    """
    Create compelling visualizations for the proposal
    """
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Geographic distribution heatmap
    ax1 = axes[0, 0]
    zone_stats_sorted = zone_stats.nlargest(20, 'trip_count')
    ax1.barh(zone_stats_sorted['zone_id'].astype(str), zone_stats_sorted['trip_count'])
    ax1.set_xlabel('Number of Trips')
    ax1.set_ylabel('Zone ID')
    ax1.set_title('Top 20 Zones by Trip Volume')
    
    # 2. Demand by hour
    ax2 = axes[0, 1]
    hourly = df.groupby('hour')['fare_amount'].count()
    ax2.plot(hourly.index, hourly.values, marker='o')
    ax2.set_xlabel('Hour of Day')
    ax2.set_ylabel('Number of Trips')
    ax2.set_title('Demand Pattern by Hour')
    ax2.grid(True, alpha=0.3)
    
    # 3. Revenue by demand tier
    ax3 = axes[1, 0]
    df_merged = df.merge(zone_stats[['zone_id', 'demand_tier']], 
                         left_on='PULocationID', right_on='zone_id', how='left')
    tier_revenue = df_merged.groupby('demand_tier')['fare_amount'].sum()
    ax3.bar(tier_revenue.index, tier_revenue.values)
    ax3.set_xlabel('Demand Tier')
    ax3.set_ylabel('Total Revenue ($)')
    ax3.set_title('Current Revenue by Demand Tier')
    
    # 4. Scenario comparison
    ax4 = axes[1, 1]
    x = np.arange(len(results_df))
    width = 0.35
    ax4.bar(x - width/2, results_df['revenue_change_pct'], width, label='Revenue Change %')
    ax4.bar(x + width/2, results_df['trips_change_pct'], width, label='Trips Change %')
    ax4.set_xlabel('Pricing Scenario')
    ax4.set_ylabel('Percentage Change')
    ax4.set_title('Impact of Differentiated Pricing')
    ax4.set_xticks(x)
    ax4.set_xticklabels(results_df['scenario'], rotation=45)
    ax4.legend()
    ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('tlc_pricing_analysis.png', dpi=300, bbox_inches='tight')
    print("\nVisualization saved as 'tlc_pricing_analysis.png'")
    
    return fig

# ============================================================================
# STEP 7: GENERATE SUMMARY REPORT
# ============================================================================

def generate_summary_report(geo_results, zone_stats, elasticity, results_df):
    """
    Create a text summary for your proposal
    """
    report = f"""
    ============================================================
    DIFFERENTIATED PRICING PROPOSAL - EXECUTIVE SUMMARY
    ============================================================
    
    PROBLEM STATEMENT:
    ------------------
    - Manhattan accounts for {geo_results['manhattan_percentage']:.1f}% of all trips
    - Generates {geo_results['manhattan_revenue_pct']:.1f}% of total revenue
    - High congestion in peak zones reduces efficiency
    - Outer boroughs remain underserved
    
    CURRENT STATE:
    -------------
    - Total zones analyzed: {len(zone_stats)}
    - High-demand zones (top 33%): {(zone_stats['demand_tier']=='High').sum()}
    - Average trips/hour in high-demand zones: {zone_stats[zone_stats['demand_tier']=='High']['trips_per_hour'].mean():.1f}
    - Average trips/hour in low-demand zones: {zone_stats[zone_stats['demand_tier']=='Low']['trips_per_hour'].mean():.1f}
    
    PRICE SENSITIVITY:
    -----------------
    - Estimated demand elasticity: {elasticity:.3f}
    - Demand is {'elastic' if abs(elasticity) > 1 else 'inelastic'} (|ε| {'>' if abs(elasticity) > 1 else '<'} 1)
    - A 10% price increase → {elasticity*10:.1f}% change in trips
    
    RECOMMENDED SCENARIO: MODERATE PRICING
    --------------------------------------
    - High-demand zones: +25% pricing
    - Medium-demand zones: No change
    - Low-demand zones: -15% pricing (incentive)
    
    PROJECTED IMPACT:
    ----------------
    - Revenue change: {results_df[results_df['scenario']=='moderate']['revenue_change_pct'].values[0]:+.2f}%
    - Trip volume change: {results_df[results_df['scenario']=='moderate']['trips_change_pct'].values[0]:+.2f}%
    - Expected annual revenue impact: ${results_df[results_df['scenario']=='moderate']['new_revenue'].values[0] - results_df[results_df['scenario']=='moderate']['baseline_revenue'].values[0]:,.0f}
    
    BENEFITS:
    --------
    1. Increased revenue from high-demand zones
    2. Better geographic distribution of taxi supply
    3. Improved service in underserved areas
    4. Reduced congestion in Manhattan core
    5. Higher driver utilization and earnings
    
    IMPLEMENTATION:
    --------------
    - Phase 1: Pilot in select zones (3 months)
    - Phase 2: Gradual rollout with monitoring
    - Phase 3: Continuous optimization based on data
    
    ============================================================
    """
    
    print(report)
    
    with open('pricing_proposal_summary.txt', 'w') as f:
        f.write(report)
    print("Summary saved as 'pricing_proposal_summary.txt'")
    
    return report

# ============================================================================
# MAIN EXECUTION PIPELINE
# ============================================================================

def main():
    """
    Execute complete analysis pipeline
    """
    print("Starting TLC Taxi Pricing Analysis...")
    print("=" * 60)
    
    # STEP 1: Load data
    print("\n[1/7] Loading and cleaning data...")
    df = load_and_sample_data('yellow_tripdata_2023-01.csv', sample_size=500000)
    df = clean_data(df)
    print(f"   Loaded {len(df):,} valid trips")
    
    # STEP 2: Geographic analysis
    print("\n[2/7] Analyzing geographic distribution...")
    geo_results = analyze_geographic_distribution(df)
    
    # STEP 3: Zone segmentation
    print("\n[3/7] Segmenting zones by demand...")
    zone_stats = segment_zones_by_demand(df)
    
    # STEP 4: Elasticity estimation
    print("\n[4/7] Estimating price elasticity...")
    elasticity, model = estimate_demand_elasticity(df, zone_stats)
    
    # STEP 5: Scenario simulation
    print("\n[5/7] Simulating pricing scenarios...")
    results_df = simulate_pricing_scenarios(df, zone_stats, elasticity)
    
    # STEP 6: Visualizations
    print("\n[6/7] Creating visualizations...")
    fig = create_visualizations(df, zone_stats, results_df)
    
    # STEP 7: Summary report
    print("\n[7/7] Generating summary report...")
    report = generate_summary_report(geo_results, zone_stats, elasticity, results_df)
    
    print("\n" + "=" * 60)
    print("Analysis complete! Check output files for results.")
    
    return df, zone_stats, elasticity, results_df

# Run the analysis
if __name__ == "__main__":
    df, zone_stats, elasticity, results_df = main()
