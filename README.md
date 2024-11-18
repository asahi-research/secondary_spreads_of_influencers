# Influencers amplify the chain of shared posts in online communities
This project is supplementally materials of "Influencers’ Reposts and Viral Diffusion: Prestige Bias in Online Communities.".

In paticular, the project holds our analysis codes.  
This project does not include data and collecting code for X (Twitter) API data.  

In consideration of privacy, we do not release data containing individual user information.
However, we will release the aggregated data used for the plots and the code used to create them.

# Data Overview
data
├── Figure3_raw.xlsx
├── Figure4a_raw.xlsx
├── Figure4b_raw.xlsx
├── FigureS1_summary.xlsx
├── FigureS2_summary.xlsx
└── RetweetStatistics.xlsx

# Code Overview
src/secondary_spreads_of_influencers
├── __init__.py
├── models.py
├── aggregate_second_spreads.py
├── analysis_second_spreads.py
├── build_follow_relations_data.py
├── build_rt_cascades.py
├── canonicalize_source_tweets.py
├── collecting_retweets.py
├── compute_centrality_from_retweets.py
├── compute_h_index.py
├── compute_retweet_stats_by_user_influence.py
├── compute_second_spread_ratio.py
├── compute_second_spread_share.py (Figure 4(b))
├── compute_structural_virality.py
├── compute_sv_per_first_reposter.py
├── explode_rt_cascades.py
├── extract_present_tweet_id_in_cascade.py
├── extract_qt_ids.py
├── extract_rt_timestamps.py
├── extract_user_bio.py
├── format_retweet_data.py
├── merge_retweet_rate_results.py
├── plot_second_spread_results.py (Figure 3)
├── plot_retweet_user_ccdf.py (Figure S1, Figure 4(a))
├── plot_sv_by_user_influence.py (Figure S2)
├── rt_path_clustering.py
└── utils.py

