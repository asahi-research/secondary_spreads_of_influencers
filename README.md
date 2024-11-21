[![arXiv](https://img.shields.io/badge/arXiv-2411.05448-b31b1b.svg)](https://arxiv.org/abs/2411.05448)

# Influencers amplify the chain of shared posts in online communities
This project is supplementally materials of "Influencers’ Reposts and Viral Diffusion: Prestige Bias in Online Communities.".  
In paticular, the project holds our analysis codes and data.  

# If you want to replicate the experiment

```python
# After `rye sync` (setup this script) and downloaded files from Google Drive.

# retweet counts >= 5000 and in 3hours
rye run python src/secondary_spreads_of_influencers/analysis_second_spreads.py --retweet_count_path data/retweet_counts.parquet --aggregated_virtual_timeline data/virtual_timeline_agggated_data_anonymized/in_3hours_anonymyzed.parquet --user_influence_score_path data/user_hgindex_anonymized.parquet --is_in_cascade --exclude_official --min_rt 5000 --save_path YOUR_SAVE_PATH
```

# Data Overview
The following data represents summary statistics that were used to generate plots and analyses in the paper:

```
data
├── Figure3_raw.xlsx
├── Figure4a_raw.xlsx
├── Figure4b_raw.xlsx
├── FigureS1_summary.xlsx
├── FigureS2_summary.xlsx
└── RetweetStatistics.xlsx
```

The source data also is available in compressed parquet format for reproducibility.  
However, due to the large file sizes, we have uploaded them at Google Drive.  
The files can be downloaded [here](https://drive.google.com/drive/folders/1-KBr37RuOi1yrtaMI-QQKmTvENBub-xr?usp=sharing).

```
├── user_hgindex_anonymized.parquet.zip      # User influence metrics
├── virtual_timeline_agggated_data_anonymized.zip
│   ├── all_anonymized.parquet               # Complete timeline data
│   ├── in_1day_anonymized.parquet          # 24hour aggregated data
│   ├── in_12hours_anonymized.parquet       # 12hour aggregated data
│   ├── in_6hours_anonymized.parquet        # 6hour aggregated data
│   └── in_3hours_anonymized.parquet        # 3hour aggregated data
├── target_retweets.parquet.zip             # Analysis target tweets
└── retweet_counts.parquet.zip              # Source tweet metrics
```

### target\_retweets.parquet
Contains three columns derived from X's API response.
This data consists of three columns derived from X's API response JSON data:
- source\_tweet\_id: corresponds to the 'id' field within the 'retweeted\_status' object
- tweet\_id: corresponds to the 'id' field
- timestamp: Unix timestamp converted from the 'created\_at' field

### virtual\_timeline\_agggated\_data\_anonymized
Contains aggregated view and retweet counts per second spreaders across different time windows.
Each file represents data aggregated over the specified time period.

### user\_hgindex\_anonymized.parquet
Contains anonymized user influence metrics including:
- Anonymized user identifiers
- hg-index values
- Related influence metrics

### retweet\_counts.parquet
Contains retweet count metrics for source tweets.


For dataset-related inquiries, please contact niitsuma-t[at]asahi.com.


# Code Overview

```
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
```

# Citation
```
@misc{takuro2024influencersrepostsviraldiffusion,
      title={Influencers' Reposts and Viral Diffusion: Prestige Bias in Online Communities}, 
      author={Takuro Niitsuma and Mitsuo Yoshida and Hideaki Tamori and Yo Nakawake},
      year={2024},
      eprint={2411.05448},
      archivePrefix={arXiv},
      primaryClass={cs.SI},
      url={https://arxiv.org/abs/2411.05448}}
```
