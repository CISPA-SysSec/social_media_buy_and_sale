# Dynamics of Buy and Sale of Social Media Profiles
In this research, we explored how social media profiles are advertised for buy and sale in various marketplaces. 
We provide source code and data that aided in produce of artifacts of the technical report.
The system apparatus requires private API keys, an in-house MongoDB database installation, and several other dependencies listed below. 

## Data Collection Pipeline
Our data collection is facilitated by multiple APIs. We provide the setup details at `code/apify_search.py`.

The crawling module and further instructions are in `code/data_collection`.


## Collected Data
We present our data in `data/` directory where `data/open_markpetlace` and `data/social_media_posts` are provided.

## Post Analysis: Scam Detection and Clustering

This setup includes analyzing and detecting scam posts using clustering techniques such as BERTopic. Follow the steps below to prepare the data, create embeddings, cluster and identify scam posts, and analyze the results.

---

### Prerequisites

The provided dataset is located at `data/social_media_posts`. Ensure the post data is placed in the `posts` directory, which is where the code expects it by default.

---

### Steps for Analysis

#### 1. Embedding Generation

Generate embeddings for the posts by running the following command:

```bash
python create_embeddings.py
```

---

#### 2. Post Analysis

Once embeddings are created, use the BERTopic framework for scam analysis and clustering:

```bash
python code/run_BERTopic.py --config=configurations/BERTopic/config.json
```

- **`--config`**: Specifies the BERTopic JSON configuration file.

To modify the clustering hyperparameters, create a new JSON configuration file in the `configurations/BERTopic` directory.

---

#### 3. CSV Results

After running the analysis script, update the `PATH_RESULT` variable in the code to point to the directory containing the results. Then, generate the CSV result files using:

```bash
python code/analyze_BERTopic.py --config=configurations/BERTopic/config.json
```

- **`--config`**: Specifies the BERTopic JSON configuration file.

---

#### 4. Extract Additional Insights

To generate additional analysis files, update the `PATH_CSV_DATA` variable in the code to point to the CSV results obtained from the previous step. Then, execute the following command:

```bash
python code/extract_results_BERTopic.py
```

---

### Directory Structure

```plaintext
.
├── data/
│   └── social_media_posts/
├── configurations/
│   └── BERTopic/
│       └── <JSON configuration files>
├── code/
│   ├── run_BERTopic.py
│   ├── analyze_BERTopic.py
│   └── extract_results_BERTopic.py
│   └── data_collection/run_pipeline.sh
├── posts/
└── create_embeddings.py
```

## Citation 

```
@inproceedings{beluri2025exploration,
  title={Exploration of the dynamics of buy and sale of social media accounts},
  author={Beluri, Mario and Acharya, Bhupendra and Khodayari, Soheil and Stivala, Giada and Pellegrino, Giancarlo and Holz, Thorsten},
  booktitle={ACM Internet Measurement Conference},
  year={2025}
}
```
