# gridcast
Anomaly detection and short-term forecasting for Portugal's electricity grid using public REN data.


grid-anomaly-detection/
├── README.md
├── pyproject.toml              # project metadata & dependencies
├── requirements.txt            # pinned versions (for Docker)
│
├── data/                       # local raw & processed data (dev only, not prod)
│   ├── raw/
│   ├── processed/
│   └── models/
│
├── src/
│   ├── __init__.py
│   │
│   ├── data/                   # data ingestion & preprocessing
│   │   ├── fetch.py            # download/scrape REN data
│   │   ├── preprocess.py       # resampling, feature engineering
│   │   └── storage.py          # read/write parquet, DB utils
│   │
│   ├── models/                 # anomaly detection models
│   │   ├── train.py            # training pipeline
│   │   ├── infer.py            # anomaly scoring
│   │   └── utils.py            # save/load models
│   │
│   ├── api/                    # FastAPI application
│   │   ├── main.py             # entrypoint: FastAPI app
│   │   ├── schemas.py          # Pydantic request/response models
│   │   └── service.py          # wraps model inference logic
│   │
│   └── airflow/                # Airflow DAGs
│       ├── dags/
│       │   └── daily_ingest.py # pulls yesterday's REN data
│       │   └── weekly_train.py # retrains model weekly
│       └── docker-compose.yaml # optional local airflow setup
│
├── docker/                     # docker configurations
│   ├── Dockerfile.api          # FastAPI service image
│   ├── Dockerfile.airflow      # Airflow worker image
│   └── docker-compose.yaml     # local orchestration
│
├── tests/                      # pytest unit tests
│   ├── test_preprocess.py
│   ├── test_train.py
│   └── test_api.py
│
└── notebooks/                  # exploratory analysis
    ├── 01_explore_data.ipynb
    ├── 02_feature_engineering.ipynb
    └── 03_model_prototype.ipynb