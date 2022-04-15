# SFYR Data Pipeline Implementation

Our Company, Sfyr (pronounced: Sapphire /sæfaɪə(r)/) aims to provide retail investors with the tools and data required to support their high-frequency retail trading decisions and needs. Through the use of a data warehouse, Apache Airflow and public data sources, Sfyr provides its clients with accurate, consolidated and updated stock data.

### _Keywords_

_Data Pipeline, Data Engineering, Data Architecture, Data Warehouse, Scheduler, DAG, Airflow, BigQuery, Firestore, FinBERT, Sentiment, Stocks Analysis, Investment Insights, Web Scraping, Google Cloud Monitoring._

## IS3107 Group 7 | Authors:

- Loh Hong Tak Edmund (A0199943H)
- Ng Ting You (A0201672N)
- Tan Yi Bing (A0204181U)
- Wong Zhou Wai (A0201509R)
- Yap Hui Yi (A0203707M)

<p align="right">(<a href="#top">back to top</a>)</p>

## Codes and Resources Used

**Python Version:** 3.9.5
**Built with:** [Microsoft Visual Studio Code](https://code.visualstudio.com/),
[Oracle VM Virtual Box](https://www.virtualbox.org/), [Git](https://git-scm.com/)
**Notable Packages:** apache-airflow, beautifulsoup4, datetime, firebase-admin, json, numpy, pandas, pandas-gbq, telethon, pandas, parse, pendulum, regex, tokenizers, torch, transformers, virtualenv, yahoo-fin, yfinance (view requirements.txt for full list)

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

### Prerequisites

Make sure you have installed all of the following on your development machine:

- Python 3.9.5
- Oracle VM Virtual Box (Highly recommended for running Airflow)

### Installation

We recommend setting up a virtual machine and virtual environment to run this project.

### _1. Virtual Machine_

To set up a VM Virtual Box, please follow the steps detailed here.

The VM image file is located here.

### _2. Virtual Python Environment_

shell commnands

```sh
pip install virtualenv
virtualenv <your_env_name>
source <your_env_name>/bin/active
```

The requirements.txt file contains Python libraries that your notebooks depend on, and they will be installed using:

```sh
`pip install -r requirements.txt`
```

### _3. Setting Up Airflow_

Edit the airflow.cfg config file with the following rules:

```python
dags_folder = /home/airflow/is3107_g7

enable_xcom_pickling = True

load_examples = False
```

### _4. Setting Up Databases Access_

Place the Google Authentication Crediential JSON files in `utils/` .

Update `utils/serviceAccount.json` with the name of the credential file

###

<p align="right">(<a href="#top">back to top</a>)</p>

## Usage
