# Ingestion Data Project: Scraping Data from Transfermarkt Website using Azure Services

This repository serves as a demonstration of building an ingestion service, utilizing various Azure services.

## Project Overview

The project aims to showcase how to scrape data from the Transfermarkt website using Azure services such as Azure Function, Azure Data Lake Storage 2 and Azure Data Factory. The data includes English Premier League players attribute such as the name, position, height, preferred foot and market value. Load date is also added to ease the data lineage.

## Components
### Ingestion

The ingestion part of the project has been implemented using Azure Functions. Azure Functions provide a serverless compute service that enables to run code on-demand without having to explicitly provision or manage infrastructure. It took advantage of Azure Durable Function capability to parallelize and speed up the process of extracting the players data.

* Azure Function: Set up to scrape data from the Transfermarkt website and ingest it into Azure Data Lake Storage 2 in Delta Lake format.
* Trigger Schedule: Ideally, the Azure Function should be at least triggered twice in a season which is after August when the first transfer market has been closed and after January when the second transfer market happened.

### Storage

We will utilize Azure Data Lake Storage or Azure Blob Storage to store both raw and processed data. Azure Data Lake Storage is a secure, massively scalable, and performance-tuned data lake for big data analytics.

## Prerequisites
* Azure Subscription
* Python 3.7 or higher
* Azure Function Core Tools
* Azure CLI

## Disclaimer

This project is intended solely for learning purposes. The data scraped from the Transfermarkt website is used only for educational demonstrations and not for any commercial purposes. I do not intend to monetize any data obtained from Transfermarkt.