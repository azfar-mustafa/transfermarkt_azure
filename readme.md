# End-to-End Data Project: Scraping Data from Transfermarkt Website using Azure Services

Welcome to the End-to-End Data Project repository! This repository serves as a demonstration of building an end-to-end data project focused on scraping data from the Transfermarkt website, utilizing various Azure services. Below, you'll find an overview of what has been implemented so far and what's planned for the project.

## Project Overview

The project aims to showcase how to scrape data from the Transfermarkt website and process it using Azure services. The scraped data could include player statistics, transfer values, market trends, and more.

### Ingestion (Completed)

The ingestion part of the project has been implemented using Azure Functions. Azure Functions provide a serverless compute service that enables you to run code on-demand without having to explicitly provision or manage infrastructure.

In this section, we have set up an Azure Function to scrape data from the Transfermarkt website and ingest it into Azure services. The Azure Function is triggered based on a predefined schedule or event, ensuring continuous data ingestion.

## Next Steps

While the ingestion part is completed, the project is still in progress. Here's what's next:

### Processing (In Progress)

The next phase of the project involves processing the scraped data, performing transformations, and cleaning it. We plan to use Azure Databricks or Azure HDInsight for this purpose. Azure Databricks is a fast, easy, and collaborative Apache Spark-based analytics platform optimized for Azure. It provides a unified analytics platform for big data and machine learning.

### Storage

We will utilize Azure Data Lake Storage or Azure Blob Storage to store both raw and processed data. Azure Data Lake Storage is a secure, massively scalable, and performance-tuned data lake for big data analytics.

### Analysis, Visualization, Deployment, and Monitoring

Following processing and storage, we will proceed with data analysis using Azure Synapse Analytics or Azure Analysis Services. Visualization will be done using Power BI or Azure Data Studio. Deployment and monitoring of the entire pipeline will be managed using Azure DevOps or Azure Monitor.

## Conclusion

Stay tuned for updates as we continue to build and expand this end-to-end data project.
