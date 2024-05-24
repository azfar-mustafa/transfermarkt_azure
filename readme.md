# End-to-End Data Project: Scraping Data from Transfermarkt Website using Azure Services

This repository serves as a demonstration of building an end-to-end data project focused on scraping data from the Transfermarkt website, utilizing various Azure services. Below, you'll find an overview of what has been implemented so far and what's planned for the project.

## Project Overview

The project aims to showcase how to scrape data from the Transfermarkt website and process it using Azure services such as Azure Function, Azure Data Lake Storage 2, Azure Data Factory, Azure SQL Database and PowerBI. The data includes English Premier League players attribute such as the name, position, height, preferred foot and market value. Load date is also added to ease the data lineage.

## Project Architecture
![alt text](<Data_Pipeline_Flow-High Level Architecture.drawio.png>)

### Ingestion (Completed)

The ingestion part of the project has been implemented using Azure Functions. Azure Functions provide a serverless compute service that enables you to run code on-demand without having to explicitly provision or manage infrastructure. It took advantage of Azure Durable Function capability to parallelize and speed up the process of extracting the players data.

In this section, we have set up an Azure Function to scrape data from the Transfermarkt website and ingest it into Azure Data Lake Storage 2 in Delta Lake format. Ideally, the Azure Function should be at least triggered twice in a season which is after August when the first transfer market has been closed and after January when the second transfer market happened.

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
