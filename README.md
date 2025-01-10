# Surivival Analysis

## Overview

This project aims to analyze the time it takes for customer-returned products to be refurbished and sold. Additionally, we investigate how long these refurbished products remain on the shelf before being sold. The project uses survival analysis techniques, including Kaplan-Meier and cohort approaches, to derive insights from the data.

## Dataset

The project includes raw datasets stored as CSV files. These datasets provide the foundation for the analysis and contain the following tables:

Products Table: Contains information about the products

Returns Table: Contains information about returned products

Return Status Table: Contains information about the status (after being refurbished) whether the product is on shelf or sold

Calendar Date Table: Contains calendar date

## Analysis Workflow

### Data Preparation

Sign up for trial snowflake account. 

Load the raw datasets from the CSV files.

Join and clean the data to create a unified dataset for analysis.

### Survival Analysis

Applied Kaplan-Meier and cohort analysis to evaluate:

The duration from product return to refurbishment and sale.

The time refurbished products spend on the shelf before being sold.

### Visualization

Survival curves and cohort-based timelines to visualize key insights (Survival Analysis.twbx)

