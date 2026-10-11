# Azure Databricks Hotel Reservation & Occupancy Analytics

## Project Overview

This portfolio project demonstrates an end-to-end hotel analytics data pipeline using Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, and Unity Catalog.

The objective is to transform hotel operational datasets into validated, analytics-ready tables for reservation analysis, daily occupancy monitoring, and hotel-level performance reporting.

This project uses synthetic/sample hotel data and is not an official Hilton production implementation.

## Architecture

Source CSV files → ADLS Gen2 → Bronze → Silver → Gold → Databricks AI/BI Dashboard

## Technology Stack

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Jobs
- Databricks AI/BI Dashboards

## Medallion Architecture

### Bronze Layer
- Ingested eight source datasets into Delta tables.
- Added ingestion metadata for source tracking.

### Silver Layer
- Standardized string fields and status values.
- Removed duplicate records based on defined keys.
- Validated reservation dates, amounts, and room counts.
- Quarantined invalid records where validation rules were implemented.
- Checked relationships between related datasets.
- Calculated occupancy percentages from rooms sold and rooms available.

### Gold Layer
Created three analytics tables:
- `hotel_daily_occupancy`
- `reservation_analytics`
- `hotel_revenue_summary`

These tables support occupancy trend analysis, reservation analysis, and hotel-level performance reporting.

## Orchestration

Created a Databricks Job with dependent Bronze ingestion, Silver transformation, and Gold analytics tasks. The end-to-end job run completed successfully.

## Validation

- Verified source and target record counts.
- Checked duplicate reservation IDs.
- Validated relationships between related datasets.
- Checked occupancy percentage ranges and reservation values.
- Reconciled reservation-level values against aggregated hotel summaries.

## Current Scope and Future Improvements

The current implementation uses full-refresh ingestion. Planned improvements include incremental ingestion, idempotent Delta MERGE operations, monitoring, audit logging, and expanded dashboard KPIs.

