# 🚗 BeejanRide Analytics Platform

A production-grade dbt analytics platform for BeejanRide, a UK mobility startup operating in 5 cities. This project transforms raw operational data into actionable business insights using a modular, well-tested, and documented approach.

## 📋 Project Overview

BeejanRide provides:
- **Ride-hailing**
- **Airport transfers**
- **Scheduled corporate rides**

The analytics platform supports key business objectives including revenue tracking, driver performance, rider lifetime value, fraud detection, and operational monitoring.

## 🏗️ Architecture

### **Layered Data Model**

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| **Staging** | Clean, type, deduplicate raw data | Views |
| **Intermediate** | Reusable business logic and metrics | Tables |
| **Marts (Core)** | Star schema (facts & dimensions) | Tables |
| **Marts (Analytics)** | Business-ready aggregations | Tables |
| **Snapshots** | SCD Type 2 historical tracking | Snapshot |

## 📊 Source Data Dictionary

### **1. trips_raw**
| Column | Description |
|--------|-------------|
| trip_id | Unique trip identifier |
| rider_id | Rider ID |
| driver_id | Driver ID |
| vehicle_id | Vehicle used |
| city_id | City identifier |
| requested_at | Time ride was requested |
| pickup_at | Pickup timestamp |
| dropoff_at | Dropoff timestamp |
| status | completed / cancelled / no_show |
| estimated_fare | Initial fare estimate |
| actual_fare | Final fare |
| surge_multiplier | Surge applied |
| payment_method | card / wallet / cash |
| is_corporate | Boolean |
| created_at | Record creation timestamp |
| updated_at | Last update timestamp |

### **2. drivers_raw**
| Column | Description |
|--------|-------------|
| driver_id | Driver ID |
| onboarding_date | Driver join date |
| driver_status | active / suspended / inactive |
| city_id | City |
| vehicle_id | Current vehicle |
| rating | Average rating |
| created_at | Created timestamp |
| updated_at | Updated timestamp |

### **3. riders_raw**
| Column | Description |
|--------|-------------|
| rider_id | Rider ID |
| signup_date | Signup date |
| country | Rider country |
| referral_code | Referral used |
| created_at | Created timestamp |

### **4. payments_raw**
| Column | Description |
|--------|-------------|
| payment_id | Payment ID |
| trip_id | Trip ID |
| payment_status | success / failed |
| payment_provider | stripe / paypal |
| amount | Charged amount |
| fee | Processing fee |
| currency | Currency |
| created_at | Payment timestamp |

### **5. cities_raw**
| Column | Description |
|--------|-------------|
| city_id | City ID |
| city_name | City name |
| country | Country |
| launch_date | City launch date |

### **6. driver_status_events_raw** (High Volume Table)
| Column | Description |
|--------|-------------|
| event_id | Event ID |
| driver_id | Driver ID |
| status | online / offline |
| event_timestamp | Event timestamp |

## 🔧 Model Structure

## 📈 Business Metrics & Models

| Business Objective | Model | Key Columns |
|-------------------|-------|-------------|
| **Daily revenue per city** | `daily_revenue` | revenue_date, city_name, gross_revenue |
| **Gross vs net revenue** | `fact_trips` | gross_revenue, net_revenue |
| **Corporate vs personal split** | `fact_trips` | trip_type, is_corporate_trip |
| **Top drivers by revenue** | `driver_leaderboard` | driver_id, total_revenue, city_rank |
| **Driver activity monitoring** | `int_driver_activity` | online_sessions, total_online_minutes |
| **Rider lifetime value** | `int_rider_metrics` | rider_ltv, ltv_segment |
| **Payment failure rate** | `payment_reliability` | failure_rate_percentage |
| **Surge impact analysis** | `fact_trips` | surge_impact_category |
| **Driver churn tracking** | `init_driver_metrics` | driver_health_status |
| **Fraud detection** | `fraud_monitoring` | fraud_reason, is_fraud_flag |

## ✅ Data Quality & Testing

### **Generic Tests**
- `not_null` - Primary keys and required fields
- `unique` - Primary key uniqueness
- `relationships` - Foreign key integrity
- `accepted_values` - Enum field validation

### **Custom Tests**
```sql
-- No negative revenue
SELECT * FROM int_trip_details WHERE actual_fare < 0

-- Trip duration > 0
SELECT * FROM int_trip_details 
WHERE trip_status = 'completed' AND trip_duration_minutes <= 0

-- Completed trip must have successful payment
SELECT t.trip_id 
FROM stg_trips t
LEFT JOIN stg_payments p ON t.trip_id = p.trip_id
WHERE t.trip_status = 'completed' 
  AND (p.payment_status != 'success' OR p.payment_id IS NULL)
  # Clone repository
git clone https://github.com/mabelyunaan/beejanride-analytics
cd beejanride-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt
pip install dbt-bigquery

# Install dbt dependencies
dbt deps

# Test connection
dbt debug

# Run staging models
dbt run --select staging

# Run intermediate models
dbt run --select intermediate

# Run marts
dbt run --select marts

# Run snapshots
dbt snapshot

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
