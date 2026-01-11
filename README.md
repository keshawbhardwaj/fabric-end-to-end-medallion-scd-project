# fabric-end-to-end-medallion-scd-project

# Microsoft Fabric End-to-End Medallion Architecture Project

This repository demonstrates a complete **end-to-end data engineering project** using **Microsoft Fabric**, covering:

- Medallion Architecture (Bronze → Stage → Gold)
- Lakehouse + Warehouse integration
- SCD Type 1 & SCD Type 2 implementations
- Incremental pipelines using Script Activities
- Semantic model & Power BI reporting

---

## 🧱 Architecture Overview

- **Bronze**: Raw CSV ingested into Fabric Lakehouse
- **Stage**: Cleaned, deduplicated tables using Dataflow Gen2
- **Gold (Mart)**:
  - Customer Dimension → SCD Type 1
  - Product Dimension → SCD Type 1
  - Sales Fact → SCD Type 2

Surrogate keys are generated using **ROW_NUMBER + control tables**, as Fabric Warehouse does not support IDENTITY or SEQUENCE.

---

---

## 🚀 Technologies Used

- Microsoft Fabric
- Fabric Lakehouse
- Fabric Warehouse
- Dataflow Gen2
- Script Activity Pipelines
- Power BI (Direct Lake / Import)

---

## 🔄 Pipeline Flow

1. Upload raw CSV to Lakehouse
2. Load data into Warehouse Stage tables
3. Apply SCD logic using Script Activities
4. Build semantic model from Mart schema
5. Create Power BI reports

---

## 📊 SCD Strategy

| Table        | Type |
|-------------|------|
| Customer     | SCD Type 1 |
| Product      | SCD Type 1 |
| Sales Fact   | SCD Type 2 |

---

## 📌 Key Highlights

- Fabric-safe surrogate key generation
- Real-world incremental loading
- Enterprise-grade design patterns
- Interview & production ready

---

## 📺 Related YouTube Series
This repository supports a 3-part YouTube series on Microsoft Fabric Data Engineering.

---

## 👤 Author
**Keshaw Bhardwaj**  
Microsoft Fabric | Power BI | Data Engineering




## 📁 Project Structure

