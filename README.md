> This is just a summary. You can read the longer version of the project details [here](project_details.md).

# Data Warehouse Project

This project aims to demonstrate relevant data warehousing skills. It involves a simple dataset (CSV formats) which includes dummy export data as would be sourced from an ERP and a CRM. It aims to highlight relevant data engineering skills and industry best practices.

## Demonstrated Skills & Tasks

- Working with SQL Server
- Writing maintainable T-SQL scripts
- ETL pipelines following the medallion framework (bronze -> silver -> gold)
- Data modeling (following the STAR method)
- Version control using Git and publishing to GitHub

## How to reproduce on your local machine

You can easily reproduce this database with all of its data tables, schemas, views, and procedures already created, for convenience.

To do so, simply follow this [guide](project_details.md#reproduce-database-locally).

## Repository Structure

```
data-warehouse-project/
│
├── datasets/*                          # Raw datasets used for the project (ERP and CRM data)
│
├── docs/
│   ├── __naming_conventions.md         # Consistent naming guidelines for tables, columns, and files
│   ├── _data_catalog.md                # Catalog of datasets (renders gold layer more user-friendly for consumers)
│   ├── 1_data_architecture.png         # Diagram showing the project's architecture
│   ├── 2_data_integration.png          # Diagram showing how the tabels are related
│   ├── 3_data_flow.png                 # Data flow diagram
│   └── 4_data_model.png                # Data model diagram (following the STAR schema)
│
├── import/
│   └── DataWarehouse.bak               # .BAK file to restore the database state on your local machine
│
├── scripts/
│   ├── 1_bronze/                       # Scripts for extracting and loading raw data
│   ├── 2_silver/                       # Scripts for cleaning and transforming data
│   └── 3_gold/                         # Scripts for creating analytical models (STAR schema)
│
├── tests/*                             # Scripts to ensure quality of silver and gold layers
│
├── LICENSE
└── README.md
```

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

<br/>

---

<h4><i>
Acknowledgement
</h4></i>
<small><i>
This project is based on original work made available by Baraa Khatib Salkini in his Data Engineering tutorial on <a href="https://www.youtube.com/watch?v=SSKVgrwhzus">YouTube</a>. Modifications include updated stored procedure logic, introduction of helper user functions, and updated documentation.
</i></small>
