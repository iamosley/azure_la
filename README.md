## Intro
This blog reviews the implamantation of several Microsoft Azure technologies to (re)create my research methodlogy for "Language Areas" (LAs), a geodemographic technique for exploring the diversity of neighbours via the langue spoken most often at home. Previous investigations have always used 'desktop' technologies, principally Python and MS SQL Server + Spatial. But now I attempted all of it "in the cloud". Furthermore, I investigaed the most cost effective, smaller scale, technology to aid companies that are just getting accustomed to Azure concepts.
#### Note
My initial exploration of Azure was done with a 'free' 30-day Azure subscription. However, all processing mentioned in this blog used a pay-as-you-go subscription, with some suprising costs incurred, which, at times, impacted the methodology and goals. Also, to prevent unexpected costs, I did not implement CosmosDB nor Azure Synapse Analytics, two technologies "out of the box" that are too expensive me to dable in (though, Synapse Analytics looks impressive, it has a seemlessly integrated ingestion-modeling-reporting ecosystem that exceeds what I explored in this post)
### Why
I had been working for a small company that was progressively adopting Azure technology for backend systems: Azure Active Directory, Azure DevOps, multiple virtual machines and SQL Databases (along with all the other Microsoft goodness: Office365, Exchange, and Teams). As a data engineer it was an interesting learning experience as to what could be achieved in a regular Microsoft SQL Server instance residing on a VM versus the constraints of a SQL Database (my first analytic SaaS).
### Overall Process
To implement LA research a broader range of data engineering and science technologies is required: BLOB storage for raw data, ELT processes to create 'silver' grade data at rest, Python for EDA (exploratory data analysis) and complex modeling, a unified code repository, and Power BI for presentation.
```flow
st=>start: Discover Data
op1=>operation: Load (Un)Structred Data
sub1=>subroutine: ELT into staging database
op2=>operation: EDA
sub2=>subroutine: Modeling
e=>end: Presentation

st->op1->sub1->op2->sub2->e
```
### Technologies Used
With the objective of doing everything in the cloud with Azure, here is a list of the technologies used, the ones that were not fit-for-purpose, and their replacements. In some cases I did resort to desktop technology since Azure was getting expensive.

* Azure BLOB storage container (hierarchical namespace was not implemented)
* Azure Key Vault to store to store share access signatures (SAS)
* Azure Databricks Service with Apache Sedona for ELT of spatial data via RDDs
* Azure Data Factory (ADF) for ETL of unstructured data and CSV data, and copy of prepared spatial data
* Azure Data for PostgresSQL (PG) Single Server (Basic 2 vCore) as target for ETL/ELT
* Azure Data Studio for managing code and connecting to PG
* Originally planned to use Python/Scala in Databricks for EDA, but did not successfully integrate Sedona with SparkSQL so opted to use the PostgresSQL staging server for EDA
* Modeled data using a second PostgresSQL Single Server (General Purpose 8 vCore) [^fn1]
* Copied results with ADF to Azure SQL Database for final implementation (a 'production' database)
* Intended to use Power BI to create maps and charts but it would not properly injest the geometry/geography data types, simply interepreted them as varchar, so instead:
* Tableau Desktop and Tableau Public to create interactive maps showing the impact of LA calculations versus simple DIssemination Area density metrics

[^fn1]: Towards the end of processing I had to move the required data to my desktop PostgresSQL (Debian) to finish processing as the 8 vCore machine was getting very expensive!
### Azure BLOB storage
Source data for this research consisted of:

* Statistics Canada (StatCan) Census 2016 CSV tabular data for SIngle Response Home Language counts by Dissemination Area (DA)
* An unstructured metadata text file describing the above data. This file is require to create a data dictionary.
* StatCan Census 2016 DA boundary files in the binary 'ShapeFile' format using their custom projection in meters

After procesing the Shapefile into Well Known Text (WKT) via DataBricks, a 'Silver' BLOB container was created for its storage and later load into PostgresSQL.
### Azure Data Factory
If you are familiar with Microsoft's SQL Server Integration Services (SSIS), then you will have an idea of the workflow for using ADF. I actually did a "side-by-side" comprison of the two, processing the metadata text file metioned above in SSIS, ADF Integration Runtime for SSIS packages[^fn2], and rewriting the whole process in native ADF dataflows and pipeline(s). The effort was similar except that native ADF dataflows are much more forgiving regarding casting datatypes than SSIS.
As you will see in the image below, all data preparation in contained in a single pipeline "PrepareCensusData" that kicks off with running a Databricks Notebook and then calling the data flow "LoadTextFiles". The notebook "readShapeFileSilver" convert the binary shapefiles into text (more on that later) .
The real value of ADF comes in the execution of the data flow, specifically for the metadata text file. Using a "no code" approach I was able to:
1. Read the file from BLOB storage
2. Strip out unnecessary rows from the file that included lengthy headers and footers, and remove non-sequential duplicate lines. This was done in a flexible way so that new metadata files could be processed dynamically and not rely on row counts etc.
2. Parse out the required columns that were semi-structured
3. Validate the columns per data type checks
4. Create the hierarchy datum that categorizes rows (very important when working with StatCan census data)
5. Insert the data into a PostgresSQL database table

For the tabular StatCan Home Language counts CSV file, the dataflow:
1. Read the file from BLOB storage
2. Removed StatCan column values that represented various forms of value suppresion and replaced with NULLs (there are four different types of suppression, so it is important to apply the correct business rules depending on the context)
3. Insert the data into a PostgresSQL database table

Finally, the shapefile data converted to WKT via the Databricks notebook was simply copied from BLOB storage directly into a PostgresSQL database table.

[^fn2]: I found the Integration Runtime somewhat buggy: if you changed any package global parameters and republised them, the ADF runtime would not integrate the change: you had to delete the package and start fresh.
### Azure Databricks
Ideally, most of the ELT, EDA and modeling, would have been accomplished with Azure Databricks principally using Scala, Python and SQL. However, as noted previously, I was unable to configure Databricks successfully to enjoy the full capabilities of Apache Sedona, especially SpatialSQL.
What did I use Sedona for? Converting the binary shapefiles into their text representation of WKT that can be consumed by SQL Server Spatial or PostGIS. Python was used for the reading and writing of data using SAS tokens via an Azure Key Store, and Scala + Sedona converted the Shapefiles to WKT. (It would have been so nice to do everything in Databricks and leverage Sedona's horizontal scaling of spatial queries via SpatialSQL).
Please review the link here for the notebook used.
### PostgreSQL for ELT and EDA
In previous work experiences I had dabbled with PostgresSQL (PG) but nothing that would qualify as "data science". Since I already had extensive experience with SQL Server and its Spatial functions, I really wanted to explore PG and its spatial extension PostGIS.
As mentioned previously, all data 'arrived' in PG via an ADF pipline.
The 2 vCore Basic server was sufficient for simple EDA: cardinality counts across dimensions, simple spatial geometry validation checks and univariate statistical distribution calculations. Not surpising since the total database size was only 16GB and larger tables had row counts of ~120M (the tables were narrow). However, when doing any sort of spatial algebra, it was slow to say the least. As mentioned in the Microsoft documnetation, the 'Basic' server configuation has "Variable I/O performance", it most centainly does. (I benchmarked some spatial opeartions against my Raspberry Pi 4 with 8GB of RAM using the same data using [dump|psq] and the 'Pi was faster.)
Once EDA was complete and prerequisites and data integrity establish for model creation, I did preliminary runs on the 2 vCore Basic server as proof-of-concept[^fn4]. Not surprisingly, in order to have the model complete in hours and not days, I would need some faster hardware, specifically more cores as the model was ammenable to parallelization (think Kmeans objective function).
[^fn4]: As part of the model development, its procedural language beginnings were replaced with set-based spatial SQL operations that took advantage of PG GiST indexes. Not a subtle process given that the presonce of a spatial operation and GiST index precludes the use of any B-tree index (Not unlike MS SQL Server spatial functions).
### PostgreSQL for Modeling
A big part of the model is spatially agglomerating adjacent DAs based on similar home language attributes. Since there are over 250 home languages in the Census, and each language is a exclusive problem set, the model can converege ||250+|| times. Moreover, when determine the descriptive statistics of the home language agglomerations, this too can be done ||250+|| times. So more cores would be ideal.
Enter the Azure 8 vCore General Purpose PG server (with 'predictable' I/O performance. Nice.) The data were moved directly from the EDA PG server using the Azure CLI using the PG dump and psql utilities. This was much faster to setup and run than using ADF[^fn3]. Using the combination of a pinned Azure PG monituring dashboard, and Windows 10 cmd sessions, I initiated eight model runs in parallel to agglomerate the 250+ home language solutions. The majority were finished within 24 hours, but three solutions proved onnerous: English, French and Non-official languages. This condition was understandable: all three languages covered a spatially extensive portion of Canada and even prudent data modelling, spatial GiST and B-tree indexes would not speed things up, I was up gainst a hard NP problem. Moreover, the 8 vCore PG server was getting expensive so I suspended the model processing (it was coded for this eventuality, allowing a restart do to server error, rollback, user intervention etc.) and deleted the server (it has no 'pause' feature). I moved the required table to the 'Pi and restarted. The French and English solution completed within the next 48 hours but the Non-official language solution went on for days, eventually I had to abandon it. An item to put in the section "Further Research" (at some future moment).
Likewise, the descriptive statisticts were performed on the 'Pi since I didn't want to incur more costs for spinning-up an expensive resource. 
[^fn3]: Note that Azure CLI should only be used for administrative tasks. Early on during EDA, I used the CLI psql utility and the CLI would timeout due to 'inactivity' and I would lose my psql session. Sad.
### Tableau Rather than Power BI
So I was stunned by this: Power BI does not know what to make of SQL Server geometry/geography types. Using ADF's copy data activity, I exported data[^fn5] from the 2 vCore Basic PG server to a newly created Azure SQL Server. Then accessing the latter from a Power BI workspace, I "dragged and dropped" the geography column onto the report canvas and it simply displayed it as a string type "MULTIPOLYGON(((...)))". What a letdown.
Conversely, in Tableau Desktop, I dragged the same column onto a worksheet and it immediately interpreted as a 'map' and created polygons across Canada with a nice base map too. In additon, adding a relational table to the map data (not a blend withn a worksheet, that wold overwhelm Tableau) allowed me to add interactive charts for each polygon (over 50,000!) so the use could see the impact of LA calculations versus simple DA densities.
Since the Azure SQL Database was created just for Power BI, and Tableau Desktop was fine connecting to a Azure PG database, I deleted the SQL Database and just used the 2 vCore Basic PG server for provisioning data for Tableau.
[^fn5]: This was a slightly involved process and beyond the scope of this post. Simply, I psql \copy a few tables from the my local 'Pi PG server to a text file and had QGIS convert its projection from the StatCan meter to the WGS Longitude Latitude projection and saved to a WKT file. Then I uploaded the tables via psql \copy into the Azure PG server.
## Conclusions
It is great having everything in one place, Azure. From source data in BLOB containers, to processing for ETL, ELT in Azure Data Factory or Azure Databricks, and EDA and modeling in Databricks or any of the SaaS SQL options, there is a lot of flexibility. Furthermore, having Azure DevOps as a single source for code spanning all these technologies is an organizational benefit. No more "who put what where".
The key to successfully leveraging Azure is to view it as principally a resource manager with integrated security policies. A huge asset for a company of any size. Its not so much deciding what exactly how to configure a resource (VM, SQL server, ADF, etc.) but to keep a sharp eye on utilization and have a clear set of business rules for pausing or delete of resource to minimize costs. For a data engineer/scientist like myself, who has gotten used to an "always on, always available" metality, the allocate-deallocate mindset required for Azure took some getting used to (but enforces a much needed discipline around resourcing and getting the largess of unlimited data science under control). 
