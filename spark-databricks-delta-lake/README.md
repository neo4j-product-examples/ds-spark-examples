# Demo of Databricks Delta Lake Integration

1. `stage-delta-tables-from-csvs` creates the delta tables
2. `demo-etl` demonstrates ETL from Delta Lake into Neo4j

The source data used is from the [H&M Personalized Fashion Recommendations Kaggle Competition](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data).

<img src="img/summary.png" alt="summary" width="1000"/>
 
> **_NOTE:_**  Databricks automatically converts python notebooks (`.ipynb`) to `.py` files when committing to git. to convert the `.py` files back to notebooks see https://github.com/Yoyodyne-Data-Science/ipynb-py-convert-databricks 
