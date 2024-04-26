# Hetio-Net-Neo4J

**Project Summary:**

The project aims to analyze the HetioNet database, a heterogeneous network that encodes complex biological information, specifically for the purpose of Project Rephetio, which explores drug efficacy and predicts new therapeutic uses. Using the PySpark framework, the project involves:

1. **Data Parsing and Loading:** Reading node and edge data from TSV files, which include various biological entities and their relationships.

2. **MapReduce Analyses:** 
   - **Compounds and Genes:** Calculate the number of genes bound to each compound.
   - **Disease and Gene Regulation:** Determine the number of genes that upregulate each disease.
   - **Compound Relationships:** Analyze how many compounds downregulate each other, if applicable to teams of three or more.

3. **Data Structure Experimentation:**
   - Implement hash tables using the mid-square method and folding method to analyze storage efficiency and performance.

4. **Output:** Results are sorted and the top entities by count in each analysis are reported, helping identify key biological interactions.

The project leverages PySpark to handle large datasets efficiently and applies MapReduce methods to perform scalable data processing, suitable for the extensive data present in HetioNet. This approach allows for systematic analysis and could potentially uncover new insights into how drugs interact with genes and diseases.
