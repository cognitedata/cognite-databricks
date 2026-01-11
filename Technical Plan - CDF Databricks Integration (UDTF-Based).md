# **Technical Plan: CDF Databricks Integration (Governance-First via UDTF)**

**Document Control**

| Field | Value |
| :---- | :---- |
| **Project** | CDF Databricks Integration (Governance-First via UDTF) |
| **Technical Lead** | TBD |
| **Target Release** | Q1/Q2 2026 |
| **Status** | **Draft** |
| **Related PRD** | PRD - CDF Data Source v7.md |
| **Databricks Runtime** | DBR 18.1+ (required for custom dependencies in UDTFs) |

---

## **1. Executive Summary**

**Note on Naming:** This document uses PyPI package names for references:
- **PyPI:** `cognite-pygen` (repository: `pygen`; import: `cognite.pygen`)
- **PyPI:** `cognite-pygen-spark` (repository: `pygen-spark`; import: `cognite.pygen_spark`)

This technical plan details the implementation approach for building a **CDF Databricks Integration** using **User-Defined Table Functions (UDTFs)** with Unity Catalog governance. The solution ensures CDF data is discoverable via the Databricks "Search Box" while maintaining secure credential management through Databricks Secret Manager.

The solution consists of three main components:
1. **cognite-pygen-spark** (PyPI: `cognite-pygen-spark`; Import: `cognite.pygen_spark`): A code generation library that extends `cognite.pygen` to generate UDTF definitions for CDF Data Models
2. **cognite-databricks**: A helper SDK providing UDTF registration utilities, Secret Manager integration, and View generation
3. **Generated UDTFs and Views**: Unity Catalog-registered functions and views that wrap cognite-sdk-python to provide discoverable, governable access to CDF data

**Key Architectural Decision:** Following technical discovery in December 2025, it was determined that the Python Data Source API lacks native Unity Catalog governance. The UDTF + View architecture ensures that CDF data assets are indexed and searchable within the Databricks UI while maintaining enterprise security through Unity Catalog permissions.

**Note:** The UDTF generation follows the same conceptual architecture as [Pygen](https://cognite-pygen.readthedocs-hosted.com/en/latest/developer_docs/index.html), adapted for generating Python UDTF functions instead of Pydantic models. See Section 4.1.3 for detailed architecture alignment.

---

## **2. Architecture Overview**

### **2.1 High-Level Architecture**

The architecture consists of three main packages working together in two distinct phases: **Code Generation** (development time) and **Runtime** (execution time).

#### **2.1.1 Package Dependency Hierarchy**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Package Dependencies                         │
│                                                                  │
│  cognite-sdk-python (base)                                      │
│       │                                                          │
│       ├──► cognite.pygen (PyPI: cognite-pygen, depends on SDK)                       │
│       │         │                                                │
│       │         └──► cognite.pygen_spark (PyPI: cognite-pygen-spark, depends on cognite-pygen)     │
│       │                   │                                      │
│       │                   └──► cognite.databricks               │
│       │                         (PyPI: cognite-databricks, depends on cognite-pygen-spark)        │
│       │                                                          │
│       └──► cognite-databricks (also uses SDK directly)          │
│                 (for UDTF runtime execution)                    │
└─────────────────────────────────────────────────────────────────┘
```

#### **2.1.2 Code Generation Phase (Development Time)**

This phase occurs when developers generate UDTF definitions and Views from CDF Data Models.

**Note:** Time Series UDTFs (`time_series_datapoints_udtf`, `time_series_datapoints_long_udtf`, `time_series_latest_datapoints_udtf`) are **template-generated** using the same template-based generation approach as Data Model UDTFs. They are generated using Jinja2 templates in `pygen-spark` for consistency in behavior, error handling, and initialization patterns. The code generation phase below applies to both Data Model UDTFs and Time Series UDTFs.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Notebook (Driver)                 │
│                                                                  │
│  Developer runs:                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  from cognite.databricks import generate_udtf_notebook  │  │
│  │  from cognite.pygen import load_cognite_client_from_toml  │  │
│  │  from databricks.sdk import WorkspaceClient            │  │
│  │                                                           │  │
│  │  # Load client from TOML file (like pygen)                │  │
│  │  client = load_cognite_client_from_toml("config.toml")   │  │
│  │  workspace_client = WorkspaceClient()  # Auto-detects    │  │
│  │                                                           │  │
│  │  # Generate UDTFs for Data Model (like pygen pattern)    │  │
│  │  generator = generate_udtf_notebook(                     │  │
│  │      data_model: DataModel (DataModelId/DataModel)      │  │
│  │      client,                                             │  │
│  │      workspace_client=workspace_client,  # Recommended   │  │
│  │      catalog="main",  # Unity Catalog catalog name      │  │
│  │      schema=None,  # Auto-generated from data model     │  │
│  │  )                                                       │  │
│  │  generator.register_udtfs_and_views()                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  cognite-databricks                                      │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  UDTFGenerator                                     │  │  │
│  │  │  - Orchestrates code generation                   │  │  │
│  │  │  - Manages Unity Catalog registration             │  │  │
│  │  │  - Creates Views with Secret injection            │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (uses)                              │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  cognite.pygen_spark                                     │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  SparkUDTFGenerator                                │  │  │
│  │  │  └──► SparkMultiAPIGenerator                       │  │  │
│  │  │      (extends pygen's MultiAPIGenerator)           │  │  │
│  │  │  - Generates UDTF Python code                      │  │  │
│  │  │  - Generates View SQL with Secret injection       │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (extends)                           │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  cognite.pygen                                          │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  MultiAPIGenerator                                 │  │  │
│  │  │  - Parses Views from Data Models                  │  │  │
│  │  │  - Creates internal representation                │  │  │
│  │  │  - Handles relationships (implements, children)   │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (uses)                              │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  cognite-sdk-python                                      │  │
│  │  - Fetches Data Models from CDF                        │  │
│  │  - Retrieves Views and properties                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (HTTP requests)                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  CDF API (REST)                                          │  │
│  │  - /models/datamodels/byids                             │  │
│  │  - /models/views/byids                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Output: Generated UDTF Python files + View SQL                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Unity Catalog Registration:                            │  │
│  │  - catalog.schema.pump_view_udtf (Python UDTF)          │  │
│  │  - catalog.schema.pump_view (SQL View)                 │  │
│  │    CREATE VIEW ... AS                                   │  │
│  │    SELECT * FROM pump_view_udtf(                        │  │
│  │      SECRET('cdf_sp_pygen_power_windturbine', 'client_id') AS client_id,     │  │
│  │      SECRET('cdf_sp_pygen_power_windturbine', 'client_secret') AS client_secret, │  │
│  │      SECRET('cdf_sp_pygen_power_windturbine', 'tenant_id') AS tenant_id,    │  │
│  │      SECRET('cdf_sp_pygen_power_windturbine', 'cdf_cluster') AS cdf_cluster, │  │
│  │      SECRET('cdf_sp_pygen_power_windturbine', 'project') AS project          │  │
│  │    )                                                    │  │
│  │    Note: Values come from TOML file, stored in Secret Manager │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

#### **2.1.3 Runtime Phase (Execution Time)**

This phase occurs when users query the registered Views or call UDTFs directly:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Notebook                          │
│                                                                  │
│  User queries:                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  -- Data Model View Query                               │  │
│  │  SELECT * FROM catalog.schema.pump_view                 │  │
│  │  WHERE timestamp > '2025-01-01'                         │  │
│  │                                                          │  │
│  │  -- Time Series UDTF Query                              │  │
│  │  SELECT * FROM time_series_datapoints_udtf(             │  │
│  │    space => 'sailboat',                                 │  │
│  │    external_id => 'vessel.speed',                       │  │
│  │    start => '1d-ago', end => 'now',                      │  │
│  │    client_id => SECRET('cdf_scope', 'client_id'), ...   │  │
│  │  )                                                       │  │
│  │                                                          │  │
│  │  -- Or PySpark                                          │  │
│  │  spark.sql("SELECT * FROM catalog.schema.pump_view")    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (queries)                           │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Unity Catalog View                                      │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  catalog.schema.pump_view                         │  │  │
│  │  │  (registered SQL View)                            │  │  │
│  │  │  - Indexed in Databricks Search Box               │  │  │
│  │  │  - Governed via UC permissions                     │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (resolves to)                       │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Unity Catalog UDTF                                     │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  catalog.schema.pump_view_udtf                    │  │  │
│  │  │  (Python UDTF function)                          │  │  │
│  │  │  - Executes on Spark workers                      │  │  │
│  │  │  - Receives Secret parameters                    │  │  │
│  │  │  - Returns table of rows                          │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (distributes to)                    │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Spark Workers (Executors)                              │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │  Each worker executes UDTF:                       │  │  │
│  │  │  1. Receives Secret value (injected by Spark)      │  │  │
│  │  │  2. Creates CogniteClient from Secret             │  │  │
│  │  │  3. Calls CDF API (with predicate pushdown)      │  │  │
│  │  │  4. Converts CDF response to Spark Rows           │  │  │
│  │  │  5. Returns rows to Spark                          │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (uses Secret from)                  │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Databricks Secret Manager                               │  │
│  │  - Stores OAuth2 credentials from TOML file             │  │
│  │  - Values: client_id, client_secret, tenant_id,         │  │
│  │    cdf_cluster, project (from config.toml)              │  │
│  │  - Scope: 'cdf_sp_pygen_power_windturbine' (data model-specific) │  │
│  │  - Keys: 'client_id', 'client_secret', 'tenant_id',     │  │
│  │    'cdf_cluster', 'project'                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (authenticates with)                │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  cognite-sdk-python (in each worker)                     │  │
│  │  - OAuth2 authentication (from Secret)                   │  │
│  │  - HTTP client with retries                              │  │
│  │  - API request handling                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           │ (HTTP requests)                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  CDF API (REST)                                          │  │
│  │  - /models/instances/query (batch) - Data Model UDTFs    │  │
│  │  - /models/instances/sync (streaming) - Data Model UDTFs│  │
│  │  - /timeseries/data/retrieve - Time Series UDTFs         │  │
│  │  - /timeseries/data/retrieve_latest - Time Series UDTFs  │  │
│  │  - /streams/{id}/records/sync (Records streaming)        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Output: Spark DataFrame                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DataFrame with CDF data                                 │  │
│  │  - Can be filtered, transformed, joined                  │  │
│  │  - Can be written to Delta, Parquet, etc.                │  │
│  │  - Fully governed via Unity Catalog                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

#### **2.1.4 Key Architectural Patterns**

1. **UDTF + View Pattern:**
   - **UDTF**: Python function that executes on Spark workers, wraps cognite-sdk-python
   - **View**: SQL view registered in Unity Catalog, provides discoverability and governance
   - **Secret Injection**: Secrets passed as UDTF parameters in View definition, never exposed to end users
   - **Two UDTF Types**:
     - **Data Model UDTFs**: Generated from CDF Data Models (via cognite-pygen-spark)
     - **Time Series UDTFs**: Template-generated UDTFs for time series datapoints (generated using same templates as Data Model UDTFs)

2. **Separation of Concerns:**
   - **Code Generation**: Happens once at development time (driver only) for Data Model UDTFs
   - **Template-Generated UDTFs**: Time Series UDTFs are generated using Jinja2 templates in pygen-spark, same as Data Model UDTFs
   - **Data Access**: Happens at runtime (distributed across workers via UDTF execution)

3. **Governance-First Design:**
   - All data access goes through Unity Catalog-registered objects (Views/UDTFs)
   - Permissions managed via Unity Catalog GRANT/REVOKE statements
   - Data assets indexed and searchable in Databricks UI

4. **Extension Pattern:**
   - `cognite.pygen_spark` extends `cognite.pygen` (inheritance)
   - Reuses 90% of pygen's logic, only overrides templates for UDTF generation

5. **Layered Architecture:**
   - **View Layer**: User-facing, discoverable, governable
   - **UDTF Layer**: Execution logic, Secret handling, CDF API calls
   - **SDK Layer**: CDF API client
   - **API Layer**: REST endpoints

### **2.2 Component Breakdown**

1. **cognite-pygen-spark** (PyPI: `cognite-pygen-spark`; Import: `cognite.pygen_spark`) - UDTF Code Generator
   - Extends `cognite.pygen` to generate Python UDTF functions
   - Reuses 90% of pygen's View parsing and internal representation
   - Provides UDTF-specific templates and code generation
   - Generates View SQL with Secret injection
   - **PyPI:** `cognite-pygen-spark` (depends on `cognite-pygen`); Import: `cognite.pygen_spark` (extends `cognite.pygen`)

2. **cognite-databricks** (Helper SDK)
   - UDTF registration utilities for Unity Catalog
   - Secret Manager integration helpers
   - View generation with Secret injection
   - Convenience wrappers for Databricks notebooks
   - **Template-Generated Time Series UDTFs**: Three specialized UDTFs for time series datapoints queries (generated using templates):
     - `time_series_datapoints_udtf`: Single time series query
     - `time_series_datapoints_long_udtf`: Multiple time series query (long format)
     - `time_series_latest_datapoints_udtf`: Latest datapoints query
     - All use instance_id (space + external_id) for Data Model integration
   - **PyPI:** `cognite-databricks` (depends on `cognite-pygen-spark` and `cognite-sdk-python`); Import: `cognite.databricks` (uses `cognite.pygen_spark`)

3. **Generated UDTFs and Views**
   - **Data Model UDTFs**: Python UDTF functions generated from CDF Data Models, registered in Unity Catalog
   - **Time Series UDTFs**: Template-generated UDTFs for time series datapoints (generated using same templates as Data Model UDTFs)
   - **SQL Views**: Registered on top of UDTFs for discoverability and governance
   - All are discoverable and governable via Unity Catalog

4. **Integration Layer**
   - Databricks Secret Manager for credential storage
   - Unity Catalog for function and view registration
   - cognite-sdk-python for CDF API access

---

## **3. DBR 18.1 Requirements and Compatibility**

### **3.1 Overview**

This solution leverages **Databricks Runtime 18.1+** for custom dependency support in Python UDTFs. This chapter details what functionality is blocked until DBR 18.1 is available and what workarounds exist for earlier versions.

### **3.2 DBR 18.1 Requirement: Custom Dependencies**

The primary feature requiring DBR 18.1 is the ability to bundle Python packages with UDTFs via the `routine_dependencies` parameter. This allows UDTFs to include `cognite-sdk-python` and other dependencies without requiring pre-installation on the cluster.

#### **3.2.1 Current Implementation: `routine_dependencies` Limitation**

**Note:** The current implementation sets `routine_dependencies=None` for all UDTFs because `DependencyList` in the Databricks SDK is designed for SQL object dependencies (tables, functions), not Python packages. For DBR 18.1+ Python package dependencies, a different mechanism may be needed (e.g., via `external_language` or a future SDK update).

**Current Implementation in `cognite-databricks/cognite/databricks/udtf_registry.py`:**

```python
# Build CreateFunction with all required fields (per OpenAPI spec)
create_function = CreateFunction(
    name=function_name,
    catalog_name=catalog,
    schema_name=schema,
    input_params=input_params_wrapped,
    # For Python UDTFs, schema is in full_data_type.
    # Unity Catalog requires return_params to be fully populated with structured metadata
    # for each output column when using TABLE_TYPE.
    return_params=return_params_wrapped,
    data_type=ColumnTypeName.TABLE_TYPE,
    full_data_type=return_type,  # DDL string: "TABLE(col1 TYPE, col2 TYPE, ...)"
    routine_body=CreateFunctionRoutineBody.EXTERNAL,  # Python UDTFs are EXTERNAL routines
    routine_definition=udtf_code,
    parameter_style=CreateFunctionParameterStyle.S,
    is_deterministic=False,
    sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
    is_null_call=False,
    security_type=CreateFunctionSecurityType.DEFINER,
    specific_name=function_name,
    comment=comment,
    external_language="PYTHON",  # Required for EXTERNAL functions
    routine_dependencies=None,  # TODO: Python package dependencies need different handling
    # Note: DependencyList is for SQL object dependencies (tables, functions), not Python packages
)
```

**Important Note on Return Types for Python UDTFs:**
For Python UDTFs registered via the Unity Catalog API as **Table Functions** (`TABLE_TYPE`), both the `full_data_type` and `return_params` fields are **required**:

1.  **`full_data_type`**: Must contain the SQL-friendly DDL string representation (e.g., `"TABLE(col1 TYPE, col2 TYPE, ...)"`).
2.  **`return_params`**: Must contain a structured list of `FunctionParameterInfo` objects, one for each output column. This metadata is used by Unity Catalog to validate the function's output schema at registration and invocation time.

Each return parameter must specify:
- `name`: The column name.
- `type_text`: The SQL type string (e.g., `"STRING"`, `"DOUBLE"`).
- `type_name`: The corresponding `ColumnTypeName` enum.
- `type_json`: The Spark StructField JSON representation (e.g., `'{"name":"col","type":"string","nullable":true,"metadata":{}}'`).
- `position`: The 0-based index of the column in the output table.

**Impact:** Currently, Python packages cannot be bundled with UDTFs via `routine_dependencies`. All required packages must be pre-installed on the cluster, regardless of DBR version.

#### **3.2.2 Current Limitation: Dependency Bundling**

**Note:** The `dependencies` parameter is accepted but currently not fully implemented. The Databricks SDK's `DependencyList` is designed for SQL object dependencies, not Python packages. The parameter is stored for future use but does not currently bundle Python packages with UDTFs.

```python
# The dependencies parameter is accepted but not yet fully functional
generator.register_udtfs_and_views(
    data_model=data_model_id,
    secret_scope=secret_scope,
    dependencies=["cognite-sdk>=7.90.1"],  # Accepted but not yet bundled
)
```

**Impact:** Currently, all required packages must be pre-installed on the cluster, regardless of DBR version. The `dependencies` parameter is reserved for future implementation when the Databricks SDK supports Python package dependencies in `routine_dependencies`.

### **3.3 Current Implementation: Pre-Installation Required**

**Current Status:** All DBR versions require pre-installed packages. The `dependencies` parameter is accepted but not yet functional.

**Implementation:**
```python
# In cognite-databricks/cognite/databricks/udtf_registry.py
# All UDTFs are created with routine_dependencies=None
create_function = CreateFunction(
    # ... other fields ...
    routine_dependencies=None,  # Python packages not yet supported via DependencyList
)
```

**Requirements:**
1. Pre-install all required packages on the cluster (e.g., `cognite-sdk-python`, `cognite-pygen-spark`)
2. The `dependencies` parameter can be provided but is currently not functional
3. Ensure the cluster has all required packages before UDTF execution

### **3.4 What Works Without DBR 18.1**

The following functionality works on all DBR versions:

- ✅ **UDTF Code Generation**: All of `cognite.pygen_spark` functionality
- ✅ **Secret Manager Integration**: Credential management works on all versions
- ✅ **View SQL Generation**: SQL View generation is version-independent
- ✅ **UDTF Registration**: UDTF registration works (without custom dependencies)
- ✅ **All Other Functionality**: Code generation, templates, predicate pushdown, etc.

### **3.5 Current Limitations**

The following features are not yet implemented:

- ❌ **Bundling Packages**: Cannot bundle `cognite-sdk-python` and other packages with UDTFs
- ⚠️ **Dependencies Parameter**: The `dependencies` parameter in `register_udtfs_and_views()` is accepted but not yet functional
- ❌ **`routine_dependencies` Field**: The `routine_dependencies` field in function registration does not support Python packages (only SQL object dependencies)

### **3.6 Migration Path**

**For Pre-DBR 18.1 Environments:**
1. Install required packages on cluster:
   ```bash
   %pip install cognite-sdk>=7.90.1 cognite-pygen-spark>=0.1.0
   ```
2. Register UDTFs without dependencies:
   ```python
   generator.register_udtfs_and_views(
       data_model=data_model_id,
       secret_scope=secret_scope,
       dependencies=None,  # Omit or set to None
   )
   ```

**For All DBR Versions (Current Implementation):**
1. Pre-install packages on cluster (required for all versions):
   ```bash
   %pip install cognite-sdk>=7.90.1 cognite-pygen-spark>=0.1.0
   ```
2. Register UDTFs (dependencies parameter accepted but not yet functional):
   ```python
   generator.register_udtfs_and_views(
       data_model=data_model_id,
       secret_scope=secret_scope,
       dependencies=["cognite-sdk>=7.90.1"],  # Accepted but not yet bundled
   )
   ```

### **3.7 Summary**

| Feature | All DBR Versions |
|---------|------------------|
| UDTF Code Generation | ✅ Works |
| Secret Manager | ✅ Works |
| View Generation | ✅ Works |
| UDTF Registration | ✅ Works (requires pre-installed packages) |
| Custom Dependencies | ⚠️ Parameter accepted but not yet functional |
| Package Bundling | ❌ Requires pre-install (not yet supported) |

**Key Takeaway:** All functionality works on all DBR versions, but requires pre-installing packages on the cluster. The `dependencies` parameter is accepted for future use but does not currently bundle packages with UDTFs due to Databricks SDK limitations (`DependencyList` is for SQL objects, not Python packages).

---

## **4. Component Design**

### **4.1 cognite-pygen-spark (UDTF Code Generator)**

#### **4.1.1 Package Structure**

```
pygen-spark/  (Repository name; PyPI: cognite-pygen-spark)
├── cognite/
│   └── pygen_spark/
│       ├── __init__.py
│       ├── generator.py          # SparkUDTFGenerator (extends SDKGenerator)
│       ├── udtf_generator.py    # SparkMultiAPIGenerator (extends MultiAPIGenerator)
│       └── templates/
│           ├── udtf_function.py.jinja      # Python UDTF function template
│           ├── view_sql.py.jinja           # SQL View template with Secret injection
│           └── udtf_init.py.jinja          # UDTF package __init__.py
├── pyproject.toml
├── README.md
└── tests/
    └── test_udtf_generation.py
```

#### **4.1.2 Core Classes**

**SparkUDTFGenerator** (extends `pygen.SDKGenerator`)

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

from cognite.client import CogniteClient, data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier

# Short-term: Import from private API (required until pygen exports these)
# This pattern applies to ALL pygen dependencies, not just the ones listed here
from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen_spark.udtf_generator import SparkMultiAPIGenerator

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View

# Define DataModel type alias (same as pygen)
# Short-term: We define our own type alias using public types from cognite.client
# This avoids depending on pygen's private API (_generator module)
# Long-term: Once pygen is updated to export all dependencies in __init__.py, we can use:
#   from cognite.pygen import SDKGenerator, MultiAPIGenerator, DataModel
# This pattern applies to ALL pygen dependencies, not just the ones listed here
DataModel = Union[DataModelIdentifier, dm.DataModel[dm.View]]

class SparkUDTFGenerator(SDKGenerator):
    """Generator for creating Python UDTF functions from CDF Data Models.

    Extends pygen's SDKGenerator to reuse View parsing logic.
    """

    def __init__(
        self,
        client: CogniteClient,
        output_dir: Path,
        top_level_package: str = "cognite_databricks",
        **kwargs: dict[str, object],
    ) -> None:
        super().__init__(client, output_dir, top_level_package, **kwargs)
        self.udtf_generator = SparkMultiAPIGenerator(
            self.client,
            self.output_dir,
            self.top_level_package,
            **kwargs,
        )

    def generate_udtfs(self, data_model: DataModel) -> UDTFGenerationResult:
        """Generate UDTF functions for all Views in a Data Model.

        Args:
            data_model: DataModel identifier (DataModelId or DataModel object)

        Returns:
            UDTFGenerationResult with structured information about generated files.
            Access individual files via result['view_id'] or result.get_file('view_id').
        """
        # Reuse pygen's View parsing (same pattern as pygen)
        views = self._load_views(data_model)

        # Generate UDTF for each View
        generated_files: dict[str, Path] = {}
        for view in views:
            udtf_code = self.udtf_generator.generate_udtf(view)
            file_path = self._write_udtf_file(view, udtf_code)
            generated_files[view.external_id] = file_path

        return UDTFGenerationResult(
            generated_files=generated_files,
            output_dir=self.output_dir,
            total_count=len(generated_files),
        )

    def generate_views(self, data_model: DataModel, secret_scope: str) -> ViewSQLGenerationResult:
        """Generate SQL View definitions with Secret injection.

        Args:
            data_model: DataModel identifier (DataModelId or DataModel object)
            secret_scope: Databricks Secret Manager scope name

        Returns:
            ViewSQLGenerationResult with structured information about generated SQL statements.
            Access individual SQL via result['view_id'] or result.get_sql('view_id').
        """
        views = self._load_views(data_model)
        view_sqls: dict[str, str] = {}

        for view in views:
            sql = self.udtf_generator.generate_view_sql(view, secret_scope)
            view_sqls[view.external_id] = sql

        return ViewSQLGenerationResult(
            view_sqls=view_sqls,
            total_count=len(view_sqls),
        )
```

**SparkMultiAPIGenerator** (extends `pygen.MultiAPIGenerator`)

```python
from __future__ import annotations

from typing import TYPE_CHECKING

# Short-term: Import from private API (required until pygen exports this)
# This pattern applies to ALL pygen dependencies, not just the ones listed here
from cognite.pygen._core.generators import MultiAPIGenerator
# Long-term: Once pygen is updated to export all dependencies in __init__.py, we can use:
#   from cognite.pygen import MultiAPIGenerator
# This pattern applies to ALL pygen dependencies, not just the ones listed here

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling import View

class SparkMultiAPIGenerator(MultiAPIGenerator):
    """Generates Python UDTF functions and SQL Views.

    Extends pygen's MultiAPIGenerator to reuse internal representation.
    """

    def __init__(self, *args: object, **kwargs: dict[str, object]) -> None:
        super().__init__(*args, **kwargs)
        # Override template loader to use UDTF templates
        self.template_loader = self._create_udtf_template_loader()

    def generate_udtf(self, view: View) -> str:
        """Generate Python UDTF function code for a View.

        Uses pygen's internal representation of the View (properties, types, etc.)
        but generates UDTF-specific code instead of Pydantic models.

        Args:
            view: The View to generate UDTF code for

        Returns:
            Python code for the UDTF function
        """
        # Reuse pygen's View to internal representation conversion
        api_class = self._view_to_api_class(view)

        # Load UDTF template
        template = self.template_loader.get_template("udtf_function.py.jinja")

        # Generate code
        return template.render(
            view=view,
            api_class=api_class,
            properties=api_class.properties,
            # ... other context
        )

    def generate_view_sql(self, view: View, secret_scope: str) -> str:
        """Generate SQL CREATE VIEW statement with Secret injection.

        The secrets referenced (client_id, client_secret, tenant_id, cdf_cluster, project)
        come from the TOML file and are stored in Secret Manager via set_cdf_credentials().

        Args:
            view: The View to generate SQL for
            secret_scope: Secret Manager scope name

        Returns:
            SQL CREATE VIEW statement

        Example output:
            CREATE VIEW catalog.schema.pump_view AS
            SELECT * FROM catalog.schema.pump_view_udtf(
                SECRET('cdf_sp_pygen_power_windturbine', 'client_id') AS client_id,
                SECRET('cdf_sp_pygen_power_windturbine', 'client_secret') AS client_secret,
                SECRET('cdf_sp_pygen_power_windturbine', 'tenant_id') AS tenant_id,
                SECRET('cdf_sp_pygen_power_windturbine', 'cdf_cluster') AS cdf_cluster,
                SECRET('cdf_sp_pygen_power_windturbine', 'project') AS project
            );
        """
        template = self.template_loader.get_template("view_sql.py.jinja")

        return template.render(
            view=view,
            udtf_name=f"{view.external_id}_udtf",
            secret_scope=secret_scope,
        )
```

#### **4.1.3 Conceptual Overview (PySpark UDTF Generation)**

The PySpark UDTF generation follows the same 4-step flow as pygen, adapted for generating UDTF functions instead of Pydantic models:

| Step | Pygen (Pydantic) | PySpark UDTF | Description |
|------|------------------|--------------|-------------|
| **1. Input Views** | CDF Data Model Views | CDF Data Model Views | Same input: Views from CDF Data Models API |
| **2. Internal Representation** | `APIClass`, `NodeAPIClass`, `EdgeAPIClass` | `APIClass`, `NodeAPIClass`, `EdgeAPIClass` | Same internal model: Reused from pygen |
| **3. Template Generation** | Jinja2 templates for Pydantic models | Jinja2 templates for Python UDTFs | Different templates: UDTF functions instead of Pydantic classes |
| **4. Generator Logic** | `SDKGenerator`, `MultiAPIGenerator` | `SparkUDTFGenerator`, `SparkMultiAPIGenerator` | Extended generators: Override template loading and output generation |

**Architecture Alignment:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pygen Architecture (Reference)               │
│                                                                  │
│  Input Views (CDF API)                                          │
│       │                                                          │
│       ▼                                                          │
│  Internal Representation (APIClass, properties, types)         │
│       │                                                          │
│       ▼                                                          │
│  Template Generation (Jinja2)                                    │
│       │                                                          │
│       ▼                                                          │
│  Generated Code (Pydantic models, client classes)              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              PySpark UDTF Architecture (Extension)             │
│                                                                  │
│  Input Views (CDF API) ──────────────┐                         │
│       │                                │                         │
│       │ (reused)                       │                         │
│       ▼                                │                         │
│  Internal Representation ─────────────┘                         │
│  (APIClass, properties, types)                                  │
│       │                                                          │
│       │ (extended)                                               │
│       ▼                                                          │
│  Template Generation (Jinja2 for UDTFs)                        │
│       │                                                          │
│       ▼                                                          │
│  Generated Code (Python UDTF functions, SQL Views)             │
└─────────────────────────────────────────────────────────────────┘
```

**Key Reuse Points:**
- **View Parsing**: 100% reused from pygen
- **Internal Representation**: 100% reused (APIClass, property types, relationships)
- **Template Logic**: 90% reused (property iteration, type mapping)
- **Customization**: Only template files and output format differ

**Reference:** See [Pygen Developer Documentation](https://cognite-pygen.readthedocs-hosted.com/en/latest/developer_docs/index.html) for the conceptual architecture that this extends.

#### **4.1.4 Architecture Alignment with Pygen-Main**

The `cognite.pygen_spark` package follows the same dependency pattern as `cognite.pygen`:

```
cognite.pygen (PyPI: cognite-pygen, depends on cognite-sdk-python)
    │
    └──► cognite.pygen_spark (PyPI: cognite-pygen-spark, depends on cognite-pygen)
            │
            └──► cognite.databricks (PyPI: cognite-databricks, depends on cognite-pygen-spark)
```

**Reuse Strategy:**
1. **Inheritance**: `SparkUDTFGenerator` extends `SDKGenerator`, `SparkMultiAPIGenerator` extends `MultiAPIGenerator`
   - **Dependency:** Currently imports from `cognite.pygen._core.generators` (private API)
   - **Required:** Pygen must export `SDKGenerator` and `MultiAPIGenerator` in `cognite.pygen.__init__.py`
2. **Composition**: Reuse pygen's View loading, parsing, and internal representation
3. **Template Override**: Provide UDTF-specific Jinja2 templates while reusing template rendering logic
4. **Minimal Override**: Only override `generate_apis()` method to produce UDTF code instead of Pydantic models

**Import Pattern for Pygen Dependencies:**

**Short-term (Current Approach):**
- **For ALL pygen dependencies**, we use private API imports:
  ```python
  from cognite.pygen._core.generators import SDKGenerator
  from cognite.pygen._core.generators import MultiAPIGenerator
  # ... and any other pygen dependencies needed
  ```
- This pattern applies to **ALL pygen dependencies**, not just the specific ones listed here
- **Note:** This approach works but relies on pygen's private API, which may change

**Long-term (Required Dependency):**
- **Request pygen to export ALL required dependencies in `cognite.pygen.__init__.py`:**
  - `SDKGenerator` (from `cognite.pygen._core.generators`)
  - `MultiAPIGenerator` (from `cognite.pygen._core.generators`)
  - `DataModel` (type alias from `cognite.pygen._generator`)
  - Any other pygen dependencies used by `pygen-spark`
- **After pygen is updated, for ALL pygen dependencies, we will use public API imports:**
  ```python
  from cognite.pygen import SDKGenerator, MultiAPIGenerator, DataModel
  # ... and any other pygen dependencies needed
  ```
- This pattern applies to **ALL pygen dependencies**, not just the specific ones listed here
- This would:
  - Eliminate dependency on private APIs
  - Ensure perfect alignment with pygen's public API
  - Provide better stability guarantees
- **Action Required:** Coordinate with pygen maintainers to add all required dependencies to `__all__` in `cognite.pygen.__init__.py`

**Type Definition Strategy:**

**Short-term (Current Approach):**
- We define our own `DataModel` type alias using public types from `cognite.client`
- Uses `DataModelIdentifier` from `cognite.client.data_classes.data_modeling` (public API)
- Extends it with `dm.DataModel[dm.View]` to match pygen's definition
- Matches pygen's type definition exactly: `Union[DataModelIdentifier, dm.DataModel[dm.View]]`

**Long-term (After Pygen Update):**
- Once pygen exports `DataModel` in `cognite.pygen.__init__.py`, we can use:
  ```python
  from cognite.pygen import DataModel
  ```
- This would reduce code duplication and ensure perfect alignment with pygen

#### **4.1.5 Usage Example**

**Notebook-style generation (aligned with pygen pattern):**

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

# Load client from TOML file (same pattern as pygen)
# config.toml format:
# [cognite]
# project = "<cdf-project>"
# tenant_id = "<tenant-id>"
# cdf_cluster = "<cdf-cluster>"
# client_id = "<client-id>"
# client_secret = "<client-secret>"
client = load_cognite_client_from_toml("config.toml")

# Create WorkspaceClient (auto-detects credentials in Databricks)
workspace_client = WorkspaceClient()

# Generate UDTFs for a Data Model (same pattern as pygen's generate_sdk_notebook)
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,  # Pass workspace_client for registration
    catalog="main",  # Unity Catalog catalog name (default: "main")
    # schema=None,  # Auto-generated: "sp_pygen_power_windturbine_1" (matches folder pattern)
    # output_dir=None,  # Default: "/local_disk0/tmp/pygen_udtf/{folder_name}"
)

# Register UDTFs and Views in Unity Catalog
# Catalog and schema are automatically created if they don't exist
# Use data model-specific scope: cdf_{space}_{external_id}
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
generator.register_udtfs_and_views(secret_scope=secret_scope)
```

**Unity Catalog Namespace Structure:**

Unity Catalog uses a three-level hierarchy: `catalog.schema.object`

- **Catalog** (top level): Organizes data assets (e.g., `"main"`, `"cdf_data"`, `"production_cdf"`)
- **Schema** (middle level): Organizes tables, views, and functions within a catalog
  - Auto-generated from data model: `{space}_{external_id.lower()}_{version}`
  - Example: `sp_pygen_power_windturbine_1` for `DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")`
- **Object** (bottom level): The actual UDTFs and Views (e.g., `pump_view_udtf`, `pump_view`)

**Catalog and Schema Auto-Creation:**

The `register_udtfs_and_views()` method automatically:
1. Creates the catalog if it doesn't exist (requires `CREATE_CATALOG` permission)
2. Creates the schema if it doesn't exist (requires `CREATE_SCHEMA` permission on the catalog)
3. Registers UDTFs and Views in the catalog.schema namespace

**Example with Custom Catalog:**

```python
# Use a custom catalog for project-specific organization
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,
    catalog="cdf_data",  # Custom catalog name
    # schema=None,  # Auto-generated: "sp_pygen_power_windturbine_1"
)

# UDTFs and Views will be registered as:
# cdf_data.sp_pygen_power_windturbine_1.{view_name}_udtf
# cdf_data.sp_pygen_power_windturbine_1.{view_name}
generator.register_udtfs_and_views(secret_scope=secret_scope)
```

**Querying Registered UDTFs and Views:**

After registration, you can query them using the full Unity Catalog path:

```sql
-- Query a UDTF from main catalog
SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view_udtf();

-- Query a View
SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view;

-- Query from custom catalog
SELECT * FROM cdf_data.sp_pygen_power_windturbine_1.pump_view;
```

**Low-level API (for advanced use cases):**

```python
from __future__ import annotations

from pathlib import Path

from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.pygen import load_cognite_client_from_toml
from cognite.pygen_spark import SparkUDTFGenerator

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Create generator
generator = SparkUDTFGenerator(
    client=client,
    output_dir=Path("./generated_udtfs"),
    top_level_package="cognite_databricks",
)

# Generate UDTFs for a Data Model
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
udtf_files = generator.generate_udtfs(data_model_id)

# Generate View SQL with Secret injection
# Use data model-specific scope: cdf_{space}_{external_id}
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
view_sqls = generator.generate_views(
    data_model_id,
    secret_scope=secret_scope,
)

# Output:
# - udtf_files: {
#     "pump_view": Path("./generated_udtfs/pump_view_udtf.py"),
#     "sensor_view": Path("./generated_udtfs/sensor_view_udtf.py"),
#   }
# - view_sqls: {
#     "pump_view": "CREATE VIEW ... AS SELECT * FROM pump_view_udtf(SECRET(...));",
#     ...
#   }
```

#### **4.1.6 UDTF Template Example**

**Template: `udtf_function.py.jinja`**

The actual UDTF template generates a complete Python class with comprehensive error handling, property extraction, and PySpark Connect compatibility. Key features:

1. **Import Error Handling**: Wraps `cognite.client` imports in try-except to handle missing dependencies (DBR < 18.1 limitation)
2. **Defensive `__init__`**: Multi-layered error handling ensures the UDTF can always be instantiated, even if initialization fails
3. **Error Classification**: Categorizes errors as `AUTHENTICATION`, `CONFIGURATION`, `NETWORK`, or `UNKNOWN`
4. **`analyze` Method**: Static method required by PySpark Connect for UDTFs with constructor arguments
5. **Property Extraction**: `_extract_property_value` method handles DirectRelation values, arrays, and JSON strings
6. **Defensive `eval()`**: Always yields at least one row, preventing "end-of-input" errors

**Simplified Example (showing key structure):**

```python
"""Auto-generated UDTF for View: {{ view.external_id }}"""

from __future__ import annotations
from typing import Iterator

# Import error handling for DBR < 18.1
try:
from cognite.client import CogniteClient
    from cognite.client.data_classes.data_modeling.ids import ViewId
    COGNITE_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as import_error:
    COGNITE_AVAILABLE = False
    IMPORT_ERROR = str(import_error)
    # Dummy classes to prevent syntax errors
    class CogniteClient:
        pass
    class ViewId:
        pass

from pyspark.sql.types import StructType, StructField, StringType, ...

class {{ view.external_id|title }}UDTF:
    """User-Defined Table Function for {{ view.external_id }} View.
    
    Generated from CDF View: {{ view.space }}.{{ view.external_id }} {{ view.version }}
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        cdf_cluster: str,
        project: str,
        **kwargs: dict[str, object],
    ) -> None:
        """Initialize UDTF with OAuth2 credentials.
        
        Args:
            client_id: OAuth2 client ID (from Secret Manager)
            client_secret: OAuth2 client secret (from Secret Manager)
            tenant_id: Azure AD tenant ID (from Secret Manager)
            cdf_cluster: CDF cluster URL (from Secret Manager)
            project: CDF project name (from Secret Manager)
            **kwargs: Additional arguments for CogniteClient
        """
        # Multi-layered error handling ensures __init__ never raises
        try:
            # Check dependencies first (handles DBR < 18.1 limitation)
            if not COGNITE_AVAILABLE:
                self._init_success = False
                self._init_error = f"Missing dependencies: {IMPORT_ERROR}"
                self._init_error_category = 'CONFIGURATION'
                self.client = None
                return
            
            try:
                self.client = self._create_client(client_id, client_secret, tenant_id, cdf_cluster, project, **kwargs)
                self._init_success = True
                self._init_error = None
                self._init_error_category = None
            except Exception as e:
                # Store error for later use in eval()
                self._init_success = False
                try:
                    self._init_error_category = self._classify_error(e)
                except Exception:
                    self._init_error_category = 'UNKNOWN'
                self._init_error = f"{type(e).__name__}: {str(e)}"
                self.client = None
        except Exception as init_fatal_error:
            # Last resort: set safe defaults
            self._init_success = False
            self._init_error_category = 'UNKNOWN'
            self._init_error = f"Fatal initialization error: {type(init_fatal_error).__name__}: {str(init_fatal_error)}"
            self.client = None

    @staticmethod
    def analyze(
        client_id,
        client_secret,
        tenant_id,
        cdf_cluster,
        project,
        {% for prop in properties %}
        {{ prop.name }}{% if not loop.last %},{% endif %}
            {% endfor %}
    ):
        """Analyze method required by PySpark Connect for UDTFs with constructor arguments.
        
        This method validates the input arguments and returns an AnalyzeResult with the output schema.
        
        Args:
            client_id: OAuth2 client ID column
            client_secret: OAuth2 client secret column
            tenant_id: Azure AD tenant ID column
            cdf_cluster: CDF cluster URL column
            project: CDF project name column
            {% for prop in properties %}
            {{ prop.name }}: {{ prop.description or prop.name }} column
            {% endfor %}
        
        Returns:
            AnalyzeResult containing the output schema
        """
        from pyspark.sql.udtf import AnalyzeResult
        return AnalyzeResult({{ view.external_id|title }}UDTF.outputSchema())

    def eval(
        self,
        {% for prop in properties %}
        {{ prop.name }}: {{ prop.python_type }}{% if not loop.last %},{% endif %}
        {% endfor %}
    ) -> Iterator[tuple[object, ...]]:
        """Execute UDTF and return rows.

        Args:
            {% for prop in properties %}
            {{ prop.name }}: {{ prop.description or prop.name }}
            {% endfor %}

        Yields:
            Tuples representing rows from the View
        """
        try:
            # Check if initialization succeeded
            if not hasattr(self, '_init_success') or not self._init_success:
                error_msg = getattr(self, '_init_error', 'Unknown initialization error')
                error_category = getattr(self, '_init_error_category', 'UNKNOWN')
                # Yield error row immediately
                yield (
                    f"ERROR: Initialization failed [{error_category}]: {error_msg}",
                    {% for prop in properties %}
                    {% if not loop.first %}
                    None{% if not loop.last %},{% endif %}
                    {% endif %}
                    {% endfor %}
                )
                return
            
            # Build CDF filter from input parameters
            filters = self._build_cdf_filter({
                {% for prop in properties %}
                "{{ prop.name }}": {{ prop.name }},
                {% endfor %}
            })
            
            # Query CDF Data Model Instances by View
            instances = self.client.data_modeling.instances.list(
                sources=ViewId(
                    space="{{ view.space }}",
                    external_id="{{ view.external_id }}",
                    version="{{ view.version }}",
                ),
                instance_type="{% if view.used_for == 'edge' %}edge{% else %}node{% endif %}",
                filter=filters if filters else None,
                limit=1000,
            )
            
            # Yield rows with property extraction
            row_count = 0
            for instance in instances:
                row = (
                    {% for prop in properties %}
                    self._extract_property_value(instance.get("{{ prop.name }}")){% if not loop.last %},{% endif %}
                    {% endfor %}
                )
                yield row
                row_count += 1
            
            # If no rows found, yield empty row to prevent "end-of-input" error
            if row_count == 0:
                yield (
                    {% for prop in properties %}
                    None{% if not loop.last %},{% endif %}
                    {% endfor %}
                )
        except Exception as outer_error:
            # Last resort: yield error row
            error_info = f"ERROR: Unexpected error in eval(): {type(outer_error).__name__}: {str(outer_error)}"
            yield (
                error_info,
                {% for prop in properties %}
                {% if not loop.first %}
                None{% if not loop.last %},{% endif %}
                {% endif %}
                {% endfor %}
            )

    def _extract_property_value(self, value: object) -> object:
        """Extract the actual value from a CDF property value using Pydantic-style validation.
        
        This follows the same pattern as pygen-main's `as_direct_relation_reference` helper,
        but extracts just the external_id string for UDTF output.
        
        Note: Unlike `as_direct_relation_reference`, this method returns `object` (not `DirectRelationReference`)
        because it can return various types (str, list, None, etc.) depending on the input.
        This matches the UDTF output schema which uses primitive types.
        
        Handles:
        - DirectRelationReference objects -> extract external_id
        - Arrays of DirectRelationReference -> extract external_ids
        - Dict representations -> extract externalId/external_id
        - String representations -> parse externalId
        - JSON string arrays -> parse to Python lists
        - Regular values -> return as-is
        - None -> return None
        
        Args:
            value: The property value from instance.get()
            
        Returns:
            The extracted value (string for DirectRelation, list for arrays, etc.)
        """
        from cognite.client.data_classes.data_modeling import DirectRelationReference
        import re
        import json
        
        if value is None:
            return None
        
        # Helper to extract external_id from various DirectRelation representations
        def extract_external_id(val: object) -> str | None:
            """Extract external_id from various DirectRelation representations.
            
            This follows the same pattern as pygen-main's as_direct_relation_reference,
            but returns just the external_id string instead of a DirectRelationReference object.
            
            Args:
                val: Value that may represent a DirectRelation (DirectRelationReference, dict, tuple, str, etc.)
            
            Returns:
                external_id string if val represents a DirectRelation, None otherwise
            """
            # Already a DirectRelationReference object
            if isinstance(val, DirectRelationReference):
                return val.external_id
            
            # NodeId-like object (has space and external_id attributes)
            if hasattr(val, 'external_id') and hasattr(val, 'space'):
                return val.external_id
            
            # Dict with camelCase or snake_case
            if isinstance(val, dict):
                if "externalId" in val:
                    return str(val["externalId"])
                if "external_id" in val:
                    return str(val["external_id"])
            
            # Tuple (space, external_id)
            if isinstance(val, tuple) and len(val) == 2:
                return str(val[1])
            
            # String representation: "{externalId=country::257, space=sailboat}"
            if isinstance(val, str) and val.startswith("{") and "externalId=" in val:
                match = re.search(r'externalId=([^,}]+)', val)
                if match:
                    return match.group(1).strip()
            
            return None
        
        # Handle arrays/lists
        if isinstance(value, (list, tuple)):
            extracted = []
            for item in value:
                item_external_id = extract_external_id(item)
                if item_external_id is not None:
                    extracted.append(item_external_id)
                else:
                    extracted.append(item)
            return extracted
        
        # Handle JSON string arrays (e.g., '["257679870"]' or '[""257679870""]')
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        extracted = []
                        for item in parsed:
                            item_external_id = extract_external_id(item)
                            if item_external_id is not None:
                                extracted.append(item_external_id)
                            else:
                                extracted.append(item)
                        return extracted
                except (json.JSONDecodeError, ValueError):
                    pass
            
            # Check if it's a DirectRelation string representation
            external_id = extract_external_id(value)
            if external_id is not None:
                return external_id
        
        # Check if it's a DirectRelation (single value, not a string)
        external_id = extract_external_id(value)
        if external_id is not None:
            return external_id
        
        # For other types, return as-is
        return value

    def _classify_error(self, error: Exception) -> str:
        """Classify the type of initialization error.
        
        Args:
            error: The exception that occurred during initialization
        
        Returns:
            Error category string: 'AUTHENTICATION', 'CONFIGURATION', 'NETWORK', or 'UNKNOWN'
        """
        error_type = type(error).__name__
        error_msg = str(error).lower()
        
        # Authentication-related errors
        auth_keywords = [
            'authentication', 'auth', 'unauthorized', 'forbidden', '401', '403',
            'invalid_client', 'invalid_grant', 'invalid_credentials', 'token',
            'oauth', 'client_id', 'client_secret', 'tenant_id', 'credential'
        ]
        if any(keyword in error_msg for keyword in auth_keywords):
            return 'AUTHENTICATION'
        
        # Configuration-related errors
        config_keywords = [
            'config', 'configuration', 'missing', 'required', 'invalid',
            'valueerror', 'typeerror', 'attributeerror', 'keyerror'
        ]
        if any(keyword in error_msg for keyword in config_keywords) or error_type in ['ValueError', 'TypeError', 'AttributeError', 'KeyError']:
            return 'CONFIGURATION'
        
        # Network-related errors
        network_keywords = [
            'connection', 'timeout', 'network', 'dns', 'resolve', 'unreachable',
            'connectionerror', 'timeouterror', 'httperror', 'urlerror'
        ]
        if any(keyword in error_msg for keyword in network_keywords) or error_type in ['ConnectionError', 'TimeoutError', 'HTTPError', 'URLError']:
            return 'NETWORK'
        
        # Default to unknown
        return 'UNKNOWN'

    def _create_client(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        cdf_cluster: str,
        project: str,
        **kwargs: dict[str, object],
    ) -> CogniteClient:
        """Create CogniteClient with OAuth2 credentials."""
        from cognite.client import CogniteClient
        from cognite.client.config import ClientConfig
        from cognite.client.credentials import OAuthClientCredentials

        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        )

        config = ClientConfig(
            project=project,
            credentials=credentials,
            base_url=f"https://{cdf_cluster}.cognitedata.com",
            **kwargs,
        )
        
        return CogniteClient(config=config)

    def _build_cdf_filter(self, filters: dict[str, object]) -> dict[str, object]:
        """Build CDF API filter from input parameters.

        Args:
            filters: Dictionary of filter conditions

        Returns:
            CDF API filter dictionary (filters out None values)
        """
        return {k: v for k, v in filters.items() if v is not None}

    @staticmethod
    def outputSchema() -> StructType:
        """Return the output schema for this UDTF."""
        return StructType([
            {% for prop in properties %}
            StructField("{{ prop.name }}", {{ prop.spark_type }}, nullable={{ prop.nullable }}),
            {% endfor %}
        ])
```

**Template: `view_sql.py.jinja`**

```sql
{# 
  Note: The secrets referenced here (client_id, client_secret, tenant_id, cdf_cluster, project)
  come from the TOML file and are stored in Secret Manager via set_cdf_credentials().
#}
CREATE VIEW {{ catalog }}.{{ schema }}.{{ view.external_id }} AS
SELECT * FROM {{ catalog }}.{{ schema }}.{{ view.external_id }}_udtf(
    SECRET('{{ secret_scope }}', 'client_id') AS client_id,
    SECRET('{{ secret_scope }}', 'client_secret') AS client_secret,
    SECRET('{{ secret_scope }}', 'tenant_id') AS tenant_id,
    SECRET('{{ secret_scope }}', 'cdf_cluster') AS cdf_cluster,
    SECRET('{{ secret_scope }}', 'project') AS project
);
```

#### **4.1.7 UDTF Implementation Details**

**4.1.7.1 Error Handling and Classification**

The UDTF template implements comprehensive error handling to prevent silent failures and provide actionable error messages:

1. **Import Error Handling** (DBR < 18.1 Support):
   - Wraps `cognite.client` imports in try-except blocks
   - Sets `COGNITE_AVAILABLE` and `IMPORT_ERROR` flags
   - Creates dummy classes to prevent syntax errors if imports fail
   - Returns `CONFIGURATION` error category if dependencies are missing

2. **Defensive `__init__` Method**:
   - Multi-layered try-except blocks ensure `__init__` never raises
   - Stores initialization status in `_init_success`, `_init_error`, and `_init_error_category`
   - Allows `eval()` to yield error rows instead of failing silently
   - Prevents "end-of-input" errors by ensuring the UDTF can always be instantiated

3. **Error Classification** (`_classify_error` method):
   - Categorizes errors into four categories:
     - **`AUTHENTICATION`**: OAuth2, credential, token, 401/403 errors
     - **`CONFIGURATION`**: Missing parameters, invalid values, type errors
     - **`NETWORK`**: Connection, timeout, DNS, unreachable errors
     - **`UNKNOWN`**: All other errors
   - Uses keyword matching and exception type checking
   - Provides structured error messages: `"ERROR: Initialization failed [CATEGORY]: message"`

4. **Defensive `eval()` Method**:
   - Checks `_init_success` before proceeding
   - Yields error row if initialization failed
   - Wraps entire method body in try-except
   - Always yields at least one row (empty row if no instances found)
   - Prevents "No content to map due to end-of-input" errors

**4.1.7.2 Property Extraction (`_extract_property_value`)**

The `_extract_property_value` method handles CDF property values, especially DirectRelation references, following the same pattern as pygen-main's `as_direct_relation_reference` helper:

1. **DirectRelationReference Objects**:
   - Extracts `external_id` from `DirectRelationReference` objects
   - Handles NodeId-like objects with `space` and `external_id` attributes

2. **Dict Representations**:
   - Supports both camelCase (`externalId`) and snake_case (`external_id`)
   - Extracts the external_id string from dict values

3. **String Representations**:
   - Parses string formats like `"{externalId=country::257, space=sailboat}"`
   - Uses regex to extract external_id from string representations

4. **Array Handling**:
   - Processes lists/tuples of DirectRelation references
   - Extracts external_ids from each item in the array
   - Returns a list of external_id strings

5. **JSON String Arrays**:
   - Handles JSON string arrays like `'["257679870"]'` or `'[""257679870""]'`
   - Parses JSON and recursively extracts external_ids
   - Falls back to string extraction if JSON parsing fails

6. **Return Type Design**:
   - Returns `object` (not `DirectRelationReference`) to match UDTF output schema
   - Can return `str`, `list[str]`, `None`, or other primitive types
   - Aligns with UDTF output schema which uses primitive types

**4.1.7.3 PySpark Connect Compatibility (`analyze` Method)**

For PySpark Connect, UDTFs with constructor arguments require a static `analyze` method:

1. **Signature**: Matches `__init__` parameters (excluding `self` and `**kwargs`)
2. **Return Type**: Returns `AnalyzeResult(UDTF.outputSchema())`
3. **Purpose**: Validates input arguments and provides output schema to Spark
4. **Note**: The `udtf()` wrapper should NOT specify `returnType` when `analyze` is present

**4.1.7.4 Row Extraction**

The `eval()` method uses `_extract_property_value()` for all property extractions:

```python
row = (
    self._extract_property_value(instance.get("property1")),
    self._extract_property_value(instance.get("property2")),
    ...
)
```

This ensures:
- DirectRelation values are extracted as external_id strings
- Arrays are properly converted to lists of external_ids
- JSON string arrays are parsed correctly
- Regular values are returned as-is

---

### **4.2 cognite-databricks Helper SDK**

#### **4.2.1 Unity Catalog Namespace and Auto-Creation**

**Unity Catalog Three-Level Namespace:**

Unity Catalog uses a hierarchical namespace structure: `catalog.schema.object`

- **Catalog** (top level): Organizes data assets, similar to a database in traditional systems
  - Default: `"main"` (the default catalog in Databricks)
  - Custom examples: `"cdf_data"`, `"production_cdf"`, `"development_cdf"`
  - Specified via `catalog` parameter in `generate_udtf_notebook()`

- **Schema** (middle level): Organizes tables, views, and functions within a catalog
  - Auto-generated from data model ID if not provided: `{space}_{external_id.lower()}_{version}`
  - Example: For `DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")`
    - Schema: `sp_pygen_power_windturbine_1`
  - Can be explicitly provided via `schema` parameter
  - Matches the folder name pattern used for UDTF code generation

- **Object** (bottom level): The actual UDTFs and Views
  - UDTF functions: `{view_name}_udtf` (e.g., `pump_view_udtf`)
  - Views: `{view_name}` (e.g., `pump_view`)

**Automatic Catalog and Schema Creation:**

The `register_udtfs_and_views()` method automatically ensures the catalog and schema exist:

1. **Catalog Creation** (`_ensure_catalog_exists()`):
   - Checks if the catalog exists using `workspace_client.catalogs.get(catalog_name)`
   - Creates the catalog if it doesn't exist using `workspace_client.catalogs.create()`
   - Requires `CREATE_CATALOG` permission
   - Handles race conditions (concurrent creation)

2. **Schema Creation** (`_ensure_schema_exists()`):
   - First ensures the catalog exists (calls `_ensure_catalog_exists()`)
   - Checks if the schema exists using `workspace_client.schemas.get(full_name)`
   - Creates the schema if it doesn't exist using `workspace_client.schemas.create()`
   - Requires `CREATE_SCHEMA` permission on the catalog
   - Handles race conditions (concurrent creation)

**Example Namespace Structure:**

```
main                                    # Catalog (default)
  └── sp_pygen_power_windturbine_1      # Schema (auto-generated from data model)
      ├── pump_view_udtf                # UDTF function
      ├── pump_view                     # SQL View
      ├── sensor_view_udtf              # UDTF function
      └── sensor_view                   # SQL View

cdf_data                                # Custom catalog
  └── sp_pygen_power_windturbine_1      # Schema (same data model)
      ├── pump_view_udtf                # UDTF function
      └── pump_view                     # SQL View
```

**Querying Registered Objects:**

After registration, objects can be queried using the full Unity Catalog path:

```sql
-- Query from default catalog
SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view;

-- Query UDTF directly
SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view_udtf();

-- Query from custom catalog
SELECT * FROM cdf_data.sp_pygen_power_windturbine_1.pump_view;
```

#### **4.2.2 Package Structure**

```
cognite-databricks/
├── cognite/
│   └── databricks/
│       ├── __init__.py            # Exports generate_udtf_notebook, UDTFGenerator, etc.
│       ├── udtf_registry.py       # UDTF registration in Unity Catalog
│       ├── secret_manager.py       # Secret Manager helpers
│       ├── generator.py            # generate_udtf_notebook helper function and UDTFGenerator
│       └── utils.py                # Utility functions
├── pyproject.toml
├── README.md
└── tests/
    └── test_udtf_registry.py
```

#### **4.2.2 Core Classes**

**UDTFRegistry**

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo, FunctionParameterInfo

if TYPE_CHECKING:
    pass

class UDTFRegistry:
    """Utility for registering Python UDTFs in Unity Catalog."""

    def __init__(self, workspace_client: WorkspaceClient) -> None:
        self.workspace_client = workspace_client

    def register_udtf(
        self,
        catalog: str,
        schema: str,
        function_name: str,
        udtf_code: str,
        input_params: list[FunctionParameterInfo],
        return_type: str,
        return_params: list[FunctionParameterInfo],  # NEW: Structured return columns
        dependencies: list[str] | None = None,  # For DBR 18.1+ custom dependencies
        comment: str | None = None,
    ) -> FunctionInfo:
        """Register a Python UDTF in Unity Catalog.

        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            function_name: UDTF function name
            udtf_code: Python code for the UDTF class
            input_params: Function parameter definitions
            return_type: Return type DDL string (e.g., "TABLE(...)")
            return_params: Structured metadata for the UDTF output columns (required for TABLE_TYPE)
            dependencies: Optional list of Python package dependencies (DBR 18.1+)
            comment: Function description

        Returns:
            Registered FunctionInfo
        """
        # Build CreateFunction object according to Databricks SDK API
        # Note: For EXTERNAL functions (Python UDTFs) that are Table Functions:
        # - schema is in full_data_type (as DDL string)
        # - return_params MUST be fully populated with structured column metadata
        
        from databricks.sdk.service.catalog import (
            CreateFunction,
            FunctionParameterInfos,
            ColumnTypeName,
            CreateFunctionRoutineBody,
            CreateFunctionParameterStyle,
            CreateFunctionSqlDataAccess,
            CreateFunctionSecurityType,
        )
        
        # Wrap input_params and return_params in FunctionParameterInfos structure
        input_params_wrapped = FunctionParameterInfos(parameters=input_params)
        return_params_wrapped = FunctionParameterInfos(parameters=return_params)
        
        # Build CreateFunction with all required fields (per OpenAPI spec)
        create_function = CreateFunction(
            name=function_name,  # Just function name, not full name
            catalog_name=catalog,
            schema_name=schema,
            input_params=input_params_wrapped,
            return_params=return_params_wrapped,  # Structured metadata for output columns
            data_type=ColumnTypeName.TABLE_TYPE,  # TABLE_TYPE for table-returning functions (UDTFs)
            full_data_type=return_type,  # Required: DDL string "TABLE(col1 TYPE, col2 TYPE, ...)"
            routine_body=CreateFunctionRoutineBody.EXTERNAL,  # Python UDTFs are EXTERNAL routines
            routine_definition=udtf_code,  # The actual Python code
            parameter_style=CreateFunctionParameterStyle.S,  # SQL parameter style
            is_deterministic=False,  # UDTFs are typically non-deterministic
            sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,  # Python UDTFs don't use SQL
            is_null_call=False,
            security_type=CreateFunctionSecurityType.DEFINER,
            specific_name=function_name,  # Use function_name as specific_name
            comment=comment,
            external_language="PYTHON",  # Required for EXTERNAL functions
            routine_dependencies=None,  # TODO: Python package dependencies need different handling
            # Note: DependencyList is for SQL object dependencies (tables, functions), not Python packages
            # For DBR 18.1+ Python package dependencies, may need to be handled via external_language
            # or a different mechanism. Setting to None for now.
        )
        
        # The API expects CreateFunctionRequest with function_info field
        return self.workspace_client.functions.create(function_info=create_function)

    def register_view(
        self,
        catalog: str,
        schema: str,
        view_name: str,
        view_sql: str,
        comment: str | None = None,
    ) -> None:
        """Register a SQL View in Unity Catalog.

        Args:
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name
            view_name: View name
            view_sql: CREATE VIEW SQL statement
            comment: View description
        """
        # Execute CREATE VIEW statement via Databricks SQL API
        # Note: This requires a warehouse_id, which should be configured
        # For now, this is a placeholder implementation
        # In production, you would need to:
        # 1. Get warehouse_id from configuration
        # 2. Execute the SQL statement
        # 3. Handle errors appropriately
        raise NotImplementedError(
            "View registration via SQL API requires warehouse configuration. "
            "Use workspace_client.statement_execution.execute_statement() with proper warehouse_id."
        )
```

#### **4.2.3 Critical: `type_json` Format for FunctionParameterInfo**

When registering UDTFs via the Databricks Functions API, each `FunctionParameterInfo` requires a `type_json` field that specifies the parameter's type schema. **This field uses the Spark StructField JSON format**, not a simple type string.

**Discovery:** The correct format was discovered by inspecting built-in Databricks functions (e.g., `system.ai.python_exec`) using the Functions API:

```python
# Inspecting system.ai.python_exec revealed:
# Parameter name: "code"
# type_json: '{"name":"code","type":"string","nullable":true,"metadata":{"comment":"..."}}'
```

**Format Specification:**

```json
{
    "name": "<parameter_name>",      // MUST match FunctionParameterInfo.name
    "type": "<spark_type_name>",     // Lowercase Spark type: "string", "long", "double", etc.
    "nullable": true,                 // Whether NULL values are allowed
    "metadata": {}                    // Additional metadata (can be empty object)
}
```

**Spark Type Name Mappings:**

| CDF Property Type | SQL Type | Spark Type Name | ColumnTypeName |
|---|---|---|---|
| `dm.Text` | STRING | "string" | `ColumnTypeName.STRING` |
| `dm.Int32`, `dm.Int64` | INT | "long" | `ColumnTypeName.INT` |
| `dm.Float32`, `dm.Float64` | DOUBLE | "double" | `ColumnTypeName.DOUBLE` |
| `dm.Boolean` | BOOLEAN | "boolean" | `ColumnTypeName.BOOLEAN` |
| `dm.Date` | DATE | "date" | `ColumnTypeName.DATE` |
| `dm.Timestamp` | TIMESTAMP | "timestamp" | `ColumnTypeName.TIMESTAMP` |
| `dm.DirectRelation` | STRING | "string" | `ColumnTypeName.STRING` |
| `SingleReverseDirectRelation` | STRING | "string" | `ColumnTypeName.STRING` |
| `MultiReverseDirectRelation` | ARRAY<STRING> | array (see below) | `ColumnTypeName.ARRAY` |

**Correct Implementation:**

```python
# For a parameter named "client_id" of type STRING (required, no default):
FunctionParameterInfo(
    name="client_id",
    type_text="STRING",
    type_name=ColumnTypeName.STRING,
    type_json='{"name":"client_id","type":"string","nullable":true,"metadata":{}}',
    position=0,
    parameter_mode=FunctionParameterMode.IN,
    parameter_type=FunctionParameterType.PARAM,
    # No parameter_default - required parameter
)

# For a view property parameter (optional, with default NULL):
FunctionParameterInfo(
    name="name",
    type_text="STRING",
    type_name=ColumnTypeName.STRING,
    type_json='{"name":"name","type":"string","nullable":true,"metadata":{}}',
    position=5,
    parameter_mode=FunctionParameterMode.IN,
    parameter_type=FunctionParameterType.PARAM,
    parameter_default="NULL",  # Makes parameter optional - can be omitted in SQL calls
)
```

**Common Mistakes to Avoid:**

1. ❌ `type_json='{"type":"STRING"}'` - Wrong: uppercase type name
2. ❌ `type_json='"string"'` - Wrong: just the type as a JSON string
3. ❌ `type_json='STRING'` - Wrong: bare string without JSON
4. ❌ `type_json='{"type":"string"}'` - Wrong: missing `name`, `nullable`, `metadata` fields
5. ✅ `type_json='{"name":"param","type":"string","nullable":true,"metadata":{}}'` - Correct format

#### **4.2.3.1 Default Parameter Values and Named Parameters**

**Problem**: UDTFs with many view property parameters (20+) require passing dozens of `NULL` values in SQL queries, making queries ugly, hard to read, and fragile to schema changes.

**Solution**: Use `parameter_default="NULL"` for view property parameters, enabling clean SQL queries with named parameter syntax.

**Implementation:**

1. **Default Values**: All view property parameters are registered with `parameter_default="NULL"`:
   ```python
   FunctionParameterInfo(
       name="name",
       type_text="STRING",
       type_name=ColumnTypeName.STRING,
       type_json='{"name":"name","type":"string","nullable":true,"metadata":{}}',
       position=5,
       parameter_mode=FunctionParameterMode.IN,
       parameter_type=FunctionParameterType.PARAM,
       parameter_default="NULL",  # Makes parameter optional
   )
   ```

2. **Named Parameter Syntax**: SQL queries can use named parameters (`param => value`) instead of positional parameters:
   ```sql
   -- Clean: Named parameters (recommended)
   SELECT * FROM catalog.schema.smallboat_udtf(
       client_id     => SECRET('scope', 'client_id'),
       client_secret => SECRET('scope', 'client_secret'),
       tenant_id     => SECRET('scope', 'tenant_id'),
       cdf_cluster   => SECRET('scope', 'cdf_cluster'),
       project       => SECRET('scope', 'project')
       -- All view property parameters use default NULL values
   ) LIMIT 10;
   
   -- Legacy: Positional parameters (still supported)
   SELECT * FROM catalog.schema.smallboat_udtf(
       SECRET('scope', 'client_id'),
       SECRET('scope', 'client_secret'),
       SECRET('scope', 'tenant_id'),
       SECRET('scope', 'cdf_cluster'),
       SECRET('scope', 'project'),
       NULL, -- name
       NULL, -- description
       -- ... 20+ more NULLs
   ) LIMIT 10;
   ```

3. **Benefits**:
   - **Cleaner SQL**: Only credentials needed, no dozens of NULLs
   - **Resilient to Changes**: Adding new properties doesn't break existing queries
   - **Better Readability**: Named parameters are self-documenting
   - **Flexible**: Can still use positional parameters when needed

4. **View SQL Template**: Generated views use named parameters:
   ```sql
   CREATE OR REPLACE VIEW catalog.schema.SmallBoat AS
   SELECT * FROM catalog.schema.smallboat_udtf(
       client_id     => SECRET('scope', 'client_id'),
       client_secret => SECRET('scope', 'client_secret'),
       tenant_id     => SECRET('scope', 'tenant_id'),
       cdf_cluster   => SECRET('scope', 'cdf_cluster'),
       project       => SECRET('scope', 'project')
       -- All view property parameters use default NULL values
   )
   ```

5. **Helper Functions**:
   - `generate_udtf_sql_query()`: Generates SQL queries with named or positional parameters
   - `generate_session_scoped_notebook_code()`: Generates notebook code snippets with named parameter examples

**Note**: Secret parameters (client_id, client_secret, tenant_id, cdf_cluster, project) do NOT have default values and must always be provided.

#### **4.2.3.2 Consistent Naming: camelCase to snake_case Conversion**

**Problem**: Inconsistent camelCase to snake_case conversion across different code paths led to mismatched function names (e.g., `SmallBoat` → `smallboat_udtf` vs `small_boat_udtf`).

**Solution**: Use pygen-main's `to_snake` function for consistent naming conversion across all code paths.

**Implementation:**

1. **Helper Function**: `to_udtf_function_name()` in `cognite.pygen_spark.utils` (moved from `cognite.databricks.utils` for generic Spark support):
   ```python
   from cognite.pygen.utils.text import to_snake
   
   def to_udtf_function_name(view_id: str) -> str:
       """Convert view external_id to UDTF function name using pygen-main's to_snake."""
       if view_id.lower().endswith('_udtf'):
           return view_id.lower()
       snake_case = to_snake(view_id)
       return f"{snake_case}_udtf"
   ```
   
   **Note**: This function is available in `cognite.pygen_spark` for generic Spark use, and is re-exported from `cognite.databricks` for backward compatibility.

2. **Usage**: All code paths use this function for consistent naming:
   - **Unity Catalog registration** (`_register_single_udtf_and_view_impl`): Converts view ID to function name when registering in Unity Catalog
   - **Session-scoped registration** (`register_session_scoped_udtfs`): Converts view ID to function name for PySpark session registration
   - **Session-scoped with prefix** (`register_session_scoped_udtfs` with `function_name_prefix`): Uses consistent conversion even with prefix
   - **File-based registration** (`register_udtf_from_file`): Converts class name to function name
   - **Notebook code generation** (`generate_session_scoped_notebook_code`): Converts view ID to function name for SQL examples

3. **Benefits**:
   - **Consistent**: Same conversion logic as pygen-main, used across all registration modes
   - **Handles Edge Cases**: "3D" → "3d", "HTTPResponse" → "http_response", etc.
   - **Single Source of Truth**: One function used everywhere (Unity Catalog, session-scoped, file-based)
   - **Predictable**: `SmallBoat` always becomes `small_boat_udtf` regardless of registration method
   - **Unified Experience**: Unity Catalog and session-scoped UDTFs use the same function names

**Examples:**
- `SmallBoat` → `small_boat_udtf` ✓
- `Cognite3DModel` → `cognite_3d_model_udtf` ✓ (pygen-main handles "3D" specially)
- `HTTPResponse` → `http_response_udtf` ✓
- `Smallboat` → `smallboat_udtf` ✓ (no capital B, so no split)

#### **4.2.3.3 Universal UDTF Template: Supporting Unity Catalog and Session-Scoped Registration**

**Problem**: The UDTF template needed to support three different invocation scenarios:
1. Unity Catalog UDTF (direct call)
2. Unity Catalog View (via view that calls UDTF)
3. Session-scoped UDTF (PySpark Connect)

Each scenario has different requirements for parameter passing and client initialization.

**Solution**: Universal template that detects the registration mode and handles initialization accordingly.

**Implementation Details:**

1. **`__init__` Method**: Parameter-free (`def __init__(self)`) to support all modes:
   - **Unity Catalog UDTF (direct)**: Unity Catalog passes all parameters to `eval()`
   - **Unity Catalog View (via view)**: Unity Catalog passes all parameters to `eval()`
   - **Session-scoped**: PySpark Connect requires parameter-free `__init__` when `analyze` method exists
   - All client initialization happens in `eval()` for consistency across all modes

2. **`analyze` Method**: Required for PySpark Connect (session-scoped), optional for Unity Catalog:
   - All view property parameters have `=None` defaults
   - Enables clean SQL with named parameters
   - Validates input arguments and returns output schema

3. **`eval` Method**: Accepts all parameters with defaults, handles client initialization:
   - **All modes**: Client initialized here on first call (cached for subsequent calls)
   - **Credentials**: Required parameters (client_id, client_secret, tenant_id, cdf_cluster, project)
   - **View properties**: Optional parameters with `None` defaults for filtering

**Complete Examples:**

##### **Scenario 1: Unity Catalog UDTF (Direct Call)**

**Registration:**
```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

# Generate and register UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    catalog="main",
    schema="sailboat_sailboat_v1",
)

# Register UDTFs in Unity Catalog
result = generator.register_udtfs_and_views(
    secret_scope="cdf_sailboat_sailboat",
    dependencies=["cognite-sdk>=6.0.0"],  # DBR 18.1+
)
```

**Usage (Direct UDTF Call):**
```sql
-- Clean SQL with named parameters (recommended)
SELECT * FROM main.sailboat_sailboat_v1.small_boat_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
    -- All view property parameters use default NULL values
) LIMIT 10;

-- With specific filter (optional)
SELECT * FROM main.sailboat_sailboat_v1.small_boat_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project'),
    mmsi          => '257679870'  -- Filter by specific MMSI
) LIMIT 10;
```

**How It Works:**
1. Unity Catalog resolves `SECRET()` calls → passes resolved strings to `eval()`
2. `eval()` receives all parameters (credentials + view properties)
3. `eval()` initializes CogniteClient on first call (cached for subsequent calls)
4. Query executes and returns rows

##### **Scenario 2: Unity Catalog View (Via View)**

**Registration:** (Same as Scenario 1 - views are created automatically)

**View Definition (Auto-generated):**
```sql
CREATE OR REPLACE VIEW main.sailboat_sailboat_v1.SmallBoat AS
SELECT * FROM main.sailboat_sailboat_v1.small_boat_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
    -- All view property parameters use default NULL values
);
```

**Usage (Via View):**
```sql
-- Simple view query - no parameters needed!
SELECT * FROM main.sailboat_sailboat_v1.SmallBoat LIMIT 10;

-- View with WHERE clause for filtering
SELECT * FROM main.sailboat_sailboat_v1.SmallBoat 
WHERE mmsi = '257679870' 
LIMIT 10;
```

**How It Works:**
1. User queries the view → Unity Catalog executes view SQL
2. View SQL calls UDTF with `SECRET()` calls → Unity Catalog resolves SECRET() values
3. Unity Catalog passes resolved strings to UDTF `eval()` (all parameters)
4. `eval()` receives all parameters (credentials + view properties)
5. `eval()` initializes CogniteClient on first call (cached for subsequent calls)
6. Query executes and returns rows

##### **Scenario 3: Session-Scoped UDTF (PySpark Connect)**

**Registration:**
```python
# Cell 1: Install dependencies
%pip install cognite-sdk
# (Restart Python kernel when prompted)

# Cell 2: Register UDTFs for session-scoped use
from cognite.databricks import generate_udtf_notebook, register_session_scoped_udtfs
from cognite.pygen import load_cognite_client_from_toml
from cognite.client.data_classes.data_modeling.ids import DataModelId

# Load client
client = load_cognite_client_from_toml("config.toml")

# Define data model
data_model_id = DataModelId(space="sailboat", external_id="sailboat", version="1")

# Generate UDTFs
generator = generate_udtf_notebook(
    data_model_id,
    client,
    output_dir="/Workspace/Users/your_email/udtf",
)

# Register all UDTFs for session-scoped use
registered = generator.register_session_scoped_udtfs()

# Print registered functions
print("\n✓ Registered UDTFs:")
for view_id, func_name in registered.items():
    print(f"  - {view_id} -> {func_name}")
```

**Usage (Session-Scoped):**
```sql
-- Clean SQL with named parameters (no catalog/schema prefix!)
SELECT * FROM small_boat_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
    -- All view property parameters use default NULL values
) LIMIT 10;

-- With specific filter (optional)
SELECT * FROM small_boat_udtf(
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project'),
    mmsi          => '257679870'  -- Filter by specific MMSI
) LIMIT 10;
```

**How It Works:**
1. PySpark Connect calls `analyze()` method → validates parameters and returns schema
2. PySpark Connect instantiates UDTF with `__init__()` (no parameters - required when `analyze` exists)
3. PySpark Connect calls `eval()` with all parameters (credentials + view properties)
4. `eval()` checks if client is initialized → if not, initializes CogniteClient on first call
5. Client is cached for subsequent calls
6. Query executes and returns rows

**Important Notes for Session-Scoped:**
- **No catalog/schema prefix**: Session-scoped UDTFs are registered in the Spark session, not Unity Catalog
- **Function name**: Use `small_boat_udtf` (with underscore), not `smallboat_udtf`
- **Same Spark session**: Ensure SQL runs in the same Spark session as registration (or use `spark.sql()` from Python)

**Alternative: Using Helper Function for Notebook Code:**
```python
# Get notebook-ready code snippets
code_snippets = generate_session_scoped_notebook_code(
    generator,
    secret_scope="cdf_sailboat_sailboat",
)

# Print and copy each cell
print("=" * 60)
print("CELL 1 - Install Dependencies:")
print("=" * 60)
print(code_snippets["cell1_dependencies"])

print("\n" + "=" * 60)
print("CELL 2 - Register UDTFs:")
print("=" * 60)
print(code_snippets["cell2_registration"])

print("\n" + "=" * 60)
print("CELL 3 - SQL Example (Named Parameters):")
print("=" * 60)
print(code_snippets["cell3_sql_example"])
```

#### **4.2.3.4 Relationship Properties: DirectRelation and MultiReverseDirectRelation**

**Critical: Relationship properties represent references to other nodes/edges in the data model**

CDF Data Modeling supports relationship properties that reference other views (nodes/edges). These relationships are represented differently in SQL:

| Relationship Type | SQL Representation | Description | Status |
|------------------|-------------------|-------------|-------|
| `dm.DirectRelation` | `STRING` | Single external_id reference to another view | ✅ Supported |
| `SingleReverseDirectRelation` | `STRING` | Single external_id reference (reverse direction) | ✅ Supported |
| `MultiReverseDirectRelation` | `ARRAY<STRING>` | **List of external_id references** (array of strings) | ⚠️ **TODO: Currently Skipped** |

**Key Implementation Details:**

1. **Single Relationships** (`DirectRelation`, `SingleReverseDirectRelation`):
   - Represented as `STRING` type in SQL
   - Contains a single `external_id` value
   - Example: `user_id STRING` where `user_id` references `Users.external_id`
   - **Runtime Extraction**: The UDTF template's `_extract_property_value()` method (see Section 4.1.7.2) extracts the `external_id` from `DirectRelationReference` objects, dict representations, and string formats at runtime
   - This ensures that DirectRelation values are properly converted to external_id strings in the UDTF output

2. **Multi Relationships** (`MultiReverseDirectRelation`):
   - **TODO: Currently NOT SUPPORTED** - Skipped during UDTF registration
   - **Intended representation**: `ARRAY<STRING>` type in SQL
   - **Issue**: The Databricks Unity Catalog API rejects all `type_json` formats we've tried for arrays:
     - ❌ `{"type":"array","elementType":{"type":"string"},"containsNull":true,"nullable":true,"metadata":{}}` - Rejected
     - ❌ `{"type":"array","elementType":{"type":"string"},"nullable":true,"metadata":{}}` - Rejected  
     - ❌ `{"type":"array","nullable":true,"metadata":{}}` - Rejected: `[INVALID_JSON_DATA_TYPE] Failed to convert the JSON string 'array' to a data type`
   - **Current Workaround**: `MultiReverseDirectRelation` properties are **skipped** in:
     - UDTF input parameters (`_parse_udtf_params`)
     - UDTF return type DDL (`_parse_return_type`) - **Note**: Now uses PySpark `StructType` via `_build_output_schema()`
     - UDTF return parameters (`_parse_return_params`) - **Note**: Now uses PySpark `StructType` via `_build_output_schema()`
   - **Impact**: Views with `MultiReverseDirectRelation` properties will be missing those columns
   - **Future Work**: Need to find the correct `type_json` format for arrays, or determine if arrays require a different approach

**⚠️ TODO: Array Type Support**

**Status**: `MultiReverseDirectRelation` properties are currently **skipped** during UDTF registration.

**Problem**: The Databricks Unity Catalog API rejects all array `type_json` formats we've attempted. The `type_json` field is required, but no valid format has been found that the API accepts.

**Attempted Formats** (all rejected):
1. Full nested format: `{"name":"...","type":"array","elementType":{"type":"string"},"containsNull":true,"nullable":true,"metadata":{}}`
2. Without containsNull: `{"name":"...","type":"array","elementType":{"type":"string"},"nullable":true,"metadata":{}}`
3. Simple format: `{"name":"...","type":"array","nullable":true,"metadata":{}}` - Error: `Failed to convert the JSON string 'array' to a data type`

**Next Steps**:
- Investigate how other Databricks functions handle array types
- Check if arrays require a different `FunctionParameterInfo` structure
- Consider if `type_json` can be omitted for arrays (currently required by API)
- Test with different array element types (not just STRING)

**Implementation Example:**

```python
# Single relationship (DirectRelation or SingleReverseDirectRelation)
FunctionParameterInfo(
    name="user_id",
    type_text="STRING",
    type_name=ColumnTypeName.STRING,
    type_json='{"name":"user_id","type":"string","nullable":true,"metadata":{}}',
    position=5,
    parameter_mode=FunctionParameterMode.IN,
    parameter_type=FunctionParameterType.PARAM,
)

# Multi relationship (MultiReverseDirectRelation) - TODO: Currently skipped
# All attempted type_json formats are rejected by the API
# This property would be skipped during UDTF registration
```

**Return Type DDL Example:**

```sql
-- Single relationship
TABLE(id INT, name STRING, user_id STRING, ...)

-- Multi relationship - TODO: Currently skipped, would be:
-- TABLE(id INT, name STRING, related_users ARRAY<STRING>, ...)
```

**Foreign Key Constraints:**

- **Single relationships**: Informational foreign key constraints can be added to document the relationship:
  ```sql
  ALTER VIEW catalog.schema.MyView 
  ADD CONSTRAINT fk_MyView_user_id 
  FOREIGN KEY (user_id) REFERENCES catalog.schema.Users(external_id) NOT ENFORCED
  ```

- **Multi relationships**: Currently skipped (see TODO section below). When support is added, foreign key constraints will not be supported for array columns in SQL, so FK constraints would be skipped for `MultiReverseDirectRelation` properties.

**Detection Logic:**

The code detects relationship properties by:
1. Checking if the property has a `.type` attribute:
   - If `hasattr(prop, 'type')` is `False`, it's a reverse relation (`MultiReverseDirectRelation` or `SingleReverseDirectRelation`)
   - Check `type(prop).__name__` to distinguish between `MultiReverseDirectRelation` (array) and `SingleReverseDirectRelation` (string)
2. If `hasattr(prop, 'type')` is `True`, check if `prop.type` is an instance of `dm.DirectRelation`

#### **4.2.4 Return Type Specification for Python UDTFs**

**Critical: Python UDTFs REQUIRE both `full_data_type` and `return_params`**

For Python UDTFs (EXTERNAL functions) registered via the Unity Catalog API as **Table Functions** (`TABLE_TYPE`), the API's validation logic requires both a DDL string representation and structured column metadata.

1.  **`full_data_type`**: Must contain the SQL-friendly DDL string (e.g., `"TABLE(id INT, name STRING)"`).
2.  **`return_params`**: Must contain a structured list of `FunctionParameterInfo` objects, one for each output column defined in the DDL string.

**Required Fields for Python UDTFs:**

| Field | Value | Description |
|-------|-------|-------------|
| `data_type` | `ColumnTypeName.TABLE_TYPE` | Indicates table-returning function |
| `full_data_type` | `"TABLE(col1 TYPE, col2 TYPE, ...)"` | DDL string defining output schema |
| `return_params` | `FunctionParameterInfos` | **Required**: Structured metadata for each output column |
| `routine_body` | `CreateFunctionRoutineBody.EXTERNAL` | Python code is external |
| `external_language` | `"PYTHON"` | Language identifier |

**Example:**

```python
from databricks.sdk.service.catalog import (
    CreateFunction, 
    ColumnTypeName, 
    CreateFunctionRoutineBody,
    CreateFunctionParameterStyle,
    CreateFunctionSqlDataAccess,
    CreateFunctionSecurityType,
    FunctionParameterInfos,
    FunctionParameterInfo
)

# Define output columns as structured metadata
return_columns = [
    FunctionParameterInfo(
        name="id", 
        type_text="INT", 
        type_name=ColumnTypeName.INT, 
        type_json='{"name":"id","type":"long","nullable":true,"metadata":{}}',
        position=0
    ),
    FunctionParameterInfo(
        name="name", 
        type_text="STRING", 
        type_name=ColumnTypeName.STRING, 
        type_json='{"name":"name","type":"string","nullable":true,"metadata":{}}',
        position=1
    )
]

create_function = CreateFunction(
    name=function_name,
    catalog_name=catalog,
    schema_name=schema,
    input_params=input_params_wrapped,
    # MUST be fully populated with structured metadata matching the DDL string
    return_params=FunctionParameterInfos(parameters=return_columns),
    data_type=ColumnTypeName.TABLE_TYPE,
    full_data_type="TABLE(id INT, name STRING)",  # Required!
    routine_body=CreateFunctionRoutineBody.EXTERNAL,
    routine_definition=udtf_code,
    external_language="PYTHON",
    is_deterministic=False,
    # These fields are required by some SDK version constructors:
    parameter_style=CreateFunctionParameterStyle.S,
    sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
    is_null_call=False,
    security_type=CreateFunctionSecurityType.DEFINER,
    specific_name=function_name,
    comment=comment,
)
```

#### **4.2.5 PySpark-Based Type Conversion and Schema Validation**

**Design Philosophy: PySpark as Source of Truth**

All type conversions in the codebase now use **PySpark DataTypes as the single source of truth**. This ensures consistency between:
- Generated UDTF code (`outputSchema()` method)
- Unity Catalog registration (`full_data_type` and `return_params`)
- Schema validation and comparison

**Benefits:**
1. **Single Source of Truth**: PySpark types drive all conversions, eliminating inconsistencies
2. **Consistency**: Registration schema matches generated UDTF code exactly
3. **Validation**: Automatic schema validation during registration
4. **Maintainability**: Centralized type conversion logic reduces duplication
5. **Proven Patterns**: Uses the same types as Spark/UDTF code, following established patterns

**Type Conversion Flow:**

```
CDF Property Type → PySpark DataType → SQL DDL / type_json / ColumnTypeName
```

**TypeConverter Utility Class**

A unified `TypeConverter` utility class (`cognite.pygen_spark.type_converter`) provides centralized type conversion. This class was moved from `cognite.databricks` to `cognite.pygen_spark` to make it available for any Spark cluster, not just Databricks. It is re-exported from `cognite.databricks` for backward compatibility.

```python
from cognite.pygen_spark import TypeConverter  # Preferred: direct from source
# or
from cognite.databricks import TypeConverter  # Backward compatible: re-exported
import cognite.client.data_modeling as dm
from pyspark.sql.types import StringType, ArrayType

# CDF property type → PySpark DataType
spark_type = TypeConverter.cdf_to_spark(dm.Text(), is_array=True)
# Returns: ArrayType(StringType(), containsNull=True)

# PySpark DataType → SQL DDL string
sql_ddl = TypeConverter.spark_to_sql_ddl(spark_type)
# Returns: "ARRAY<STRING>"

# PySpark DataType → SQL type info
sql_type, column_type_name = TypeConverter.spark_to_sql_type_info(spark_type)
# Returns: ("ARRAY<STRING>", ColumnTypeName.STRING)

# PySpark DataType → StructField JSON
type_json = TypeConverter.spark_to_type_json(spark_type, name="tags", nullable=True)
# Returns: '{"name":"tags","type":"array","elementType":{"type":"string"},"containsNull":true,"nullable":true,"metadata":{}}'

# PySpark StructType → SQL DDL
from pyspark.sql.types import StructType, StructField
struct_type = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("tags", ArrayType(StringType()), nullable=True)
])
ddl = TypeConverter.struct_type_to_ddl(struct_type)
# Returns: "TABLE(name STRING, tags ARRAY<STRING>)"
```

**Schema Building and Validation**

The `UDTFGenerator` class uses PySpark `StructType` to build schemas that match the generated UDTF code:

```python
# Build output schema (same as generated UDTF's outputSchema())
expected_schema = generator._build_output_schema(view_id)
# Returns: StructType([StructField("name", StringType()), ...])

# Convert to SQL DDL
return_type = generator._parse_return_type(view_id)
# Uses: _struct_type_to_ddl(expected_schema)

# Parse return parameters
return_params = generator._parse_return_params(view_id)
# Uses: expected_schema.fields → FunctionParameterInfo objects
```

**Schema Validation Methods**

Automatic schema validation ensures registered schemas match generated code:

```python
# Validate schema consistency (automatic when debug=True)
is_consistent, error_msg = generator.validate_schema_consistency(
    view_id="SmallBoat",
    registered_function=function_info
)

# Extract schema from registered function
registered_schema = generator._extract_schema_from_function(function_info)
# Parses return_params or full_data_type → StructType

# Compare schemas
is_match, error_msg = generator._schemas_match(expected_schema, registered_schema)
```

**Schema Validation Flow:**

1. **Build Expected Schema**: Uses `_build_output_schema()` which:
   - Uses `UDTFField.from_property()` to match generated code logic
   - Builds PySpark `StructType` with same fields as `outputSchema()`
   - Handles skipped properties (e.g., `MultiReverseDirectRelation`) consistently

2. **Extract Registered Schema**: Uses `_extract_schema_from_function()` which:
   - Parses `return_params.type_json` back to PySpark DataTypes
   - Falls back to parsing `full_data_type` DDL string if needed
   - Returns `StructType` for comparison

3. **Compare Schemas**: Uses `_schemas_match()` which:
   - Compares field names, types, and nullability
   - Uses PySpark's built-in type equality
   - Returns detailed error messages for mismatches

**UDTFField Improvements**

The `UDTFField` class in `pygen-spark` now includes:

```python
from cognite.pygen_spark.fields import UDTFField

# String representation (for code generation)
spark_type_str = UDTFField._get_spark_type(prop)
# Returns: "StringType()" or "ArrayType(StringType())"

# PySpark DataType object (for validation)
spark_type_obj = UDTFField._get_spark_type_object(prop)
# Returns: StringType() or ArrayType(StringType(), containsNull=True)
```

**Deprecated Methods**

The following method is deprecated in favor of PySpark-based conversion:

- `_property_type_to_sql_type()`: **DEPRECATED**
  - **Replacement**: Use `_property_type_to_spark_type()` + `_spark_type_to_sql_type_info()`
  - **Reason**: PySpark types provide a single source of truth and better consistency

**Example: Complete Type Conversion Flow**

```python
# 1. Get property type from view
view = data_model.views[0]
prop = view.properties["name"]
property_type, is_relationship, is_multi = generator._get_property_type(prop)

# 2. Convert to PySpark DataType
spark_type = generator._property_type_to_spark_type(property_type, is_array=is_multi)
# Returns: StringType() or ArrayType(StringType(), containsNull=True)

# 3. Generate type_json using PySpark StructField
type_json = generator._generate_type_json("name", spark_type, nullable=True)
# Uses: StructField("name", spark_type, nullable=True).json()

# 4. Convert to SQL type info
sql_type, type_name = generator._spark_type_to_sql_type_info(spark_type)
# Returns: ("STRING", ColumnTypeName.STRING) or ("ARRAY<STRING>", ColumnTypeName.STRING)

# 5. Build complete schema
struct_type = generator._build_output_schema(view.external_id)
ddl = generator._struct_type_to_ddl(struct_type)
# Returns: "TABLE(name STRING, tags ARRAY<STRING>, ...)"
```

**Integration with Registration**

Schema validation is automatically performed during registration when `debug=True`:

```python
registered_functions = generator.register_udtfs_and_views(
    secret_scope="cdf_scope",
    if_exists="replace",
    debug=True,  # Enables schema validation
)

# Output:
# [DEBUG] Schema validation passed for SmallBoat
# [DEBUG] Schema validation passed for Cognite360Image
```

**Type Conversion Methods Reference**

| Method | Input | Output | Purpose |
|--------|-------|--------|---------|
| `TypeConverter.cdf_to_spark()` | CDF property type, `is_array` | PySpark `DataType` | Convert CDF types to PySpark |
| `TypeConverter.spark_to_sql_ddl()` | PySpark `DataType` | SQL DDL string | Generate SQL type string |
| `TypeConverter.spark_to_sql_type_info()` | PySpark `DataType` | `(sql_type, ColumnTypeName)` | Get SQL type and enum |
| `TypeConverter.spark_to_type_json()` | PySpark `DataType`, name, nullable | JSON string | Generate StructField JSON |
| `TypeConverter.struct_type_to_ddl()` | PySpark `StructType` | SQL DDL string | Convert schema to DDL |
| `_build_output_schema()` | View ID | PySpark `StructType` | Build expected schema |
| `_extract_schema_from_function()` | `FunctionInfo` | PySpark `StructType` | Extract registered schema |
| `_schemas_match()` | Two `StructType` objects | `(bool, str\|None)` | Compare schemas |
| `validate_schema_consistency()` | View ID, `FunctionInfo` | `(bool, str\|None)` | Validate registered vs expected |

#### **4.2.6 Pydantic Models and API Design Principles**

**Design Philosophy: Type-Safe Return Types**

Following the architectural alignment with `pygen-main` (see Section 4.1.4), the codebase uses **Pydantic models** instead of dictionaries for all return types. This ensures:

1. **Type Safety**: Pydantic validates data at runtime, catching errors early
2. **Better IDE Support**: Full autocomplete and type hints for all fields
3. **Self-Documenting**: Model fields serve as inline documentation
4. **Consistency**: Aligns with `pygen-main`'s dataclass pattern
5. **Extensibility**: Easy to add fields or methods without breaking changes
6. **Backward Compatibility**: Dict-like access via properties and `__getitem__()`

**Pydantic Models in `cognite-databricks`:**

```python
from cognite.databricks.models import (
    RegisteredUDTFResult,
    UDTFRegistrationResult,
)

# RegisteredUDTFResult: Result for a single registered UDTF and view
@dataclass(frozen=True)
class RegisteredUDTFResult(BaseModel):
    """Result of registering a single UDTF and its view."""
    view_id: str
    function_info: FunctionInfo
    view_name: str | None = None
    udtf_file_path: Path | None = None
    view_registered: bool = False

# UDTFRegistrationResult: Complete registration result
@dataclass(frozen=True)
class UDTFRegistrationResult(BaseModel):
    """Complete result of UDTF and view registration."""
    registered_udtfs: list[RegisteredUDTFResult] = Field(default_factory=list)
    catalog: str
    schema: str
    total_count: int = 0
    
    @property
    def by_view_id(self) -> dict[str, RegisteredUDTFResult]:
        """Convenience property for dict-like access (backward compatibility)."""
        return {r.view_id: r for r in self.registered_udtfs}
    
    def get(self, view_id: str) -> RegisteredUDTFResult | None:
        """Get result for a specific view_id."""
        return self.by_view_id.get(view_id)
```

**Pydantic Models in `cognite-pygen-spark`:**

```python
from cognite.pygen_spark.models import (
    UDTFGenerationResult,
    ViewSQLGenerationResult,
)

# UDTFGenerationResult: Result of UDTF code generation
@dataclass(frozen=True)
class UDTFGenerationResult(BaseModel):
    """Result of UDTF code generation."""
    generated_files: dict[str, Path] = Field(default_factory=dict)
    output_dir: Path
    total_count: int = 0
    
    @property
    def file_paths(self) -> list[Path]:
        """List of all generated file paths."""
        return list(self.generated_files.values())
    
    def get_file(self, view_id: str) -> Path | None:
        """Get file path for a specific view_id."""
        return self.generated_files.get(view_id)

# ViewSQLGenerationResult: Result of View SQL generation
@dataclass(frozen=True)
class ViewSQLGenerationResult(BaseModel):
    """Result of View SQL generation."""
    view_sqls: dict[str, str] = Field(default_factory=dict)
    total_count: int = 0
    
    def get_sql(self, view_id: str) -> str | None:
        """Get SQL for a specific view_id."""
        return self.view_sqls.get(view_id)
```

**Updated Method Signatures:**

**Before (Dictionary-based):**
```python
def register_udtfs_and_views(...) -> dict[str, FunctionInfo]:
    """Returns dict mapping view_id to FunctionInfo."""
    registered_functions: dict[str, FunctionInfo] = {}
    # ...
    return registered_functions

def generate_udtfs(...) -> dict[str, Path]:
    """Returns dict mapping view_id to file path."""
    generated_files: dict[str, Path] = {}
    # ...
    return generated_files
```

**After (Pydantic-based):**
```python
def register_udtfs_and_views(...) -> UDTFRegistrationResult:
    """Returns structured result with type-safe access."""
    registered_udtfs: list[RegisteredUDTFResult] = []
    # ...
    return UDTFRegistrationResult(
        registered_udtfs=registered_udtfs,
        catalog=self.catalog,
        schema=self.schema,
        total_count=len(registered_udtfs),
    )

def generate_udtfs(...) -> UDTFGenerationResult:
    """Returns structured result with type-safe access."""
    generated_files: dict[str, Path] = {}
    # ...
    return UDTFGenerationResult(
        generated_files=generated_files,
        output_dir=self.output_dir,
        total_count=len(generated_files),
    )
```

**Usage Examples:**

**Accessing Results (Type-Safe):**
```python
# Register UDTFs and views
result = generator.register_udtfs_and_views(
    data_model=data_model_id,
    secret_scope="cdf_my_scope",
)

# Type-safe access to structured data
print(f"Registered {result.total_count} UDTFs in {result.catalog}.{result.schema}")

# Access individual results
for udtf_result in result.registered_udtfs:
    print(f"View: {udtf_result.view_id}")
    print(f"Function: {udtf_result.function_info.name}")
    print(f"View registered: {udtf_result.view_registered}")

# Dict-like access (backward compatibility)
smallboat_result = result['SmallBoat']  # or result.get('SmallBoat')
print(f"Function info: {smallboat_result.function_info}")
```

**Code Generation Results:**
```python
# Generate UDTF code
udtf_result = code_generator.generate_udtfs(data_model)

# Type-safe access
print(f"Generated {udtf_result.total_count} files in {udtf_result.output_dir}")

# Access individual files
for view_id, file_path in udtf_result.generated_files.items():
    print(f"{view_id}: {file_path}")

# Or use convenience methods
smallboat_file = udtf_result.get_file('SmallBoat')
if smallboat_file:
    print(f"SmallBoat UDTF: {smallboat_file}")
```

**Benefits of Pydantic Models:**

1. **Runtime Validation**: Pydantic automatically validates data types and constraints
2. **IDE Autocomplete**: Full IntelliSense support for all fields and methods
3. **Type Hints**: Static type checkers (mypy, pyright) can verify correctness
4. **Documentation**: Model fields serve as self-documenting API contracts
5. **Evolution**: Easy to add new fields without breaking existing code
6. **Serialization**: Built-in JSON serialization for logging/debugging
7. **Backward Compatibility**: Dict-like access via `by_view_id` property and `__getitem__()`

**What Stays as Dictionaries:**

The following remain as dictionaries (by design):
- **Template variables**: Jinja2 templates require dictionaries
- **Internal temporary mappings**: Fine for internal use
- **`**kwargs`**: Python convention for flexible parameter passing
- **CDF API filters**: Match the CDF SDK's expected format

**Python UDTF Code Structure:**

The Python code must contain a class with an `eval()` method that uses `yield` to return rows:

```python
class MyUDTF:
    def eval(self, param1: str, param2: int):
        # Process and yield rows
        yield value1, value2, value3
        yield value4, value5, value6
```

**Common Mistake:**

❌ Setting `full_data_type=None` will cause `InvalidParameterValue: Missing required field: function_info.full_data_type`

✅ Always provide `full_data_type` with the DDL string format: `"TABLE(col1 TYPE, col2 TYPE, ...)"`

**SecretManagerHelper**

```python
from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import SecretScope

class SecretManagerHelper:
    """Helper for managing Databricks Secret Manager scopes and secrets."""

    def __init__(self, workspace_client: WorkspaceClient) -> None:
        self.workspace_client = workspace_client

    def create_scope_if_not_exists(self, scope_name: str) -> SecretScope:
        """Create a Secret Manager scope if it doesn't exist.

        Args:
            scope_name: Name of the scope to create

        Returns:
            The created or existing SecretScope
        """
        # Check if scope exists by listing all scopes
        existing_scopes = list(self.workspace_client.secrets.list_scopes())
        for scope in existing_scopes:
            if scope.name == scope_name:
                return scope
        
        # Scope doesn't exist, create it (scope is positional parameter)
        self.workspace_client.secrets.create_scope(scope_name)
        
        # Return the newly created scope (need to fetch it)
        # Note: create_scope doesn't return the scope, so we list again
        scopes = list(self.workspace_client.secrets.list_scopes())
        for scope in scopes:
            if scope.name == scope_name:
                return scope
        raise RuntimeError(f"Failed to create scope: {scope_name}")

    def set_cdf_credentials(
        self,
        scope_name: str,
        project: str,
        cdf_cluster: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ) -> None:
        """Store CDF credentials in Secret Manager.

        These credentials typically come from the TOML file (same format as used by
        load_cognite_client_from_toml()). The values are stored in Secret Manager
        and referenced in View SQL via SECRET() function.

        Args:
            scope_name: Secret Manager scope name
            project: CDF project name (from TOML: [cognite].project)
            cdf_cluster: CDF cluster URL (from TOML: [cognite].cdf_cluster)
            client_id: OAuth2 client ID (from TOML: [cognite].client_id)
            client_secret: OAuth2 client secret (from TOML: [cognite].client_secret)
            tenant_id: Azure AD tenant ID (from TOML: [cognite].tenant_id)
        """
        self.create_scope_if_not_exists(scope_name)

        secrets = {
            "project": project,
            "cdf_cluster": cdf_cluster,
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        }

        for key, value in secrets.items():
            # put_secret signature: put_secret(scope: str, key: str, *, string_value: str)
            # scope and key are positional parameters
            self.workspace_client.secrets.put_secret(
                scope_name,  # positional
                key,  # positional
                string_value=value,  # keyword
            )
```

#### **4.2.7 Helper Functions for SQL Query Generation and Notebook Code**

**generate_udtf_sql_query** (SQL Query Generator)

Generates clean SQL queries for UDTFs using named parameters, avoiding the need for dozens of positional NULL values.

```python
def generate_udtf_sql_query(
    catalog: str,
    schema: str,
    function_name: str,
    secret_scope: str,
    view_properties: list[str] | None = None,
    use_named_parameters: bool = True,
    limit: int = 10,
) -> str:
    """Generate SQL query for a UDTF with optional named parameters.
    
    When use_named_parameters=True (default), generates clean SQL with only credentials:
    
    SELECT * FROM catalog.schema.function_name(
        client_id     => SECRET('scope', 'client_id'),
        client_secret => SECRET('scope', 'client_secret'),
        tenant_id     => SECRET('scope', 'tenant_id'),
        cdf_cluster   => SECRET('scope', 'cdf_cluster'),
        project       => SECRET('scope', 'project')
    ) LIMIT 10;
    
    When use_named_parameters=False, generates positional parameters with NULLs.
    """
```

**generate_session_scoped_notebook_code** (Notebook Code Generator)

Generates notebook-ready code snippets for session-scoped UDTF registration, including:
- Cell 1: Dependency installation (`%pip install cognite-sdk`)
- Cell 2: UDTF registration code
- Cell 3: SQL query example with named parameters (without catalog/schema prefix)

**Important**: The generated SQL queries use function names without catalog/schema prefixes (e.g., `small_boat_udtf` instead of `catalog.schema.small_boat_udtf`), since session-scoped UDTFs are registered directly in the Spark session, not in Unity Catalog.

```python
def generate_session_scoped_notebook_code(
    generator: UDTFGenerator,
    secret_scope: str | None = None,
    data_model: DataModel | None = None,
    view_id: str | None = None,
) -> dict[str, str]:
    """Generate notebook-ready code snippets for session-scoped UDTF registration.
    
    The generated SQL queries are formatted for session-scoped UDTFs, which means:
    - No catalog/schema prefix (e.g., `small_boat_udtf` not `catalog.schema.small_boat_udtf`)
    - Function names use consistent snake_case conversion via `to_udtf_function_name()`
    - Named parameters are used by default for clean SQL syntax
    
    Returns:
        Dictionary with keys:
        - "cell1_dependencies": Code for installing dependencies
        - "cell2_registration": Code for registering UDTFs
        - "cell3_sql_example": Example SQL query (using named parameters, no catalog/schema prefix)
        - "all_cells": Combined code for all cells
    
    Example output for Cell 3:
        SELECT * FROM small_boat_udtf(
            client_id     => SECRET('scope', 'client_id'),
            client_secret => SECRET('scope', 'client_secret'),
            ...
        ) LIMIT 10;
    """
```

**generate_udtf_notebook** (Helper Function - Aligned with pygen)

This function mirrors pygen's `generate_sdk_notebook` pattern, providing a convenient notebook-friendly API for generating UDTFs. It follows the same conceptual flow as described in the [Pygen Quickstart Documentation](https://cognite-pygen.readthedocs-hosted.com/en/latest/quickstart/notebook.html).

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

from cognite.client import CogniteClient, data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from databricks.sdk import WorkspaceClient

from cognite.databricks.udtf_registry import UDTFRegistry
from cognite.pygen_spark import SparkUDTFGenerator

if TYPE_CHECKING:
    from cognite.databricks import UDTFGenerator

# Define DataModel type alias (same as pygen)
# Short-term: We define our own type alias using public types from cognite.client
# This avoids depending on pygen's private API (_generator module)
# Long-term: Once pygen exports DataModel in __init__.py, we can use:
#   from cognite.pygen import DataModel
DataModel = Union[DataModelIdentifier, dm.DataModel[dm.View]]

def generate_udtf_notebook(
    data_model: DataModel,
    client: CogniteClient,
    workspace_client: WorkspaceClient | None = None,
    catalog: str = "main",
    schema: str | None = None,
    output_dir: Path | str | None = None,
) -> UDTFGenerator:
    """Generate UDTFs for a Data Model in a notebook (aligned with pygen's generate_sdk_notebook).

    This function:
    1. Downloads the data model
    2. Generates UDTF code
    3. Creates a UDTFGenerator instance for registration

    Args:
        data_model: DataModel identifier (DataModelId or DataModel object)
        client: CogniteClient instance (can be created via load_cognite_client_from_toml)
        workspace_client: Optional WorkspaceClient (if None, will need to be set later before registration).
                         In Databricks notebooks, WorkspaceClient() auto-detects credentials.
        catalog: Unity Catalog catalog name (default: "main")
        schema: Unity Catalog schema name. If None, auto-generates from data model:
                {space}_{external_id.lower()}_{version} (matches folder pattern)
        output_dir: Optional output directory path. If None, uses /local_disk0/tmp/pygen_udtf/{folder_name}.
                   Can be a string or Path object.

    Returns:
        UDTFGenerator instance ready for registration

    Example:
        from cognite.databricks import generate_udtf_notebook
        from cognite.pygen import load_cognite_client_from_toml
        from cognite.client.data_classes.data_modeling.ids import DataModelId
        from databricks.sdk import WorkspaceClient

        client = load_cognite_client_from_toml("config.toml")
        
        # Option 1 (Recommended): Pass workspace_client during generation
        workspace_client = WorkspaceClient()  # Auto-detects credentials in Databricks
        data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
        generator = generate_udtf_notebook(
            data_model_id,
            client,
            workspace_client=workspace_client,  # Pass it here for immediate registration capability
            catalog="main",  # Unity Catalog catalog name
            # schema=None,  # Auto-generated: "sp_pygen_power_windturbine_1"
        )
        
        # Option 2: Set workspace_client after generation (if not passed above)
        # workspace_client = WorkspaceClient()
        # generator.workspace_client = workspace_client
        # generator.udtf_registry = UDTFRegistry(workspace_client)
        # generator.secret_helper = SecretManagerHelper(workspace_client)
        
        # Use data model-specific scope: cdf_{space}_{external_id}
        secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
        generator.register_udtfs_and_views(secret_scope=secret_scope)
    """
    # Create temporary output directory (similar to pygen's generate_sdk_notebook)
    # Extract identifier for folder name and schema name
    # Note: DataModelIdentifier supports tuples, but examples use DataModelId for clarity
    if isinstance(data_model, dm.DataModel):
        model_id = data_model.as_id()
        folder_name = f"{model_id.space}_{model_id.external_id}_{model_id.version}"
        # Auto-generate schema name if not provided (matches folder pattern but lowercase external_id)
        if schema is None:
            schema = f"{model_id.space}_{model_id.external_id.lower()}_{model_id.version}"
    elif isinstance(data_model, tuple):
        # Backward compatibility: DataModelIdentifier supports tuples
        folder_name = f"{data_model[0]}_{data_model[1]}_{data_model[2]}"
        if schema is None:
            schema = f"{data_model[0]}_{data_model[1].lower()}_{data_model[2]}"
    else:
        # DataModelId (preferred in examples)
        folder_name = f"{data_model.space}_{data_model.external_id}_{data_model.version}"
        if schema is None:
            schema = f"{data_model.space}_{data_model.external_id.lower()}_{data_model.version}"

    # Use provided output_dir or default to /local_disk0/tmp/pygen_udtf (Databricks writable location)
    if output_dir is None:
        output_dir = Path("/local_disk0/tmp/pygen_udtf") / folder_name
    else:
        output_dir = Path(output_dir) / folder_name
    
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create code generator - pass data_model here so it's loaded in __init__
    code_generator = SparkUDTFGenerator(
        client=client,
        output_dir=output_dir,
        data_model=data_model,  # Pass data_model here
        top_level_package="cognite_databricks",
    )

    # Generate UDTF files to disk
    # Note: __init__ only prepares the generator; generate_udtfs() actually writes files
    code_generator.generate_udtfs()

    # Create UDTFGenerator for registration
    from cognite.databricks import UDTFGenerator

    return UDTFGenerator(
        workspace_client=workspace_client,
        cognite_client=client,
        catalog=catalog,
        schema=schema,
        code_generator=code_generator,
    )
```

**UDTFGenerator** (High-level API)

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

from cognite.client import CogniteClient, data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo

from cognite.databricks.secret_manager import SecretManagerHelper
from cognite.databricks.udtf_registry import UDTFRegistry
from cognite.pygen_spark import SparkUDTFGenerator

if TYPE_CHECKING:
    pass

# Define DataModel type alias (same as pygen)
# Short-term: We define our own type alias using public types from cognite.client
# This avoids depending on pygen's private API (_generator module)
# Long-term: Once pygen exports DataModel in __init__.py, we can use:
#   from cognite.pygen import DataModel
DataModel = Union[DataModelIdentifier, dm.DataModel[dm.View]]

class UDTFGenerator:
    """High-level API for generating and registering UDTFs and Views."""

    def __init__(
        self,
        workspace_client: WorkspaceClient | None = None,
        cognite_client: CogniteClient | None = None,
        catalog: str = "main",
        schema: str | None = None,
        code_generator: SparkUDTFGenerator | None = None,
    ) -> None:
        """Initialize UDTF generator.

        Args:
            workspace_client: Optional Databricks WorkspaceClient
            cognite_client: Optional CogniteClient instance
            catalog: Unity Catalog catalog name
            schema: Unity Catalog schema name. Should be provided or auto-generated from data_model.
            code_generator: Optional pre-configured SparkUDTFGenerator
        """
        self.workspace_client = workspace_client
        self.cognite_client = cognite_client
        self.catalog = catalog
        if schema is None:
            raise ValueError("schema must be provided or auto-generated from data_model")
        self.schema = schema

        if workspace_client:
            self.udtf_registry = UDTFRegistry(workspace_client)
            self.secret_helper = SecretManagerHelper(workspace_client)
        else:
            self.udtf_registry = None
            self.secret_helper = None

        self.code_generator = code_generator or SparkUDTFGenerator(
            client=cognite_client,
            output_dir=Path("./temp_udtfs"),  # Temporary, for code generation
            top_level_package="cognite_databricks",
        )

    def register_udtfs_and_views(
        self,
        data_model: DataModel | None = None,
        secret_scope: str | None = None,
        dependencies: list[str] | None = None,
    ) -> UDTFRegistrationResult:
        """Generate and register UDTFs and Views for a Data Model.

        Args:
            data_model: Optional DataModel identifier (DataModelId or DataModel object).
                       If None, uses the data model from code_generator initialization.
            secret_scope: Secret Manager scope name. If None, auto-generates from data model:
                         `cdf_{space}_{external_id}` (e.g., "cdf_sp_pygen_power_windturbine")
            dependencies: Optional Python package dependencies (DBR 18.1+)
                Example: ["cognite-sdk>=7.90.1", "cognite-pygen>=1.2.29"]

        Returns:
            UDTFRegistrationResult with structured information about registered UDTFs and views.
            Access individual results via result['view_id'] or result.get('view_id').
        """
        if not self.workspace_client:
            raise ValueError("WorkspaceClient must be set before registration")

        # Ensure catalog and schema exist before registering functions
        self._ensure_schema_exists()

        # Auto-generate scope name from data model if not provided
        if secret_scope is None:
            if data_model:
                if isinstance(data_model, dm.DataModel):
                    model_id = data_model.as_id()
                else:
                    model_id = data_model
                secret_scope = f"cdf_{model_id.space}_{model_id.external_id.lower()}"
            else:
                raise ValueError("secret_scope must be provided if data_model is None")

        # If data_model not provided, use the one from code_generator initialization
        if data_model:
            # Generate UDTF code (will use the provided data_model)
            udtf_result = self.code_generator.generate_udtfs(data_model)
            udtf_files = udtf_result.generated_files

            # Generate View SQL
            view_sql_result = self.code_generator.generate_views(
                data_model=data_model,
                secret_scope=secret_scope,
            )
            view_sqls = view_sql_result.view_sqls
        else:
            # Use the data model from code_generator initialization
            # UDTF code was already generated in __init__, just get the files
            udtf_files = self._find_generated_udtf_files()
            
            # Generate View SQL using the data model from code_generator
            view_sql_result = self.code_generator.generate_views(
                data_model=None,  # Use the one from __init__
                secret_scope=secret_scope,
            )
            view_sqls = view_sql_result.view_sqls

        registered_udtfs: list[RegisteredUDTFResult] = []

        # Register each UDTF
        for view_id, udtf_file in udtf_files.items():
            udtf_code = udtf_file.read_text()

            # Extract parameters from view properties (like pygen-main does)
            # This is more aligned with pygen-main's approach than parsing generated code
            input_params = self._parse_udtf_params(view_id)
            return_type = self._parse_return_type(view_id)

            function_info = self.udtf_registry.register_udtf(
                catalog=self.catalog,
                schema=self.schema,
                function_name=f"{view_id}_udtf",
                udtf_code=udtf_code,
                input_params=input_params,
                return_type=return_type,
                dependencies=dependencies,  # DBR 18.1+ custom dependencies
                comment=f"UDTF for {view_id} from CDF Data Model",
            )

            # Track view registration status
            view_registered = False
            view_name = None

            # Register Views on top of UDTFs
            if view_id in view_sqls:
                try:
                    self.udtf_registry.register_view(
                        catalog=self.catalog,
                        schema=self.schema,
                        view_name=view_id,
                        view_sql=view_sqls[view_id],
                    )
                    view_registered = True
                    view_name = f"{self.catalog}.{self.schema}.{view_id}"
                except Exception:
                    pass  # View registration failed, but continue

            # Create result object
            registered_udtfs.append(
                RegisteredUDTFResult(
                    view_id=view_id,
                    function_info=function_info,
                    view_name=view_name,
                    udtf_file_path=udtf_file,
                    view_registered=view_registered,
                )
            )

        return UDTFRegistrationResult(
            registered_udtfs=registered_udtfs,
            catalog=self.catalog,
            schema=self.schema,
            total_count=len(registered_udtfs),
        )

    def _ensure_catalog_exists(self) -> None:
        """Create catalog if it doesn't exist.
        
        Checks if the catalog exists, and creates it if it doesn't.
        """
        if not self.workspace_client:
            return
        
        try:
            # Try to get the catalog - if it exists, we're done
            self.workspace_client.catalogs.get(self.catalog)
        except Exception:
            # Catalog doesn't exist, create it
            try:
                self.workspace_client.catalogs.create(
                    name=self.catalog,
                    comment=f"Catalog for CDF Data Model UDTFs and Views",
                )
            except Exception as e:
                # If creation fails, it might be a permission issue or catalog already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.catalogs.get(self.catalog)
                except Exception:
                    # Re-raise the original creation error
                    raise RuntimeError(
                        f"Failed to create catalog '{self.catalog}'. "
                        f"Ensure you have CREATE_CATALOG permission. "
                        f"Original error: {e}"
                    ) from e

    def _ensure_schema_exists(self) -> None:
        """Create schema if it doesn't exist.
        
        Checks if the schema exists in the catalog, and creates it if it doesn't.
        """
        if not self.workspace_client:
            return
        
        # First ensure the catalog exists
        self._ensure_catalog_exists()
        
        try:
            # Try to get the schema - if it exists, we're done
            full_name = f"{self.catalog}.{self.schema}"
            self.workspace_client.schemas.get(full_name)
        except Exception:
            # Schema doesn't exist, create it
            try:
                self.workspace_client.schemas.create(
                    name=self.schema,
                    catalog_name=self.catalog,
                    comment=f"Schema for CDF Data Model UDTFs and Views",
                )
            except Exception as e:
                # If creation fails, it might be a permission issue or schema already exists
                # Try to get it again in case it was created concurrently
                try:
                    self.workspace_client.schemas.get(full_name)
                except Exception:
                    # Re-raise the original creation error
                    raise RuntimeError(
                        f"Failed to create schema {full_name}. "
                        f"Ensure you have CREATE_SCHEMA permission on catalog '{self.catalog}'. "
                        f"Original error: {e}"
                    ) from e

    def _find_generated_udtf_files(self) -> dict[str, Path]:
        """Find generated UDTF files in output directory.
        
        Returns:
            Dict mapping view external_id to Path of UDTF file
        """
        if not self.code_generator:
            raise ValueError("code_generator must be set to find generated files")
        
        # Files are stored in: output_dir / top_level_package / "{view_external_id}_udtf.py"
        udtf_dir = self.code_generator.output_dir / self.code_generator.top_level_package
        
        if not udtf_dir.exists():
            raise FileNotFoundError(
                f"UDTF directory not found: {udtf_dir}. "
                "Make sure generate_udtfs() was called first."
            )
        
        # Find all *_udtf.py files
        udtf_files: dict[str, Path] = {}
        for file_path in udtf_dir.glob("*_udtf.py"):
            # Extract view external_id from filename: "{view_external_id}_udtf.py"
            view_id = file_path.stem.replace("_udtf", "")
            udtf_files[view_id] = file_path
        
        if not udtf_files:
            raise FileNotFoundError(
                f"No UDTF files found in {udtf_dir}. "
                "Make sure generate_udtfs() was called first."
            )
        
        return udtf_files

    def _parse_udtf_params(self, view_id: str, debug: bool = False) -> list[FunctionParameterInfo]:
        """Parse UDTF function parameters from view properties (aligned with pygen-main approach).
        
        Instead of parsing generated code, we extract parameters from view.properties,
        similar to how pygen-main creates FilterParameter from Field objects.
        
        IMPORTANT: Parameters include both secret credentials (first 5 params) and
        view properties (remaining params), matching the UDTF __init__ signature
        and the View SQL template.
        
        **PySpark-Based Type Conversion:**
        - Uses `_property_type_to_spark_type()` to convert CDF types to PySpark DataTypes
        - Uses `_spark_type_to_sql_type_info()` to derive SQL types from PySpark types
        - Uses `_generate_type_json()` with PySpark StructField to generate type_json
        - This ensures consistency with generated UDTF code
        
        Args:
            view_id: View external_id
            debug: Enable debug output
            
        Returns:
            List of FunctionParameterInfo objects for the UDTF parameters
        """
        from databricks.sdk.service.catalog import (
            FunctionParameterInfo,
            FunctionParameterMode,
            FunctionParameterType,
            ColumnTypeName,
        )
        
        # Get the view from code_generator's data model (like pygen-main works with views)
        view = self._get_view_by_id(view_id)
        
        if not view:
            raise ValueError(f"View '{view_id}' not found in data model")
        
        input_params: list[FunctionParameterInfo] = []
        position = 0
        
        # Add secret parameters first (matching the view SQL template)
        # These are passed via SECRET() calls when the view is queried
        # Format: (param_name, sql_type, spark_type_name)
        #
        # IMPORTANT: type_json must use Spark StructField JSON format:
        # '{"name":"param_name","type":"string","nullable":true,"metadata":{}}'
        #
        # This format was discovered by inspecting system.ai.python_exec function.
        # The "name" field must match the parameter name.
        secret_params = [
            ("client_id", "STRING", "string"),
            ("client_secret", "STRING", "string"),
            ("tenant_id", "STRING", "string"),
            ("cdf_cluster", "STRING", "string"),
            ("project", "STRING", "string"),
        ]
        
        for param_name, sql_type, spark_type_name in secret_params:
            # Use Spark StructField JSON format
            type_json_value = f'{{"name":"{param_name}","type":"{spark_type_name}","nullable":true,"metadata":{{}}}}'
            input_params.append(
                FunctionParameterInfo(
                    name=param_name,
                    type_text=sql_type,
                    type_name=ColumnTypeName.STRING,
                    type_json=type_json_value,
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                )
            )
            position += 1
        
        # Add view property parameters
        for prop_name, prop in view.properties.items():
            property_type, is_relationship, is_multi = self._get_property_type(prop)
            
            if is_relationship:
                if is_multi:
                    # MultiReverseDirectRelation: ARRAY<STRING> (list of external_id references)
                    sql_type = "ARRAY<STRING>"
                    type_name = ColumnTypeName.ARRAY
                    # Spark StructField JSON for array
                    type_json_value = f'{{"name":"{prop_name}","type":"array","elementType":{{"type":"string"}},"containsNull":true,"nullable":true,"metadata":{{}}}}'
                else:
                    # DirectRelation or SingleReverseDirectRelation: STRING (single external_id reference)
                    sql_type = "STRING"
                    type_name = ColumnTypeName.STRING
                    spark_type_name = "string"
                    type_json_value = f'{{"name":"{prop_name}","type":"{spark_type_name}","nullable":true,"metadata":{{}}}}'
            else:
                sql_type, type_name, spark_type_name = self._property_type_to_sql_type(property_type)
                type_json_value = f'{{"name":"{prop_name}","type":"{spark_type_name}","nullable":true,"metadata":{{}}}}'
            
            input_params.append(
                FunctionParameterInfo(
                    name=prop_name,
                    type_text=sql_type,
                    type_name=type_name,
                    type_json=type_json_value,
                    position=position,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                )
            )
            position += 1
        
        return input_params

    def _parse_return_type(self, view_id: str) -> str:
        """Parse UDTF return type using PySpark StructType.
        
        **PySpark-Based Implementation:**
        - Uses `_build_output_schema()` to create the same StructType as generated UDTF's `outputSchema()`
        - Uses `_struct_type_to_ddl()` to convert StructType to SQL DDL string
        - Ensures consistency between registration and generated code
        
        Args:
            view_id: View external_id
            
        Returns:
            SQL return type string (e.g., "TABLE(col1 STRING, col2 INT, col3 ARRAY<STRING>)")
        """
        # Build StructType (same as generated UDTF's outputSchema())
        struct_type = self._build_output_schema(view_id)
        # Convert to SQL DDL
        return self._struct_type_to_ddl(struct_type)

    def _get_view_by_id(self, view_id: str):
        """Get view from code_generator's data model by external_id.
        
        Args:
            view_id: View external_id
            
        Returns:
            View object from data model, or None if not found (raises ValueError)
        """
        if not self.code_generator:
            raise ValueError("code_generator must be set")
        
        # Get data model from code_generator (similar to how pygen-main accesses data models)
        data_model = (
            self.code_generator._data_model[0]
            if isinstance(self.code_generator._data_model, list)
            else self.code_generator._data_model
        )
        
        # Find view by external_id
        for view in data_model.views:
            if view.external_id == view_id:
                return view
        
        return None
    
    def _get_property_type(self, prop) -> tuple[object, bool, bool]:
        """Safely extract property type, handling relationship properties.
        
        Args:
            prop: Property object from view.properties
            
        Returns:
            Tuple of (property_type, is_relationship, is_multi)
            - property_type: The actual type object, or None for relationship properties
            - is_relationship: True if this is a relationship property
            - is_multi: True if this is a multi-relationship (list/array), False for single
        """
        # Check if property has a .type attribute
        if not hasattr(prop, 'type'):
            # MultiReverseDirectRelation, SingleReverseDirectRelation don't have .type
            # Check the property object type to determine if it's multi or single
            prop_type_name = type(prop).__name__
            if prop_type_name == 'MultiReverseDirectRelation':
                return (None, True, True)  # is_relationship=True, is_multi=True (ARRAY<STRING>)
            elif prop_type_name == 'SingleReverseDirectRelation':
                return (None, True, False)  # is_relationship=True, is_multi=False (STRING)
            else:
                # Default to single relationship if we can't determine
                return (None, True, False)
        
        property_type = prop.type
        
        # Check if it's a DirectRelation (single relationship)
        if isinstance(property_type, dm.DirectRelation):
            return (property_type, True, False)  # is_relationship=True, is_multi=False (STRING)
        
        # Check if it's a relationship type by checking the property object itself
        prop_type_name = type(prop).__name__
        if prop_type_name == 'MultiReverseDirectRelation':
            return (None, True, True)  # is_relationship=True, is_multi=True (ARRAY<STRING>)
        elif 'Relation' in prop_type_name or 'Reverse' in prop_type_name:
            return (None, True, False)  # is_relationship=True, is_multi=False (STRING)
        
        return (property_type, False, False)  # Not a relationship
    
    def _property_type_to_sql_type(self, property_type) -> tuple[str, ColumnTypeName, str]:
        """Convert CDF property type to SQL type and Spark type name.
        
        Args:
            property_type: CDF property type (e.g., dm.Text, dm.Int32, etc.)
            
        Returns:
            Tuple of (sql_type_string, ColumnTypeName, spark_type_name)
            - sql_type_string: SQL type like "STRING", "INT", "DOUBLE"
            - ColumnTypeName: Databricks SDK enum
            - spark_type_name: Lowercase Spark type name for StructField JSON: "string", "double", etc.
        """
        # dm is already imported at module level
        
        # Map CDF property types to SQL types and Spark type names (lowercase)
        # These type names are used in Spark StructField JSON format:
        # {"name":"<param>","type":"<spark_type_name>","nullable":true,"metadata":{}}
        if isinstance(property_type, (dm.Int32, dm.Int64)):
            return ("INT", ColumnTypeName.INT, "long")
        elif isinstance(property_type, dm.Boolean):
            return ("BOOLEAN", ColumnTypeName.BOOLEAN, "boolean")
        elif isinstance(property_type, (dm.Float32, dm.Float64)):
            return ("DOUBLE", ColumnTypeName.DOUBLE, "double")
        elif isinstance(property_type, dm.Date):
            return ("DATE", ColumnTypeName.DATE, "date")
        elif isinstance(property_type, dm.Timestamp):
            return ("TIMESTAMP", ColumnTypeName.TIMESTAMP, "timestamp")
        elif isinstance(property_type, dm.Text):
            return ("STRING", ColumnTypeName.STRING, "string")
        elif isinstance(property_type, dm.DirectRelation):
            # Direct relations are typically represented as strings (external_id references)
            return ("STRING", ColumnTypeName.STRING, "string")
        else:
            # Default to STRING for unknown types
            return ("STRING", ColumnTypeName.STRING, "string")
```

#### **4.2.3 Usage Example**

**Notebook-style (Recommended - Aligned with pygen):**

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

# Load client from TOML file (same pattern as pygen)
# config.toml format:
# [cognite]
# project = "<cdf-project>"
# tenant_id = "<tenant-id>"
# cdf_cluster = "<cdf-cluster>"
# client_id = "<client-id>"
# client_secret = "<client-secret>"
client = load_cognite_client_from_toml("config.toml")

# Create WorkspaceClient (auto-detects credentials in Databricks notebooks)
# Note: WorkspaceClient() automatically uses Databricks notebook credentials
workspace_client = WorkspaceClient()

# Generate UDTFs for Data Model (same pattern as pygen's generate_sdk_notebook)
# Option 1 (Recommended): Pass workspace_client during generation
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    workspace_client=workspace_client,  # Pass it here for immediate registration capability
    catalog="main",  # Unity Catalog catalog name (default: "main")
    # schema=None,  # Auto-generated: "sp_pygen_power_windturbine_1" (matches folder pattern)
    output_dir="/local_disk0/tmp/pygen_udtf",  # Optional: custom output directory
)

# Option 2: Set up WorkspaceClient after generation (if not passed above)
# Uncomment if you didn't pass workspace_client to generate_udtf_notebook:
# workspace_client = WorkspaceClient()
# generator.workspace_client = workspace_client
# generator.udtf_registry = UDTFRegistry(workspace_client)
# generator.secret_helper = SecretManagerHelper(workspace_client)

# Set up Secret Manager (one-time setup)
# Note: These values come from the TOML file (config.toml)
# Use data model-specific scope: cdf_{space}_{external_id}
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project="my-project",  # from config.toml: [cognite].project
    cdf_cluster="api.cognitedata.com",  # from config.toml: [cognite].cdf_cluster
    client_id="...",  # from config.toml: [cognite].client_id
    client_secret="...",  # from config.toml: [cognite].client_secret
    tenant_id="...",  # from config.toml: [cognite].tenant_id
)

# Register UDTFs and Views
# Catalog and schema are automatically created if they don't exist
# DBR 18.1+ allows custom dependencies
# secret_scope can be omitted - will auto-generate from data_model_id
result = generator.register_udtfs_and_views(
    secret_scope=secret_scope,  # Optional: auto-generated if None
    dependencies=[
        "cognite-sdk>=7.90.1",
        "cognite-pygen>=1.2.29",
    ],
)

# Access structured results (Pydantic models)
print(f"Registered {result.total_count} UDTFs in {result.catalog}.{result.schema}")

# Access individual UDTF results
for udtf_result in result.registered_udtfs:
    print(f"View: {udtf_result.view_id}")
    print(f"Function: {udtf_result.function_info.name}")
    print(f"View registered: {udtf_result.view_registered}")

# Dict-like access (backward compatibility)
pump_result = result['pump_view']  # or result.get('pump_view')
print(f"Pump UDTF: {pump_result.function_info.name}")

# Now users can query using the auto-generated schema name:
# SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view WHERE timestamp > '2025-01-01'
# Or query the UDTF directly:
# SELECT * FROM main.sp_pygen_power_windturbine_1.pump_view_udtf()
```

**Low-level API (for advanced use cases):**

```python
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.databricks import UDTFGenerator
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

# Load client from TOML file
client = load_cognite_client_from_toml("config.toml")

# Initialize clients
workspace_client = WorkspaceClient()

# Create generator
# Schema should match the auto-generated pattern: {space}_{external_id.lower()}_{version}
generator = UDTFGenerator(
    workspace_client=workspace_client,
    cognite_client=client,
    catalog="main",  # Unity Catalog catalog name
    schema="sp_pygen_power_windturbine_1",  # Auto-generated from data model, or provide explicitly
)

# Generate and register UDTFs and Views
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")

# Set up Secret Manager (one-time setup)
# Note: These values come from the TOML file (config.toml)
# Use data model-specific scope: cdf_{space}_{external_id}
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project="my-project",  # from config.toml: [cognite].project
    cdf_cluster="api.cognitedata.com",  # from config.toml: [cognite].cdf_cluster
    client_id="...",  # from config.toml: [cognite].client_id
    client_secret="...",  # from config.toml: [cognite].client_secret
    tenant_id="...",  # from config.toml: [cognite].tenant_id
)

# Register UDTFs and Views
# secret_scope can be omitted - will auto-generate from data_model_id
result = generator.register_udtfs_and_views(
    data_model=data_model_id,
    secret_scope=secret_scope,  # Optional: auto-generated if None
    dependencies=["cognite-sdk>=7.90.1"],
)

# Access structured results (Pydantic models)
print(f"Registered {result.total_count} UDTFs")
for udtf_result in result.registered_udtfs:
    print(f"  - {udtf_result.view_id}: {udtf_result.function_info.name}")
```

---

### **4.3 Predicate Pushdown Implementation**

#### **4.3.1 Filter Translation**

The UDTF must translate Spark SQL WHERE clause predicates into CDF API filter objects to optimize performance.

**Example Translation:**

```python
# Spark SQL Query:
# SELECT * FROM pump_view WHERE timestamp > '2025-01-01' AND pressure > 100.0

# Translated to CDF Filter:
{
    "range": {
        "timestamp": {"gt": "2025-01-01T00:00:00Z"}
    },
    "range": {
        "pressure": {"gt": 100.0}
    }
}
```

**Implementation in UDTF:**

```python
from __future__ import annotations

from collections.abc import Iterator

class PumpViewUDTF:
    def eval(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
        **filters: dict[str, object],
    ) -> Iterator[tuple[object, ...]]:
        """Execute UDTF with predicate pushdown.

        Args:
            client_id: OAuth2 client ID (from Secret)
            client_secret: OAuth2 client secret (from Secret)
            tenant_id: Azure AD tenant ID (from Secret)
            cdf_cluster: CDF cluster URL (from Secret)
            project: CDF project name (from Secret)
            **filters: Spark WHERE clause predicates
                Example: filters = {"timestamp >": "2025-01-01", "pressure >": 100.0}

        Yields:
            Rows matching the View schema
        """
        # Spark passes WHERE clause predicates as **filters
        cdf_filter = self._translate_filters(filters)

        instances = client.data_modeling.instances.list(
            view=("equipment_space", "pump_view"),
            filter=cdf_filter,  # Applied server-side in CDF
        )

        # ... yield rows

    def _translate_filters(self, spark_filters: dict[str, object]) -> dict[str, object]:
        """Translate Spark predicates to CDF filter format.

        Args:
            spark_filters: Spark filter predicates

        Returns:
            CDF API filter format
        """
        cdf_filter: dict[str, object] = {}

        for key, value in spark_filters.items():
            # Parse "column >", "column <=", etc.
            if " >" in key:
                column = key.replace(" >", "")
                cdf_filter.setdefault("range", {})[column] = {"gt": value}
            elif " <" in key:
                column = key.replace(" <", "")
                cdf_filter.setdefault("range", {})[column] = {"lt": value}
            elif " ==" in key or "=" in key:
                column = key.replace(" ==", "").replace("=", "")
                cdf_filter.setdefault("equals", {})[column] = value
            # ... more operators

        return cdf_filter
```

#### **4.3.2 Supported Filter Operators**

| Spark Operator | CDF Filter Type | Example |
|----------------|----------------|---------|
| `=` or `==` | `equals` | `pressure = 100.0` → `{"equals": {"pressure": 100.0}}` |
| `>` | `range.gt` | `timestamp > '2025-01-01'` → `{"range": {"timestamp": {"gt": "2025-01-01"}}}` |
| `<` | `range.lt` | `temperature < 50.0` → `{"range": {"temperature": {"lt": 50.0}}}` |
| `>=` | `range.gte` | `pressure >= 10.0` → `{"range": {"pressure": {"gte": 10.0}}}` |
| `<=` | `range.lte` | `voltage <= 220.0` → `{"range": {"voltage": {"lte": 220.0}}}` |
| `IN` | `in` | `status IN ('active', 'running')` → `{"in": {"status": ["active", "running"]}}` |
| `LIKE` | `prefix` or `contains` | `name LIKE 'pump%'` → `{"prefix": {"name": "pump"}}` |

**Note:** Complex predicates (AND, OR, NOT) are handled by Spark's query optimizer before reaching the UDTF. The UDTF receives simplified predicates that can be directly translated to CDF filters.

---

## **5. Integration Patterns**

### **5.1 Secret Manager Setup**

#### **5.1.1 Creating Secret Scope**

The credentials stored in Secret Manager come from the TOML file (same file used by `load_cognite_client_from_toml()`).

```python
from cognite.databricks import SecretManagerHelper
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient
import tomli  # or tomllib for Python 3.11+

workspace_client = WorkspaceClient()
secret_helper = SecretManagerHelper(workspace_client)

# Define data model and create data model-specific scope
data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"

# Create scope (one-time setup per data model)
secret_helper.create_scope_if_not_exists(secret_scope)

# Load credentials from TOML file
with open("config.toml", "rb") as f:
    config = tomli.load(f)
    cognite_config = config["cognite"]

# Store credentials from TOML file into Secret Manager
secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project=cognite_config["project"],
    cdf_cluster=cognite_config["cdf_cluster"],
    client_id=cognite_config["client_id"],
    client_secret=cognite_config["client_secret"],
    tenant_id=cognite_config["tenant_id"],
)
```

#### **5.1.2 Secret Injection in Views**

When generating Views, secrets are injected as UDTF parameters. These secrets reference values that come from the TOML file and are stored in Secret Manager:

```sql
CREATE VIEW main.cdf_models.pump_view AS
SELECT * FROM main.cdf_models.pump_view_udtf(
    SECRET('cdf_sp_pygen_power_windturbine', 'client_id') AS client_id,
    SECRET('cdf_sp_pygen_power_windturbine', 'client_secret') AS client_secret,
    SECRET('cdf_sp_pygen_power_windturbine', 'tenant_id') AS tenant_id,
    SECRET('cdf_sp_pygen_power_windturbine', 'cdf_cluster') AS cdf_cluster,
    SECRET('cdf_sp_pygen_power_windturbine', 'project') AS project
);
```

**Key Points:**
- Secret values come from the TOML file (config.toml) and are stored in Secret Manager
- Secrets are resolved by Spark at query time via SECRET() function
- End users never see the secret values
- Access is controlled via Unity Catalog permissions on the View

### **5.2 UDTF Registration Workflow**

#### **5.2.1 Parallel Registration Architecture**

**Design Philosophy: ThreadPoolExecutor-based Parallelization**

Following the approach used in `cognite-sdk-scala-master`, the registration process uses **ThreadPoolExecutor** for parallel execution rather than Spark workers. This is appropriate because:

1. **I/O-Bound Operations**: Registration is primarily network I/O (API calls to Unity Catalog), not CPU-bound computation
2. **Right Tool for the Job**: ThreadPoolExecutor is designed for I/O-bound parallel tasks, while Spark is for distributed data processing
3. **Proven Pattern**: `cognite-sdk-scala` uses STTP backend with Semaphore-based rate limiting, not Spark orchestration
4. **Simplicity**: Avoids Spark context overhead for orchestration tasks

**Implementation Details:**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

def register_udtfs_and_views(
    self,
    max_workers: int = 5,  # ThreadPoolExecutor workers
    max_parallel_requests: int | None = None,  # Optional Semaphore-based rate limiting
    ...
):
    # Create rate limiter if specified (similar to cognite-sdk-scala's RateLimitingBackend)
    rate_limiter = Semaphore(max_parallel_requests) if max_parallel_requests else None
    
    # Parallel execution using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                self._register_single_udtf_and_view,
                view_id, udtf_file, view_sql, ...
            ): view_id
            for view_id, udtf_file in udtf_files.items()
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            result = future.result()
            # Handle results...
```

**Rate Limiting (Optional):**

Similar to `cognite-sdk-scala`'s `RateLimitingBackend`, optional rate limiting can be applied using a `Semaphore`:

```python
def _register_single_udtf_and_view(self, ..., rate_limiter: Semaphore | None = None):
    if rate_limiter:
        rate_limiter.acquire()  # Block until permit available
        try:
            return self._register_single_udtf_and_view_impl(...)
        finally:
            rate_limiter.release()  # Release permit
    else:
        return self._register_single_udtf_and_view_impl(...)
```

**Error Handling:**

- Errors are handled per view (don't fail entire registration)
- Failed registrations are logged with detailed error messages
- Results include both successful and failed registrations

**Comparison with cognite-sdk-scala:**

| Aspect | cognite-sdk-scala | pygen-spark |
|--------|-------------------|-------------|
| **Concurrency Model** | STTP backend + Semaphore/Queue | ThreadPoolExecutor + Semaphore |
| **Rate Limiting** | `RateLimitingBackend` with Semaphore | Optional Semaphore parameter |
| **Backpressure** | `BackpressureThrottleBackend` with Queue | Future work (can be added) |
| **Worker Orchestration** | HTTP client level (not Spark) | ThreadPoolExecutor (not Spark) |
| **Effect System** | Cats Effect (`F[_]`) | Python `concurrent.futures` |

**Benefits:**

1. **Performance**: Parallel registration reduces total time for multiple UDTFs
2. **Scalability**: Configurable `max_workers` allows tuning for different environments
3. **Rate Limiting**: Optional Semaphore prevents overwhelming Unity Catalog API
4. **Error Resilience**: Per-view error handling ensures partial success scenarios
5. **Proven Pattern**: Aligns with established patterns from `cognite-sdk-scala`

#### **5.2.2 Complete Workflow**

```python
from cognite.databricks import generate_udtf_notebook
from cognite.pygen import load_cognite_client_from_toml
from databricks.sdk import WorkspaceClient

# Step 1: Load client from TOML file (aligned with pygen pattern)
# config.toml format:
# [cognite]
# project = "<cdf-project>"
# tenant_id = "<tenant-id>"
# cdf_cluster = "<cdf-cluster>"
# client_id = "<client-id>"
# client_secret = "<client-secret>"
client = load_cognite_client_from_toml("config.toml")

# Step 2: Generate UDTFs for Data Model (aligned with pygen's generate_sdk_notebook)
from cognite.client.data_classes.data_modeling.ids import DataModelId

data_model_id = DataModelId(space="sp_pygen_power", external_id="WindTurbine", version="1")
generator = generate_udtf_notebook(
    data_model_id,
    client,
    catalog="main",
    schema="cdf_models",
)

# Step 3: Set up WorkspaceClient (if not provided during generation)
workspace_client = WorkspaceClient()
generator.workspace_client = workspace_client
generator.udtf_registry = UDTFRegistry(workspace_client)
generator.secret_helper = SecretManagerHelper(workspace_client)

# Step 4: Set up Secret Manager (one-time)
# Note: These values come from the TOML file (config.toml)
# Use data model-specific scope: cdf_{space}_{external_id}
secret_scope = f"cdf_{data_model_id.space}_{data_model_id.external_id.lower()}"
generator.secret_helper.set_cdf_credentials(
    scope_name=secret_scope,
    project="my-project",  # from config.toml: [cognite].project
    cdf_cluster="api.cognitedata.com",  # from config.toml: [cognite].cdf_cluster
    client_id="...",  # from config.toml: [cognite].client_id
    client_secret="...",  # from config.toml: [cognite].client_secret
    tenant_id="...",  # from config.toml: [cognite].tenant_id
)

# Step 5: Register UDTFs and Views (with parallel execution)
# secret_scope can be omitted - will auto-generate from data_model_id
result = generator.register_udtfs_and_views(
    secret_scope=secret_scope,  # Optional: auto-generated if None
    dependencies=["cognite-sdk>=7.90.1"],  # DBR 18.1+
    max_workers=5,  # Number of parallel worker threads (default: 5)
    max_parallel_requests=3,  # Optional: Rate limiting via Semaphore (similar to cognite-sdk-scala)
)

# Access structured results (Pydantic models)
print(f"Successfully registered {result.total_count} UDTFs in {result.catalog}.{result.schema}")
for udtf_result in result.registered_udtfs:
    if udtf_result.view_registered:
        print(f"  ✓ {udtf_result.view_id} (UDTF + View)")
    else:
        print(f"  ✓ {udtf_result.view_id} (UDTF only)")

# Step 6: Grant permissions (via Unity Catalog)
# GRANT SELECT ON VIEW main.cdf_models.pump_view TO `data-scientists@company.com`;
```

#### **5.2.2 User Query Experience**

After registration, users can query CDF data via SQL or PySpark:

```sql
-- SQL Query
SELECT * FROM main.cdf_models.pump_view
WHERE timestamp > '2025-01-01'
  AND pressure > 100.0
LIMIT 1000;
```

```python
# PySpark Query
df = spark.sql("""
    SELECT * FROM main.cdf_models.pump_view
    WHERE timestamp > '2025-01-01'
""")

# Further processing
df.filter(df.pressure > 100.0).show()
```

**Discovery Experience:**
- Users can search for "pump" in Databricks Search Box
- View appears in Unity Catalog browser
- No manual credential handling required

### **5.3 Time Series Datapoints UDTFs**

The CDF Databricks Integration provides three specialized UDTFs for querying Time Series datapoints. These UDTFs use **instance_id** (space + external_id) to identify time series, enabling seamless integration with Data Model views that reference time series.

**Key Features:**
- **Instance ID-based queries**: Uses `space` and `external_id` (not numeric IDs) for better integration with Data Models
- **Multiple query patterns**: Single time series, multiple time series (long format), and latest datapoints
- **Aggregation support**: Built-in support for aggregates (average, max, min, count) with granularity
- **Flexible time ranges**: Supports ISO 8601 timestamps and relative time expressions (e.g., "2w-ago", "now")
- **Template-based generation**: Uses the same Jinja2 template-based generation approach as Data Model UDTFs for consistent behavior, error handling, and initialization patterns

#### **5.3.1 Available Time Series UDTFs**

The integration provides three UDTFs, all generated using templates in `pygen-spark`:

1. **`time_series_datapoints_udtf`**: Query a single time series
   - Returns: `(timestamp, value)`
   - Use case: Simple queries for one time series

2. **`time_series_datapoints_long_udtf`**: Query multiple time series in long format
   - Returns: `(timestamp, time_series_external_id, value)`
   - Use case: Query multiple time series, can be pivoted to wide format

3. **`time_series_latest_datapoints_udtf`**: Get latest datapoint(s) for one or more time series
   - Returns: `(time_series_external_id, timestamp, value, status_code)`
   - Use case: Real-time monitoring, current state queries

#### **5.3.2 Single Time Series Query (`time_series_datapoints_udtf`)**

Query datapoints from a single time series using instance_id (space + external_id).

**Parameters:**
- `instance_id` (STRING, required): Instance ID in format "space:external_id" (e.g., "sailboat:ts1")
- `start` (STRING, required): Start timestamp (ISO 8601 or relative like "2w-ago", "1d-ago")
- `end` (STRING, required): End timestamp (ISO 8601 or relative like "now", "1d-ahead")
- `aggregates` (STRING, optional): Aggregate type ("average", "max", "min", "count")
- `granularity` (STRING, optional): Granularity for aggregates (e.g., "1h", "1d", "30s")
- Credential parameters: `client_id`, `client_secret`, `tenant_id`, `cdf_cluster`, `project` (from Secrets)

**Returns:** `TABLE(timestamp TIMESTAMP, value DOUBLE)`

**Example SQL Queries:**

```sql
-- Basic query: Get raw datapoints for a time series
SELECT * FROM time_series_datapoints_udtf(
  instance_id => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
  start => '47w-ago',
  end => '46w-ago',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY timestamp
LIMIT 10;

-- With aggregates: Get hourly averages
SELECT * FROM time_series_datapoints_udtf(
  instance_id => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
  start => '47w-ago',
  end => '46w-ago',
  aggregates => 'average',
  granularity => '1h',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY timestamp;
```

#### **5.3.3 Multiple Time Series Query - Long Format (`time_series_datapoints_long_udtf`)**

Query datapoints from multiple time series in long format. Similar to `client.time_series.data.retrieve_dataframe()` but returns long format that can be pivoted to wide format.

**Parameters:**
- `instance_ids` (STRING, required): Comma-separated list of instance IDs in format "space:external_id" (e.g., "sailboat:ts1,otherspace:ts2"). Supports time series from different spaces.
- `start` (STRING, required): Start timestamp
- `end` (STRING, required): End timestamp
- `aggregates` (STRING, optional): Aggregate type
- `granularity` (STRING, optional): Granularity for aggregates
- `include_aggregate_name` (BOOLEAN, optional): Include aggregate name in time_series_external_id (for compatibility with retrieve_dataframe)
- Credential parameters: Same as above

**Returns:** `TABLE(timestamp TIMESTAMP, time_series_external_id STRING, value DOUBLE)`

**Note:** The `time_series_external_id` in the output is in format `"space:external_id"` to support time series from different spaces.

**Example SQL Queries:**

```sql
-- Query multiple time series in long format (supports different spaces)
SELECT * FROM time_series_datapoints_long_udtf(
  instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',
  start => '47w-ago',
  end => '46w-ago',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY time_series_external_id, timestamp
LIMIT 20;

-- Convert long format to wide format (pivot)
-- Note: time_series_external_id is now in format "space:external_id"
SELECT 
  timestamp,
  MAX(CASE WHEN time_series_external_id = 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround' THEN value END) AS speedOverGround,
  MAX(CASE WHEN time_series_external_id = 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue' THEN value END) AS courseOverGroundTrue
FROM time_series_datapoints_long_udtf(
  instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',
  start => '47w-ago',
  end => '46w-ago',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
GROUP BY timestamp
ORDER BY timestamp
LIMIT 20;

-- With aggregates
SELECT * FROM time_series_datapoints_long_udtf(
  instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',
  start => '47w-ago',
  end => '46w-ago',
  aggregates => 'average',
  granularity => '1h',
  include_aggregate_name => true,
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY time_series_external_id, timestamp
LIMIT 20;
```

#### **5.3.4 Latest Datapoints Query (`time_series_latest_datapoints_udtf`)**

Get the latest datapoint(s) for one or more time series. Useful for real-time monitoring and current state queries.

**Parameters:**
- `instance_ids` (STRING, required): Comma-separated list of instance IDs in format "space:external_id" (e.g., "sailboat:ts1,otherspace:ts2"). Supports time series from different spaces.
- `before` (STRING, optional): Get latest before this time (default: "now", can use "1h-ago", ISO 8601, etc.)
- `include_status` (BOOLEAN, optional): Include status code in output (default: false)
- Credential parameters: Same as above

**Returns:** `TABLE(time_series_external_id STRING, timestamp TIMESTAMP, value DOUBLE, status_code INT)`

**Note:** The `time_series_external_id` in the output is in format `"space:external_id"` to support time series from different spaces.

**Example SQL Queries:**

```sql
-- Get latest datapoints for multiple time series (supports different spaces)
SELECT * FROM time_series_latest_datapoints_udtf(
  instance_ids => 'sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,sailboat:vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',
  before => 'now',
  include_status => true,
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
ORDER BY time_series_external_id;

-- Get latest datapoint before a specific time
SELECT * FROM time_series_latest_datapoints_udtf(
  space => 'sailboat',
  external_ids => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
  before => '1h-ago',
  include_status => false,
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
);
```

#### **5.3.5 Using Instance ID from Data Model Views**

One of the key advantages of using instance_id (space + external_id) is the ability to join time series queries with Data Model views. This enables powerful queries that combine structured data with time series data.

**Example: Join Data Model View with Time Series Datapoints**

```sql
-- Step 1: Query Data Model view to get time series references
WITH vessel_time_series AS (
  SELECT 
    external_id AS vessel_external_id,
    space,
    navigation_speedOverGround.external_id AS speed_ts_external_id,
    navigation_courseOverGround.external_id AS course_ts_external_id
  FROM main.cdf_models.vessel_view
  WHERE external_id = 'vessels.urn:mrn:imo:mmsi:258219000::129038'
)

-- Step 2: Use the instance_id from the view to query time series datapoints
SELECT 
  v.vessel_external_id,
  ts.timestamp,
  ts.value AS speed
FROM vessel_time_series v
CROSS JOIN LATERAL (
  SELECT timestamp, value
  FROM time_series_datapoints_udtf(
    space => v.space,
    external_id => v.speed_ts_external_id,
    start => '1d-ago',
    end => 'now',
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
  )
) ts
ORDER BY v.vessel_external_id, ts.timestamp;
```

**Example: Query Multiple Time Series from Data Model View**

```sql
-- Get all time series external_ids from a Data Model view
WITH vessel_ts_list AS (
  SELECT 
    external_id AS vessel_external_id,
    space,
    -- Collect all time series external_ids into a comma-separated string
    CONCAT_WS(',',
      navigation_speedOverGround.external_id,
      navigation_courseOverGround.external_id
    ) AS time_series_external_ids
  FROM main.cdf_models.vessel_view
  WHERE external_id = 'vessels.urn:mrn:imo:mmsi:258219000::129038'
)

-- Query all time series for the vessel
SELECT 
  v.vessel_external_id,
  ts.time_series_external_id,
  ts.timestamp,
  ts.value
FROM vessel_ts_list v
CROSS JOIN LATERAL (
  SELECT time_series_external_id, timestamp, value
  FROM time_series_datapoints_long_udtf(
    space => v.space,
    external_ids => v.time_series_external_ids,
    start => '1d-ago',
    end => 'now',
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
  )
) ts
ORDER BY v.vessel_external_id, ts.time_series_external_id, ts.timestamp;
```

**Example: Get Latest Values for All Vessels**

```sql
-- Get latest time series values for all vessels in a space
WITH vessel_ts_list AS (
  SELECT 
    external_id AS vessel_external_id,
    space,
    CONCAT_WS(',',
      navigation_speedOverGround.external_id,
      navigation_courseOverGround.external_id
    ) AS time_series_external_ids
  FROM main.cdf_models.vessel_view
  WHERE space = 'sailboat'
)

SELECT 
  v.vessel_external_id,
  ts.time_series_external_id,
  ts.timestamp AS latest_timestamp,
  ts.value AS latest_value,
  ts.status_code
FROM vessel_ts_list v
CROSS JOIN LATERAL (
  SELECT time_series_external_id, timestamp, value, status_code
  FROM time_series_latest_datapoints_udtf(
    space => v.space,
    external_ids => v.time_series_external_ids,
    before => 'now',
    include_status => true,
    client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
    client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
    tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
    cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
    project       => SECRET('cdf_sailboat_sailboat', 'project')
  )
) ts
ORDER BY v.vessel_external_id, ts.time_series_external_id;
```

#### **5.3.6 Advanced Query Patterns**

**Aggregations and Statistics:**

```sql
-- Calculate statistics for a time series
SELECT 
  time_series_external_id,
  COUNT(*) AS datapoint_count,
  MIN(timestamp) AS first_timestamp,
  MAX(timestamp) AS last_timestamp,
  MIN(value) AS min_value,
  MAX(value) AS max_value,
  AVG(value) AS avg_value,
  STDDEV(value) AS stddev_value
FROM time_series_datapoints_long_udtf(
  space => 'sailboat',
  external_ids => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround,vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.courseOverGroundTrue',
  start => '47w-ago',
  end => '46w-ago',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
GROUP BY time_series_external_id;
```

**Time-based Filtering:**

```sql
-- Get datapoints for a specific time window
SELECT * FROM time_series_datapoints_udtf(
  space => 'sailboat',
  external_id => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
  start => '2025-01-01T00:00:00Z',
  end => '2025-01-31T23:59:59Z',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
)
WHERE timestamp >= '2025-01-15T00:00:00Z'
  AND timestamp < '2025-01-16T00:00:00Z'
ORDER BY timestamp;
```

#### **5.3.7 Registration and View Creation**

Time Series UDTFs are automatically generated and registered when using the `cognite-databricks` package. They can be used directly in SQL queries or wrapped in Views for easier access.

**Creating Views for Time Series:**

```sql
-- Create a view for a specific time series
CREATE OR REPLACE VIEW main.cdf_models.vessel_speed_ts AS
SELECT * FROM time_series_datapoints_udtf(
  space => 'sailboat',
  external_id => 'vessels.urn:mrn:imo:mmsi:258219000::129038::navigation.speedOverGround',
  start => '2w-ago',
  end => 'now',
  client_id     => SECRET('cdf_sailboat_sailboat', 'client_id'),
  client_secret => SECRET('cdf_sailboat_sailboat', 'client_secret'),
  tenant_id     => SECRET('cdf_sailboat_sailboat', 'tenant_id'),
  cdf_cluster   => SECRET('cdf_sailboat_sailboat', 'cdf_cluster'),
  project       => SECRET('cdf_sailboat_sailboat', 'project')
);

-- Query the view
SELECT * FROM main.cdf_models.vessel_speed_ts
ORDER BY timestamp DESC
LIMIT 100;
```

**Note:** Views with hardcoded time ranges are useful for recent data queries. For historical queries, use the UDTF directly with specific time ranges.

### **5.4 Streaming Support**

For real-time data (Time Series subscriptions, Records streams), UDTFs can support streaming queries:

```python
from __future__ import annotations

import time
from collections.abc import Iterator

from cognite.client import CogniteClient
from cognite.client.credentials import OAuthClientCredentials

class TimeSeriesStreamUDTF:
    """UDTF for streaming Time Series datapoints (subscriptions)."""

    def eval(
        self,
        subscription_id: int,
        client_id: str | None = None,
        client_secret: str | None = None,
        tenant_id: str | None = None,
        cdf_cluster: str | None = None,
        project: str | None = None,
        **kwargs: dict[str, object],
    ) -> Iterator[tuple[object, ...]]:
        """Stream Time Series datapoints from a subscription.

        Args:
            subscription_id: CDF Time Series subscription ID
            client_id: OAuth2 client ID (from Secret)
            client_secret: OAuth2 client secret (from Secret)
            tenant_id: Azure AD tenant ID (from Secret)
            cdf_cluster: CDF cluster URL (from Secret)
            project: CDF project name (from Secret)
            **kwargs: Additional keyword arguments

        Yields:
            (timestamp, value) tuples

        Note:
            This requires Spark Structured Streaming support for UDTFs.
        """
        client = self._create_client(client_id, client_secret, tenant_id, cdf_cluster, project, **kwargs)

        # Poll subscription endpoint
        while True:
            updates = client.time_series.subscriptions.poll(subscription_id)
            for update in updates:
                yield (update.timestamp, update.value)

            time.sleep(1)  # Poll interval
```

**Note:** Streaming UDTFs require Spark Structured Streaming support, which may have limitations in DBR 18.1. This is a Phase 2 enhancement.

---

## **6. Implementation Phases**

### **Phase 1: Foundation (Q1 2026)**

**Timeline:** Late January 2026 - End of Q1 2026

**Dependencies:**
- **DBR 18.1** (End of January 2026) for custom dependencies in UDTFs
- Databricks Secret Manager API access
- Unity Catalog API access

**Deliverables:**

1. **cognite-pygen-spark Package**
   - `SparkUDTFGenerator` class extending `pygen.SDKGenerator`
   - `SparkMultiAPIGenerator` class extending `pygen.MultiAPIGenerator`
   - UDTF Jinja2 templates (`udtf_function.py.jinja`)
   - View SQL templates with Secret injection (`view_sql.py.jinja`)
   - Basic predicate pushdown (equality, range filters)

2. **cognite-databricks Package**
   - `UDTFRegistry` for Unity Catalog function registration
   - `SecretManagerHelper` for Secret Manager integration
   - `UDTFGenerator` high-level API
   - View generation and registration utilities

3. **Time Series UDTFs**
   - `time_series_datapoints_udtf`: Single time series query
   - `time_series_datapoints_long_udtf`: Multiple time series query (long format)
   - `time_series_latest_datapoints_udtf`: Latest datapoints query
   - Instance ID-based queries (space + external_id) for Data Model integration
   - Aggregation support (average, max, min, count) with granularity
   - Time Series View generation utilities

4. **Documentation**
   - Bootstrap guide for Secret Manager setup
   - UDTF registration guide
   - User query examples

**Success Metrics:**
- A Data Scientist finds a CDF asset in the UC search box
- User queries CDF data via SQL with zero manual authentication steps
- UDTF executes successfully with custom dependencies (DBR 18.1+)
- Predicate pushdown reduces data transfer by >50% for filtered queries

**Testing:**
- Unit tests for UDTF code generation
- Integration tests for Unity Catalog registration
- End-to-end tests for Secret injection and query execution

### **Phase 2: Optimization (Q2 2026)**

**Timeline:** Q2 2026

**Deliverables:**

1. **Enhanced Code Generation**
   - Automated pygen generation for all CDF Views
   - Support for complex View relationships (implements, children)
   - Advanced predicate pushdown (IN, LIKE, complex AND/OR)

2. **Knowledge Graph Navigation**
   - UDTFs for node and edge queries
   - Graph traversal UDTFs
   - Relationship filtering

3. **Streaming Support**
   - Time Series subscription UDTFs
   - Records stream UDTFs
   - Spark Structured Streaming integration

4. **Simulator/Job Status Reporting**
   - UDTFs for querying Simulator job status
   - Integration with CDF Simulator API

5. **Performance Optimizations**
   - Parallel UDTF execution
   - Caching strategies
   - Query plan optimization

**Success Metrics:**
- All CDF Data Model Views can be generated automatically
- Streaming queries achieve <5s latency
- Complex predicate pushdown reduces data transfer by >80%

### **Phase 3: Strategic Evolution (Late 2026)**

**Timeline:** Late 2026

**Deliverables:**

1. **Iceberg REST Catalog Integration**
   - Native tabular consumption via Iceberg
   - Alignment with Databricks Iceberg REST Catalog

2. **Delta Sharing**
   - Share CDF data via Delta Sharing protocol
   - Cross-workspace data access

3. **Metric Views Alignment**
   - Integration with Databricks Metric Views
   - Time Series aggregation at scale

**Note:** Phase 3 deliverables are exploratory and depend on Databricks roadmap and customer feedback.

---

## **7. Testing Strategy**

### **7.1 Unit Tests**

**cognite-pygen-spark:**
- UDTF code generation for various View types
- View SQL generation with Secret injection
- Predicate translation logic
- Template rendering

**cognite-databricks:**
- UDTF registration logic
- Secret Manager helper functions
- View registration logic

### **7.2 Integration Tests**

**Unity Catalog Integration:**
- Register UDTF in Unity Catalog
- Query registered UDTF
- Verify Secret injection works
- Test permissions (GRANT/REVOKE)

**CDF API Integration:**
- UDTF execution with real CDF data
- Predicate pushdown verification
- Error handling (authentication failures, API errors)

**End-to-End Tests:**
- Complete workflow: Generate → Register → Query
- Multiple Views from same Data Model
- Time Series UDTF execution

### **7.3 Performance Tests**

- **Predicate Pushdown Effectiveness:**
  - Measure data transfer reduction with filters
  - Compare with/without predicate pushdown

- **UDTF Execution Performance:**
  - Query latency for various data sizes
  - Parallel execution efficiency
  - Memory usage

- **Scalability:**
  - Large number of Views (100+)
  - Large result sets (1M+ rows)
  - Concurrent queries

### **7.4 Test Infrastructure**

- **Databricks Test Cluster:**
  - DBR 18.1+ cluster for custom dependencies testing
  - Unity Catalog enabled
  - Secret Manager access

- **CDF Test Project:**
  - Test Data Models with various View types
  - Test Time Series data
  - Test OAuth2 credentials

---

## **8. Dependencies and Prerequisites**

### **8.1 External Dependencies**

**Python Packages:**
- `cognite-sdk >= 7.90.1` (CDF API client; PyPI package name: `cognite-sdk`)
- `cognite-pygen >= 1.2.29` (PyPI package name; Code generation base)
- `databricks-sdk >= 0.20.0` (Databricks API client)
- `pyspark` (Spark Python API) - **Provided by Databricks runtime, not installed via pip**
  - For local development: `pip install cognite-databricks[local]`
- `jinja2 >= 3.0.0` (Template engine)

**Databricks Runtime:**
- **DBR 18.1+** (required for custom dependencies in UDTFs)
- Unity Catalog enabled
- Secret Manager access

**CDF Requirements:**
- CDF project with Data Models
- OAuth2 credentials (client_id, client_secret, tenant_id)
- Appropriate CDF API permissions

### **8.2 Internal Dependencies**

**cognite-pygen-spark:**
- **PyPI:** `cognite-pygen-spark` (depends on `cognite-pygen`); **Import:** `cognite.pygen_spark` (extends `cognite.pygen`)
- Reuses pygen's View parsing and internal representation

**cognite-databricks:**
- **PyPI:** `cognite-databricks` (depends on `cognite-pygen-spark`); **Import:** `cognite.databricks` (uses `cognite.pygen_spark`)
- Depends on `cognite-sdk-python` (for UDTF runtime execution)

### **8.3 Infrastructure Requirements**

**Databricks Workspace:**
- Unity Catalog enabled
- Secret Manager access
- Workspace admin permissions (for UDTF registration)
- SQL Warehouse or Cluster for testing

**Network:**
- Outbound access to CDF API endpoints
- Outbound access to Azure AD (for OAuth2)

---

## **9. Deployment and Distribution**

### **9.1 Package Distribution**

**PyPI Packages:**
- `cognite-pygen-spark`: Published to PyPI
- `cognite-databricks`: Published to PyPI

**Installation:**

**In Databricks:**
```bash
pip install cognite-databricks
# This installs cognite-pygen-spark as a dependency
# Note: PySpark is provided by Databricks runtime - do NOT install separately
```

**For Local Development:**
```bash
pip install cognite-databricks[local]
# The [local] extra includes pyspark>=3.0.0 for local testing
```

**Databricks Cluster/Workspace Installation:**
- For DBR 18.1+, packages can be specified as UDTF dependencies
- For older DBR versions, install via cluster libraries:
  ```python
  # In Databricks notebook
  # IMPORTANT: Do NOT install pyspark - it's provided by Databricks runtime
  # Installing PySpark from PyPI can cause kernel crashes (ImportError: cannot import name 'ClientContext')
  %pip install cognite-sdk>=7.90.1 cognite-pygen-spark>=0.1.0 cognite-databricks>=0.1.0
  ```

### **9.2 Databricks Integration**

**Workspace Setup:**
1. Install packages in workspace or cluster
2. Create Secret Manager scope
3. Store CDF credentials in Secret Manager
4. Grant appropriate permissions to users

**UDTF Registration:**
- Run registration script in Databricks notebook
- UDTFs and Views are registered in Unity Catalog
- Permissions are managed via Unity Catalog GRANT/REVOKE

### **9.3 Documentation**

**User Documentation:**
- Quick start guide
- Secret Manager setup guide
- UDTF registration guide
- Query examples (SQL and PySpark)
- Troubleshooting guide

**Developer Documentation:**
- Architecture overview
- Extension points for custom UDTFs
- Template customization guide
- API reference

---

## **10. Risks and Mitigations**

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| **DBR 18.1 delayed** | High | Medium | Fallback: Pre-install packages on cluster, use older UDTF registration pattern |
| **Custom dependencies not working as expected** | High | Low | Early validation with Databricks Alpha environment |
| **Secret injection pattern not supported** | High | Low | Validate SECRET() function support in DBR 18.1 |
| **UDTF performance issues** | Medium | Medium | Optimize predicate pushdown, implement caching, parallel execution |
| **Unity Catalog API limitations** | Medium | Low | Work with Databricks to address API gaps |
| **Predicate pushdown complexity** | Medium | Medium | Start with simple filters, iterate based on feedback |
| **Streaming UDTF limitations** | Low | High | Phase 2 deliverable, explore alternatives if needed |

---

## **11. Success Criteria**

### **10.1 Functional Criteria**

- ✅ Users can discover CDF data via Databricks Search Box
- ✅ Users can query CDF data via SQL without manual credential handling
- ✅ UDTFs execute successfully with custom dependencies (DBR 18.1+)
- ✅ Predicate pushdown reduces data transfer by >50% for filtered queries
- ✅ All CDF Data Model Views can be generated as UDTFs
- ✅ Time Series data is accessible via UDTFs

### **10.2 Performance Criteria**

- ✅ Query latency <10s for typical queries (100K rows)
- ✅ Predicate pushdown reduces data transfer by >50%
- ✅ UDTF registration completes in <5 minutes for 50 Views
- ✅ Concurrent queries (10+) execute without degradation

### **10.3 Quality Criteria**

- ✅ Unit test coverage >80%
- ✅ Integration tests pass in Databricks environment
- ✅ Documentation is complete and accurate
- ✅ Zero security vulnerabilities (credentials never exposed)

---

## **12. Open Questions**

1. **Streaming UDTF Support:**
   - Does Spark Structured Streaming support UDTFs in DBR 18.1?
   - What are the limitations for real-time data?

2. **Custom Dependencies:**
   - Exact API for specifying dependencies in DBR 18.1?
   - Can we bundle multiple packages?
   - Version resolution strategy?

3. **Secret Injection:**
   - Does SECRET() function work in UDTF parameters?
   - Are there any limitations on Secret scope access?

4. **Performance:**
   - What is the maximum result set size for UDTFs?
   - How does Spark handle large UDTF outputs?

5. **Governance:**
   - Can we set column-level permissions on Views?
   - How do we handle View updates when Data Model changes?

6. **Pygen Export Dependency (Required):**
   - **Short-term (Current Approach):**
     - **Import Pattern:** For ALL pygen dependencies, we use private API imports:
       ```python
       from cognite.pygen._core.generators import SDKGenerator
       from cognite.pygen._core.generators import MultiAPIGenerator
       # ... and any other pygen dependencies needed
       ```
     - This pattern applies to **ALL pygen dependencies**, not just the specific ones listed here
     - Define our own `DataModel` type alias using public types from `cognite.client`
     - Uses `DataModelIdentifier` from `cognite.client.data_classes.data_modeling` (public API)
     - Extends it with `dm.DataModel[dm.View]` to match pygen's definition
   - **Long-term (Required Dependency):**
     - **Request pygen to export ALL required dependencies in `cognite.pygen.__init__.py`:**
       - `SDKGenerator` (from `cognite.pygen._core.generators`)
       - `MultiAPIGenerator` (from `cognite.pygen._core.generators`)
       - `DataModel` (type alias from `cognite.pygen._generator`)
       - Any other pygen dependencies used by `pygen-spark`
     - **Import Pattern:** After pygen is updated, for ALL pygen dependencies, we will use public API imports:
       ```python
       from cognite.pygen import SDKGenerator, MultiAPIGenerator, DataModel
       # ... and any other pygen dependencies needed
       ```
     - This pattern applies to **ALL pygen dependencies**, not just the specific ones listed here
   - **Benefits:**
     - Eliminates dependency on private APIs
     - Reduces code duplication
     - Ensures perfect alignment with pygen's public API
     - Provides better stability guarantees
   - **Action Required:** Coordinate with pygen maintainers to add all required dependencies to `__all__` in `cognite.pygen.__init__.py`

---

## **13. References**

### **12.1 External Documentation**

- [Databricks Unity Catalog Functions](https://docs.databricks.com/en/udf/index.html)
- [Databricks Secret Manager](https://docs.databricks.com/en/security/secrets/index.html)
- [Databricks Python UDTFs](https://docs.databricks.com/en/udf/python-udf.html) (DBR 18.1+)
- [Cognite Python SDK Documentation](https://cognite-sdk-python.readthedocs.io/)
- [CDF Data Models API](https://api-docs.cognite.com/)
- [Pygen Developer Documentation](https://cognite-pygen.readthedocs-hosted.com/en/latest/developer_docs/index.html)
- [Pygen Quickstart - Notebook](https://cognite-pygen.readthedocs-hosted.com/en/latest/quickstart/notebook.html) (Pattern reference for `generate_udtf_notebook`)

### **12.2 Internal Repositories (Cross-References)**

**pygen-main Repository:**
- **Location:** `pygen-main/pygen-main/`
- **Key Files:**
  - `cognite/pygen/_generator.py`: Main SDK generation logic (`generate_sdk()` function)
  - `cognite/pygen/_core/generators.py`: `SDKGenerator` and `MultiAPIGenerator` classes
  - `cognite/pygen/_core/models/api_classes.py`: API class definitions (`NodeAPIClass`, `EdgeAPIClass`, etc.)
  - `cognite/pygen/_core/templates/`: Jinja2 templates for code generation
    - `api_class_node.py.jinja`: Template for Node API classes
    - `api_core.py.jinja`: Core API infrastructure
  - `cognite/pygen/utils/cdf.py`: CDF client utilities (`load_cognite_client_from_toml()`)
  - `cognite/pygen/_generator.py`: `generate_sdk_notebook()` function (pattern reference for `generate_udtf_notebook`)
  - `cognite/pygen/utils/text.py`: Naming utilities (`to_pascal()`, `to_snake()`)
- **Patterns to Reuse:**
  - Template-based code generation using Jinja2
  - Data Model and View parsing logic
  - View property to type conversion logic
  - Naming conventions and utilities
  - Code structure and organization

**cognite-sdk-python Repository:**
- **Location:** `python-sdk/cognite-sdk-python-master/`
- **Key Files:**
  - `cognite/client/_cognite_client.py`: `CogniteClient` class and factory methods
  - `cognite/client/config.py`: `ClientConfig` and `GlobalConfig` classes
  - `cognite/client/credentials.py`: `CredentialProvider` implementations
    - `OAuthClientCredentials`: OAuth2 client credentials flow (recommended)
    - `Token`: Bearer token authentication (legacy, for backward compatibility)
    - `OAuthInteractive`: Interactive OAuth flow
- **Authentication Patterns:**
  - Use `CogniteClient.default()` with `CredentialProvider`
  - Use `CogniteClient.default_oauth_client_credentials()` for OAuth2
  - **OAuth2 authentication only** (API key authentication is not supported)
  - Connection properties should include: `project`, `cdf_cluster`, `tenant_id`, `client_id`, `client_secret`

### **12.3 Related Documents**

- PRD - CDF Data Source v7.md
- PRD - CDP Spark Connector Unity Catalog Integration.md

---

**Document Version:** 1.0  
**Last Updated:** January 2026  
**Next Review:** After Phase 1 completion

