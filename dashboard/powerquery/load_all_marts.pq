// RideFlow — load every mart in one query
//
// Power BI's Parquet connector takes ONE file per query, so loading 13 tables
// by hand means 13 trips through the file dialog. This does it in one paste and
// then splits into separate tables.
//
// ── How to use ───────────────────────────────────────────────────────────
//  1. Power BI Desktop → Home → Transform data (opens Power Query Editor)
//  2. Home → New Source → Blank Query
//  3. Home → Advanced Editor
//  4. Delete everything, paste the block below
//  5. Edit RideFlowPath on line 1 of the `let` to match YOUR folder
//  6. Done → the query returns one row per mart, with a [Data] column
//  7. Right-click the query → Reference, once per table you want (see step 8
//     in dashboard/README.md), then filter Name and expand Data
//
// If any of that feels fiddly, the manual route in dashboard/README.md step 1
// is slower but completely foolproof. Both end in the same place.

let
    // ── EDIT THIS ────────────────────────────────────────────────────────
    RideFlowPath = "C:\Users\Rajat Pandey\Desktop\Projects\RideFlow-Data-Engineering-Pipeline\data\processed",

    Source = Folder.Files(RideFlowPath),

    // Parquet only. The folder also holds _FRESHNESS.json and
    // _LAST_SUCCESSFUL_RUN.json, which are loaded separately as JSON.
    OnlyParquet = Table.SelectRows(
        Source,
        each Text.EndsWith([Name], ".parquet")
    ),

    // fct_trip_events is 90k rows and DELIBERATELY contains events that failed
    // business assertions - it is the audit record of what ARRIVED, not what
    // was accepted. Excluded here so nobody aggregates measures from it by
    // accident. Remove this filter only if you are building a pipeline-health
    // page and know what the rows mean.
    Excluded = Table.SelectRows(
        OnlyParquet,
        each [Name] <> "fct_trip_events.parquet"
    ),

    AddTableName = Table.AddColumn(
        Excluded,
        "TableName",
        each Text.BeforeDelimiter([Name], ".parquet"),
        type text
    ),

    LoadEach = Table.AddColumn(
        AddTableName,
        "Data",
        each Parquet.Document([Content])
    ),

    Result = Table.SelectColumns(LoadEach, {"TableName", "Data"})
in
    Result
