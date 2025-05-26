Create schema [source];
GO
CREATE TABLE [source].sales_data (
    Branch_ID      NVARCHAR(200),
    Dealer_ID      NVARCHAR(200),
    Model_ID       NVARCHAR(200),
    Revenue        BIGINT,
    Units_Sold     BIGINT,
    Date_ID        NVARCHAR(200),
    Day            TINYINT,
    Month          TINYINT,
    Year           SMALLINT,
    BranchName     NVARCHAR(2000),
    DealerName     NVARCHAR(2000),
    Product_Name   NVARCHAR(2000)
);

CREATE TABLE [source].[water_mark] (
    last_load      NVARCHAR(200)
);