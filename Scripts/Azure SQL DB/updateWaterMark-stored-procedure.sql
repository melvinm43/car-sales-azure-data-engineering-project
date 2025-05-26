-- =======================================================
-- Create Stored Procedure Template for Azure SQL Database
-- =======================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [source].[updateWaterMark]
(
    -- Add the parameters for the stored procedure here
    @lastLoad nvarchar(50) = NULL
)
AS
/*
-- =============================================
-- Author:      Melvin Mathew
-- Create Date: 26/05/2025
-- Description: Update the water mark table with last load date
-- =============================================
*/
BEGIN
    -- Insert statements for procedure here
    UPDATE [source].[water_mark]
    SET last_load = @lastLoad
END
GO