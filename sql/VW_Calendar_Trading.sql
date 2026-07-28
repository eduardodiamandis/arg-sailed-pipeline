USE ArgentinaBD;
GO

--creat a calendar
CREATE VIEW VW_Calendar_Trading AS
SELECT DISTINCT
    CAST(DATE AS DATE) as Data_ID,
    YEAR(DATE) as Year,
    MONTH(DATE) as Month_Num,
    FORMAT(DATE, 'MMMM', 'en-US') as Month_Name, -- Nome do mês em português
    FORMAT(DATE, 'yyyy-MM') as Year_Month_Key -- Chave para ordenação
FROM Arg_Sailed;

