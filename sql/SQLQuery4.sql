CREATE VIEW VW_Lineup_Comparative_Analysis AS
WITH Monthly_Summary AS (
    SELECT 
        (Year * 100) + Month as Month_Year_ID, -- Mesma lógica: 202603
        Year,
        Month as Month_No,
        Cargo,
        Origin,
        SUM(Tons) as Current_Volume
    FROM Arg_Sailed
    GROUP BY Year, Month, Cargo, Origin
)
SELECT 
    Month_Year_ID,
    Year,
    Month_No,
    A.Cargo,
    A.Origin,
    A.Current_Volume,
    ISNULL(B.Current_Volume, 0) as Last_Year_Volume,
    -- YoY Calculation
    CASE 
        WHEN ISNULL(B.Current_Volume, 0) > 0 
        THEN (A.Current_Volume - B.Current_Volume) / B.Current_Volume 
        ELSE NULL 
    END as YoY_Pct
FROM Monthly_Summary A
LEFT JOIN Monthly_Summary B ON A.Year = B.Year + 1 
                          AND A.Month_No = B.Month_No 
                          AND A.Cargo = B.Cargo 
                          AND A.Origin = B.Origin;