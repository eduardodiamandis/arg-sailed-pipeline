CREATE NONCLUSTERED INDEX IDX_Lineup_Trading_Fast
ON Arg_Sailed (DATE, Cargo, Origin) -- Colunas que você usa em Filtros/Slicers
INCLUDE (Tons, Destination); -- Colunas que você soma ou exibe