USE ArgentinaBD;
GO

select distinct *
from Arg_Sailed
where year = 2026
order by Date desc 
