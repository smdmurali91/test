SELECT * FROM (
	SELECT 'Agreements' As 'Table Name', COUNT(*) AS 'Count' from Agreements
	where PrincipalId in
		(select EntityId from Entities where Entitytype & 1 >0 and DomicileId=32)
    UNION
	select 'Movements' AS 'Table Name',  COUNT(*) AS 'Count' from Movements
	where AgreementId in(select distinct AgreementId from agreements
	where PrincipalId in(select distinct EntityId from entities
	where Entitytype & 1 >0 and DomicileId=32))
	) AS Temp_Table
	ORDER BY 'Table Name'