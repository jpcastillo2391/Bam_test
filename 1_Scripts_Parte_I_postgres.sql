'''               INCISO 1 
desde POSTGRES queries, dado a que me equivoque al pensar que era en python
pero aproveche a hacer el ETL de un solo con el log que mas adelante van a 
solicitar. :D
'''
select to_char(sh.orderdate, 'YYYY-MM') as Mes,name_t as NombreTerritorio 
,sum(case when sh.status = 1 THEN 1 else 0 end) as TrProceso
,sum(case when sh.status = 2 THEN 1 else 0 end) as TrAprobadas 
,sum(case when sh.status = 3 THEN 1 else 0 end) as TrAtrasadas 
,sum(case when sh.status = 4 THEN 1 else 0 end) as TrRechazadas 
,sum(case when sh.status = 5 THEN 1 else 0 end) as TrEnviadas 
,sum(case when sh.status = 6 THEN 1 else 0 end) as TrCanceladas
,sum(case when sh.status = 1 THEN sh.totaldue else 0 end) as MntProceso 
,sum(case when sh.status = 2 THEN sh.totaldue else 0 end) as MntAprobadas  
,sum(case when sh.status = 3 THEN sh.totaldue else 0 end) as MntAtrasadas  
,sum(case when sh.status = 4 THEN sh.totaldue else 0 end) as MntRechazadas  
,sum(case when sh.status = 5 THEN sh.totaldue else 0 end) as MntEnviadas  
,sum(case when sh.status = 6 THEN sh.totaldue else 0 end) as MntCancelados 
from sales_salesorderheader sh join 
sales_salesterritory st on sh.territoryid=st.territoryid
group by 1,2

'''               INCISO 2 
Este me demoro un poco por inconpatibilidad entre varchar y text (no deberia, segun mi logica, pero si hay diferencias)
les dejo la extension tablefunc la cual requiren para hacer crosstab (pivot tables) en postgres
'''
CREATE EXTENSION IF NOT EXISTS tablefunc;

select rt.*,rr.Cliente_N1,rr.Cliente_N2,rr.Cliente_N3 from (
	select * from crosstab(
		$ct$select mes,top_position,product from (
			select to_char(sh.orderdate, 'YYYY-MM') as Mes,pp.name_t as product--,sum(sd.orderqty) as total
			,cast(row_number() over (partition by to_char(sh.orderdate, 'YYYY-MM') order by sum(sd.orderqty) desc)as text) as top_position 
			from sales_salesorderheader sh join 
			sales_salesorderdetail sd on sh.salesorderid=sd.salesorderid
			join production_product pp on pp.productid=sd.productid
			join sales_customer cc on cc.customerid=sh.customerid
			where sh.status=5 
			group by 1,2
			order by Mes desc--,total desc 
			) ranked
		where top_position in ('1','2','3')
		order by Mes asc,top_position asc$ct$
		) as ct (mes text ,Producto_N1 varchar, Producto_N2 varchar,Producto_N3 varchar )
	) as rt
join (
	select * from crosstab(
		$cr$select mes,top_position,cliente from (
			select to_char(sh.orderdate, 'YYYY-MM') as Mes
			,cast(per.title||''||per.firstname_t||''||per.middlename_t||''||per.lastname_t as varchar) as cliente 
			,cast(row_number() over (partition by to_char(sh.orderdate, 'YYYY-MM') order by sum(sh.totaldue) desc)as text) as top_position 
			from sales_salesorderheader sh join 
			sales_salesorderdetail sd on sh.salesorderid=sd.salesorderid
			join sales_customer cc on cc.customerid=sh.customerid
			join person_person per on per.businessentityid=cc.personid
			where sh.status=5
			group by 1,2
			order by Mes desc--,total desc 
		) ranked
		where top_position in ('1','2','3')
		order by Mes asc,top_position asc$cr$
		) as cr (mes text ,Cliente_N1 varchar, Cliente_N2 varchar,Cliente_N3 varchar )
	) as rr
on rr.mes=rt.mes

'''
	INCISO 3
'''
select prmt.Mes, prmt.NombreTerritorio,prmm.Mntproceso as MonthAvg ,(prmm.MntProceso-prmt.MntProceso) as Diff
	, (case when prmm.MntProceso-prmt.MntProceso>0 then 'Above' 
	when prmm.MntProceso-prmt.MntProceso=0 then 'Equal'
	else 'Below'end) as Indicador 
	from (
	select to_char(sh.orderdate, 'YYYY-MM') as Mes,name_t as NombreTerritorio 
	,avg(sh.subtotal ) as MntProceso  
	from sales_salesorderheader sh join 
	sales_salesterritory st on sh.territoryid=st.territoryid
	group by 1,2 ) prmt
join( 	
	select to_char(sh.orderdate, 'YYYY-MM') as Mes
	,avg(sh.subtotal ) as MntProceso  
	from sales_salesorderheader sh join 
	sales_salesterritory st on sh.territoryid=st.territoryid
	group by 1 ) prmm
	on prmt.mes=prmm.mes

'''
	INCISO 4
Esta estaba buena. sencilla pero muy capciosa
'''
select s.codigo,sum(case when s.codigo=d.codigo and d.codigo=f.codigo then 1 else 0 end ) as Cantidad_veces_seguidos from secuenciales s 
join secuenciales d on s.secuencial=d.secuencial+1 
join secuenciales f on s.secuencial=f.secuencial+2
group by s.codigo
order by Cantidad_veces_seguidos desc
'''
     INCISO 5
lo dejare pendiente
'''
