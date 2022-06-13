select s.codigo,sum(case when s.codigo=d.codigo and d.codigo=f.codigo then 1 else 0 end ) as Cantidad_veces_seguidos from secuenciales s 
join secuenciales d on s.secuencial=d.secuencial+1 
join secuenciales f on s.secuencial=f.secuencial+2
group by s.codigo
order by Cantidad_veces_seguidos desc